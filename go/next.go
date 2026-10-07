package hermetix

// 넥스트증권 모의투자 어댑터 (미국주식) — 공개 스펙 v1.3 기준.
//
// v1.3 응답(quotes outcome, 캔들 time, 계좌 cashAmount, 보유 averageBuyPrice, 캘린더 status+sessions[] …)을
// 브로커 중립 공통 모델(Quote/Candle/Holding/Order …)로 정규화한다. 공통 모델은 전략이 보는 타입이므로
// 서버 스펙 변경은 이 어댑터 안에서만 흡수한다 (KIS/키움 어댑터와 같은 방식).
//
//   - OAuth client_credentials, 토큰 12h(expires_in=43200), 401 시 1회 재발급-재시도
//   - 공통 헤더(v1.3): X-Request-Id 는 토큰 발급 외 전 API 필수, 계좌·자산·주문 API 는 X-Next-Account-Id 필수
//     (구 X-Nextsecurities-Account 에서 개명)
//   - 시각은 ISO 8601 · KST (오프셋 생략 시 KST). 등락률·손익률은 % 단위 → 공통 모델 규약(비율)로 /100
//   - 토큰 발급 400/401 만 OAuth 표준 {error, error_description} 형식

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const (
	NextAccountHeader   = "X-Next-Account-Id"
	NextRequestIDHeader = "X-Request-Id"

	nextMarket   = "US"
	nextNewYork  = "America/New_York"
	nextKstFixed = 9 * 60 * 60
)

var (
	nextKST = time.FixedZone("KST", nextKstFixed)
	hundred = decimal.NewFromInt(100)
)

// NewRequestID 는 v1.3 X-Request-Id 규칙(영숫자·점·밑줄·하이픈, 최대 64자)에 맞는 ID 를 만든다.
// hmx- 프리픽스 + 16바이트 hex = 36자.
func NewRequestID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("hmx-%d", time.Now().UnixNano())
	}
	return "hmx-" + hex.EncodeToString(buf)
}

// parseNextTime 은 v1.3 시각(ISO 8601, 오프셋 생략 시 KST)을 파싱한다. 실패 시 zero time.
func parseNextTime(value string) time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, value); err == nil {
			return t
		}
	}
	for _, layout := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05", "2006-01-02T15:04"} {
		if t, err := time.ParseInLocation(layout, value, nextKST); err == nil {
			return t
		}
	}
	return time.Time{}
}

// nextNYClock 은 KST ISO 시각을 뉴욕 현지 HH:MM 으로 바꾼다 (공통 캘린더 모델은 현지 타임존 + HH:MM).
func nextNYClock(value string) (string, bool) {
	t := parseNextTime(value)
	if t.IsZero() {
		return "", false
	}
	loc, err := time.LoadLocation(nextNewYork)
	if err != nil {
		return "", false
	}
	return t.In(loc).Format("15:04"), true
}

// % 단위 → 비율 (3.3333 → 0.033333)
func pctOrNil(value any) *decimal.Decimal {
	rate := dOrNil(value)
	if rate == nil {
		return nil
	}
	r := rate.Div(hundred)
	return &r
}

type NextClient struct {
	clientID     string
	clientSecret string
	accountID    string
	baseURL      string
	environment  TradingEnvironment
	http         *http.Client
	// 429 는 Retry-After 만큼 기다렸다가 최대 2회 재시도. 쓰로틀은 없다 (초당 한도가 넉넉함)
	limiter *rateLimiter
	tokens  *tokenManager
	// 테스트에서 교체 가능한 호출 지점
	call func(method, path string, account bool, jsonBody map[string]any) (map[string]any, error)
}

func NewNextClient(clientID, clientSecret string) *NextClient {
	c := &NextClient{
		clientID: clientID, clientSecret: clientSecret,
		accountID:   "acc_main",
		baseURL:     "https://openapi.nextsecurities.dev",
		environment: Paper,
		http:        &http.Client{Timeout: 30 * time.Second},
		limiter:     newRateLimiter(0, 2, func(attempt int) time.Duration { return time.Duration(attempt) * time.Second }),
	}
	c.tokens = newTokenManager("next", clientID, time.Minute, c.issueToken)
	c.call = c.request
	return c
}

// SetBaseURL - 호스트를 직접 지정 (테스트·사설 배포용).
func (c *NextClient) SetBaseURL(baseURL string) *NextClient {
	c.baseURL = baseURL
	return c
}

// SetEnvironment - 거래 환경 지정. 넥스트증권은 키 프리픽스로 환경이 정해지므로(pk_test_=모의, pk_live_=실전)
// 설정과 키가 어긋나면 에러 (실전 키를 모의로 착각하는 사고 방지).
func (c *NextClient) SetEnvironment(env TradingEnvironment) error {
	expected := "pk_test_"
	if env == Live {
		expected = "pk_live_"
	}
	if strings.HasPrefix(c.clientID, "pk_") && !strings.HasPrefix(c.clientID, expected) {
		return fmt.Errorf("environment=%s 인데 clientID 가 '%s' 로 시작하지 않습니다 (모의=pk_test_, 실전=pk_live_)", env, expected)
	}
	c.environment = env
	return nil
}

func (c *NextClient) Environment() TradingEnvironment { return c.environment }

// usage - 사용량 텔레메트리 핸들 (docs/telemetry.md). 환경은 호출 시점 값을 쓴다
func (c *NextClient) usage() BrokerUsage {
	return BrokerUsage{BrokerID: "next", Environment: c.environment}
}

func (c *NextClient) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "next", Market: "US", Currency: "USD",
		CandleIntervals:  map[CandleInterval]bool{Min1: true, Day1: true}, // v1.3: 1m · 1d
		ClientOrderID:    true,
		NativeBracket:    false, // 서버 /v2/orders/advanced(BRACKET) 연동 전까지 소프트웨어 브라켓
		FractionalShares: false, // v1.3 주문 수량은 정수만
		ServerOpenOrders: true,
		Environments:     map[TradingEnvironment]bool{Paper: true, Live: true}, // 키 프리픽스로 결정
	}
}

// ------------------------------------------------------------------- market

func (c *NextClient) GetQuotes(symbols []string) (_ []Quote, err error) {
	defer c.usage().Measure("quotes")(&err)
	caps := c.Capabilities()
	codes := make([]string, 0, len(symbols))
	requested := map[string]string{}
	for _, s := range symbols {
		code, err := caps.SymbolCode(s)
		if err != nil {
			return nil, err
		}
		codes = append(codes, code)
		requested[code] = s
	}
	body, err := c.call("GET", "/v1/market/quotes?symbols="+strings.Join(codes, ","), false, nil)
	if err != nil {
		return nil, err
	}
	quotes := make([]Quote, 0)
	for _, q := range rows(body, "quotes") {
		// NOT_FOUND / NO_DATA 는 개별 종목의 정상 결과 — 가격이 없으므로 제외한다 (ctx.Quote() 가 nil)
		if str(q["outcome"]) != "OK" || q["price"] == nil {
			continue
		}
		symbol := str(q["symbol"])
		if req, ok := requested[symbol]; ok {
			symbol = req // 요청받은 표기(시장 접두 포함)로
		}
		ts := parseNextTime(str(q["lastTradeAt"]))
		if ts.IsZero() {
			ts = parseNextTime(str(q["requestedAt"]))
		}
		if ts.IsZero() {
			ts = time.Now()
		}
		quotes = append(quotes, Quote{
			Symbol: symbol, Price: d(q["price"]),
			BidPrice: dOrNil(q["bidPrice"]), AskPrice: dOrNil(q["askPrice"]),
			Volume: d(q["volume"]).IntPart(),
			Change: dOrNil(q["change"]), ChangeRate: pctOrNil(q["changeRate"]),
			Timestamp: ts,
		})
	}
	return quotes, nil
}

func (c *NextClient) GetCandles(symbol string, interval CandleInterval, limit int) (_ []Candle, err error) {
	defer c.usage().Measure("candles")(&err)
	code, err := c.Capabilities().SymbolCode(symbol)
	if err != nil {
		return nil, err
	}
	path := fmt.Sprintf("/v1/market/candles?symbol=%s&interval=%s", code, interval)
	if limit > 0 {
		path += fmt.Sprintf("&limit=%d", limit)
	}
	body, err := c.call("GET", path, false, nil)
	if err != nil {
		return nil, err
	}
	candles := make([]Candle, 0)
	for _, r := range rows(body, "candles") {
		candles = append(candles, Candle{
			Timestamp: parseNextTime(str(r["time"])),
			Open:      d(r["open"]), High: d(r["high"]), Low: d(r["low"]), Close: d(r["close"]),
			Volume: d(r["volume"]).IntPart(),
		})
	}
	return candles, nil
}

// GetCalendar — v1.3: date 는 거래소 현지 일자, 세션 시각은 KST → 뉴욕 현지 HH:MM 으로 바꿔 담는다.
func (c *NextClient) GetCalendar() (_ []MarketDay, err error) {
	defer c.usage().Measure("calendar")(&err)
	body, err := c.call("GET", "/v1/market/calendar", false, nil)
	if err != nil {
		return nil, err
	}
	days := make([]MarketDay, 0)
	for _, r := range rows(body, "calendar") {
		day := MarketDay{
			Date: str(r["date"]), Timezone: nextNewYork, Holiday: str(r["holidayName"]),
		}
		status := str(r["status"])
		if status == "OPEN" || status == "HALF_DAY" {
			for _, s := range rows(r, "sessions") {
				if str(s["type"]) != "REGULAR" {
					continue
				}
				start, ok1 := nextNYClock(str(s["open"]))
				end, ok2 := nextNYClock(str(s["close"]))
				if ok1 && ok2 {
					day.Open = true
					day.Regular = &SessionHours{Start: start, End: end}
				}
			}
		}
		days = append(days, day)
	}
	return days, nil
}

// ------------------------------------------------------------------ account

// GetAccount — v1.3 계좌 응답은 예수금(cashAmount)만 준다. 총평가는 예수금 + 보유 평가금액 합 (보유 조회 1회 추가).
func (c *NextClient) GetAccount() (_ Account, err error) {
	defer c.usage().Measure("account")(&err)
	body, err := c.call("GET", "/v1/account", true, nil)
	if err != nil {
		return Account{}, err
	}
	holdings, err := c.GetHoldings()
	if err != nil {
		return Account{}, err
	}
	cash := d(body["cashAmount"])
	marketValue := decimal.Zero
	for _, h := range holdings {
		if h.MarketValue != nil {
			marketValue = marketValue.Add(*h.MarketValue)
		}
	}
	currency := str(body["currency"])
	if currency == "" {
		currency = "USD"
	}
	return Account{
		AccountID: str(body["accountId"]), Currency: currency,
		Cash: cash, PortfolioValue: cash.Add(marketValue),
		Status: "ACTIVE",
	}, nil
}

func (c *NextClient) GetHoldings() (_ []Holding, err error) {
	defer c.usage().Measure("holdings")(&err)
	body, err := c.call("GET", "/v1/account/holdings", true, nil)
	if err != nil {
		return nil, err
	}
	holdings := make([]Holding, 0)
	for _, h := range rows(body, "holdings") {
		holdings = append(holdings, Holding{
			Symbol: str(h["symbol"]), Quantity: d(h["quantity"]), AvgEntryPrice: d(h["averageBuyPrice"]),
			CurrentPrice: dOrNil(h["currentPrice"]), MarketValue: dOrNil(h["evaluationAmount"]),
			UnrealizedPnl: dOrNil(h["evaluationPnl"]), UnrealizedPnlRate: pctOrNil(h["evaluationPnlRate"]),
		})
	}
	return holdings, nil
}

func (c *NextClient) GetBuyingPower() (_ decimal.Decimal, err error) {
	defer c.usage().Measure("buying_power")(&err)
	body, err := c.call("GET", "/v1/account/buying-power", true, nil)
	if err != nil {
		return decimal.Zero, err
	}
	return d(body["buyingPower"]), nil
}

// ------------------------------------------------------------------- orders

func (c *NextClient) CreateOrder(request CreateOrderRequest) (_ Order, err error) {
	defer c.usage().Measure("create_order")(&err)
	tif := request.TimeInForce
	if tif == "" {
		tif = Day
	}
	clientOrderID := request.ClientOrderID
	if clientOrderID == "" {
		// v1.3: clientOrderId(멱등키) 필수 — 호출자가 안 주면 어댑터가 만든다
		clientOrderID = NewRequestID()
	}
	code, err := c.Capabilities().SymbolCode(request.Symbol)
	if err != nil {
		return Order{}, err
	}
	payload := map[string]any{
		"clientOrderId": clientOrderID,
		"market":        nextMarket,
		"symbol":        code, "side": string(request.Side),
		"orderType": string(request.OrderType), "quantity": request.Quantity.String(),
		"timeInForce": string(tif),
	}
	if request.LimitPrice != nil {
		payload["limitPrice"] = request.LimitPrice.String()
	}
	body, err := c.call("POST", "/v1/orders", true, payload)
	if err != nil {
		return Order{}, err
	}
	return nextOrder(body), nil
}

func (c *NextClient) GetOrders() (_ []Order, err error) {
	defer c.usage().Measure("get_orders")(&err)
	body, err := c.call("GET", "/v1/orders", true, nil)
	if err != nil {
		return nil, err
	}
	orders := make([]Order, 0)
	for _, o := range rows(body, "orders") {
		orders = append(orders, nextOrder(o))
	}
	return orders, nil
}

func (c *NextClient) GetOrder(orderID string) (_ Order, err error) {
	defer c.usage().Measure("get_order")(&err)
	body, err := c.call("GET", "/v1/orders/"+orderID, true, nil)
	if err != nil {
		return Order{}, err
	}
	return nextOrder(body), nil
}

func (c *NextClient) CancelOrder(orderID string) (_ Order, err error) {
	defer c.usage().Measure("cancel_order")(&err)
	body, err := c.call("DELETE", "/v1/orders/"+orderID, true, nil)
	if err != nil {
		return Order{}, err
	}
	return nextOrder(body), nil
}

func (c *NextClient) GetFills() (_ []Fill, err error) {
	defer c.usage().Measure("fills")(&err)
	body, err := c.call("GET", "/v1/orders/fills", true, nil)
	if err != nil {
		return nil, err
	}
	fills := make([]Fill, 0)
	for _, f := range rows(body, "fills") {
		fills = append(fills, Fill{
			FillID:  "", // v1.3: 원장이 체결 ID 를 발급하지 않는다
			OrderID: str(f["orderId"]), Symbol: str(f["symbol"]),
			Side: OrderSide(str(f["side"])), Quantity: dOrNil(f["quantity"]), Price: dOrNil(f["price"]),
		})
	}
	return fills, nil
}

// ----------------------------------------------------------------- internal

// nextOrder — 생성/상세/취소 응답 공통. 생성·취소는 orderId/status/requestedAt 만 온다.
func nextOrder(body map[string]any) Order {
	status := OrderStatus(str(body["status"]))
	switch status {
	case Submitted, PartiallyFilled, PendingCancel, Filled, Canceled, Rejected, Expired:
	default:
		status = Unknown
	}
	orderType := OrderType(strings.ToUpper(str(body["orderType"])))
	if orderType != Market && orderType != Limit {
		orderType = ""
	}
	order := Order{
		OrderID: str(body["orderId"]), Status: status,
		Symbol: str(body["symbol"]), Side: OrderSide(str(body["side"])),
		OrderType: orderType,
		Quantity:  dOrNil(body["quantity"]), LimitPrice: dOrNil(body["limitPrice"]),
		FilledQuantity: dOrNil(body["filledQuantity"]), AvgFillPrice: dOrNil(body["avgFillPrice"]),
		ClientOrderID: str(body["requestId"]), // v1.3 상세 조회의 clientOrderId 필드명
	}
	if ts := parseNextTime(str(body["requestedAt"])); !ts.IsZero() {
		order.SubmittedAt = &ts
	}
	return order
}

func (c *NextClient) request(method, path string, account bool, jsonBody map[string]any) (map[string]any, error) {
	do := func(token string) (map[string]any, error) {
		headers := map[string]string{
			"Authorization":     "Bearer " + token,
			NextRequestIDHeader: NewRequestID(),
		}
		if account {
			headers[NextAccountHeader] = c.accountID
		}
		var payload []byte
		if jsonBody != nil {
			headers["Content-Type"] = "application/json"
			payload, _ = json.Marshal(jsonBody)
		}
		status, body, resHeaders, err := httpJSONHeaders(c.http, method, c.baseURL+path, headers, payload)
		if err != nil {
			return nil, err
		}
		if status < 200 || status >= 300 {
			retryAfter, _ := strconv.ParseFloat(strings.TrimSpace(resHeaders.Get("Retry-After")), 64)
			return nil, mapNextError(status, obj(body, "error"), retryAfter)
		}
		return body, nil
	}

	// 토큰 거부(만료) 시 1회 재발급 후 재시도는 tokenManager.call
	return c.limiter.execute("next", func() (map[string]any, error) { return c.tokens.call(do) })
}

// mapNextError — v1.3 에러 type ↔ HTTP: validation(400) authentication(401) permission(403) not_found(404)
// conflict(409) business_rule(422) locked(423, 킬스위치 trading-halted) rate_limit(429) server(5xx)
func mapNextError(status int, e map[string]any, retryAfterSeconds float64) error {
	code := str(e["code"])
	errType := str(e["type"])
	msg := fmt.Sprintf("Next(%s) %s requestId=%s", code, str(e["message"]), str(e["requestId"]))
	switch {
	case status == 401 || errType == "authentication":
		return newAuthError(status, code, msg)
	case status == 429:
		return newRateLimitErrorWithRetryAfter(status, code, msg, retryAfterSeconds)
	case errType == "permission":
		// 조회전용 키(insufficient-scope)·계좌 불일치 — 자금 부족으로 오인하지 않는다
		return &BrokerAPIError{status, code, msg}
	case code == "order-not-found":
		return newOrderNotFoundError(code, msg)
	case strings.Contains(code, "insufficient"):
		return &InsufficientFundsError{BrokerAPIError{status, code, msg}}
	case code == "trading-halted" || strings.Contains(code, "market-closed"):
		return newMarketClosedError(status, code, msg)
	case errType == "validation" || errType == "business_rule":
		return &InvalidOrderError{BrokerAPIError{status, code, msg}}
	default:
		return &BrokerAPIError{status, code, msg}
	}
}

// nextTokenError 는 토큰 발급 API 전용 에러 형식을 AuthError(429 는 RateLimitError) 로 바꾼다.
// 400/401 은 OAuth 표준 {"error":"invalid_client","error_description":"..."}, 429/5xx 는 플랫폼 엔벨로프.
func nextTokenError(status int, body map[string]any, retryAfterSeconds float64) error {
	if code, ok := body["error"].(string); ok {
		return newAuthError(status, code, fmt.Sprintf("Next 토큰 발급 실패(%s): %s", code, str(body["error_description"])))
	}
	e := obj(body, "error")
	msg := fmt.Sprintf("Next 토큰 발급 실패(%s): %s", str(e["code"]), str(e["message"]))
	if status == 429 {
		if retryAfterSeconds <= 0 {
			retryAfterSeconds = TokenFailureCooldown.Seconds()
		}
		return newRateLimitErrorWithRetryAfter(status, str(e["code"]), msg, retryAfterSeconds)
	}
	return newAuthError(status, str(e["code"]), msg)
}

func (c *NextClient) getToken() (string, error) { return c.tokens.get() }

func (c *NextClient) issueToken() (_ string, _ time.Time, err error) {
	defer c.usage().Measure("auth")(&err) // 실제 발급 경로만 센다 (캐시 히트는 제외)
	form := url.Values{
		"grant_type":    {"client_credentials"},
		"client_id":     {c.clientID},
		"client_secret": {c.clientSecret},
	}
	status, body, resHeaders, err := httpJSONHeaders(c.http, "POST", c.baseURL+"/v1/oauth/token",
		map[string]string{
			"Content-Type":      "application/x-www-form-urlencoded",
			NextRequestIDHeader: NewRequestID(),
		},
		[]byte(form.Encode()))
	if err != nil {
		return "", time.Time{}, err
	}
	token := str(body["access_token"])
	if status != 200 || token == "" {
		retryAfter, _ := strconv.ParseFloat(strings.TrimSpace(resHeaders.Get("Retry-After")), 64)
		return "", time.Time{}, nextTokenError(status, body, retryAfter)
	}
	return token, tokenExpiresIn(body["expires_in"], 12*time.Hour), nil
}
