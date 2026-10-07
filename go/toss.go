package hermetix

// 토스증권 Open API 어댑터 (REST v1.2.15).
//
// ⚠️ 실전 전용 · 실측 전 — 토스증권은 모의투자 샌드박스가 없다. 공식 OpenAPI 문서로 구현했고 실계좌 소액 검증 전까지 "미검증".
// 반드시 LiveTradingEnabled 와 주문 금액 상한을 설정하고 소액으로 시작하라.
//   - /api/v1/… + Authorization: Bearer. 계좌·자산·주문 API 는 X-Tossinvest-Account: {accountSeq} 헤더 필수
//   - 성공 {"result": …}, 에러 {"error": {requestId, code, message, data}}. client 당 유효 토큰 1개(재발급 시 이전 토큰 무효)
//   - 한 계좌로 KRX·미국을 다룬다 → 보유·주문 심볼은 KRX:005930 / US:AAPL 로 접두를 붙여 돌려준다
//   - 공통 모델과의 차이: 시세에 등락·거래량 없음, 예수금 없음(KRW 매수가능금액으로 대체), 체결 엔드포인트 없음(종료 주문 execution 집계),
//     취소·정정은 새 orderId 발급(원주문 ID 유지 + PendingCancel), 캘린더는 KRX 합성
//   - 보유 평가금액·손익은 원화(미국 종목은 USD 금액을 매매기준율로 환산) → 계좌 총평가도 원화 한 숫자

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const TossBaseURL = "https://openapi.tossinvest.com"

var tossStatus = map[string]OrderStatus{
	"PENDING": Submitted, "PENDING_REPLACE": Submitted, "PENDING_CANCEL": PendingCancel,
	"PARTIAL_FILLED": PartiallyFilled, "FILLED": Filled,
	"CANCELED": Canceled, "REPLACED": Canceled,
	"REJECTED": Rejected, "CANCEL_REJECTED": Rejected, "REPLACE_REJECTED": Rejected,
}

type TossClient struct {
	clientID, clientSecret string
	accountSeq             string
	baseURL                string
	wsURL                  string
	environment            TradingEnvironment
	http                   *http.Client
	limiter                *rateLimiter
	tokens                 *tokenManager
	call                   func(method, path string, query map[string]string, jsonBody map[string]any, account bool) (any, error)
}

// NewTossClient - accountSeq 를 비우면 GET /api/v1/accounts 의 첫 BROKERAGE 계좌를 쓴다. 환경은 Live 고정(샌드박스 없음).
func NewTossClient(clientID, clientSecret, accountSeq string) *TossClient {
	c := &TossClient{
		clientID: clientID, clientSecret: clientSecret, accountSeq: accountSeq, baseURL: TossBaseURL, wsURL: TossWsURL, environment: Live,
		http:    &http.Client{Timeout: 30 * time.Second},
		limiter: newRateLimiter(200*time.Millisecond, 3, func(a int) time.Duration { return time.Duration(1<<(a-1)) * time.Second }),
	}
	c.tokens = newTokenManager("toss", clientID, time.Minute, c.issueToken)
	c.call = c.request
	return c
}

func (c *TossClient) SetBaseURL(u string) *TossClient { c.baseURL = u; return c }
func (c *TossClient) SetEnvironment(env TradingEnvironment) *TossClient {
	c.environment = env
	return c
}
func (c *TossClient) Environment() TradingEnvironment { return c.environment }

// usage - 사용량 텔레메트리 핸들 (docs/telemetry.md). 환경은 호출 시점 값을 쓴다
func (c *TossClient) usage() BrokerUsage {
	return BrokerUsage{BrokerID: "toss", Environment: c.environment}
}

// SetWSURL - 실시간 웹소켓 주소를 직접 지정 (기본 wss://openapi-ws.tossinvest.com/ws/v1 — 모의 환경 없음).
// 계정당 연결 2개(3번째가 오면 가장 오래된 것 종료), 구독 100개, 선언 5회/초, 180초 무송신 시 서버가 끊어 60초 PING. AsyncAPI 1.2.2 기반, 실측 전.
func (c *TossClient) SetWSURL(wsURL string) *TossClient {
	c.wsURL = wsURL
	return c
}

// OpenStream - 실시간 스트림(trade/orderbook:{kr,us}·personal:order 선언형 구독). REST 토큰을 그대로 쓰고 재발급하지 않는다.
func (c *TossClient) OpenStream() MarketStream {
	s := newTossMarketStream(c.wsURL, c.getToken, c.account)
	s.withUsage(c.usage())
	u := c.usage()
	s.listeners.usage = &u
	return s
}
func (c *TossClient) SetThrottle(d time.Duration) *TossClient {
	c.limiter = newRateLimiter(d, 3, func(a int) time.Duration { return time.Duration(1<<(a-1)) * time.Second })
	return c
}

func (c *TossClient) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "toss", Market: "KRX", Currency: "KRW",
		CandleIntervals: map[CandleInterval]bool{Min1: true, Day1: true},
		ClientOrderID:   true, NativeBracket: false, FractionalShares: false, ServerOpenOrders: true,
		Environments: map[TradingEnvironment]bool{Live: true},
		// trade/orderbook:{kr,us}·personal:order — AsyncAPI 1.2.2 기반, 실측 전
		Streams: []StreamChannel{StreamTrades, StreamOrderBook, StreamOrderEvents},
		Markets: map[string]bool{"KRX": true, "US": true},
	}
}

func tossTime(v any) *time.Time {
	text := strings.TrimSpace(str(v))
	if text == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, text); err == nil {
			return &t
		}
	}
	return nil
}

// tossKRW - 종목 통화 금액에 원화 환율을 곱한다 (금액이 없으면 nil).
func tossKRW(v any, rate decimal.Decimal) *decimal.Decimal {
	amount := dOrNil(v)
	if amount == nil {
		return nil
	}
	krw := amount.Mul(rate)
	return &krw
}

func tossMarketSymbol(o map[string]any) string {
	market := "KRX"
	if str(o["currency"]) == "USD" {
		market = "US"
	}
	return market + ":" + str(o["symbol"])
}

func tossList(v any) []map[string]any {
	list, _ := v.([]any)
	result := make([]map[string]any, 0, len(list))
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			result = append(result, m)
		}
	}
	return result
}

func tossObj(v any) map[string]any {
	m, _ := v.(map[string]any)
	if m == nil {
		return map[string]any{}
	}
	return m
}

// ------------------------------------------------------------------- market

func (c *TossClient) GetQuotes(symbols []string) (_ []Quote, err error) {
	defer c.usage().Measure("quotes")(&err)
	caps := c.Capabilities()
	requested := map[string]string{}
	codes := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		code, err := caps.SymbolCode(symbol)
		if err != nil {
			return nil, err
		}
		requested[code] = symbol
		codes = append(codes, code)
	}
	result, err := c.call("GET", "/api/v1/prices", map[string]string{"symbols": strings.Join(codes, ",")}, nil, false)
	if err != nil {
		return nil, err
	}
	quotes := make([]Quote, 0)
	for _, q := range tossList(result) {
		price := dOrNil(q["lastPrice"])
		if price == nil {
			continue
		}
		symbol := str(q["symbol"])
		if s, ok := requested[symbol]; ok {
			symbol = s
		}
		ts := time.Now()
		if t := tossTime(q["timestamp"]); t != nil {
			ts = *t
		}
		quotes = append(quotes, Quote{Symbol: symbol, Price: *price, Volume: 0, Timestamp: ts}) // /prices 에 등락·거래량 없음
	}
	return quotes, nil
}

func (c *TossClient) GetCandles(symbol string, interval CandleInterval, limit int) (_ []Candle, err error) {
	defer c.usage().Measure("candles")(&err)
	if !c.Capabilities().CandleIntervals[interval] {
		return nil, fmt.Errorf("토스 어댑터는 1m/1d 캔들만 지원합니다 (%s)", interval)
	}
	code, err := c.Capabilities().SymbolCode(symbol)
	if err != nil {
		return nil, err
	}
	count := limit
	if count <= 0 {
		count = 100
	}
	if count > 200 {
		count = 200
	}
	result, err := c.call("GET", "/api/v1/candles", map[string]string{"symbol": code, "interval": string(interval), "count": strconv.Itoa(count)}, nil, false)
	if err != nil {
		return nil, err
	}
	candles := make([]Candle, 0)
	for _, r := range tossList(tossObj(result)["candles"]) {
		ts := tossTime(r["timestamp"])
		if ts == nil {
			continue
		}
		candles = append(candles, Candle{
			Timestamp: *ts, Open: d(r["openPrice"]), High: d(r["highPrice"]), Low: d(r["lowPrice"]), Close: d(r["closePrice"]), Volume: d(r["volume"]).IntPart(),
		})
	}
	sortCandles(candles)
	return candles, nil
}

func (c *TossClient) GetCalendar() (_ []MarketDay, err error) {
	defer c.usage().Measure("calendar")(&err)
	return krxCalendar(31), nil
}

// ------------------------------------------------------------------ account

func (c *TossClient) GetAccount() (_ Account, err error) {
	defer c.usage().Measure("account")(&err)
	cash, err := c.buyingPower("KRW")
	if err != nil {
		return Account{}, err
	}
	holdings, err := c.GetHoldings()
	if err != nil {
		return Account{}, err
	}
	marketValue := decimal.Zero
	for _, h := range holdings {
		if h.MarketValue != nil {
			marketValue = marketValue.Add(*h.MarketValue)
		}
	}
	acct, err := c.account()
	if err != nil {
		return Account{}, err
	}
	return Account{AccountID: acct, Currency: "KRW", Cash: cash, PortfolioValue: cash.Add(marketValue), Status: "ACTIVE"}, nil
}

// GetHoldings - 평가금액·평가손익은 원화. 미국 종목은 종목 통화(USD) 금액을 매매기준율(midRate)로 환산한다. 단가는 종목 통화 그대로.
func (c *TossClient) GetHoldings() (_ []Holding, err error) {
	defer c.usage().Measure("holdings")(&err)
	result, err := c.call("GET", "/api/v1/holdings", nil, nil, true)
	if err != nil {
		return nil, err
	}
	rates := map[string]decimal.Decimal{"KRW": decimal.NewFromInt(1)}
	holdings := make([]Holding, 0)
	for _, h := range tossList(tossObj(result)["items"]) {
		qty := dOrNil(h["quantity"])
		if qty == nil || !qty.IsPositive() {
			continue
		}
		market, currency := "KRX", "KRW"
		if str(h["marketCountry"]) == "US" {
			market, currency = "US", "USD"
		}
		if v := str(h["currency"]); v != "" {
			currency = v
		}
		rate, ok := rates[currency]
		if !ok {
			if rate, err = c.krwRate(currency); err != nil {
				return nil, err
			}
			rates[currency] = rate
		}
		holdings = append(holdings, Holding{
			Symbol: market + ":" + str(h["symbol"]), Quantity: *qty, AvgEntryPrice: d(h["averagePurchasePrice"]), CurrentPrice: dOrNil(h["lastPrice"]),
			MarketValue: tossKRW(tossObj(h["marketValue"])["amount"], rate), UnrealizedPnl: tossKRW(tossObj(h["profitLoss"])["amount"], rate),
			UnrealizedPnlRate: dOrNil(tossObj(h["profitLoss"])["rate"]), // 이미 소수 비율
		})
	}
	return holdings, nil
}

func (c *TossClient) GetBuyingPower() (_ decimal.Decimal, err error) {
	defer c.usage().Measure("buying_power")(&err)
	return c.buyingPower("KRW")
}

// ------------------------------------------------------------------- orders

func (c *TossClient) CreateOrder(request CreateOrderRequest) (_ Order, err error) {
	defer c.usage().Measure("create_order")(&err)
	market, _ := ParseSymbol(request.Symbol)
	code, err := c.Capabilities().SymbolCode(request.Symbol)
	if err != nil {
		return Order{}, err
	}
	isKrx := market == "" || market == "KRX"
	isLimit := request.OrderType == Limit
	clientOrderID := request.ClientOrderID
	if clientOrderID == "" {
		clientOrderID = NewRequestID()
	}
	body := map[string]any{
		"clientOrderId": clientOrderID, "symbol": code, "side": string(request.Side), "orderType": string(request.OrderType),
		"timeInForce": "DAY", "quantity": request.Quantity.String(), "confirmHighValueOrder": false,
	}
	if isLimit && request.LimitPrice != nil {
		price := *request.LimitPrice
		if isKrx {
			price = KrxTickRound(price)
		}
		body["price"] = price.String()
	}
	result, err := c.call("POST", "/api/v1/orders", nil, body, true)
	if err != nil {
		return Order{}, err
	}
	res := tossObj(result)
	orderID := strings.TrimSpace(str(res["orderId"]))
	if orderID == "" {
		return Order{}, &BrokerAPIError{200, "", "토스 주문 응답에 orderId 가 없습니다"}
	}
	prefix := "US:"
	if isKrx {
		prefix = "KRX:"
	}
	if id := str(res["clientOrderId"]); id != "" {
		clientOrderID = id
	}
	zero := decimal.Zero
	now := time.Now()
	return Order{
		OrderID: orderID, Status: Submitted, Symbol: prefix + code, Side: request.Side, OrderType: request.OrderType,
		Quantity: &request.Quantity, LimitPrice: request.LimitPrice, FilledQuantity: &zero, ClientOrderID: clientOrderID, SubmittedAt: &now,
	}, nil
}

func (c *TossClient) GetOrders() (_ []Order, err error) {
	defer c.usage().Measure("get_orders")(&err)
	result, err := c.call("GET", "/api/v1/orders", map[string]string{"status": "OPEN"}, nil, true)
	if err != nil {
		return nil, err
	}
	orders := make([]Order, 0)
	for _, o := range tossList(tossObj(result)["orders"]) {
		orders = append(orders, tossOrder(o))
	}
	return orders, nil
}

func (c *TossClient) GetOrder(orderID string) (_ Order, err error) {
	defer c.usage().Measure("get_order")(&err)
	result, err := c.call("GET", "/api/v1/orders/"+orderID, nil, nil, true)
	if err != nil {
		return Order{}, err
	}
	return tossOrder(tossObj(result)), nil
}

func (c *TossClient) CancelOrder(orderID string) (_ Order, err error) {
	defer c.usage().Measure("cancel_order")(&err)
	if _, err := c.call("POST", "/api/v1/orders/"+orderID+"/cancel", nil, map[string]any{}, true); err != nil { // 새 orderId 발급 — 원주문 ID 유지
		return Order{}, err
	}
	now := time.Now()
	return Order{OrderID: orderID, Status: PendingCancel, CanceledAt: &now}, nil
}

func (c *TossClient) GetFills() (_ []Fill, err error) {
	defer c.usage().Measure("fills")(&err)
	result, err := c.call("GET", "/api/v1/orders", map[string]string{"status": "CLOSED"}, nil, true)
	if err != nil {
		return nil, err
	}
	fills := make([]Fill, 0)
	for _, o := range tossList(tossObj(result)["orders"]) {
		ex := tossObj(o["execution"])
		qty := dOrNil(ex["filledQuantity"])
		if qty == nil || !qty.IsPositive() {
			continue
		}
		fills = append(fills, Fill{OrderID: str(o["orderId"]), Symbol: tossMarketSymbol(o), Side: OrderSide(str(o["side"])), Quantity: qty, Price: dOrNil(ex["averageFilledPrice"])})
	}
	return fills, nil
}

// ----------------------------------------------------------------- internal

func tossOrder(o map[string]any) Order {
	ex := tossObj(o["execution"])
	status, ok := tossStatus[str(o["status"])]
	if !ok {
		status = Unknown
	}
	filled := d(ex["filledQuantity"])
	return Order{
		OrderID: str(o["orderId"]), Status: status, Symbol: tossMarketSymbol(o), Side: OrderSide(str(o["side"])), OrderType: OrderType(str(o["orderType"])),
		Quantity: dOrNil(o["quantity"]), LimitPrice: dOrNil(o["price"]), FilledQuantity: &filled, AvgFillPrice: dOrNil(ex["averageFilledPrice"]),
		ClientOrderID: str(o["clientOrderId"]), SubmittedAt: tossTime(o["orderedAt"]), CanceledAt: tossTime(o["canceledAt"]),
	}
}

// krwRate - 1 {currency} 의 원화 매매기준율 (GET /api/v1/exchange-rate, 약 5분마다 갱신)
func (c *TossClient) krwRate(currency string) (decimal.Decimal, error) {
	result, err := c.call("GET", "/api/v1/exchange-rate", map[string]string{"baseCurrency": currency, "quoteCurrency": "KRW"}, nil, false)
	if err != nil {
		return decimal.Zero, err
	}
	obj := tossObj(result)
	rate := dOrNil(obj["midRate"])
	if rate == nil {
		rate = dOrNil(obj["rate"])
	}
	if rate == nil || !rate.IsPositive() {
		return decimal.Zero, &BrokerAPIError{200, "", "토스 " + currency + "/KRW 환율을 받지 못했습니다"}
	}
	return *rate, nil
}

func (c *TossClient) buyingPower(currency string) (decimal.Decimal, error) {
	result, err := c.call("GET", "/api/v1/buying-power", map[string]string{"currency": currency}, nil, true)
	if err != nil {
		return decimal.Zero, err
	}
	return d(tossObj(result)["cashBuyingPower"]), nil
}

func (c *TossClient) account() (string, error) {
	if c.accountSeq != "" {
		return c.accountSeq, nil
	}
	result, err := c.call("GET", "/api/v1/accounts", nil, nil, false)
	if err != nil {
		return "", err
	}
	accounts := tossList(result)
	if len(accounts) == 0 {
		return "", &BrokerAPIError{200, "", "토스 계좌 목록이 비어 있습니다"}
	}
	picked := accounts[0]
	for _, a := range accounts {
		if str(a["accountType"]) == "BROKERAGE" {
			picked = a
			break
		}
	}
	c.accountSeq = str(picked["accountSeq"])
	return c.accountSeq, nil
}

func (c *TossClient) request(method, path string, query map[string]string, jsonBody map[string]any, account bool) (any, error) {
	var result any
	_, err := c.limiter.execute("toss "+path, func() (map[string]any, error) {
		body, err := c.tokens.call(func(token string) (map[string]any, error) {
			return c.requestOnce(method, path, query, jsonBody, account, token)
		})
		if err != nil {
			return nil, err
		}
		result = body["result"]
		return body, nil
	})
	return result, err
}

func (c *TossClient) requestOnce(method, path string, query map[string]string, jsonBody map[string]any, account bool, token string) (map[string]any, error) {
	headers := map[string]string{"Authorization": "Bearer " + token}
	if account {
		acct, err := c.account()
		if err != nil {
			return nil, err
		}
		headers["X-Tossinvest-Account"] = acct
	}
	var payload []byte
	if jsonBody != nil {
		headers["Content-Type"] = "application/json"
		payload, _ = json.Marshal(jsonBody)
	}
	rawURL := c.baseURL + path
	if len(query) > 0 {
		rawURL += "?" + encodeQuery(query)
	}
	status, parsed, resHeaders, err := httpJSONHeaders(c.http, method, rawURL, headers, payload)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		e := obj(parsed, "error")
		code := str(e["code"])
		msg := strings.TrimSpace(fmt.Sprintf("Toss(%s) [%s] %s requestId=%s", path, code, str(e["message"]), str(e["requestId"])))
		retryAfter, _ := strconv.ParseFloat(strings.TrimSpace(resHeaders.Get("Retry-After")), 64)
		switch {
		case status == 429:
			return nil, newRateLimitErrorWithRetryAfter(status, code, msg, retryAfter)
		case status == 401:
			return nil, newAuthError(status, code, msg)
		case code == "order-not-found":
			return nil, newOrderNotFoundError(code, msg)
		case code == "insufficient-buying-power":
			return nil, &InsufficientFundsError{BrokerAPIError{status, code, msg}}
		case code == "order-hours-closed":
			return nil, newMarketClosedError(status, code, msg)
		case status == 400 || status == 409 || status == 422:
			return nil, &InvalidOrderError{BrokerAPIError{status, code, msg}}
		default:
			return nil, &BrokerAPIError{status, code, msg}
		}
	}
	return parsed, nil
}

func (c *TossClient) getToken() (string, error) { return c.tokens.get() }

func (c *TossClient) issueToken() (_ string, _ time.Time, err error) {
	defer c.usage().Measure("auth")(&err) // 실제 발급 경로만 센다 (캐시 히트는 제외)
	c.limiter.throttle.wait()
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {c.clientID}, "client_secret": {c.clientSecret}}
	status, body, resHeaders, err := httpJSONHeaders(c.http, "POST", c.baseURL+"/oauth2/token",
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, []byte(form.Encode()))
	if err != nil {
		return "", time.Time{}, err
	}
	token := str(body["access_token"])
	if status != 200 || token == "" {
		// OAuth 표준 {"error":"invalid_client","error_description":..} 또는 플랫폼 엔벨로프 {"error":{"code":..,"message":..}}
		code, detail := str(body["error"]), str(body["error_description"])
		if e, ok := body["error"].(map[string]any); ok {
			code = str(e["code"])
			if detail == "" {
				detail = str(e["message"])
			}
		}
		msg := fmt.Sprintf("토스 토큰 발급 실패(%s): %s", code, detail)
		if status == 429 {
			retryAfter, _ := strconv.ParseFloat(strings.TrimSpace(resHeaders.Get("Retry-After")), 64)
			if retryAfter <= 0 {
				retryAfter = TokenFailureCooldown.Seconds()
			}
			return "", time.Time{}, newRateLimitErrorWithRetryAfter(status, code, msg, retryAfter)
		}
		return "", time.Time{}, newAuthError(status, code, msg)
	}
	return token, tokenExpiresIn(body["expires_in"], 24*time.Hour), nil
}
