package hermetix

// NH투자증권 NH PLUG(나무 PLUG) REST OpenAPI 어댑터.
//
// ⚠️ 문서 기반 구현 (실측 전) — 공식 Python SDK(PLUG-OpenAPI/nhplug-sdk)와 포털 OpenAPI 문서(2026-09-08)에서
// 엔드포인트·필드명·에러 코드를 역추적했다. 모의서버 실측 전까지 상태는 "미검증".
//   - 모든 API 는 POST, 본문 {"Input_0": {...}}, 응답 rsp_cd/rsp_msg + Output_0(+Output_1). TR 헤더 없이 경로로 식별
//   - 인증 헤더 authorization: Bearer + x-client-id / x-client-secret. 토큰은 운영 호스트 /oauth2/token 에서 쿼리스트링으로 발급(24h)
//   - 모의/운영은 호스트로만 구분(moapi / api). 계좌 acct_type 은 환경과 맞아야 한다(모의 03, 운영 01)
//   - HTTP 200 이어도 업무 오류 가능 — rsp_cd ∈ {00000,00166,00221,13578} 또는 rsp_msg 에 "완료" 면 성공(SDK 판정식)
//   - 초당 5회 한도 → 250ms 쓰로틀 + 429(IGW4290x, Retry-After) 재시도
//
// 미확인(실측 필요): mkt_orr_no 와 itg_orr_no 의 동일 여부, ost_cns_dit 코드 의미, 등락률·수익률 단위(% 추정), 응답 숫자 타입

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

const (
	NhPaperURL = "https://moapi.nhplug.com:8443"
	NhLiveURL  = "https://api.nhplug.com:8443"
	NhAuthURL  = "https://api.nhplug.com:8443"
)

var (
	nhSuccessCodes = map[string]bool{"00000": true, "00166": true, "00221": true, "13578": true, "00165": true, "00218": true}
	nhFallingSigns = map[string]bool{"4": true, "5": true, "8": true, "9": true}
)

type NhClient struct {
	appKey, appSecret string
	accountNo         string
	baseURL, authURL  string
	wsURL             string
	customWsURL       bool
	marketCd          string
	orderMarketCd     string
	environment       TradingEnvironment
	http              *http.Client
	limiter           *rateLimiter
	tokens            *tokenManager
	call              func(path string, body map[string]any) (map[string]any, error)
}

// NewNhClient - accountNo 를 비우면 /n2/acctinfo 에서 환경에 맞는 acct_type(모의 03 / 운영 01)의 첫 계좌를 고른다.
func NewNhClient(appKey, appSecret, accountNo string) *NhClient {
	c := &NhClient{
		appKey: appKey, appSecret: appSecret, accountNo: accountNo,
		baseURL: NhPaperURL, authURL: NhAuthURL, wsURL: NhWsPaperURL, marketCd: "KRX", orderMarketCd: "KRX",
		environment: Paper,
		http:        &http.Client{Timeout: 30 * time.Second},
		limiter:     newRateLimiter(250*time.Millisecond, 3, func(a int) time.Duration { return time.Duration(a) * time.Second }),
	}
	c.tokens = newTokenManager("nh", appKey, 5*time.Minute, c.issueToken)
	c.call = c.request
	return c
}

func (c *NhClient) SetBaseURL(u string) *NhClient { c.baseURL = u; return c }
func (c *NhClient) SetAuthURL(u string) *NhClient { c.authURL = u; return c }
func (c *NhClient) SetThrottle(d time.Duration) *NhClient {
	c.limiter = newRateLimiter(d, 3, func(a int) time.Duration { return time.Duration(a) * time.Second })
	return c
}

// SetEnvironment - 호스트(모의 moapi / 운영 api)가 결정된다. SetBaseURL 을 먼저 썼다면 그 값을 덮어쓴다.
func (c *NhClient) SetEnvironment(env TradingEnvironment) *NhClient {
	c.environment = env
	if !c.customWsURL {
		c.wsURL = NhWsPaperURL
		if env == Live {
			c.wsURL = NhWsLiveURL
		}
	}
	c.baseURL = NhPaperURL
	if env == Live {
		c.baseURL = NhLiveURL
	}
	return c
}

func (c *NhClient) Environment() TradingEnvironment { return c.environment }

// usage - 사용량 텔레메트리 핸들 (docs/telemetry.md). 환경은 호출 시점 값을 쓴다
func (c *NhClient) usage() BrokerUsage {
	return BrokerUsage{BrokerID: "nh", Environment: c.environment}
}

// SetWSURL - 실시간 웹소켓 주소를 직접 지정 (환경 자동 결정 무시 — 모의 wss://moapi…:17070/websocket, 운영 wss://api…:7070/websocket).
// 포털 가이드는 모의 시세 채널을 "미제공" 으로 표기한다(통보만 올 수 있음). 세션당 등록 10건(SDK 실측)/30건(공식), 앱키당 세션 2개.
func (c *NhClient) SetWSURL(wsURL string) *NhClient {
	c.wsURL = wsURL
	c.customWsURL = true
	return c
}

// OpenStream - 실시간 스트림(체결·호가·주문 통보). 문서 기반, 실측 전. 채널은 marketCd 에 따라 oc/ob·nc/nb·mc/mb.
func (c *NhClient) OpenStream() MarketStream {
	s := newNhMarketStream(c.wsURL, c.marketCd, c.accountNo, c.getToken)
	s.withUsage(c.usage())
	u := c.usage()
	s.listeners.usage = &u
	return s
}

func (c *NhClient) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "nh", Market: "KRX", Currency: "KRW",
		CandleIntervals: map[CandleInterval]bool{Day1: true},
		ClientOrderID:   false, NativeBracket: false, FractionalShares: false, ServerOpenOrders: true,
		Environments: map[TradingEnvironment]bool{Paper: true, Live: true},
		// oc/nc/mc 체결·ob/nb/mb 호가·d2/d3 주문 통보 — 문서 기반, 실측 전
		Streams: []StreamChannel{StreamTrades, StreamOrderBook, StreamOrderEvents},
	}
}

// nhNum - 숫자/문자열 어느 쪽으로 와도 파싱 (문서상 와이어 타입이 API 마다 다르다).
func nhNum(v any) *decimal.Decimal {
	if v == nil {
		return nil
	}
	text := strings.ReplaceAll(strings.TrimSpace(str(v)), ",", "")
	if text == "" {
		return nil
	}
	d, err := decimal.NewFromString(text)
	if err != nil {
		return nil
	}
	return &d
}

func nhVal(v any) decimal.Decimal {
	if d := nhNum(v); d != nil {
		return *d
	}
	return decimal.Zero
}

func nhPositive(v any) *decimal.Decimal {
	if d := nhNum(v); d != nil && d.IsPositive() {
		return d
	}
	return nil
}

func nhPct(v any) *decimal.Decimal {
	d := nhNum(v)
	if d == nil {
		return nil
	}
	r := d.Div(hundred)
	return &r
}

// NhNormalizeCode - 계좌·주문 API 의 iem_cd 는 길이 12(선행 0)일 수 있고 A 접두가 붙을 수 있다 → 6자리 코드.
func NhNormalizeCode(raw any) string {
	text := strings.TrimPrefix(strings.TrimSpace(str(raw)), "A")
	if len(text) > 6 {
		digits := true
		for _, ch := range text {
			if ch < '0' || ch > '9' {
				digits = false
			}
		}
		if digits {
			return text[len(text)-6:]
		}
	}
	return text
}

func nhParseDate(raw any) (time.Time, bool) {
	t := strings.TrimSpace(str(raw))
	for _, layout := range []string{"20060102", "2006-01-02"} {
		if ts, err := time.ParseInLocation(layout, t, kst); err == nil {
			return ts, true
		}
	}
	if len(t) == 8 && t[2] == '/' { // 문서 표기 "YY/MM/DD"
		if ts, err := time.ParseInLocation("2006/01/02", "20"+t, kst); err == nil {
			return ts, true
		}
	}
	return time.Time{}, false
}

func nhSameNo(a, b any) bool {
	return strings.TrimLeft(strings.TrimSpace(str(a)), "0") == strings.TrimLeft(strings.TrimSpace(str(b)), "0")
}

func nhSide(row map[string]any) OrderSide {
	if strings.Contains(str(row["sby_dit_cd_nm"]), "매수") {
		return Buy
	}
	return Sell
}

// ------------------------------------------------------------------- market

func (c *NhClient) GetQuotes(symbols []string) (_ []Quote, err error) {
	defer c.usage().Measure("quotes")(&err)
	caps := c.Capabilities()
	quotes := make([]Quote, 0, len(symbols))
	for _, symbol := range symbols {
		code, err := caps.SymbolCode(symbol)
		if err != nil {
			return nil, err
		}
		body, err := c.call("/krstock/quote/v1/currentPrice", map[string]any{"market_cd": c.marketCd, "iem_cd": code})
		if err != nil {
			return nil, err
		}
		out := obj(body, "Output_0")
		sign := str(out["prdy_vrss_sign"])
		change := nhNum(out["prdy_vrss"])
		rate := nhNum(out["prdy_ctrt"])
		if nhFallingSigns[sign] {
			if change != nil && change.IsPositive() {
				n := change.Neg()
				change = &n
			}
			if rate != nil && rate.IsPositive() {
				n := rate.Neg()
				rate = &n
			}
		}
		var changeRate *decimal.Decimal
		if rate != nil {
			r := rate.Div(hundred)
			changeRate = &r
		}
		quotes = append(quotes, Quote{
			Symbol: symbol, Price: nhVal(out["stck_prpr"]),
			BidPrice: nhPositive(out["bidp"]), AskPrice: nhPositive(out["askp"]),
			Volume: nhVal(out["acml_vol"]).IntPart(), Change: change, ChangeRate: changeRate,
			Timestamp: time.Now(),
		})
	}
	return quotes, nil
}

func (c *NhClient) GetCandles(symbol string, interval CandleInterval, limit int) (_ []Candle, err error) {
	defer c.usage().Measure("candles")(&err)
	if interval != Day1 {
		return nil, fmt.Errorf("NH 어댑터는 일봉(1d)만 지원합니다")
	}
	code, err := c.Capabilities().SymbolCode(symbol)
	if err != nil {
		return nil, err
	}
	count := limit
	if count <= 0 {
		count = 30
	}
	body, err := c.call("/krstock/quote/v1/currentDaily", map[string]any{"market_cd": c.marketCd, "iem_cd": code, "array_cnt": strconv.Itoa(count)})
	if err != nil {
		return nil, err
	}
	candles := make([]Candle, 0)
	for _, r := range rows(body, "Output_0") {
		ts, ok := nhParseDate(r["bsop_date"])
		if !ok {
			continue
		}
		candles = append(candles, Candle{
			Timestamp: ts, Open: nhVal(r["stck_oprc"]), High: nhVal(r["stck_hgpr"]), Low: nhVal(r["stck_lwpr"]), Close: nhVal(r["stck_clpr"]),
			Volume: nhVal(r["acml_vol"]).IntPart(),
		})
	}
	sortCandles(candles) // 문서상 최신일 우선 → 과거→최신
	if len(candles) > count {
		candles = candles[len(candles)-count:]
	}
	return candles, nil
}

func (c *NhClient) GetCalendar() (_ []MarketDay, err error) {
	defer c.usage().Measure("calendar")(&err)
	return krxCalendar(31), nil
}

// ------------------------------------------------------------------ account

func (c *NhClient) GetAccount() (_ Account, err error) {
	defer c.usage().Measure("account")(&err)
	body, err := c.balance()
	if err != nil {
		return Account{}, err
	}
	summary := obj(body, "Output_0")
	cash := nhVal(summary["dca"])
	portfolio := nhVal(summary["tot_aet_amt"])
	if !portfolio.IsPositive() {
		portfolio = cash.Add(nhVal(summary["tot_eal_amt"]))
	}
	acct, err := c.account()
	if err != nil {
		return Account{}, err
	}
	return Account{AccountID: acct, Currency: "KRW", Cash: cash, PortfolioValue: portfolio, Status: "ACTIVE"}, nil
}

func (c *NhClient) GetHoldings() (_ []Holding, err error) {
	defer c.usage().Measure("holdings")(&err)
	body, err := c.balance()
	if err != nil {
		return nil, err
	}
	holdings := make([]Holding, 0)
	for _, row := range rows(body, "Output_1") {
		qty := nhNum(row["itg_bnc_qty"])
		if qty == nil || !qty.IsPositive() {
			continue
		}
		holdings = append(holdings, Holding{
			Symbol: NhNormalizeCode(row["iem_cd"]), Quantity: *qty, AvgEntryPrice: nhVal(row["phs_pr"]),
			CurrentPrice: nhNum(row["now_pr"]), MarketValue: nhNum(row["eal_amt"]), UnrealizedPnl: nhNum(row["eal_pls_amt"]),
			UnrealizedPnlRate: nhPct(row["pft_rt"]),
		})
	}
	return holdings, nil
}

func (c *NhClient) GetBuyingPower() (_ decimal.Decimal, err error) {
	defer c.usage().Measure("buying_power")(&err)
	body, err := c.balance()
	if err != nil {
		return decimal.Zero, err
	}
	summary := obj(body, "Output_0")
	if d := nhNum(summary["orr_pbl_amt"]); d != nil {
		return *d, nil
	}
	return nhVal(summary["dca"]), nil
}

// ------------------------------------------------------------------- orders

func (c *NhClient) CreateOrder(request CreateOrderRequest) (_ Order, err error) {
	defer c.usage().Measure("create_order")(&err)
	code, err := c.Capabilities().SymbolCode(request.Symbol)
	if err != nil {
		return Order{}, err
	}
	acct, err := c.account()
	if err != nil {
		return Order{}, err
	}
	isLimit := request.OrderType == Limit
	path := "/krstock/order/v1/cashSell"
	if request.Side == Buy {
		path = "/krstock/order/v1/cashBuy"
	}
	body := map[string]any{
		"act_no": acct, "iem_cd": code, "orr_qty": request.Quantity.IntPart(),
		"nmn_pr_tp_cd":   map[bool]string{true: "01", false: "05"}[isLimit],
		"orr_cnd_dit_cd": "00", "ssl_nmn_pr_dit_cd": "00", "rmt_mkt_cd": c.orderMarketCd, "sor_mkt_sli_yn": "N",
	}
	if isLimit && request.LimitPrice != nil {
		body["orr_pr"] = KrxTickRound(*request.LimitPrice).IntPart() // KRX 호가단위 보정
	}
	res, err := c.call(path, body)
	if err != nil {
		return Order{}, err
	}
	orderID := strings.TrimSpace(str(obj(res, "Output_0")["mkt_orr_no"]))
	if orderID == "" {
		return Order{}, &BrokerAPIError{200, "", "NH 주문 응답에 mkt_orr_no 가 없습니다"}
	}
	zero := decimal.Zero
	now := time.Now()
	return Order{
		OrderID: orderID, Status: Submitted, Symbol: code, Side: request.Side, OrderType: request.OrderType,
		Quantity: &request.Quantity, LimitPrice: request.LimitPrice, FilledQuantity: &zero,
		ClientOrderID: request.ClientOrderID, SubmittedAt: &now,
	}, nil
}

func (c *NhClient) GetOrders() (_ []Order, err error) {
	defer c.usage().Measure("get_orders")(&err)
	rowsList, err := c.executionRows()
	if err != nil {
		return nil, err
	}
	orders := make([]Order, 0)
	for _, r := range rowsList {
		if o := c.toOrder(r); o.Status.IsOpen() {
			orders = append(orders, o)
		}
	}
	return orders, nil
}

func (c *NhClient) GetOrder(orderID string) (_ Order, err error) {
	defer c.usage().Measure("get_order")(&err)
	rowsList, err := c.executionRows()
	if err != nil {
		return Order{}, err
	}
	for _, r := range rowsList {
		if nhSameNo(r["itg_orr_no"], orderID) {
			return c.toOrder(r), nil
		}
	}
	return Order{OrderID: orderID, Status: Canceled}, nil // 당일 조회에 없으면 종료로 간주
}

func (c *NhClient) CancelOrder(orderID string) (_ Order, err error) {
	defer c.usage().Measure("cancel_order")(&err)
	rowsList, err := c.executionRows()
	if err != nil {
		return Order{}, err
	}
	var row map[string]any
	for _, r := range rowsList {
		if nhSameNo(r["itg_orr_no"], orderID) {
			row = r
		}
	}
	if row == nil {
		return Order{}, newOrderNotFoundError("order-not-found", "NH 당일 주문에서 찾을 수 없습니다: "+orderID)
	}
	acct, err := c.account()
	if err != nil {
		return Order{}, err
	}
	orgNo, _ := strconv.ParseInt(strings.TrimLeft(orderID, "0"), 10, 64)
	if _, err := c.call("/krstock/order/v1/cancel", map[string]any{
		"act_no": acct, "org_mkt_orr_no": orgNo, "all_pat_dit_cd": "1", "iem_cd": NhNormalizeCode(row["iem_cd"]),
	}); err != nil {
		return Order{}, err
	}
	now := time.Now()
	return Order{OrderID: orderID, Status: Canceled, CanceledAt: &now}, nil
}

func (c *NhClient) GetFills() (_ []Fill, err error) {
	defer c.usage().Measure("fills")(&err)
	rowsList, err := c.executionRows()
	if err != nil {
		return nil, err
	}
	fills := make([]Fill, 0)
	for _, r := range rowsList {
		qty := nhNum(r["tot_cns_qty"])
		if qty == nil || !qty.IsPositive() {
			continue
		}
		fills = append(fills, Fill{
			OrderID: strings.TrimSpace(str(r["itg_orr_no"])), Symbol: NhNormalizeCode(r["iem_cd"]), Side: nhSide(r),
			Quantity: qty, Price: nhNum(r["cns_avg_uit_pr"]),
		})
	}
	return fills, nil
}

// ----------------------------------------------------------------- internal

func (c *NhClient) balance() (map[string]any, error) {
	acct, err := c.account()
	if err != nil {
		return nil, err
	}
	return c.call("/krstock/inquiry/v1/balance", map[string]any{
		"act_no": acct, "bnc_bse_cd": "5", "ltg_aot_dit_cd": "9", "aet_bse": "2", "qut_dit_cd": c.marketCd,
	})
}

// executionRows - 당일 주문·체결 전체(ost_cns_dit=0). 문서마다 체결구분 코드 의미가 달라 전체를 받아 ny_cns_qty 로 가른다.
func (c *NhClient) executionRows() ([]map[string]any, error) {
	acct, err := c.account()
	if err != nil {
		return nil, err
	}
	body, err := c.call("/krstock/inquiry/v1/dailyOrderExecution", map[string]any{
		"orr_dt": time.Now().In(kst).Format("20060102"), "act_no": acct, "ost_cns_dit": "0", "orr_mkt_cd": "00",
	})
	if err != nil {
		return nil, err
	}
	return rows(body, "Output_1"), nil
}

func (c *NhClient) toOrder(row map[string]any) Order {
	ordQty := nhVal(row["orr_qty"])
	filled := nhVal(row["tot_cns_qty"])
	remaining := ordQty.Sub(filled)
	if r := nhNum(row["ny_cns_qty"]); r != nil {
		remaining = *r
	}
	canceled := nhVal(row["can_qty"])
	reject := strings.TrimSpace(str(row["orr_rjt_rsn_cd_nm"]))
	var status OrderStatus
	switch {
	case remaining.IsPositive() && filled.IsPositive():
		status = PartiallyFilled
	case remaining.IsPositive():
		status = Submitted
	case filled.IsPositive() && filled.GreaterThanOrEqual(ordQty):
		status = Filled
	case filled.IsPositive():
		status = PartiallyFilled
	case canceled.IsPositive():
		status = Canceled
	case reject != "":
		status = Rejected
	default:
		status = Canceled
	}
	orderType := Limit
	if strings.Contains(str(row["nmn_pr_tp_cd_nm"]), "시장가") {
		orderType = Market
	}
	return Order{
		OrderID: strings.TrimSpace(str(row["itg_orr_no"])), Status: status, Symbol: NhNormalizeCode(row["iem_cd"]), Side: nhSide(row),
		OrderType: orderType, Quantity: &ordQty, LimitPrice: nhPositive(row["orr_pr"]), FilledQuantity: &filled, AvgFillPrice: nhPositive(row["cns_avg_uit_pr"]),
	}
}

func (c *NhClient) account() (string, error) {
	if c.accountNo != "" {
		return c.accountNo, nil
	}
	expected := "03"
	if c.environment == Live {
		expected = "01"
	}
	body, err := c.call("/n2/acctinfo", map[string]any{})
	if err != nil {
		return "", err
	}
	for _, a := range rows(body, "Output_0") {
		if str(a["acct_type"]) == expected {
			c.accountNo = str(a["acct_no"])
			return c.accountNo, nil
		}
	}
	return "", &BrokerAPIError{200, "", fmt.Sprintf("NH 계좌 목록에 %s 용 계좌(acct_type=%s)가 없습니다", c.environment, expected)}
}

func (c *NhClient) request(path string, body map[string]any) (map[string]any, error) {
	return c.limiter.execute("NH "+path, func() (map[string]any, error) {
		return c.tokens.call(func(token string) (map[string]any, error) { return c.requestOnce(path, body, token) })
	})
}

func (c *NhClient) requestOnce(path string, body map[string]any, token string) (map[string]any, error) {
	payload, _ := json.Marshal(map[string]any{"Input_0": body})
	status, parsed, resHeaders, err := httpJSONHeaders(c.http, "POST", c.baseURL+path, map[string]string{
		"Content-Type": "application/json; charset=UTF-8", "authorization": "Bearer " + token,
		"x-client-id": c.appKey, "x-client-secret": c.appSecret,
	}, payload)
	if err != nil {
		return nil, err
	}
	rspCd := strings.TrimSpace(str(parsed["rsp_cd"]))
	rspMsg := str(parsed["rsp_msg"])
	gw := str(parsed["code"])
	if gw == "" {
		gw = str(obj(parsed, "error")["code"])
	}
	if gw == "" {
		gw = rspCd
	}
	msg := strings.TrimSpace(fmt.Sprintf("NH(%s) [%s] %s", path, gw, rspMsg))
	if status < 200 || status >= 300 {
		retryAfter, _ := strconv.ParseFloat(strings.TrimSpace(resHeaders.Get("Retry-After")), 64)
		switch {
		case status == 429 || strings.HasPrefix(gw, "IGW429"):
			return nil, newRateLimitErrorWithRetryAfter(status, gw, msg, retryAfter)
		case status == 401 || strings.HasPrefix(gw, "IGW4004") || strings.HasPrefix(gw, "IGW4003") || gw == "IGW40051":
			return nil, newAuthError(status, gw, msg)
		case status == 400:
			return nil, &InvalidOrderError{BrokerAPIError{status, gw, msg}}
		default:
			return nil, &BrokerAPIError{status, gw, msg}
		}
	}
	if !nhSuccessCodes[rspCd] && !strings.Contains(rspMsg, "완료") {
		if strings.Contains(rspMsg, "부족") {
			return nil, &InsufficientFundsError{BrokerAPIError{status, rspCd, msg}}
		}
		return nil, &BrokerAPIError{status, rspCd, msg}
	}
	return parsed, nil
}

func (c *NhClient) getToken() (string, error) { return c.tokens.get() }

func (c *NhClient) issueToken() (_ string, _ time.Time, err error) {
	defer c.usage().Measure("auth")(&err) // 실제 발급 경로만 센다 (캐시 히트는 제외)
	c.limiter.throttle.wait()
	// SDK 규약: 파라미터는 쿼리스트링, 본문 없음, content-type 은 form-urlencoded
	query := url.Values{"appkey": {c.appKey}, "appsecretkey": {c.appSecret}, "grant_type": {"client_credentials"}, "scope": {"oob"}}
	status, body, err := httpJSON(c.http, "POST", c.authURL+"/oauth2/token?"+query.Encode(),
		map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, nil)
	if err != nil {
		return "", time.Time{}, err
	}
	token := str(body["access_token"])
	if status != 200 || token == "" {
		code := str(body["code"])
		if code == "" {
			code = str(body["rsp_cd"])
		}
		msg := fmt.Sprintf("NH 토큰 발급 실패(%s): %s", code, str(body["message"])+str(body["rsp_msg"]))
		if status == 429 || strings.HasPrefix(code, "IGW429") {
			return "", time.Time{}, newRateLimitErrorWithRetryAfter(status, code, msg, TokenFailureCooldown.Seconds())
		}
		return "", time.Time{}, newAuthError(status, code, msg)
	}
	return token, tokenExpiresIn(body["expires_in"], 24*time.Hour), nil
}
