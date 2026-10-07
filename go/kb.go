package hermetix

// KB증권 Open API(개인 오픈베타) 어댑터.
//
// ⚠️ 실전 전용 · 실측 전 — 모의투자가 없는 운영 단일 환경. 포털 공개 명세(TR 별 입출력 필드·샘플)와 공식 GitHub 예제로 구현했고
// 실계좌 소액 검증 전까지 "미검증". 오픈베타라 스펙이 바뀔 수 있다.
//   - 모든 API 는 POST /api/v1/{tr}, 본문·응답 모두 {"dataHeader", "dataBody"} 봉투. 헤더 Authorization: bearer + appKey
//   - 성공은 dataHeader.processFlag == "A"(HTTP 200 이어도 "B" 면 업무 오류). 숫자는 zero-padded, 문자열은 공백 패딩 → trim
//   - 계좌번호 필드 없음(appKey 바인딩 추정). 게이트웨이 5초당 200건(추정) → 0.1s 쓰로틀
//
// 미확인(실측 필요): bdy_cmpr_ccd 부호 코드(4·5 하락 가정), 체결 조회 레코드 이름(Record1 가정), 표준코드(KR7005930003)→6자리 환산, 차트 시장구분(KOSPI 기본)

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const KbBaseURL = "https://developer.kbsec.com:32484"

var kbFallingSigns = map[string]bool{"4": true, "5": true}

type KbClient struct {
	appKey, appSecret string
	baseURL           string
	excgClsf          string
	sorOrderCcd       string
	chartMarketClsf   string
	environment       TradingEnvironment
	http              *http.Client
	limiter           *rateLimiter
	tokens            *tokenManager
	call              func(path string, body map[string]any) (map[string]any, error)
}

// NewKbClient - 운영 단일 환경(Live 고정). 차트 TR 은 종목의 시장(0 KOSPI / 1 KOSDAQ)을 요구한다 → SetChartMarketClsf.
func NewKbClient(appKey, appSecret string) *KbClient {
	c := &KbClient{
		appKey: appKey, appSecret: appSecret, baseURL: KbBaseURL, excgClsf: "1", sorOrderCcd: "K", chartMarketClsf: "0", environment: Live,
		http:    &http.Client{Timeout: 30 * time.Second},
		limiter: newRateLimiter(100*time.Millisecond, 3, func(a int) time.Duration { return time.Duration(a) * time.Second }),
	}
	c.tokens = newTokenManager("kb", appKey, 5*time.Minute, c.issueToken)
	c.call = c.request
	return c
}

func (c *KbClient) SetBaseURL(u string) *KbClient                   { c.baseURL = u; return c }
func (c *KbClient) SetExcgClsf(v string) *KbClient                  { c.excgClsf = v; return c }
func (c *KbClient) SetSorOrderCcd(v string) *KbClient               { c.sorOrderCcd = v; return c }
func (c *KbClient) SetChartMarketClsf(v string) *KbClient           { c.chartMarketClsf = v; return c }
func (c *KbClient) SetEnvironment(env TradingEnvironment) *KbClient { c.environment = env; return c }
func (c *KbClient) Environment() TradingEnvironment                 { return c.environment }

// usage - 사용량 텔레메트리 핸들 (docs/telemetry.md). 환경은 호출 시점 값을 쓴다
func (c *KbClient) usage() BrokerUsage {
	return BrokerUsage{BrokerID: "kb", Environment: c.environment}
}
func (c *KbClient) SetThrottle(d time.Duration) *KbClient {
	c.limiter = newRateLimiter(d, 3, func(a int) time.Duration { return time.Duration(a) * time.Second })
	return c
}

func (c *KbClient) Capabilities() BrokerCapabilities {
	return BrokerCapabilities{
		BrokerID: "kb", Market: "KRX", Currency: "KRW",
		CandleIntervals: map[CandleInterval]bool{Day1: true},
		ClientOrderID:   false, NativeBracket: false, FractionalShares: false, ServerOpenOrders: true,
		Environments: map[TradingEnvironment]bool{Live: true}, // 모의투자 "추후 제공 예정"
	}
}

// KbNormalizeCode - 잔고 A005930 → 005930, 체결 조회 표준코드 KR7005930003(ISIN) → 4~9번째 자리.
func KbNormalizeCode(raw any) string {
	text := strings.TrimSpace(str(raw))
	switch {
	case len(text) == 7 && text[0] == 'A':
		return text[1:]
	case len(text) == 12 && strings.HasPrefix(text, "KR"):
		return text[3:9]
	}
	return text
}

func kbText(v any) string { return strings.TrimSpace(str(v)) }

func kbSide(row map[string]any) OrderSide {
	if strings.Contains(kbText(row["trd_dl_ccd_nm"]), "매수") {
		return Buy
	}
	return Sell
}

func kbCode(row map[string]any) string {
	if v := kbText(row["stnd_is_no"]); v != "" {
		return KbNormalizeCode(v)
	}
	return KbNormalizeCode(row["stnd_is_cd"])
}

func kbRemaining(row map[string]any) decimal.Decimal {
	if r := nhNum(row["nccls_q"]); r != nil {
		return *r
	}
	return nhVal(row["ordr_q"]).Sub(nhVal(row["tl_ccls_q"]))
}

// ------------------------------------------------------------------- market

func (c *KbClient) GetQuotes(symbols []string) (_ []Quote, err error) {
	defer c.usage().Measure("quotes")(&err)
	caps := c.Capabilities()
	quotes := make([]Quote, 0, len(symbols))
	for _, symbol := range symbols {
		code, err := caps.SymbolCode(symbol)
		if err != nil {
			return nil, err
		}
		out, err := c.call("/api/v1/ivu10140", map[string]any{"excg_clsf": c.excgClsf, "shrt_cd": code})
		if err != nil {
			return nil, err
		}
		change := nhNum(out["bdy_cmpr"])
		rate := nhNum(out["up_dwn_r_p2"])
		if kbFallingSigns[kbText(out["bdy_cmpr_ccd"])] {
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
			Symbol: symbol, Price: nhVal(out["now_prc"]), BidPrice: nhPositive(out["b_sq1_askprc"]), AskPrice: nhPositive(out["s_sq1_askprc"]),
			Volume: nhVal(out["acml_vlm"]).IntPart(), Change: change, ChangeRate: changeRate, Timestamp: time.Now(),
		})
	}
	return quotes, nil
}

func (c *KbClient) GetCandles(symbol string, interval CandleInterval, limit int) (_ []Candle, err error) {
	defer c.usage().Measure("candles")(&err)
	if interval != Day1 {
		return nil, fmt.Errorf("KB 어댑터는 일봉(1d)만 지원합니다")
	}
	code, err := c.Capabilities().SymbolCode(symbol)
	if err != nil {
		return nil, err
	}
	count := limit
	if count <= 0 {
		count = 30
	}
	out, err := c.call("/api/v1/ivs11560", map[string]any{
		"chrt_clsf": "D", "inq_clsf": "2", "strt_dy": time.Now().In(kst).Format("20060102"), "is_cd": code,
		"minute_tck_indx": "일", "info_ccd": "1", "mkt_clsf": c.chartMarketClsf, "inq_cnt": strconv.Itoa(count),
	})
	if err != nil {
		return nil, err
	}
	candles := make([]Candle, 0)
	for _, r := range rows(out, "out2") {
		ts, err := time.ParseInLocation("20060102", kbText(r["dt"]), kst)
		if err != nil {
			continue
		}
		candles = append(candles, Candle{
			Timestamp: ts, Open: nhVal(r["opn_prc_p2"]), High: nhVal(r["hgh_prc_p2"]), Low: nhVal(r["lw_prc_p2"]), Close: nhVal(r["cls_prc_p2"]), Volume: nhVal(r["vlm"]).IntPart(),
		})
	}
	sortCandles(candles)
	if len(candles) > count {
		candles = candles[len(candles)-count:]
	}
	return candles, nil
}

func (c *KbClient) GetCalendar() (_ []MarketDay, err error) {
	defer c.usage().Measure("calendar")(&err)
	return krxCalendar(31), nil
}

// ------------------------------------------------------------------ account

func (c *KbClient) GetAccount() (_ Account, err error) {
	defer c.usage().Measure("account")(&err)
	out, err := c.balance()
	if err != nil {
		return Account{}, err
	}
	cash := nhVal(out["dy_tfnd"])
	portfolio := nhVal(out["nt_asts_val_amt"])
	if !portfolio.IsPositive() {
		portfolio = cash.Add(nhVal(out["val_amt_sum"]))
	}
	return Account{AccountID: "kb-live", Currency: "KRW", Cash: cash, PortfolioValue: portfolio, Status: "ACTIVE"}, nil
}

func (c *KbClient) GetHoldings() (_ []Holding, err error) {
	defer c.usage().Measure("holdings")(&err)
	out, err := c.balance()
	if err != nil {
		return nil, err
	}
	holdings := make([]Holding, 0)
	for _, row := range rows(out, "Record1") {
		qty := nhNum(row["ec_q"])
		if h := nhNum(row["hld_q"]); h != nil && (qty == nil || h.GreaterThan(*qty)) {
			qty = h
		}
		if qty == nil || !qty.IsPositive() {
			continue
		}
		holdings = append(holdings, Holding{
			Symbol: KbNormalizeCode(row["is_cd"]), Quantity: *qty, AvgEntryPrice: nhVal(row["byng_avr_prc"]), CurrentPrice: nhNum(row["now_prc"]),
			MarketValue: nhNum(row["val_amt"]), UnrealizedPnl: nhNum(row["val_pl"]), UnrealizedPnlRate: nhPct(row["val_yld"]),
		})
	}
	return holdings, nil
}

func (c *KbClient) GetBuyingPower() (_ decimal.Decimal, err error) {
	defer c.usage().Measure("buying_power")(&err)
	out, err := c.call("/api/v1/ssqm1802", map[string]any{"bnd_mktio_ccd": "1", "is_no": ""})
	if err != nil {
		return decimal.Zero, err
	}
	if d := nhPositive(out["ordr_psbl_csh"]); d != nil {
		return *d, nil
	}
	return nhVal(out["ordr_psbl_tl_amt"]), nil
}

// ------------------------------------------------------------------- orders

func (c *KbClient) orderBody(jbClsf, code, qty, price, ordrCcd string) map[string]any {
	return map[string]any{
		"mkt_tm_clsf": "1", "ordr_jb_clsf": jbClsf, "s_clsf": "", "is_cd": code, "ordr_q": qty, "ordr_uprc": price,
		"ordr_ccd": ordrCcd, "crdt_typ_cd": "00", "ln_dt": "", "crct_clsf": "", "orgn_ordr_no": "", "gtc_ccd": "",
		"ordr_mng_no": "", "spclz_ordr_ccd": "", "acct_cd": "", "sor_ordr_ccd": c.sorOrderCcd, "stpd_prc": "",
	}
}

func (c *KbClient) CreateOrder(request CreateOrderRequest) (_ Order, err error) {
	defer c.usage().Measure("create_order")(&err)
	code, err := c.Capabilities().SymbolCode(request.Symbol)
	if err != nil {
		return Order{}, err
	}
	isLimit := request.OrderType == Limit
	price := "0"
	ordrCcd := "03"
	if isLimit {
		ordrCcd = "00"
		if request.LimitPrice != nil {
			price = strconv.FormatInt(KrxTickRound(*request.LimitPrice).IntPart(), 10) // KRX 호가단위 보정
		}
	}
	path, jb := "/api/v1/ssam1801", "1"
	if request.Side == Buy {
		path, jb = "/api/v1/ssam1802", "2"
	}
	out, err := c.call(path, c.orderBody(jb, code, strconv.FormatInt(request.Quantity.IntPart(), 10), price, ordrCcd))
	if err != nil {
		return Order{}, err
	}
	orderID := kbText(out["ordr_no"])
	if strings.TrimLeft(orderID, "0") == "" {
		return Order{}, &BrokerAPIError{200, "", "KB 주문 응답에 ordr_no 가 없습니다: " + kbText(out["o_msg"])}
	}
	zero := decimal.Zero
	now := time.Now()
	return Order{
		OrderID: orderID, Status: Submitted, Symbol: code, Side: request.Side, OrderType: request.OrderType,
		Quantity: &request.Quantity, LimitPrice: request.LimitPrice, FilledQuantity: &zero, ClientOrderID: request.ClientOrderID, SubmittedAt: &now,
	}, nil
}

func (c *KbClient) GetOrders() (_ []Order, err error) {
	defer c.usage().Measure("get_orders")(&err)
	rowsList, err := c.orderRows()
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

func (c *KbClient) GetOrder(orderID string) (_ Order, err error) {
	defer c.usage().Measure("get_order")(&err)
	rowsList, err := c.orderRows()
	if err != nil {
		return Order{}, err
	}
	for _, r := range rowsList {
		if nhSameNo(r["ordr_no"], orderID) {
			return c.toOrder(r), nil
		}
	}
	return Order{OrderID: orderID, Status: Canceled}, nil
}

func (c *KbClient) CancelOrder(orderID string) (_ Order, err error) {
	defer c.usage().Measure("cancel_order")(&err)
	rowsList, err := c.orderRows()
	if err != nil {
		return Order{}, err
	}
	var row map[string]any
	for _, r := range rowsList {
		if nhSameNo(r["ordr_no"], orderID) {
			row = r
		}
	}
	if row == nil {
		return Order{}, newOrderNotFoundError("order-not-found", "KB 당일 주문에서 찾을 수 없습니다: "+orderID)
	}
	body := c.orderBody("4", kbCode(row), strconv.FormatInt(kbRemaining(row).IntPart(), 10), "0", "00")
	body["crct_clsf"] = "2"
	body["orgn_ordr_no"] = fmt.Sprintf("%010s", orderID)
	if _, err := c.call("/api/v1/ssam1806", body); err != nil {
		return Order{}, err
	}
	now := time.Now()
	return Order{OrderID: orderID, Status: PendingCancel, CanceledAt: &now}, nil
}

func (c *KbClient) GetFills() (_ []Fill, err error) {
	defer c.usage().Measure("fills")(&err)
	rowsList, err := c.orderRows()
	if err != nil {
		return nil, err
	}
	fills := make([]Fill, 0)
	for _, r := range rowsList {
		qty := nhNum(r["tl_ccls_q"])
		if qty == nil || !qty.IsPositive() {
			continue
		}
		fills = append(fills, Fill{OrderID: kbText(r["ordr_no"]), Symbol: kbCode(r), Side: kbSide(r), Quantity: qty, Price: nhPositive(r["ccls_uprc"])})
	}
	return fills, nil
}

// ----------------------------------------------------------------- internal

func (c *KbClient) balance() (map[string]any, error) {
	return c.call("/api/v1/ssqm2952", map[string]any{"excg_mktpr_ccd": ""})
}

func (c *KbClient) orderRows() ([]map[string]any, error) {
	out, err := c.call("/api/v1/ssqm2341", map[string]any{
		"inq_clsf": "9", "ccls_clsf": "0", "ordr_dt": time.Now().In(kst).Format("20060102"), "is_cd": "", "ordr_no": "",
		"mthr_ordr_no": "", "orgn_ordr_no": "", "s_ccls_amt": "", "b_ccls_amt": "", "s_ccls_q": "", "b_ccls_q": "",
		"ac_nm": "", "is_nm": "", "cn_clsf": "", "nxt_key": "",
	})
	if err != nil {
		return nil, err
	}
	return rows(out, "Record1"), nil
}

func (c *KbClient) toOrder(row map[string]any) Order {
	qty := nhVal(row["ordr_q"])
	filled := nhVal(row["tl_ccls_q"])
	remaining := kbRemaining(row)
	cancelText := kbText(row["crct_cncl_ccd"])
	reject := kbText(row["rfsl_rsn_nm"])
	var status OrderStatus
	switch {
	case strings.Contains(cancelText, "취소") && remaining.IsPositive():
		status = PendingCancel
	case remaining.IsPositive() && filled.IsPositive():
		status = PartiallyFilled
	case remaining.IsPositive():
		status = Submitted
	case filled.IsPositive() && filled.GreaterThanOrEqual(qty):
		status = Filled
	case filled.IsPositive():
		status = PartiallyFilled
	case reject != "":
		status = Rejected
	default:
		status = Canceled
	}
	orderType := Limit
	if strings.TrimLeft(kbText(row["ordr_ccd"]), "0") == "3" {
		orderType = Market
	}
	return Order{
		OrderID: kbText(row["ordr_no"]), Status: status, Symbol: kbCode(row), Side: kbSide(row), OrderType: orderType,
		Quantity: &qty, LimitPrice: nhPositive(row["ordr_uprc"]), FilledQuantity: &filled, AvgFillPrice: nhPositive(row["ccls_uprc"]),
	}
}

func (c *KbClient) request(path string, body map[string]any) (map[string]any, error) {
	return c.limiter.execute("KB "+path, func() (map[string]any, error) {
		return c.tokens.call(func(token string) (map[string]any, error) { return c.requestOnce(path, body, token) })
	})
}

func (c *KbClient) requestOnce(path string, body map[string]any, token string) (map[string]any, error) {
	payload, _ := json.Marshal(map[string]any{"dataHeader": map[string]any{"ipAddr": "", "macAddr": ""}, "dataBody": body})
	status, parsed, resHeaders, err := httpJSONHeaders(c.http, "POST", c.baseURL+path, map[string]string{
		"Content-Type": "application/json; charset=utf-8", "Authorization": "bearer " + token, "appKey": c.appKey,
	}, payload)
	if err != nil {
		return nil, err
	}
	header := obj(parsed, "dataHeader")
	data := obj(parsed, "dataBody")
	flag := kbText(header["processFlag"])
	code := kbText(header["processCode"])
	if code == "" {
		code = kbText(header["resultCode"])
	}
	message := ""
	for _, m := range []string{kbText(header["processMessage"]), kbText(header["resultMessage"]), kbText(data["o_msg"])} {
		if m != "" {
			message = m
			break
		}
	}
	msg := strings.TrimSpace(fmt.Sprintf("KB(%s) [%s] %s", path, code, message))
	if status < 200 || status >= 300 || (flag != "" && flag != "A") {
		retryAfter, _ := strconv.ParseFloat(strings.TrimSpace(resHeaders.Get("Retry-After")), 64)
		switch {
		case status == 429 || containsAny(message, "한도", "초과"):
			return nil, newRateLimitErrorWithRetryAfter(status, code, msg, retryAfter)
		case status == 401 || status == 403 || containsAny(message, "토큰", "인증"):
			return nil, newAuthError(status, code, msg)
		case containsAny(message, "장종료", "장운영", "장마감", "휴장"):
			return nil, newMarketClosedError(status, code, msg)
		case strings.Contains(message, "부족"):
			return nil, &InsufficientFundsError{BrokerAPIError{status, code, msg}}
		case containsAny(message, "호가", "수량", "단위"):
			return nil, &InvalidOrderError{BrokerAPIError{status, code, msg}}
		case containsAny(message, "주문번호", "원주문"):
			return nil, newOrderNotFoundError(code, msg)
		default:
			return nil, &BrokerAPIError{status, code, msg}
		}
	}
	return data, nil
}

func (c *KbClient) issueToken() (_ string, _ time.Time, err error) {
	defer c.usage().Measure("auth")(&err) // 실제 발급 경로만 센다 (캐시 히트는 제외)
	c.limiter.throttle.wait()
	payload, _ := json.Marshal(map[string]any{
		"dataHeader": map[string]any{"ipAddr": "", "macAddr": ""},
		"dataBody":   map[string]any{"appKey": c.appKey, "appSecret": c.appSecret, "grantType": "client_credentials"},
	})
	status, body, err := httpJSON(c.http, "POST", c.baseURL+"/oauth2/token", map[string]string{"Content-Type": "application/json; charset=utf-8"}, payload)
	if err != nil {
		return "", time.Time{}, err
	}
	data := obj(body, "dataBody")
	token := str(data["access_token"])
	if status != 200 || token == "" {
		h := obj(body, "dataHeader")
		detail := kbText(h["processMessage"])
		if detail == "" {
			detail = kbText(h["resultMessage"])
		}
		code, msg := kbText(h["processCode"]), fmt.Sprintf("KB 토큰 발급 실패(%s): %s", kbText(h["resultCode"]), detail)
		if status == 429 {
			return "", time.Time{}, newRateLimitErrorWithRetryAfter(status, code, msg, TokenFailureCooldown.Seconds())
		}
		return "", time.Time{}, newAuthError(status, code, msg)
	}
	return token, tokenExpiresIn(data["expires_in"], 24*time.Hour), nil
}
