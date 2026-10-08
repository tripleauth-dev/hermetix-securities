package hermetix

// 사용량 텔레메트리 — 계약(docs/telemetry.md)대로 합산·분류·직렬화되고, 전송 실패가 호출자에게 새지 않는지.

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type telemetryPayload struct {
	Schema         int    `json:"schema"`
	InstallationID string `json:"installationId"`
	SDK            struct {
		Language string `json:"language"`
		Version  string `json:"version"`
	} `json:"sdk"`
	SentAt  string `json:"sentAt"`
	Buckets []struct {
		Hour        string `json:"hour"`
		Broker      string `json:"broker"`
		Environment string `json:"environment"`
		Ops         []struct {
			Op        string           `json:"op"`
			OK        int64            `json:"ok"`
			Errors    map[string]int64 `json:"errors"`
			LatencyMs map[string]int64 `json:"latencyMs"`
		} `json:"ops"`
		Streams []struct {
			Channel       string `json:"channel"`
			Subscriptions int64  `json:"subscriptions"`
			Messages      int64  `json:"messages"`
		} `json:"streams"`
		Reconnects int64 `json:"reconnects"`
	} `json:"buckets"`
}

func drainPayload(t *testing.T) telemetryPayload {
	t.Helper()
	body := DrainTelemetry(time.Date(2026, 9, 15, 1, 2, 3, 0, time.UTC))
	if body == nil {
		t.Fatal("페이로드가 비어 있다")
	}
	var p telemetryPayload
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func (p telemetryPayload) ops(t *testing.T, broker, env string) map[string]struct {
	OK        int64
	Errors    map[string]int64
	LatencyMs map[string]int64
} {
	t.Helper()
	out := map[string]struct {
		OK        int64
		Errors    map[string]int64
		LatencyMs map[string]int64
	}{}
	for _, b := range p.Buckets {
		if b.Broker != broker || b.Environment != env {
			continue
		}
		for _, o := range b.Ops {
			out[o.Op] = struct {
				OK        int64
				Errors    map[string]int64
				LatencyMs map[string]int64
			}{o.OK, o.Errors, o.LatencyMs}
		}
	}
	return out
}

func resetTelemetry(t *testing.T) {
	t.Helper()
	DrainTelemetry(time.Now())
	original := TelemetryTransport
	t.Cleanup(func() { TelemetryTransport = original; DrainTelemetry(time.Now()) })
}

func TestTelemetryContractShape(t *testing.T) {
	resetTelemetry(t)
	u := BrokerUsage{BrokerID: "kis", Environment: Paper}
	for i := 0; i < 3; i++ {
		func() (err error) { defer u.Measure("quotes")(&err); return nil }()
	}
	func() (err error) {
		defer u.Measure("quotes")(&err)
		return newRateLimitError(429, "EGW00201", "too many")
	}()
	func() (err error) {
		defer u.Measure("create_order")(&err)
		return &InsufficientFundsError{BrokerAPIError{400, "X", "부족"}}
	}()
	func() (err error) { defer u.Measure("candles")(&err); return io.ErrUnexpectedEOF }()

	p := drainPayload(t)
	if p.Schema != 1 || p.SDK.Language != "go" || p.SDK.Version != Version || p.SentAt != "2026-09-15T01:02:03Z" {
		t.Fatalf("payload header = %+v", p)
	}
	if len(p.Buckets) != 1 || p.Buckets[0].Broker != "kis" || p.Buckets[0].Environment != "PAPER" || !strings.HasSuffix(p.Buckets[0].Hour, ":00:00Z") {
		t.Fatalf("bucket = %+v", p.Buckets)
	}
	ops := p.ops(t, "kis", "PAPER")
	if ops["quotes"].OK != 3 || ops["quotes"].Errors["rate_limit"] != 1 || ops["quotes"].LatencyMs["count"] != 4 {
		t.Fatalf("quotes = %+v", ops["quotes"])
	}
	if ops["create_order"].Errors["insufficient_funds"] != 1 || ops["candles"].Errors["network"] != 1 {
		t.Fatalf("ops = %+v", ops)
	}
	if p.Buckets[0].Reconnects != 0 {
		t.Fatal("reconnects 는 0")
	}
	if DrainTelemetry(time.Now()) != nil {
		t.Fatal("drain 뒤에는 비어 있어야 한다")
	}
}

func TestTelemetryMeasurePassthrough(t *testing.T) {
	resetTelemetry(t)
	u := BrokerUsage{BrokerID: "kis", Environment: Paper}
	early := func(flag bool) (s string, err error) {
		defer u.Measure("account")(&err)
		if flag {
			return "early", nil
		}
		return "late", nil
	}
	if v, _ := early(true); v != "early" {
		t.Fatal(v)
	}
	if v, _ := early(false); v != "late" {
		t.Fatal(v)
	}
	_, err := func() (_ string, err error) {
		defer u.Measure("holdings")(&err)
		return "", newAuthError(401, "EGW00123", "expired")
	}()
	var auth *AuthError
	if !errors.As(err, &auth) {
		t.Fatalf("예외가 그대로 나와야 한다: %v", err)
	}
	ops := drainPayload(t).ops(t, "kis", "PAPER")
	if ops["account"].OK != 2 || ops["holdings"].Errors["auth"] != 1 {
		t.Fatalf("ops = %+v", ops)
	}
}

func TestTelemetryStreamsAndReconnectsPerBrokerEnvironment(t *testing.T) {
	resetTelemetry(t)
	paper := BrokerUsage{BrokerID: "kiwoom", Environment: Paper}
	live := BrokerUsage{BrokerID: "kiwoom", Environment: Live}
	paper.StreamSubscribed(StreamTrades, 2)
	for i := 0; i < 5; i++ {
		paper.StreamMessage(StreamTrades, 1)
	}
	paper.StreamSubscribed(StreamOrderEvents, 1)
	paper.Reconnected()
	live.StreamMessage(StreamOrderBook, 3)

	p := drainPayload(t)
	if len(p.Buckets) != 2 {
		t.Fatalf("buckets = %d", len(p.Buckets))
	}
	for _, b := range p.Buckets {
		streams := map[string][2]int64{}
		for _, s := range b.Streams {
			streams[s.Channel] = [2]int64{s.Subscriptions, s.Messages}
		}
		switch b.Environment {
		case "PAPER":
			if streams["TRADES"] != [2]int64{2, 5} || streams["ORDER_EVENTS"] != [2]int64{1, 0} || b.Reconnects != 1 {
				t.Fatalf("paper = %+v", b)
			}
		case "LIVE":
			if streams["ORDER_BOOK"] != [2]int64{0, 3} {
				t.Fatalf("live = %+v", b)
			}
		}
	}
}

func TestTelemetryLatencyPercentilesUseLast256(t *testing.T) {
	resetTelemetry(t)
	for i := 1; i <= 300; i++ {
		RecordUsage("next", Paper, "quotes", time.Duration(i)*time.Millisecond, nil)
	}
	lat := drainPayload(t).ops(t, "next", "PAPER")["quotes"].LatencyMs
	if lat["count"] != 300 {
		t.Fatalf("count = %d", lat["count"])
	}
	// 최근 256개 = 45..300ms → p50 ≈ 172, p95 ≈ 287
	if lat["p50"] < 165 || lat["p50"] > 180 || lat["p95"] < 280 || lat["p95"] > 295 {
		t.Fatalf("latency = %v", lat)
	}
}

func TestTelemetryFlushNoopSwallowsAndNeverResends(t *testing.T) {
	resetTelemetry(t)
	var sent [][]byte
	TelemetryTransport = func(body []byte) error { sent = append(sent, body); return nil }
	FlushTelemetry()
	if len(sent) != 0 {
		t.Fatal("비어 있으면 보내지 않는다")
	}

	BrokerUsage{BrokerID: "kis", Environment: Paper}.StreamMessage(StreamTrades, 1)
	TelemetryTransport = func([]byte) error { return errors.New("서버 없음") }
	FlushTelemetry() // 예외가 밖으로 나오지 않는다
	if DrainTelemetry(time.Now()) != nil {
		t.Fatal("실패한 페이로드는 재전송하지 않는다")
	}

	TelemetryTransport = func([]byte) error { panic("transport panic") }
	BrokerUsage{BrokerID: "kis", Environment: Paper}.StreamMessage(StreamTrades, 1)
	FlushTelemetry() // panic 도 삼킨다

	BrokerUsage{BrokerID: "kis", Environment: Paper}.StreamMessage(StreamTrades, 1)
	TelemetryTransport = func(body []byte) error { sent = append(sent, body); return nil }
	FlushTelemetry()
	if len(sent) != 1 || !strings.Contains(string(sent[0]), `"channel":"TRADES"`) {
		t.Fatalf("sent = %d", len(sent))
	}
}

func TestTelemetryClassifyTable(t *testing.T) {
	cases := map[string]error{
		"rate_limit":         newRateLimitError(429, "", ""),
		"auth":               newAuthError(401, "", ""),
		"market_closed":      newMarketClosedError(200, "", ""),
		"insufficient_funds": &InsufficientFundsError{},
		"invalid_order":      &InvalidOrderError{},
		"order_not_found":    newOrderNotFoundError("", ""),
		"server":             &BrokerAPIError{HTTPStatus: 500},
		"other":              &BrokerAPIError{HTTPStatus: 200, Code: "1"},
		"network":            &net.OpError{Op: "dial", Err: errors.New("refused")},
	}
	for want, err := range cases {
		if got := ClassifyTelemetryError(err); got != want {
			t.Errorf("%T → %s, want %s", err, got, want)
		}
	}
	if ClassifyTelemetryError(errors.New("boom")) != "other" {
		t.Error("일반 에러는 other")
	}
	if ClassifyTelemetryError(errors.Join(errors.New("wrap"), newRateLimitError(429, "", ""))) != "rate_limit" {
		t.Error("감싼 에러도 errors.As 로 분류")
	}
}

func TestTelemetryInstallationIDIsUUID(t *testing.T) {
	id := TelemetryInstallationID()
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(id) {
		t.Fatalf("installation id = %q", id)
	}
	if id != TelemetryInstallationID() {
		t.Fatal("프로세스 안에서 고정")
	}
}

// 여덟 어댑터의 공개 메서드가 전부 계측되는지 — 컨포먼스 시나리오(가짜 HTTP)를 돌리고 버킷을 본다.
func TestTelemetryInstrumentsAllAdapters(t *testing.T) {
	resetTelemetry(t)
	factories := map[string]func(baseURL string) BrokerClient{
		"next": func(u string) BrokerClient { return NewNextClient("pk_test_conf", "sk_test_conf").SetBaseURL(u) },
		"kis": func(u string) BrokerClient {
			return NewKisClient("k", "s", "50199202").SetBaseURL(u).SetThrottle(time.Millisecond)
		},
		"kiwoom": func(u string) BrokerClient {
			return NewKiwoomClient("k", "s").SetBaseURL(u).SetThrottle(time.Millisecond)
		},
		"nh": func(u string) BrokerClient {
			return NewNhClient("k", "s", "").SetBaseURL(u).SetAuthURL(u).SetThrottle(time.Millisecond)
		},
		"db": func(u string) BrokerClient { return NewDbClient("k", "s").SetBaseURL(u).SetThrottle(time.Millisecond) },
		"ls": func(u string) BrokerClient {
			return NewLsClient("k", "s").SetBaseURL(u).SetThrottle(time.Millisecond).SetChartThrottle(time.Millisecond)
		},
		"toss": func(u string) BrokerClient {
			return NewTossClient("c_conf", "s_conf", "").SetBaseURL(u).SetThrottle(time.Millisecond)
		},
		"kb": func(u string) BrokerClient { return NewKbClient("k", "s").SetBaseURL(u).SetThrottle(time.Millisecond) },
	}
	for broker, factory := range factories {
		fx, srv := loadFixture(t, broker)
		qty, _ := decimal.NewFromString(fx.Scenario.Quantity)
		limit, _ := decimal.NewFromString(fx.Scenario.LimitPrice)
		report := VerifyBrokerConformance(factory(srv.URL), ConformanceScenario{Symbol: fx.Scenario.Symbol, Quantity: qty, LimitPrice: limit})
		srv.Close()
		if !report.Passed() {
			t.Fatalf("%s: %s", broker, report.String())
		}
	}
	p := drainPayload(t)
	restOps := []string{"quotes", "candles", "calendar", "account", "holdings", "buying_power", "create_order", "get_orders", "get_order", "cancel_order", "fills"}
	for broker := range factories {
		env := "PAPER"
		if broker == "toss" || broker == "kb" {
			env = "LIVE"
		}
		ops := p.ops(t, broker, env)
		for _, op := range restOps {
			if ops[op].OK < 1 {
				t.Errorf("%s: op %s 가 계측되지 않았다 (%+v)", broker, op, ops[op])
			}
		}
		if ops["auth"].OK < 1 {
			t.Errorf("%s: auth 가 계측되지 않았다", broker)
		}
	}
}

// 스트림 구독·메시지 카운트 — KIS 가짜 웹소켓 서버로 구독 1건, 체결 프레임 1건.
func TestTelemetryCountsStreamSubscriptionsAndMessages(t *testing.T) {
	resetTelemetry(t)
	srv, connections := newWsServer(t)
	stream := newKisMarketStream(wsURL(srv), "P", func() (string, error) { return "APPROVAL-KEY", nil })
	stream.withUsage(BrokerUsage{BrokerID: "kis", Environment: Paper})
	defer stream.Close()
	ticks := make(chan TradeTick, 16)
	stream.SubscribeTrades([]string{"KRX:005930", "000660"}, func(tick TradeTick) { ticks <- tick })
	stream.Connect()
	conn := waitConn(t, connections, 5*time.Second)
	conn.take(t)
	conn.take(t)
	conn.send(t, "0|H0STCNT0|001|005930^093012^71500^5^300^-0.42^71480^71800^71900^71300^71500^71400^15^1234567^88000000000^1200^1300^100^105.2^600000")
	waitTick(t, ticks)

	p := drainPayload(t)
	for _, b := range p.Buckets {
		for _, s := range b.Streams {
			if s.Channel == "TRADES" {
				if s.Subscriptions != 2 || s.Messages != 1 {
					t.Fatalf("TRADES = %+v", s)
				}
				return
			}
		}
	}
	t.Fatal("TRADES 스트림 통계가 없다")
}

// 요청 서명 — 계약 검증 벡터(네 언어 공용)와 일치하고, 타임스탬프가 바뀌면 서명도 바뀐다
func TestTelemetrySignatureMatchesContractVector(t *testing.T) {
	body := []byte(`{"schema":1}`)
	sig := SignTelemetry(body, 1700000000)
	if sig != "c0ce56d2a2b120597403cc70160e8db7ae60d242916857319ecc5845522739d2" {
		t.Fatalf("signature mismatch: %s", sig)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(sig) {
		t.Fatalf("signature must be lowercase hex 64: %s", sig)
	}
	if SignTelemetry(body, 1700000001) == sig {
		t.Fatal("signature must change with timestamp")
	}
}

// 기본 전송은 서명 헤더 3개를 싣는다
func TestTelemetryDefaultTransportSendsSignatureHeaders(t *testing.T) {
	type captured struct {
		keyID, timestamp, signature, body string
	}
	got := make(chan captured, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		got <- captured{
			keyID:     r.Header.Get("X-Hermetix-Key-Id"),
			timestamp: r.Header.Get("X-Hermetix-Timestamp"),
			signature: r.Header.Get("X-Hermetix-Signature"),
			body:      string(raw),
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	original := telemetryEndpoint
	telemetryEndpoint = server.URL
	defer func() { telemetryEndpoint = original }()

	body := []byte(`{"schema":1,"buckets":[]}`)
	before := time.Now().Unix()
	if err := telemetryPost(body); err != nil {
		t.Fatalf("post failed: %v", err)
	}
	select {
	case c := <-got:
		if c.keyID != TelemetrySigningKeyID {
			t.Fatalf("key id: %q", c.keyID)
		}
		ts, err := strconv.ParseInt(c.timestamp, 10, 64)
		if err != nil || ts < before || ts > time.Now().Unix() {
			t.Fatalf("timestamp not current unix seconds: %q", c.timestamp)
		}
		if c.body != string(body) {
			t.Fatalf("body altered: %q", c.body)
		}
		if c.signature != SignTelemetry(body, ts) {
			t.Fatalf("signature does not verify against sent timestamp+body")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("request not received")
	}
}
