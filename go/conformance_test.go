package hermetix

// 네 언어가 공유하는 골든 픽스처(conformance/fixtures)를 httptest 서버로 재생해 모든 어댑터를 컨포먼스 시나리오에 통과시킨다.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type fixtureRoute struct {
	Method string          `json:"method"`
	Path   string          `json:"path"`
	Header []string        `json:"header"`
	Status int             `json:"status"`
	Body   json.RawMessage `json:"body"`
}

type fixtureFile struct {
	Scenario struct {
		Symbol     string `json:"symbol"`
		Quantity   string `json:"quantity"`
		LimitPrice string `json:"limitPrice"`
	} `json:"scenario"`
	Routes []fixtureRoute `json:"routes"`
}

func loadFixture(t *testing.T, broker string) (fixtureFile, *httptest.Server) {
	t.Helper()
	raw, err := os.ReadFile("../conformance/fixtures/" + broker + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var fx fixtureFile
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	// 픽스처 routes 를 순서대로 매칭 - method / path(정확히) / header(이름·값) 중 지정된 조건만 검사
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, route := range fx.Routes {
			if route.Method != "" && route.Method != r.Method {
				continue
			}
			if route.Path != "" && route.Path != r.URL.Path {
				continue
			}
			if len(route.Header) == 2 && r.Header.Get(route.Header[0]) != route.Header[1] {
				continue
			}
			status := route.Status
			if status == 0 {
				status = 200
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write(route.Body)
			return
		}
		w.WriteHeader(599)
		_, _ = w.Write([]byte(`{"error":"no fixture route for ` + r.Method + " " + r.URL.Path + `"}`))
	}))
	return fx, srv
}

func runConformance(t *testing.T, broker string, client func(baseURL string) BrokerClient) {
	t.Helper()
	fx, srv := loadFixture(t, broker)
	defer srv.Close()
	qty, _ := decimal.NewFromString(fx.Scenario.Quantity)
	limit, _ := decimal.NewFromString(fx.Scenario.LimitPrice)
	report := VerifyBrokerConformance(client(srv.URL), ConformanceScenario{Symbol: fx.Scenario.Symbol, Quantity: qty, LimitPrice: limit})
	if !report.Passed() {
		t.Fatal(report.String())
	}
	for _, s := range []string{"quotes", "candles", "calendar", "account", "holdings", "buyingPower", "createOrder", "getOrder", "getOrders", "cancelOrder", "fills"} {
		if !strings.Contains(strings.Join(report.Steps, ","), s) {
			t.Fatalf("step %s missing", s)
		}
	}
}

func TestNextConformance(t *testing.T) {
	runConformance(t, "next", func(baseURL string) BrokerClient {
		return NewNextClient("pk_test_conf", "sk_test_conf").SetBaseURL(baseURL)
	})
}

func TestKisConformance(t *testing.T) {
	runConformance(t, "kis", func(baseURL string) BrokerClient {
		return NewKisClient("k", "s", "50199202").SetBaseURL(baseURL).SetThrottle(time.Millisecond)
	})
}

func TestKiwoomConformance(t *testing.T) {
	runConformance(t, "kiwoom", func(baseURL string) BrokerClient {
		return NewKiwoomClient("k", "s").SetBaseURL(baseURL).SetThrottle(time.Millisecond)
	})
}

// accountNo 를 비워 /n2/acctinfo 로 모의(acct_type=03) 계좌를 고르는 경로까지 검증. 토큰은 운영 호스트 전용 — 테스트에선 같은 서버
func TestNhConformance(t *testing.T) {
	runConformance(t, "nh", func(baseURL string) BrokerClient {
		return NewNhClient("k", "s", "").SetBaseURL(baseURL).SetAuthURL(baseURL).SetThrottle(time.Millisecond)
	})
}

func TestDbConformance(t *testing.T) {
	runConformance(t, "db", func(baseURL string) BrokerClient {
		return NewDbClient("k", "s").SetBaseURL(baseURL).SetThrottle(time.Millisecond)
	})
}

func TestRateLimiterRetryAndThrottle(t *testing.T) {
	var sleeps []time.Duration
	now := time.Unix(0, 0)
	limiter := newRateLimiter(600*time.Millisecond, 2, func(a int) time.Duration { return time.Duration(a) * time.Second })
	limiter.sleep = func(d time.Duration) { sleeps = append(sleeps, d); now = now.Add(d) }
	limiter.throttle.sleep = limiter.sleep
	limiter.throttle.now = func() time.Time { return now }
	_, _ = limiter.execute("t", func() (map[string]any, error) { now = now.Add(100 * time.Millisecond); return nil, nil })
	_, _ = limiter.execute("t", func() (map[string]any, error) { return nil, nil })
	if len(sleeps) != 1 || sleeps[0] != 500*time.Millisecond {
		t.Fatalf("throttle sleeps = %v", sleeps)
	}

	sleeps = nil
	calls := 0
	limiter = newRateLimiter(0, 2, func(a int) time.Duration { return time.Duration(a) * time.Second })
	limiter.sleep = func(d time.Duration) { sleeps = append(sleeps, d) }
	body, err := limiter.execute("t", func() (map[string]any, error) {
		calls++
		if calls < 3 {
			return nil, newRateLimitError(429, "EGW00201", "초당 거래건수 초과")
		}
		return map[string]any{"ok": true}, nil
	})
	if err != nil || body["ok"] != true || len(sleeps) != 2 || sleeps[0] != time.Second || sleeps[1] != 2*time.Second {
		t.Fatalf("retry: err=%v sleeps=%v", err, sleeps)
	}

	sleeps = nil
	first := true
	limiter = newRateLimiter(0, 1, func(a int) time.Duration { return time.Second })
	limiter.sleep = func(d time.Duration) { sleeps = append(sleeps, d) }
	_, err = limiter.execute("t", func() (map[string]any, error) {
		if first {
			first = false
			return nil, newRateLimitErrorWithRetryAfter(429, "", "rl", 3600)
		}
		return nil, nil
	})
	if err != nil || len(sleeps) != 1 || sleeps[0] != maxRetryAfter {
		t.Fatalf("retry-after: err=%v sleeps=%v", err, sleeps)
	}

	_, err = limiter.execute("t", func() (map[string]any, error) { return nil, newRateLimitError(429, "", "x") })
	if _, ok := err.(*RateLimitError); !ok {
		t.Fatalf("소진 후 RateLimitError 전파: %v", err)
	}
}

func TestLsConformance(t *testing.T) {
	runConformance(t, "ls", func(baseURL string) BrokerClient {
		return NewLsClient("k", "s").SetBaseURL(baseURL).SetThrottle(time.Millisecond).SetChartThrottle(time.Millisecond)
	})
}

// accountSeq 를 비워 /api/v1/accounts 로 BROKERAGE 계좌를 고르는 경로까지 검증
func TestTossConformance(t *testing.T) {
	runConformance(t, "toss", func(baseURL string) BrokerClient {
		return NewTossClient("c_conf", "s_conf", "").SetBaseURL(baseURL).SetThrottle(time.Millisecond)
	})
}

// 미국 종목 평가금액·손익은 USD 로 오므로 매매기준율(midRate 1400)로 원화 환산해 계좌 총평가에 더한다 (실계좌 응답 형태)
func TestTossUSHoldingsValuedInKRW(t *testing.T) {
	_, srv := loadFixture(t, "toss")
	defer srv.Close()
	client := NewTossClient("c_conf", "s_conf", "").SetBaseURL(srv.URL).SetThrottle(time.Millisecond)
	holdings, err := client.GetHoldings()
	if err != nil {
		t.Fatal(err)
	}
	var aapl *Holding
	for i := range holdings {
		if holdings[i].Symbol == "US:AAPL" {
			aapl = &holdings[i]
		}
	}
	if aapl == nil || aapl.CurrentPrice.String() != "250" || aapl.MarketValue.String() != "700000" || aapl.UnrealizedPnl.String() != "140000" {
		t.Fatalf("AAPL = %+v", aapl)
	}
	account, err := client.GetAccount()
	if err != nil || account.PortfolioValue.String() != "1910000" {
		t.Fatalf("portfolio = %v, err = %v", account.PortfolioValue, err)
	}
}

func TestKbConformance(t *testing.T) {
	runConformance(t, "kb", func(baseURL string) BrokerClient {
		return NewKbClient("k", "s").SetBaseURL(baseURL).SetThrottle(time.Millisecond)
	})
}
