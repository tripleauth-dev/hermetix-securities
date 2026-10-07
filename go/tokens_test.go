package hermetix

// 토큰 수명주기(tokenManager) — 여덟 어댑터 공통 시나리오 (Python test_tokens.py 와 동일).
//   - 발급 유량 초과(429·EGW00133·IGW00201)는 RateLimitError, 발급 실패 후 60초는 서버에 다시 묻지 않는다
//   - 업무 호출이 토큰을 거부(401)하면 토큰을 버리고 1회 재발급 후 재시도한다
//   - 파일 캐시: 다른 프로세스가 새로 저장한 토큰은 지우지 않고 이어 쓴다, 만료 직전 같은 토큰 재발급은 반복하지 않는다

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

var tokenTestBrokers = []string{"next", "kis", "kiwoom", "nh", "db", "ls", "toss", "kb"}

type tokenReply struct {
	status int
	body   map[string]any
}

// withTokenClock - tokenNow 를 2026-09 고정 시계로 바꾼다 (키움 픽스처 토큰(2026-12-31 만료)이 유효한 시점)
func withTokenClock(t *testing.T) *time.Time {
	now := time.Unix(1_790_000_000, 0)
	original := tokenNow
	tokenNow = func() time.Time { return now }
	t.Cleanup(func() { tokenNow = original })
	return &now
}

// tokenFixtureServer - 픽스처 routes 재생. 토큰 발급 응답과 업무 호출 응답을 앞에서부터 덮어쓸 수 있다
type tokenFixtureServer struct {
	mu             sync.Mutex
	tokenOverrides []tokenReply
	apiOverrides   []tokenReply
	tokenCalls     int
	apiCalls       int
	url            string
}

func newTokenFixtureServer(t *testing.T, broker string, tokenOverrides, apiOverrides []tokenReply) *tokenFixtureServer {
	t.Helper()
	raw, err := os.ReadFile("../conformance/fixtures/" + broker + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var fx fixtureFile
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	tokenPath := fx.Routes[0].Path
	s := &tokenFixtureServer{tokenOverrides: tokenOverrides, apiOverrides: apiOverrides}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		overrides := &s.apiOverrides
		if r.URL.Path == tokenPath {
			s.tokenCalls++
			overrides = &s.tokenOverrides
		} else {
			s.apiCalls++
		}
		if len(*overrides) > 0 {
			reply := (*overrides)[0]
			*overrides = (*overrides)[1:]
			w.WriteHeader(reply.status)
			_ = json.NewEncoder(w).Encode(reply.body)
			return
		}
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
			w.WriteHeader(status)
			_, _ = w.Write(route.Body)
			return
		}
		w.WriteHeader(599)
		_, _ = w.Write([]byte(`{"error":"no fixture route for ` + r.Method + " " + r.URL.Path + `"}`))
	}))
	t.Cleanup(srv.Close)
	s.url = srv.URL
	return s
}

func (s *tokenFixtureServer) setAPIOverrides(replies ...tokenReply) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apiOverrides = replies
}

// tokenTestClient - 재시도 없이 첫 오류를 보고, 레이트리미터 대기는 시계를 앞으로 돌린다
func tokenTestClient(broker, baseURL string, now *time.Time) BrokerClient {
	ms := time.Millisecond
	var client BrokerClient
	var limiter *rateLimiter
	switch broker {
	case "next":
		c := NewNextClient("pk_test_conf", "sk_test_conf").SetBaseURL(baseURL)
		client, limiter = c, c.limiter
	case "kis":
		c := NewKisClient("k", "s", "50199202").SetBaseURL(baseURL).SetThrottle(ms)
		client, limiter = c, c.limiter
	case "kiwoom":
		c := NewKiwoomClient("k", "s").SetBaseURL(baseURL).SetThrottle(ms)
		client, limiter = c, c.limiter
	case "nh":
		c := NewNhClient("k", "s", "").SetBaseURL(baseURL).SetAuthURL(baseURL).SetThrottle(ms)
		client, limiter = c, c.limiter
	case "db":
		c := NewDbClient("k", "s").SetBaseURL(baseURL).SetThrottle(ms)
		client, limiter = c, c.limiter
	case "ls":
		c := NewLsClient("k", "s").SetBaseURL(baseURL).SetThrottle(ms).SetChartThrottle(ms)
		client, limiter = c, c.limiter
	case "toss":
		c := NewTossClient("c_conf", "s_conf", "").SetBaseURL(baseURL).SetThrottle(ms)
		client, limiter = c, c.limiter
	case "kb":
		c := NewKbClient("k", "s").SetBaseURL(baseURL).SetThrottle(ms)
		client, limiter = c, c.limiter
	}
	limiter.maxRetries = 0
	limiter.sleep = func(d time.Duration) { *now = now.Add(d) }
	return client
}

func TestTokenRateLimitIsRateLimitErrorWithCooldown(t *testing.T) {
	type tc struct {
		broker    string
		rejection tokenReply
	}
	cases := []tc{}
	for _, b := range tokenTestBrokers {
		cases = append(cases, tc{b, tokenReply{429, map[string]any{}}})
	}
	cases = append(cases,
		tc{"kis", tokenReply{403, map[string]any{"error_code": "EGW00133", "error_description": "접근토큰 발급 잠시 후 다시 시도하세요(1분당 1회)"}}},
		tc{"ls", tokenReply{403, map[string]any{"rsp_cd": "IGW00201", "rsp_msg": "초당 거래건수를 초과하였습니다."}}},
	)
	for _, c := range cases {
		t.Run(c.broker, func(t *testing.T) {
			now := withTokenClock(t)
			s := newTokenFixtureServer(t, c.broker, []tokenReply{c.rejection}, nil)
			client := tokenTestClient(c.broker, s.url, now)
			_, err := client.GetAccount()
			var first *RateLimitError
			if !errors.As(err, &first) || first.RetryAfterSeconds != 60 {
				t.Fatalf("첫 오류: %v", err)
			}
			*now = now.Add(20 * time.Second)
			_, err = client.GetAccount()
			var second *RateLimitError
			if !errors.As(err, &second) || math.Round(second.RetryAfterSeconds) != 40 {
				t.Fatalf("쿨다운 오류: %v", err)
			}
			if s.tokenCalls != 1 {
				t.Fatalf("쿨다운 동안 서버에 다시 물었다: %d", s.tokenCalls)
			}
			*now = now.Add(41 * time.Second)
			if _, err := client.GetAccount(); err != nil { // 쿨다운 뒤 재발급
				t.Fatal(err)
			}
			if s.tokenCalls != 2 {
				t.Fatalf("tokenCalls=%d", s.tokenCalls)
			}
		})
	}
}

func TestTokenAuthFailureIsNotRetriedDuringCooldown(t *testing.T) {
	for _, broker := range tokenTestBrokers {
		t.Run(broker, func(t *testing.T) {
			now := withTokenClock(t)
			s := newTokenFixtureServer(t, broker, []tokenReply{{401, map[string]any{"error": "invalid_client"}}}, nil)
			client := tokenTestClient(broker, s.url, now)
			for i := 0; i < 3; i++ {
				var authErr *AuthError
				if _, err := client.GetAccount(); !errors.As(err, &authErr) {
					t.Fatalf("AuthError 가 아니다: %v", err)
				}
			}
			if s.tokenCalls != 1 {
				t.Fatalf("쿨다운 동안 서버에 다시 물었다: %d", s.tokenCalls)
			}
			*now = now.Add(61 * time.Second)
			if _, err := client.GetAccount(); err != nil {
				t.Fatal(err)
			}
			if s.tokenCalls != 2 {
				t.Fatalf("tokenCalls=%d", s.tokenCalls)
			}
		})
	}
}

func TestRejectedTokenIsDiscardedAndCallRetriedOnce(t *testing.T) {
	for _, broker := range tokenTestBrokers {
		t.Run(broker, func(t *testing.T) {
			now := withTokenClock(t)
			s := newTokenFixtureServer(t, broker, nil, []tokenReply{{401, map[string]any{}}})
			client := tokenTestClient(broker, s.url, now)
			if _, err := client.GetAccount(); err != nil {
				t.Fatal(err)
			}
			if s.tokenCalls != 2 { // 거부된 토큰을 버리고 1회 재발급
				t.Fatalf("tokenCalls=%d", s.tokenCalls)
			}

			s.setAPIOverrides(tokenReply{401, map[string]any{}}, tokenReply{401, map[string]any{}})
			var authErr *AuthError
			if _, err := client.GetAccount(); !errors.As(err, &authErr) { // 재발급 토큰도 거부되면 그대로 올린다 (무한 재시도 없음)
				t.Fatalf("AuthError 가 아니다: %v", err)
			}
		})
	}
}

// tokenIssuer - 결과를 차례로 돌려주는(마지막 결과 반복) 발급 함수와 호출 횟수
func tokenIssuer(results ...any) (func() (string, time.Time, error), *int) {
	calls := 0
	return func() (string, time.Time, error) {
		calls++
		result := results[min(calls, len(results))-1]
		if err, ok := result.(error); ok {
			return "", time.Time{}, err
		}
		issued := result.([2]any)
		return issued[0].(string), issued[1].(time.Time), nil
	}, &calls
}

func withTokenCacheDir(t *testing.T) string {
	dir := t.TempDir()
	original := tokenCacheDir
	tokenCacheDir = dir
	t.Cleanup(func() { tokenCacheDir = original })
	return dir
}

func TestInvalidateKeepsTokenSavedByAnotherProcess(t *testing.T) {
	now := withTokenClock(t)
	dir := withTokenCacheDir(t)
	issueA, callsA := tokenIssuer([2]any{"tok-a", now.Add(24 * time.Hour)})
	issueB, callsB := tokenIssuer([2]any{"tok-b", now.Add(24 * time.Hour)})
	a := newTokenManager("toss", "k", time.Minute, issueA)
	b := newTokenManager("toss", "k", time.Minute, issueB)
	mustToken := func(m *tokenManager, want string) {
		t.Helper()
		if got, err := m.get(); err != nil || got != want {
			t.Fatalf("got %q err=%v, want %q", got, err, want)
		}
	}
	mustToken(a, "tok-a")
	mustToken(b, "tok-a") // 파일 캐시 공유
	// 토스처럼 재발급이 이전 토큰을 무효로 만드는 서버: b 가 거부당해 재발급하면 a 는 거부당한 뒤 b 의 새 토큰을 쓴다
	b.invalidate("tok-a")
	mustToken(b, "tok-b")
	a.invalidate("tok-a")
	mustToken(a, "tok-b")
	if *callsA != 1 || *callsB != 1 {
		t.Fatalf("calls a=%d b=%d", *callsA, *callsB)
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 1 || !strings.HasPrefix(files[0].Name(), "toss-") {
		t.Fatalf("캐시 파일: %v", files)
	}
}

func TestSameTokenReissuedNearExpiryIsUsedUntilExpiry(t *testing.T) {
	now := withTokenClock(t)
	expires := now.Add(300 * time.Second) // 여유(600초) 안 — DB 는 만료 전 재발급에 남은 시간 그대로의 같은 토큰을 준다
	issue, calls := tokenIssuer([2]any{"tok-1", expires})
	m := newTokenManager("db", "k", 10*time.Minute, issue)
	for i := 0; i < 5; i++ {
		if token, err := m.get(); err != nil || token != "tok-1" {
			t.Fatalf("token=%q err=%v", token, err)
		}
	}
	if *calls != 1 {
		t.Fatalf("여유 구간에서 매번 재발급했다: %d", *calls)
	}
	*now = expires
	_, _ = m.get()
	if *calls != 2 {
		t.Fatalf("만료 시각에 재발급하지 않았다: %d", *calls)
	}
}

func TestCallDoesNotRetryOnIssueFailure(t *testing.T) {
	withTokenClock(t)
	issue, calls := tokenIssuer(newAuthError(401, "invalid_client", "rejected"))
	m := newTokenManager("kis", "k", 5*time.Minute, issue)
	_, err := m.call(func(token string) (map[string]any, error) { return map[string]any{"token": token}, nil })
	var authErr *AuthError
	if !errors.As(err, &authErr) || *calls != 1 {
		t.Fatalf("err=%v calls=%d", err, *calls)
	}
}
