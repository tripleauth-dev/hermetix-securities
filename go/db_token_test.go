package hermetix

// DB증권 토큰 수명주기 — 발급 1분 1건 제한(403 + IGW00201)을 레이트리밋으로 분류하고, 발급 실패 후 60초는 서버에 다시 묻지 않으며,
// 발급 토큰을 파일로 저장해 같은 키를 쓰는 다른 프로세스(클라이언트)가 재사용하는지 (Python test_db_token.py 와 동일 시나리오).

import (
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type dbReply struct {
	status int
	body   map[string]any
}

var (
	dbRateLimited = dbReply{403, map[string]any{"rsp_cd": "IGW00201", "rsp_msg": "초당 거래건수를 초과하였습니다."}}
	dbRejected    = dbReply{401, map[string]any{"rsp_cd": "IGW00121", "rsp_msg": "유효하지 않은 앱키입니다."}}
)

func dbIssued(token string) dbReply {
	return dbReply{200, map[string]any{"access_token": token, "token_type": "Bearer", "expires_in": 86400}}
}

// dbTokenServer - 토큰 응답을 차례로 돌려주고(마지막 응답 반복), 업무 호출은 잔고 응답으로 받는 가짜 서버
type dbTokenServer struct {
	mu         sync.Mutex
	replies    []dbReply
	tokenCalls int
	apiTokens  []string
	url        string
}

func newDbTokenServer(t *testing.T, replies ...dbReply) *dbTokenServer {
	s := &dbTokenServer{replies: replies}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		reply := dbReply{200, map[string]any{"rsp_cd": "00000", "rsp_msg": "정상", "Out": map[string]any{"DpsastAmt": "1000", "Dps2": "1000", "TotEvalAmt": "0"}}}
		if strings.HasSuffix(r.URL.Path, "/oauth2/token") {
			s.tokenCalls++
			reply = s.replies[0]
			if len(s.replies) > 1 {
				s.replies = s.replies[1:]
			}
		} else {
			s.apiTokens = append(s.apiTokens, r.Header.Get("authorization"))
		}
		w.WriteHeader(reply.status)
		_ = json.NewEncoder(w).Encode(reply.body)
	}))
	t.Cleanup(srv.Close)
	s.url = srv.URL
	return s
}

// withDbClock - tokenNow 를 고정 시계로 바꾸고, 클라이언트의 레이트리미터 대기가 시계를 앞으로 돌리게 한다
func withDbClock(t *testing.T) *time.Time {
	now := time.Unix(1_800_000_000, 0)
	original := tokenNow
	tokenNow = func() time.Time { return now }
	t.Cleanup(func() { tokenNow = original })
	return &now
}

func dbTokenClient(s *dbTokenServer, now *time.Time, appKey string, maxRetries int) *DbClient {
	c := NewDbClient(appKey, "s").SetBaseURL(s.url).SetThrottle(time.Millisecond)
	c.limiter.maxRetries = maxRetries
	c.limiter.sleep = func(d time.Duration) { *now = now.Add(d) }
	return c
}

func withDbTokenCacheDir(t *testing.T) string {
	dir := t.TempDir()
	original := tokenCacheDir
	tokenCacheDir = dir
	t.Cleanup(func() { tokenCacheDir = original })
	return dir
}

func TestDbTokenRateLimitIsRateLimitErrorAndNotRetriedDuringCooldown(t *testing.T) {
	now := withDbClock(t)
	s := newDbTokenServer(t, dbRateLimited)
	c := dbTokenClient(s, now, "k", 0)
	_, err := c.GetAccount()
	var first *RateLimitError
	if !errors.As(err, &first) || first.RetryAfterSeconds != 60 {
		t.Fatalf("첫 오류: %v", err)
	}
	*now = now.Add(20 * time.Second)
	_, err = c.GetAccount()
	var second *RateLimitError
	if !errors.As(err, &second) || math.Round(second.RetryAfterSeconds) != 40 {
		t.Fatalf("쿨다운 오류: %v", err)
	}
	if s.tokenCalls != 1 {
		t.Fatalf("쿨다운 동안 서버에 다시 물었다: %d", s.tokenCalls)
	}
}

func TestDbRateLimiterWaitsOutCooldownThenIssuesAgain(t *testing.T) {
	now := withDbClock(t)
	s := newDbTokenServer(t, dbRateLimited, dbIssued("tok-1"))
	if _, err := dbTokenClient(s, now, "k", 4).GetAccount(); err != nil {
		t.Fatal(err)
	}
	if s.tokenCalls != 2 || len(s.apiTokens) != 1 || s.apiTokens[0] != "Bearer tok-1" {
		t.Fatalf("tokenCalls=%d apiTokens=%v", s.tokenCalls, s.apiTokens)
	}
}

func TestDbAuthFailureIsNotRetriedAgainstServerDuringCooldown(t *testing.T) {
	now := withDbClock(t)
	s := newDbTokenServer(t, dbRejected, dbIssued("tok-1"))
	c := dbTokenClient(s, now, "k", 4)
	for i := 0; i < 3; i++ {
		var authErr *AuthError
		if _, err := c.GetAccount(); !errors.As(err, &authErr) {
			t.Fatalf("AuthError 가 아니다: %v", err)
		}
	}
	if s.tokenCalls != 1 {
		t.Fatalf("쿨다운 동안 서버에 다시 물었다: %d", s.tokenCalls)
	}
	*now = now.Add(61 * time.Second)
	if _, err := c.GetAccount(); err != nil {
		t.Fatal(err)
	}
	if s.tokenCalls != 2 {
		t.Fatalf("쿨다운 뒤 재발급하지 않았다: %d", s.tokenCalls)
	}
}

func TestDbIssuedTokenIsSharedThroughFileCache(t *testing.T) {
	now := withDbClock(t)
	dir := withDbTokenCacheDir(t)
	if _, err := dbTokenClient(newDbTokenServer(t, dbIssued("tok-shared")), now, "k", 4).GetAccount(); err != nil {
		t.Fatal(err)
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 1 || !strings.HasPrefix(files[0].Name(), "db-") {
		t.Fatalf("캐시 파일: %v", files)
	}
	path := filepath.Join(dir, files[0].Name())
	if info, _ := os.Stat(path); info.Mode().Perm() != 0o600 {
		t.Fatalf("권한 %v", info.Mode().Perm())
	}
	raw, _ := os.ReadFile(path)
	var saved map[string]any
	_ = json.Unmarshal(raw, &saved)
	if len(saved) != 2 || saved["access_token"] != "tok-shared" || saved["expires_at"] != float64(now.Unix()+86400) {
		t.Fatalf("캐시 내용(키·시크릿은 담지 않는다): %v", saved)
	}

	second := newDbTokenServer(t, dbIssued("tok-other"))
	if _, err := dbTokenClient(second, now, "k", 4).GetAccount(); err != nil {
		t.Fatal(err)
	}
	if second.tokenCalls != 0 || second.apiTokens[0] != "Bearer tok-shared" {
		t.Fatalf("캐시를 쓰지 않았다: tokenCalls=%d apiTokens=%v", second.tokenCalls, second.apiTokens)
	}

	otherKey := newDbTokenServer(t, dbIssued("tok-other"))
	if _, err := dbTokenClient(otherKey, now, "k2", 4).GetAccount(); err != nil {
		t.Fatal(err)
	}
	if otherKey.tokenCalls != 1 {
		t.Fatalf("키가 다른데 캐시를 나눠 썼다")
	}
}

func TestDbCachedTokenNearExpiryIsIgnored(t *testing.T) {
	now := withDbClock(t)
	withDbTokenCacheDir(t)
	if _, err := dbTokenClient(newDbTokenServer(t, dbIssued("tok-old")), now, "k", 4).GetAccount(); err != nil {
		t.Fatal(err)
	}
	*now = now.Add((86400 - 300) * time.Second)
	s := newDbTokenServer(t, dbIssued("tok-new"))
	if _, err := dbTokenClient(s, now, "k", 4).GetAccount(); err != nil {
		t.Fatal(err)
	}
	if s.tokenCalls != 1 || s.apiTokens[0] != "Bearer tok-new" {
		t.Fatalf("tokenCalls=%d apiTokens=%v", s.tokenCalls, s.apiTokens)
	}
}
