package hermetix

// 증권사 접근 토큰 수명주기 — 여덟 어댑터 공용 (Python hermetix/tokens.py 와 동일 규칙).
//
//   - 메모리 → 파일 캐시 → 발급 순으로 찾는다. 파일 캐시는 같은 키를 쓰는 여러 프로세스가 토큰을 나눠 쓰게 한다
//     (KIS·DB 는 발급 1분 1건, 토스는 재발급 시 이전 토큰 무효라 프로세스마다 발급하면 서로 부딪친다).
//     위치 ~/.hermetix/tokens/<broker>-<sha256(appKey) 앞 16자>.json, 내용은 access_token·expires_at 뿐 — 키·시크릿은 담지 않는다.
//   - 발급에 실패(*AuthError / *RateLimitError)하면 TokenFailureCooldown 동안 서버에 다시 묻지 않고 같은 오류(유량 초과면 남은 시간)를 돌려준다.
//     네트워크 오류는 쿨다운 없이 다음 호출에서 다시 시도한다.
//   - 업무 호출이 토큰을 거부(*AuthError)하면 그 토큰을 메모리·파일에서 버리고 한 번 다시 시도한다 (call).

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TokenFailureCooldown - 발급 실패 후 서버에 다시 묻지 않는 시간 (KIS·DB 발급 1분 1건 제한에 맞춘다)
const TokenFailureCooldown = 60 * time.Second

// tokenNow - 토큰 수명 계산용 시계 (테스트에서 교체한다)
var tokenNow = time.Now

// tokenCacheDir - 발급 토큰 파일 캐시 위치. 빈 문자열이면 파일 캐시를 쓰지 않는다 (테스트)
var tokenCacheDir = func() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".hermetix", "tokens")
}()

// tokenManager - issue 는 (토큰, 만료 시각) 을 돌려주거나 *AuthError / *RateLimitError 를 낸다. 텔레메트리 "auth" 측정은 issue 가 한다.
type tokenManager struct {
	broker, key string
	issue       func() (string, time.Time, error)
	margin      time.Duration
	mu          sync.Mutex
	token       string
	refreshAt   time.Time
	// 마지막 발급 실패 — 쿨다운(failUntil) 동안은 서버에 묻지 않고 같은 오류를 돌려준다
	failUntil time.Time
	failErr   error
}

func newTokenManager(broker, key string, margin time.Duration, issue func() (string, time.Time, error)) *tokenManager {
	return &tokenManager{broker: broker, key: key, issue: issue, margin: margin}
}

func (m *tokenManager) get() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := tokenNow()
	if m.token != "" && now.Before(m.refreshAt) {
		return m.token, nil
	}
	if token, expires, ok := m.load(); ok && token != m.token && now.Before(expires.Add(-m.margin)) {
		m.token, m.refreshAt = token, expires.Add(-m.margin)
		return token, nil
	}
	if m.failErr != nil && now.Before(m.failUntil) {
		return "", tokenCooldownError(m.failErr, m.failUntil.Sub(now))
	}
	token, expires, err := m.issue()
	if err != nil {
		var authErr *AuthError
		var rateLimited *RateLimitError
		if errors.As(err, &authErr) || errors.As(err, &rateLimited) { // 네트워크 오류는 쿨다운 없이 다음 호출에서 다시 시도
			m.failUntil, m.failErr = tokenNow().Add(TokenFailureCooldown), err
		}
		return "", err
	}
	m.failErr = nil
	// 만료 전 재발급에 남은 시간 그대로의 같은 토큰을 돌려주는 서버(DB·KIS)는 여유 구간에서 매번 재발급하지 않도록 만료까지 쓴다
	m.token, m.refreshAt = token, expires.Add(-m.margin)
	if !m.refreshAt.After(tokenNow()) {
		m.refreshAt = expires
	}
	m.save(token, expires)
	return token, nil
}

// invalidate - 업무 호출이 거부한 토큰을 버린다. 다른 프로세스가 새로 저장한 토큰은 지우지 않는다
func (m *tokenManager) invalidate(token string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.token == token {
		m.token, m.refreshAt = "", time.Time{}
	}
	if cached, _, ok := m.load(); ok && cached == token {
		_ = os.Remove(m.path())
	}
}

// call - 토큰을 받아 fn 을 부르고, 업무 호출이 토큰을 거부(*AuthError)하면 버린 뒤 한 번 다시 부른다
func (m *tokenManager) call(fn func(token string) (map[string]any, error)) (map[string]any, error) {
	token, err := m.get()
	if err != nil {
		return nil, err
	}
	body, err := fn(token)
	var authErr *AuthError
	if !errors.As(err, &authErr) {
		return body, err
	}
	m.invalidate(token)
	if token, err = m.get(); err != nil {
		return nil, err
	}
	return fn(token)
}

type cachedToken struct {
	AccessToken string  `json:"access_token"`
	ExpiresAt   float64 `json:"expires_at"`
}

func (m *tokenManager) path() string {
	if tokenCacheDir == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(m.key))
	return filepath.Join(tokenCacheDir, m.broker+"-"+hex.EncodeToString(sum[:])[:16]+".json")
}

func (m *tokenManager) load() (string, time.Time, bool) {
	path := m.path()
	if path == "" {
		return "", time.Time{}, false
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", time.Time{}, false
	}
	var cached cachedToken
	if json.Unmarshal(raw, &cached) != nil || cached.AccessToken == "" {
		return "", time.Time{}, false
	}
	return cached.AccessToken, time.Unix(0, int64(cached.ExpiresAt*float64(time.Second))), true
}

// save - 실패해도 이번 호출에는 영향을 주지 않는다
func (m *tokenManager) save(token string, expires time.Time) {
	path := m.path()
	if path == "" {
		return
	}
	raw, err := json.Marshal(cachedToken{AccessToken: token, ExpiresAt: float64(expires.UnixNano()) / float64(time.Second)})
	if err != nil || os.MkdirAll(filepath.Dir(path), 0o700) != nil {
		return
	}
	tmp := fmt.Sprintf("%s.%d.tmp", path, os.Getpid())
	if os.WriteFile(tmp, raw, 0o600) != nil {
		return
	}
	_ = os.Chmod(tmp, 0o600)
	if os.Rename(tmp, path) != nil {
		_ = os.Remove(tmp)
	}
}

// tokenCooldownError - 쿨다운 중 재요청. 서버에 묻지 않고 같은 종류의 오류를 남은 시간과 함께 돌려준다
func tokenCooldownError(failure error, remaining time.Duration) error {
	var rateLimited *RateLimitError
	if errors.As(failure, &rateLimited) {
		return newRateLimitErrorWithRetryAfter(rateLimited.HTTPStatus, rateLimited.Code, rateLimited.Message, remaining.Seconds())
	}
	return failure
}

// tokenExpiresIn - 발급 응답의 expires_in(초) → 만료 시각. 없거나 1분 미만이면 fallback 을 쓴다
func tokenExpiresIn(expiresIn any, fallback time.Duration) time.Time {
	now := tokenNow()
	expires := now.Add(time.Duration(nhVal(expiresIn).IntPart()) * time.Second)
	if expires.Before(now.Add(time.Minute)) {
		return now.Add(fallback)
	}
	return expires
}
