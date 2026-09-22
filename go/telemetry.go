package hermetix

// 사용량 텔레메트리 — 어느 증권사가 얼마나 쓰이는지 시간 버킷으로 합산해 hermetix-service 로 보낸다 (Kotlin UsageTelemetry 대응).
// 계약(필드·전송 규칙·보내지 않는 것)은 docs/telemetry.md 가 정본이다.
//
//   - 매매 경로와 분리: 카운터는 메모리, 전송은 백그라운드 고루틴. 실패는 조용히 버리고 큐를 쌓지 않는다
//   - 개인정보·매매 내용 없음: 브로커·환경·호출 종류·건수·에러 분류·응답 시간 분포·스트림 건수·SDK 버전·설치 ID 뿐
//   - 기본 배포본은 항상 켜져 있다 (설정 없음). 테스트는 TelemetryTransport 와 FlushTelemetry 로 전송을 가로챈다
//   - Go 에는 종료 훅이 없으므로 프로세스가 끝나기 전에 FlushTelemetry() 를 부르면 남은 버킷을 한 번 더 보낸다 (StrategyEngine.Stop 이 부른다)

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// TelemetryEndpoint - 수신 엔드포인트. 언어별로 상수 한 곳에만 있으며 사용자 설정은 없다
	TelemetryEndpoint = "https://service-api-prod.hermetix.dev/v1/usage"
	// TelemetrySchema - 계약 버전
	TelemetrySchema = 1
	telemetrySDK    = "go"

	// TelemetrySigningKeyID / TelemetrySigningKey - 요청 서명 (docs/telemetry.md "요청 서명").
	// 공개 SDK 라 비밀이 아니며, 아무 curl 이나 스캐너가 보내지 못하게 하는 문턱이다. 키 회전 시 ID 를 올린다
	TelemetrySigningKeyID = "v1"
	TelemetrySigningKey   = "d97f20cb942540462ea83648ee30a9786bd658b3dc813f74ef011845da258503"

	telemetryFlushInterval = 60 * time.Second
	telemetryLatencySample = 256
)

// TelemetryTransport - 전송 함수. 기본은 HTTP POST. 테스트에서 교체한다.
var TelemetryTransport = telemetryPost

type telemetryBucketKey struct {
	hour        time.Time
	broker      string
	environment string
}

type telemetryOpStats struct {
	ok      int64
	errors  map[string]int64
	samples [telemetryLatencySample]int64
	count   int64
}

type telemetryStreamStats struct {
	subscriptions int64
	messages      int64
}

type telemetryBucket struct {
	ops        map[string]*telemetryOpStats
	streams    map[string]*telemetryStreamStats
	reconnects int64
}

type telemetryState struct {
	mu             sync.Mutex
	buckets        map[telemetryBucketKey]*telemetryBucket
	startOnce      sync.Once
	installationID string
	idOnce         sync.Once
}

var telemetry = &telemetryState{buckets: map[telemetryBucketKey]*telemetryBucket{}}

// BrokerUsage - 브로커 어댑터 하나가 쥐는 계측 핸들.
//
//	func (c *X) GetQuotes(...) (_ []Quote, err error) {
//	    defer c.usage().Measure("quotes")(&err)
//	    ...
//	}
type BrokerUsage struct {
	BrokerID    string
	Environment TradingEnvironment
}

// Measure - 호출 시작 시각을 잡고, 돌려준 함수를 defer 로 부르면 건수·에러 분류·응답 시간이 기록된다.
func (u BrokerUsage) Measure(op string) func(err *error) {
	start := time.Now()
	return func(err *error) {
		var failure error
		if err != nil {
			failure = *err
		}
		RecordUsage(u.BrokerID, u.Environment, op, time.Since(start), failure)
	}
}

// StreamSubscribed - 구독(등록) 수를 더한다.
func (u BrokerUsage) StreamSubscribed(channel StreamChannel, count int) {
	if count > 0 {
		RecordStreamUsage(u.BrokerID, u.Environment, channel, int64(count), 0)
	}
}

// StreamMessage - 리스너에 전달한 틱/이벤트 수를 더한다.
func (u BrokerUsage) StreamMessage(channel StreamChannel, count int) {
	if count > 0 {
		RecordStreamUsage(u.BrokerID, u.Environment, channel, 0, int64(count))
	}
}

// Reconnected - 웹소켓 재접속 1회.
func (u BrokerUsage) Reconnected() { RecordReconnect(u.BrokerID, u.Environment) }

// RecordUsage - 어댑터 계측용 — 보통 BrokerUsage.Measure 를 통해 호출된다.
func RecordUsage(brokerID string, environment TradingEnvironment, op string, elapsed time.Duration, failure error) {
	telemetry.ensureStarted()
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	b := telemetry.bucketLocked(brokerID, environment)
	s := b.ops[op]
	if s == nil {
		s = &telemetryOpStats{errors: map[string]int64{}}
		b.ops[op] = s
	}
	if failure == nil {
		s.ok++
	} else {
		s.errors[ClassifyTelemetryError(failure)]++
	}
	s.samples[int(s.count%telemetryLatencySample)] = elapsed.Milliseconds()
	s.count++
}

// RecordStreamUsage - 스트림 구독·메시지 수.
func RecordStreamUsage(brokerID string, environment TradingEnvironment, channel StreamChannel, subscriptions, messages int64) {
	telemetry.ensureStarted()
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	b := telemetry.bucketLocked(brokerID, environment)
	s := b.streams[string(channel)]
	if s == nil {
		s = &telemetryStreamStats{}
		b.streams[string(channel)] = s
	}
	s.subscriptions += subscriptions
	s.messages += messages
}

// RecordReconnect - 웹소켓 재접속 횟수.
func RecordReconnect(brokerID string, environment TradingEnvironment) {
	telemetry.ensureStarted()
	telemetry.mu.Lock()
	defer telemetry.mu.Unlock()
	telemetry.bucketLocked(brokerID, environment).reconnects++
}

// ClassifyTelemetryError - 예외를 계약의 에러 분류로 (docs/telemetry.md).
func ClassifyTelemetryError(err error) string {
	if err == nil {
		return "other"
	}
	var (
		rate  *RateLimitError
		auth  *AuthError
		mc    *MarketClosedError
		funds *InsufficientFundsError
		inv   *InvalidOrderError
		nf    *OrderNotFoundError
		api   *BrokerAPIError
		nerr  net.Error
		uerr  *url.Error
	)
	switch {
	case errors.As(err, &rate):
		return "rate_limit"
	case errors.As(err, &auth):
		return "auth"
	case errors.As(err, &mc):
		return "market_closed"
	case errors.As(err, &funds):
		return "insufficient_funds"
	case errors.As(err, &inv):
		return "invalid_order"
	case errors.As(err, &nf):
		return "order_not_found"
	case errors.As(err, &api):
		return "other"
	case errors.As(err, &nerr), errors.As(err, &uerr),
		errors.Is(err, context.DeadlineExceeded), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		return "network"
	}
	return "other"
}

// DrainTelemetry - 지금까지 쌓인 버킷을 페이로드 JSON 으로 만들고 비운다. 비어 있으면 nil.
func DrainTelemetry(now time.Time) []byte {
	telemetry.mu.Lock()
	buckets := telemetry.buckets
	telemetry.buckets = map[telemetryBucketKey]*telemetryBucket{}
	telemetry.mu.Unlock()
	if len(buckets) == 0 {
		return nil
	}
	keys := make([]telemetryBucketKey, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if !keys[i].hour.Equal(keys[j].hour) {
			return keys[i].hour.Before(keys[j].hour)
		}
		if keys[i].broker != keys[j].broker {
			return keys[i].broker < keys[j].broker
		}
		return keys[i].environment < keys[j].environment
	})
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, buckets[k].toJSON(k))
	}
	payload := map[string]any{
		"schema":         TelemetrySchema,
		"installationId": TelemetryInstallationID(),
		"sdk":            map[string]string{"language": telemetrySDK, "version": Version},
		"sentAt":         now.UTC().Format(time.RFC3339),
		"buckets":        out,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	return body
}

// FlushTelemetry - 즉시 전송 시도. 비어 있으면 아무것도 안 하고, 전송 실패는 삼킨다 (재전송 없음).
// 프로세스 종료 전에 한 번 부르면 남은 버킷이 나간다 — StrategyEngine.Stop 이 부른다.
func FlushTelemetry() {
	body := DrainTelemetry(time.Now())
	if body == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("DEBUG hermetix telemetry: send skipped - %v", r)
		}
	}()
	if err := TelemetryTransport(body); err != nil {
		log.Printf("DEBUG hermetix telemetry: send skipped - %v", err)
	}
}

// TelemetryInstallationID - ~/.hermetix/installation-id 의 UUID. 못 읽고 못 쓰면 프로세스 수명 동안만 유지되는 임시 ID.
func TelemetryInstallationID() string {
	telemetry.idOnce.Do(func() { telemetry.installationID = loadOrCreateInstallationID() })
	return telemetry.installationID
}

// ------------------------------------------------------------------ internals

func (t *telemetryState) ensureStarted() {
	t.startOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(telemetryFlushInterval)
			defer ticker.Stop()
			for range ticker.C {
				FlushTelemetry()
			}
		}()
	})
}

func (t *telemetryState) bucketLocked(brokerID string, environment TradingEnvironment) *telemetryBucket {
	key := telemetryBucketKey{hour: time.Now().UTC().Truncate(time.Hour), broker: brokerID, environment: string(environment)}
	b := t.buckets[key]
	if b == nil {
		b = &telemetryBucket{ops: map[string]*telemetryOpStats{}, streams: map[string]*telemetryStreamStats{}}
		t.buckets[key] = b
	}
	return b
}

func (b *telemetryBucket) toJSON(key telemetryBucketKey) map[string]any {
	opNames := make([]string, 0, len(b.ops))
	for op := range b.ops {
		opNames = append(opNames, op)
	}
	sort.Strings(opNames)
	ops := make([]map[string]any, 0, len(opNames))
	for _, op := range opNames {
		s := b.ops[op]
		errs := map[string]int64{}
		for k, v := range s.errors {
			errs[k] = v
		}
		ops = append(ops, map[string]any{"op": op, "ok": s.ok, "errors": errs, "latencyMs": s.latencySummary()})
	}
	chNames := make([]string, 0, len(b.streams))
	for ch := range b.streams {
		chNames = append(chNames, ch)
	}
	sort.Strings(chNames)
	streams := make([]map[string]any, 0, len(chNames))
	for _, ch := range chNames {
		s := b.streams[ch]
		streams = append(streams, map[string]any{"channel": ch, "subscriptions": s.subscriptions, "messages": s.messages})
	}
	return map[string]any{
		"hour": key.hour.Format(time.RFC3339), "broker": key.broker, "environment": key.environment,
		"ops": ops, "streams": streams, "reconnects": b.reconnects,
	}
}

// latencySummary - op 당 최근 256개 표본으로 p50/p95 를 계산한다 (count 는 전체 건수).
func (s *telemetryOpStats) latencySummary() map[string]int64 {
	n := int(s.count)
	if n > telemetryLatencySample {
		n = telemetryLatencySample
	}
	if n == 0 {
		return map[string]int64{"count": 0, "p50": 0, "p95": 0}
	}
	sorted := make([]int64, n)
	copy(sorted, s.samples[:n])
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pct := func(p float64) int64 {
		i := int(float64(n-1) * p)
		if i < 0 {
			i = 0
		}
		if i > n-1 {
			i = n - 1
		}
		return sorted[i]
	}
	return map[string]int64{"count": s.count, "p50": pct(0.50), "p95": pct(0.95)}
}

var telemetryHTTP = &http.Client{
	Timeout:   3 * time.Second,
	Transport: &http.Transport{DialContext: (&net.Dialer{Timeout: 2 * time.Second}).DialContext},
}

// telemetryEndpoint - 실제 전송 주소. 상수 TelemetryEndpoint 와 같으며 테스트에서만 바꾼다
var telemetryEndpoint = TelemetryEndpoint

// SignTelemetry - 계약의 요청 서명: hex(HMAC-SHA256(key, timestamp + "\n" + body)), 소문자 hex 64자
func SignTelemetry(body []byte, timestampSeconds int64) string {
	mac := hmac.New(sha256.New, []byte(TelemetrySigningKey))
	mac.Write([]byte(fmt.Sprintf("%d\n", timestampSeconds)))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func telemetryPost(body []byte) error {
	req, err := http.NewRequest("POST", telemetryEndpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	timestamp := time.Now().Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "hermetix-"+telemetrySDK+"/"+Version)
	req.Header.Set("X-Hermetix-Key-Id", TelemetrySigningKeyID)
	req.Header.Set("X-Hermetix-Timestamp", fmt.Sprintf("%d", timestamp))
	req.Header.Set("X-Hermetix-Signature", SignTelemetry(body, timestamp))
	resp, err := telemetryHTTP.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return nil
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func newUUIDv4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		now := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(now >> (uint(i%8) * 8))
		}
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func loadOrCreateInstallationID() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return newUUIDv4()
	}
	path := filepath.Join(home, ".hermetix", "installation-id")
	if data, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(data)); uuidPattern.MatchString(strings.ToLower(id)) {
			return strings.ToLower(id)
		}
	}
	id := newUUIDv4()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err == nil {
		_ = os.WriteFile(path, []byte(id), 0o600)
	}
	return id
}
