package hermetix

// 테스트 실행은 운영 텔레메트리로 보내지 않는다 — 픽스처 재생 호출이 사용량 랭킹·API 현황에 섞이지 않도록
// 전송 함수를 버린다. 전송 자체를 검증하는 테스트(telemetry_test.go)는 자기 TelemetryTransport 로 다시 바꿔 쓴다.

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	TelemetryTransport = func([]byte) error { return nil }
	tokenCacheDir = "" // 토큰 파일 캐시도 끈다 — 가짜 키로 받은 토큰이 ~/.hermetix/tokens 에 남거나 테스트끼리 공유되지 않도록
	os.Exit(m.Run())
}
