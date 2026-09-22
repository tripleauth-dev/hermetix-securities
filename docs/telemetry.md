# 사용량 텔레메트리 (Usage Telemetry) — 계약 v1

Hermetix SDK(Kotlin·Python·JS·Go)는 **어느 증권사가 얼마나 쓰이는지**를 집계해 `hermetix-service` 로 보내고, 그 데이터로 [증권사 사용량 랭킹](https://github.com/tauthdev/hermetix-service)을 공개합니다. OpenRouter 의 모델 랭킹이 실사용량으로 만들어지듯, 국내외 증권사 오픈 API 가 실제로 어떻게 쓰이는지를 보여주는 데이터입니다.

이 문서는 네 언어 SDK 와 서버가 공유하는 **유일한 계약**입니다. 페이로드 필드·값의 의미·전송 규칙을 바꾸려면 여기부터 고치고 `schema` 를 올립니다.

## 원칙

1. **매매 경로와 완전히 분리** — 집계는 메모리 카운터, 전송은 백그라운드 스레드. 전송 실패·타임아웃·서버 장애는 조용히 버리고 큐를 쌓지 않는다. 텔레메트리 때문에 주문이 늦어지거나 실패하는 일은 없다
2. **개인정보·매매 내용은 한 바이트도 보내지 않는다** — 아래 "보내지 않는 것" 은 코드 리뷰의 거부 조건이다
3. **합산만 보낸다** — 개별 호출을 보내지 않고 시간(hour) 버킷으로 합산한 건수·분포만 보낸다
4. **기본 배포본은 항상 켜져 있다** — 끄는 설정은 없다. 대신 무엇을 보내는지 이 문서와 README 에 그대로 공개한다. (MIT 라 지우고 쓰는 것은 사용자 자유)
5. **새 의존성 없음** — 각 언어의 내장 HTTP 클라이언트만 쓴다

## 보내는 것 / 보내지 않는 것

| 보내는 것 | 보내지 않는 것 |
|---|---|
| 브로커 ID (`kis`, `toss` …) | 종목 코드·이름 |
| 환경 (`PAPER` / `LIVE`) | 수량·가격·금액 |
| 호출 종류별 성공/에러 건수 (시간 버킷 합계) | 주문번호·체결번호·계좌번호·고객 ID |
| 에러 분류 (레이트리밋·인증·휴장·자금부족·주문거부·미존재·네트워크·기타) | API 키·토큰·HTS ID |
| 응답 시간 분포 (p50·p95·count, ms) | 전략 이름·설정값 |
| 실시간 채널별 구독 수·수신 메시지 수·재접속 수 | IP 주소 (서버가 저장하지 않음) |
| SDK 언어·버전 | OS·호스트명·사용자명 |
| 설치 단위 무작위 ID (`~/.hermetix/installation-id` 의 UUID) | |

## 페이로드 (`POST {endpoint}` · `Content-Type: application/json`)

```json
{
  "schema": 1,
  "installationId": "3f1c…-uuid",
  "sdk": { "language": "kotlin", "version": "0.11.0" },
  "sentAt": "2026-09-15T01:02:03Z",
  "buckets": [
    {
      "hour": "2026-09-15T01:00:00Z",
      "broker": "kis",
      "environment": "PAPER",
      "ops": [
        { "op": "quotes",       "ok": 118, "errors": { "rate_limit": 2 }, "latencyMs": { "count": 120, "p50": 84, "p95": 230 } },
        { "op": "create_order", "ok": 3,   "errors": {},                  "latencyMs": { "count": 3,   "p50": 140, "p95": 310 } }
      ],
      "streams": [
        { "channel": "TRADES",       "subscriptions": 2, "messages": 15230 },
        { "channel": "ORDER_EVENTS", "subscriptions": 1, "messages": 4 }
      ],
      "reconnects": 1
    }
  ]
}
```

### 필드

| 필드 | 값 | 비고 |
|---|---|---|
| `schema` | `1` | 계약 버전. 호환 안 되는 변경 시 증가 |
| `installationId` | UUID v4 문자열 | 최초 실행 시 생성해 `~/.hermetix/installation-id` 에 저장. 파일을 못 쓰면 프로세스 수명 동안만 유지되는 임시 ID |
| `sdk.language` | `kotlin` \| `python` \| `js` \| `go` | |
| `sdk.version` | 패키지 버전 문자열 | |
| `sentAt` | ISO-8601 UTC | 전송 시각 |
| `buckets[].hour` | ISO-8601 UTC, 분·초 0 | 집계 버킷. 한 페이로드에 여러 시간·브로커 버킷이 올 수 있다 |
| `buckets[].broker` | 브로커 ID | `BrokerCapabilities.brokerId` |
| `buckets[].environment` | `PAPER` \| `LIVE` | |
| `ops[].op` | 아래 표 | REST 호출 종류 |
| `ops[].ok` / `errors` | 정수 / 분류→정수 | `errors` 는 0 인 분류를 생략해도 된다 |
| `ops[].latencyMs` | `count`, `p50`, `p95` | 성공·실패 모두 포함한 응답 시간. 표본은 op 당 최근 256개까지만 보관해 계산 |
| `streams[].channel` | `TRADES` \| `ORDER_BOOK` \| `ORDER_EVENTS` | |
| `streams[].subscriptions` | 정수 | 그 시간 버킷에 보낸 구독(등록) 수 |
| `streams[].messages` | 정수 | 리스너에 전달한 틱/이벤트 수 |
| `buckets[].reconnects` | 정수 | 웹소켓 재접속 횟수 |

### `op` 값

`quotes` `candles` `calendar` `account` `holdings` `buying_power` `create_order` `get_orders` `get_order` `cancel_order` `fills` `auth`

`auth` 는 토큰·접속키 발급 호출. 어댑터 내부의 보조 조회(예: KIS 가 주문 추적을 위해 부르는 잔고 조회)는 바깥 op 에 포함되며 따로 세지 않는다.

### 에러 분류

| 분류 | 조건 |
|---|---|
| `rate_limit` | RateLimitError |
| `auth` | AuthError |
| `market_closed` | MarketClosedError |
| `insufficient_funds` | InsufficientFundsError |
| `invalid_order` | InvalidOrderError |
| `order_not_found` | OrderNotFoundError |
| `network` | 연결·타임아웃 등 HTTP 응답을 받지 못한 경우 |
| `other` | 그 외 모든 예외 |

## 전송 규칙

- 첫 전송은 프로세스 시작 **60초 후**, 이후 **60초마다**. 버킷이 비어 있으면 보내지 않는다
- 프로세스 종료 시 남은 버킷을 한 번 더 보내려 시도한다 (최대 2초, 실패 무시)
- HTTP 타임아웃 연결 2초·전체 3초. 재시도 없음. 응답 본문은 읽지 않는다. 2xx 가 아니어도 버린다
- 전송 후 카운터는 비운다. 실패한 페이로드는 다시 보내지 않는다 (누락은 허용, 중복은 없음)
- 시간 버킷은 UTC 정시 기준. 전송 시점에 현재 진행 중인 시간 버킷도 포함해 보낸다 (서버는 같은 `installationId`+`hour`+`broker`+`environment` 를 **더해서** 집계한다)
- 엔드포인트 기본값은 `https://service-api-prod.hermetix.dev/v1/usage`. 언어별로 상수 한 곳에만 있으며 사용자 설정은 없다
- 사용자 에이전트: `hermetix-{language}/{version}`
- 모든 전송에 위 요청 서명 헤더 3개를 싣는다

## 요청 서명 (schema 1, 2026-09-15 추가)

수신 엔드포인트는 공개돼 있으므로 무심코 오는 스팸·스캐너를 걸러내기 위해 모든 페이로드에 서명을 싣는다. **SDK 가 오픈소스라 키도 공개된 것과 같다** — 서명은 "아무 curl 이나 보낼 수 없게" 하는 문턱이지, 진짜 SDK 를 증명하는 장치가 아니다. 조작 내성은 서버의 타당성 검사와 랭킹 산식이 맡는다.

| 헤더 | 값 |
|---|---|
| `X-Hermetix-Key-Id` | `v1` (키 회전 시 증가) |
| `X-Hermetix-Timestamp` | 전송 시각, Unix 초 (정수 문자열) |
| `X-Hermetix-Signature` | `hex(HMAC-SHA256(key, timestamp + "\n" + body))` — body 는 전송하는 JSON 바이트 그대로(UTF-8), 소문자 hex 64자 |

- `v1` 키: `d97f20cb942540462ea83648ee30a9786bd658b3dc813f74ef011845da258503` — 네 언어 SDK 와 서버가 같은 상수를 가진다 (언어별 텔레메트리 모듈의 `SIGNING_KEY`)
- 서버는 `Key-Id` 를 모르거나, `|now − timestamp| > 300초` 이거나, 서명이 다르면 `401` 로 거부한다 (본문은 저장·로깅하지 않는다)
- 리플레이: 같은 (installationId, sentAt) 페이로드를 10분 안에 다시 받으면 무시한다 (202 는 그대로 준다)
- **검증 벡터** (네 언어 테스트 공용): body `{"schema":1}`, timestamp `1700000000` → signature `c0ce56d2a2b120597403cc70160e8db7ae60d242916857319ecc5845522739d2`

## 타당성 검사 (서버, 조용히 버림)

형식은 맞지만 있을 수 없는 값은 `202` 를 주되 저장하지 않는다 (조작자에게 힌트를 주지 않기 위해). 기준:

| 항목 | 상한 |
|---|---|
| 페이로드당 버킷 수 | 500 |
| `hour` | 현재 −48시간 ~ +2시간 (SDK 재시작 지연 허용) |
| op 당 `ok + Σerrors` | 50,000 / 시간 |
| `latencyMs.p50`·`p95` | 0 ~ 60,000, `p50 ≤ p95`, `count ≤ 10,000,000` |
| 스트림 `messages` | 2,000,000 / 시간·채널, `subscriptions ≤ 10,000` |
| `reconnects` | 10,000 / 시간 |
| 설치당 하루 집계 행 수 | 브로커 × 환경 × 언어 조합 ≤ 64 |

## 랭킹 산식의 조작 내성

- 설치 하나가 브로커 랭킹에 기여하는 호출량은 **그 브로커 설치들의 중앙값 × 20** 으로 상한을 둔다 (설치가 3개 미만이면 캡 없음, 대신 `installations` 가 그대로 노출된다)
- 랭킹 응답은 `installations`(설치 수)를 항상 함께 내려 한 사람이 부풀린 브로커가 "설치 3개짜리 1위" 로 드러나게 한다
- 에러율·p50/p95 는 캡 적용 후 값으로 계산한다
- **설치 신뢰도 가중**: 전체 이력에서 관측된 날짜가 3일 미만인 설치는 기여를 0.2배로 잡는다 (캡과 곱해서 적용). 설치 ID 는 무작위 UUID 라 사람을 구분하지 못하므로, 설치를 여럿 만들어 조금씩 부풀리는 공격의 효과를 줄이기 위한 것이다. 응답의 `trust` 에 규칙을 노출한다

## 서버 (`hermetix-service`) 처리

- `POST /v1/usage` — 서명 검증(`401`) → 형식 검증(`400`, 256KB 초과 `413`) → 타당성 검사(위반은 `202` 후 폐기) → **시간 버킷을 저장하지 않고** `hour` 의 UTC 날짜 기준 **일 단위 집계**(`usage_daily`, 설치·브로커·환경·언어 키)에 더한다 (건수 합산, 지연 분포는 count 가중 병합). 응답 `202` 빈 본문. SDK 의 시간 버킷은 전송 단위일 뿐이다
- IP 는 로그에도 남기지 않는다. `installationId` 는 설치 수 집계에만 쓰고 랭킹 API 로 노출하지 않는다
- 랭킹 API: `GET /v1/rankings?period=1d|7d|30d` — 브로커별 호출량·직전 창 호출량(`previousCalls`, 증감률용)·실전 비율·에러율·p50/p95, 언어별 비중·설치 수, 채널별 스트림 사용량, 설치 수, 작업(op)별 호출 분포와 브로커별 내역(`ops`), 최근 16주 주간 추이(`weekly`, 월요일 시작, 브로커·언어별). 추세 순위는 페이지가 `calls`/`previousCalls` 로 계산하되 `trend.minCalls`(1,000건) 미만은 제외한다. 정적 페이지가 이 JSON 을 그린다
- **API 현황 대시보드**용으로 브로커 단위(설치 차원 없음) 시간 집계 `broker_hourly*` 도 같은 트랜잭션에서 더한다 — 브로커·환경·언어·op 별 시간당 ok/에러 분류/지연/스트림/재접속. 35일 보존. `GET /v1/status`(브로커별 배지·최근 1시간 지표), `GET /v1/brokers/{id}`(프로필 + 24시간·7일 추이). 배지는 최근 1시간을 7일 기준선과 비교해 판정하며 표본 50건 미만이면 UNKNOWN
- 랭킹은 요청마다 계산하지 않는다 — 일 단위 집계에서 **매시 스냅샷**(7d·30d)을 계산해 저장하며 API 는 최신 스냅샷을 돌려준다 (최대 1시간 지연, 응답의 `computedAt`). 일 집계는 영구 보존. 시간대별 통계는 요구 사항에 없어 저장하지 않는다 (필요해지면 그때 시간 테이블을 추가)

## SDK 구현 위치

| 언어 | 코어 | 계측 지점 |
|---|---|---|
| Kotlin | `hermetix-broker` `broker/UsageTelemetry.kt` | 각 어댑터의 공개 메서드(`usage.measure("quotes") { … }`), `ReconnectingWebSocket`(재접속)·각 스트림(구독·메시지) |
| Python | `hermetix/telemetry.py` | `BrokerClient.__init_subclass__` 로 공개 메서드 자동 계측, 스트림 공용 부품 |
| JS | `src/telemetry.ts` | 어댑터 생성자에서 공개 메서드 자동 계측, 스트림 공용 부품 |
| Go | `telemetry.go` | 각 어댑터 메서드의 `defer usage.Measure(...)`, 스트림 공용 부품 |

버전 0.11.0 부터 적용.
