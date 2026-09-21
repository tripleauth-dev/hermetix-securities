# 코어 아키텍처

이 문서는 hermetix-securities 의 **내부 동작**을 설명합니다. 전략을 작성하려면 [전략 작성 가이드](strategy-guide.md)를 보세요 — 이 문서는 코어에 기여하거나 동작을 깊이 이해하려는 사람을 위한 것입니다.

## 설계 원칙

1. **전략 작성자의 표면적 최소화** — 사용자가 만지는 것은 `TradingStrategy` / `StrategyContext` / `Signal` 세 타입뿐. 나머지는 전부 자동설정으로 숨긴다. 이 세 타입의 시그니처 변경은 모든 전략 레포를 깨뜨리므로 신중히 다룬다
2. **DB 없음, 서버가 source of truth** — 보유/미체결/체결은 항상 모의투자 서버에서 조회한다. 커뮤니티 사용자가 DB 설정 없이 `yml + 전략 클래스` 만으로 봇을 띄우는 것이 목표. 대가로 일부 상태(브라켓, 감쇠 카운터 등)는 메모리에만 있어 재시작 시 사라진다 — 이 트레이드오프는 각 지점에 문서화한다
3. **안전 우선** — 공매도 방지(매도 클램프), 주문 멱등성(clientOrderId), 연속 실패 시 자동 정지. 모의투자라도 폭주하는 봇은 커뮤니티 신뢰를 깎는다
4. **호스트 앱을 오염시키지 않는다** — 코어의 Jackson 설정, 스케줄러 스레드는 전부 내부 전용. 앱의 전역 빈을 건드리지 않는다 (0.2.1 에서 ObjectMapper 빈 노출을 제거한 이유)

## 레포 구조 (언어별 모노레포)

```
kotlin/            <- 레퍼런스 구현 (여기서 브로커 변경을 먼저 실측/수정한다)
  hermetix-broker  <- 연결 계층: BrokerClient/Capabilities/에러 계층 + next/kis/kiwoom/nh/db/ls/toss/kb 어댑터
                      Spring 컨테이너 없이도 사용 가능 (어댑터는 일반 생성자 주입)
  hermetix-engine  <- 전략 계층: 전략 SPI + 실행 엔진 + PnL + 자동설정 (broker 에 api 의존)
python/            <- Python 구현 (stdlib 만)
js/                <- TypeScript 구현 (decimal.js)
go/                <- Go 구현 (shopspring/decimal)
                      * 모든 포트는 실측 골든 픽스처 재생 테스트로 레퍼런스와 동작 일치를 보증한다
jitpack.yml        <- JitPack 이 kotlin/ 에서 빌드하도록 지정
```

의존성 좌표: `com.github.tripleauth-dev.hermetix-securities:hermetix-engine` (봇) 또는 `:hermetix-broker` (연결만).

## 컴포넌트 맵

```
                    NextTradingAutoConfiguration (자동설정 진입점)
                                   │
      ┌──────────────┬─────────────┼──────────────┬─────────────┐
      ▼              ▼             ▼              ▼             ▼
 TokenManager → NextApiClient  MarketCalendar  PnlService   TradingGuard
 (토큰 캐시/갱신)  (전 API 래핑)    Service        │  ├ PnlLogger (주기 로그)
                     ▲          (개장 판단,      │  └ PnlController (GET /pnl)
                     │           6h 캐시)       │
                     │                          │
                 StrategyEngine ────────────────┘
                 (전략별 스케줄 루프)
                     │      ▲
                     │      │ TradeTick (ON_TRADE 전략만)
                     │   MarketStream (0.8.0, 브로커가 StreamingBrokerClient 일 때)
                     │   (웹소켓 체결가 · 재연결 · 구독 복원)
              ┌──────┴──────┐
              ▼             ▼
        OrderExecutor   BracketMonitor
        (Signal→주문)    (소프트웨어 익절/손절)
```

- 모든 빈은 `@ConditionalOnMissingBean` — 앱이 같은 타입의 빈을 정의하면 코어 구현을 교체할 수 있다
- `StrategyEngine` 은 `hermetix.engine.enabled=false` 로 끌 수 있다 (API 클라이언트만 쓰는 용도)

## 인증 흐름 (TokenManager)

```
getToken()
 ├─ 캐시 토큰이 있고 만료까지 margin(기본 60초) 이상 남음 → 그대로 반환
 └─ 아니면 @Synchronized refresh()
      └─ POST /v1/oauth/token (client_credentials) → 캐시 갱신
```

- `NextApiClient` 는 모든 인증 호출을 `executeWithRetry` 로 감싼다: 401(또는 `type=authentication`) 응답이면 `invalidate()` 후 **정확히 1회** 재발급-재시도. 재시도도 실패하면 예외 전파
- 토큰 유효기간은 공개 스펙 v1.3 기준 12시간(`expires_in=43200`), refresh 토큰 없음. 어댑터는 응답의 `expires_in` 을 그대로 신뢰한다
- 토큰 발급 API 만 에러 형식이 다르다 — 400/401 은 OAuth 표준 `{error, error_description}`, 429/5xx 는 플랫폼 엔벨로프. `TokenManager` 가 둘 다 `AuthError` 로 변환한다

### 넥스트증권 공통 헤더 (공개 스펙 v1.3)

| 헤더 | 대상 | 규칙 |
|---|---|---|
| `Authorization: Bearer {token}` | 전 API | 12h 토큰 |
| `X-Request-Id` | 토큰 발급 외 **전 고객 API 필수** | 영숫자·`.`·`_`·`-` 만, 64자 이하. 누락 시 400 `request-id-required`. 어댑터가 호출마다 `hmx-{uuid}` 를 생성하며, 에러 엔벨로프의 `requestId` 로 되돌아온다 |
| `X-Next-Account-Id` | 계좌·자산·주문 API 필수 (`GET /v1/account` 포함) | v1.1 의 `X-Nextsecurities-Account` 에서 개명. 토큰의 계좌와 불일치 시 403 `account-mismatch` |

## 엔진 틱 파이프라인 (StrategyEngine.tick)

엔진은 기동 시 모든 `TradingStrategy` 빈을 찾아 각자의 `pollInterval` 로 `scheduleWithFixedDelay` 등록한다. 스케줄러는 **poolSize 1** — 전략이 여러 개여도 틱은 순차 실행된다 (같은 계좌를 공유하므로 동시 주문 경합을 원천 차단).

```
tick(strategy):
 1. TradingGuard.isHalted → 즉시 반환
 2. spec.regularHoursOnly && !MarketCalendarService.isRegularOpen() → 반환
 3. buildContext(): quotes(1회) + candles(심볼당 1회) + account + holdings + orders + buyingPower
 4. BracketMonitor.check(context) → 청산 시그널이 있으면 먼저 실행   ← 전략보다 우선
 5. strategy.decide(context) → 반환된 시그널을 OrderExecutor 로 실행
 6. 성공 → guard.recordSuccess() / 예외 → guard.recordFailure() (임계치 도달 시 비상정지)
```

- 3단계의 API 호출 수 = `2 + 심볼 수 + 2` (quotes 는 다심볼 일괄). `candleLimit`/심볼 수가 틱 비용을 결정한다
- 예외는 틱 단위로 격리된다 — 한 틱이 실패해도 다음 틱은 정상 진행

## 실시간 계층 (MarketStream, 0.8.0)

폴링 위에 얹는 선택 계층이다. 전략 코드는 바뀌지 않고 `StrategySpec.trigger` 만 바꾼다.

```
브로커 웹소켓 ─▶ ReconnectingWebSocket ─▶ KisMarketStream / KiwoomMarketStream ─▶ TradeTick ─▶ StrategyEngine.requestTick
               (JDK java.net.http.WebSocket,  (프로토콜·프레임 파싱·구독 복원)          (latestTrades 갱신 → 스케줄러에 tick 등록)
                지수 백오프 재접속, 유휴 감시,
                직렬 전송, 부분 프레임 합침)
```

- **SPI**: `StreamingBrokerClient.openStream(): MarketStream`, `MarketStream.subscribeTrades(symbols, listener)`. 지원 채널은 `BrokerCapabilities.streams` 로 선언한다 — 엔진은 `is StreamingBrokerClient` 와 선언 둘 다 확인하고, 아니면 경고 후 폴링만 한다
- **트리거 합치기**: 틱은 스트림 스레드에서 오고 tick 은 엔진의 단일 스레드 스케줄러에서 돈다. 전략마다 "대기 중 플래그" 하나 — 대기 중이면 새 틱은 버려지고(마지막 가격은 `latestTrades` 에 남는다), tick 실행 도중 도착한 틱은 종료 후 `minTickInterval` 이 지나면 한 번 더 돌린다. 최소 간격은 실행 직전에 다시 확인한다
- **폴링 안전망**: ON_TRADE 전략도 `pollInterval` 고정 지연 스케줄은 그대로 유지한다. 스트림이 죽어도 전략은 계속 호출되고, 스트림은 뒤에서 재접속한다
- **현재가 절약**: 전략의 모든 심볼에 스트림 틱이 있으면 `quotes` REST 호출을 건너뛰고 `TradeTick.toQuote()` 를 쓴다. 캔들·계좌·보유·미체결·매수가능은 여전히 REST — 틱당 REST 호출 수는 `1 + 심볼 수 + 3`
- **채널**: 체결가(TRADES, 전략 트리거·현재가), 호가(ORDER_BOOK, `StrategySpec.orderBook=true` 전략의 `StrategyContext.orderBook()`), 주문통보(ORDER_EVENTS, 브로커가 제공하면 엔진이 자동 구독 → `StreamingBrokerClient.applyOrderEvent` 로 어댑터 추적 갱신 + `BracketMonitor.onOrderEvent` 로 진입 체결 즉시 활성화·취소 시 폐기). 호가는 틱을 촉발하지 않는다
- **브로커별 프로토콜** (2026-09-14 모의 장중 실측 — 체결가·호가. 주문통보는 문서 기반): KIS 는 `POST /oauth2/Approval` 접속키 → `ws://ops.koreainvestment.com:31000`(모의)/`:21000`(실전) → JSON 구독(`H0STCNT0`) → `0|TR|건수|필드^필드…` 프레임(체결가 폭 47, 한 프레임에 최대 3건 / 호가 `H0STASP0` 폭 63), 주문통보 `H0STCNI9`(모의)/`H0STCNI0`(실전)는 tr_key 가 HTS ID 이고 프레임이 AES-256-CBC 암호문(구독 응답 `output.key/iv` 로 복호화), `PINGPONG` 에코. 키움은 REST 토큰으로 `wss://mockapi.kiwoom.com:10000/api/dostk/websocket` LOGIN → `REG`(type `0B` 체결 / `0D` 호가 / `00` 주문체결 — 주문체결은 item 빈 문자열) → `{"trnm":"REAL"}` JSON, `PING` 에코. 프레임 샘플과 기대값은 `conformance/fixtures/{kis,kiwoom}.json#stream`
- **넥스트증권·KB증권**: 웹소켓이 없다 (넥스트 공개 스펙 v1.3, KB 개인 오픈베타 명세 2026-09) — `streams` 미선언, 폴링만
- **nh·db·ls·toss (문서 기반, 실측 전)**: 각 어댑터의 `*MarketStream` KDoc 에 프로토콜 근거와 미확정 항목을 적었다. 해당 증권사 계좌가 있는 사용자의 실측 제보로 승격한다

## 사용량 텔레메트리 (UsageTelemetry, 0.11.0)

브로커 어댑터의 공개 메서드는 `usage.measure("op") { … }` 로 감싸져 건수·에러 분류·응답 시간이 `UsageTelemetry` 의 시간 버킷(브로커·환경)에 쌓이고, 스트림은 구독·메시지·재접속 수를 더한다. 데몬 스레드가 60초마다 합산 JSON 을 `hermetix-service` 로 보내고 실패는 버린다 — 매매 경로와 완전히 분리되어 있고 예외가 호출자에게 새지 않는다. 무엇을 보내고 보내지 않는지는 [docs/telemetry.md](telemetry.md) 가 정본이며 네 언어 SDK 와 서버가 같은 계약을 따른다. 끄는 설정은 없다(기본 배포본 항상 켜짐).

## 주문 실행 (OrderExecutor)

| Signal | 처리 |
|---|---|
| `Buy` | `POST /v1/orders`. 성공 시 tp/sl 가격이 있으면 BracketMonitor 에 등록 |
| `Sell` | 수량을 `min(요청, 보유)` 로 클램프 (공매도 방지). 0 이면 스킵+경고 |
| `Cancel` | `DELETE /v1/orders/{id}` |

- 모든 주문의 `clientOrderId` = `{전략이름}-{uuid8}` → 서버가 24h 중복 제출을 거부 (엔진 재시도/중복 틱에 대한 안전망)
- 시그널 하나의 실패는 로그만 남기고 다음 시그널을 계속 실행한다 (부분 실패 허용)
- 비상정지 중에는 모든 시그널을 스킵한다

## 소프트웨어 브라켓 (BracketMonitor)

서버의 네이티브 BRACKET 주문(`POST /v2/orders/advanced`, 공개 스펙 v1.3 시점 `/v2` 로 제공)을 아직 연동하지 않아 코어가 소프트웨어로 대체한다:

```
등록: Buy 주문 접수 직후 {entryOrderId, symbol, qty, tp?, sl?} 저장 (메모리 Map)
매 틱 check():
 ├─ 미활성 브라켓: 진입 주문 상태 조회
 │    ├─ FILLED → 활성화
 │    └─ CANCELED/REJECTED/EXPIRED → 브라켓 폐기
 └─ 활성 브라켓: 현재가 vs tp/sl
      └─ 도달 → 브라켓 제거 후 시장가 Sell 시그널 생성 (수량은 보유로 클램프)
```

알려진 제약 (의도된 트레이드오프):
- **재시작 시 소실** — DB 없음 원칙의 대가. 전략 가이드에서 "재시작 후 포지션 수동 점검"을 권고
- 청산이 시장가라 급변동 시 슬리피지 존재
- 판정 주기가 엔진 틱 주기와 같으므로 틱 사이의 순간 스파이크는 놓칠 수 있다

네이티브 전환 로드맵: `OrderExecutor.buy()` 에서 tp/sl 존재 시 `/v2/orders/advanced` BRACKET 주문으로 보내는 fast-path 를 추가하고(`BrokerCapabilities.nativeBracket=true`), BracketMonitor 는 폴백으로 강등한다.

## 거래 환경과 실전 게이트 (0.6.0)

- `TradingEnvironment { PAPER, LIVE }` — 어댑터 설정(`hermetix.<broker>.environment`)으로 정한다. `BrokerClient.environment` 로 노출되고, `BrokerCapabilities.environments` 에 어댑터가 실측한 지원 환경을 선언한다
- 어댑터별 환경 처리: next 는 키 프리픽스(`pk_test_`/`pk_live_`)와 설정이 어긋나면 생성 실패. kis 는 호스트(openapivts:29443 / openapi:9443)·계좌 TR 프리픽스(V/T)·쓰로틀(600ms/100ms)을 바꾼다. kiwoom 은 호스트(mockapi / api)만 바뀐다
- `StrategyEngine.start()`: 브로커 환경이 capabilities 에 없거나, LIVE 인데 `hermetix.live.enabled=false` 면 **전략을 스케줄하지 않는다** (로그 에러, 예외 없음 — 앱은 뜨되 봇은 멈춘다). `scheduledStrategies` 로 결과를 확인할 수 있다
- 키는 항상 사용자 기기에서만 쓰인다. Hermetix 가 운영하는 서버로 키를 받는 구조는 한국 금융 라이선스 문제로 설계상 금지다

## 주문 금액 상한 (RiskGuard)

- `hermetix.risk.max-order-value`(1건) / `max-daily-order-value`(UTC 일 누적, 매수·매도 합산). 둘 다 없으면 비활성
- `OrderExecutor` 가 제출 직전에 `tryReserve(symbol, qty, price)` — price 는 지정가 ?: 현재가. 가격을 모르면 상한이 설정된 경우 거부. 허용 시 누적을 먼저 잡고 제출 실패해도 되돌리지 않는다(보수적)
- 누적치는 메모리 — 재시작 시 0 (상태 지도 참조)

## 시장 접두 심볼 (MarketSymbol)

- `MARKET:CODE` (`KRX:005930`, `US:AAPL`). 접두는 대문자 2~6자. 접두 없는 심볼은 `capabilities.market` 으로 해석 → 기존 전략 무변경
- 어댑터는 `capabilities.symbolCode(symbol)` 로 시장 지원 여부를 검증하고 코드만 API 에 보낸다. 시세/캔들은 요청받은 표기로 돌려주고, 보유/주문은 어댑터 표기(단일 시장이면 접두 없음)로 돌려준다
- `StrategyContext` 조회 메서드는 `MarketSymbol.matches` 로 접두 유무를 무시하고 코드로 맞춘다 (둘 다 시장을 명시했다면 시장도 같아야 한다)
- 다중 시장 브로커(NH·토스 등)는 `capabilities.markets` 에 여러 시장을 선언하고 보유/주문 심볼을 접두 포함으로 돌려준다

## 비상정지 (TradingGuard)

서버 킬 스위치(`/v2/kill-switch`, v1.3 시점 `/v2` 로 제공)를 아직 연동하지 않아 클라이언트 측에서 같은 효과를 낸다:

- 틱 연속 실패가 `hermetix.engine.max-consecutive-failures`(기본 5) 도달 → `halt()`
- `halt()`: 미체결 전량 개별 취소 + `halted=true` (이후 모든 주문 차단, 틱 스킵)
- 해제: `TradingGuard.resume()` 호출 또는 앱 재시작. **자동 해제는 없다** — 사람이 원인을 보게 만드는 것이 의도
- 연동 로드맵: `halt()` 에서 `POST /v2/kill-switch` 를 함께 호출해 서버 측에서도 주문을 차단(423 `trading-halted`)하도록 확장 예정

## 상태 지도 — 무엇이 어디에 있는가

| 상태 | 위치 | 재시작 시 |
|---|---|---|
| 보유 포지션, 미체결 주문, 체결 내역, 평균단가 | **서버** | 유지 (API 로 재조회) |
| 액세스 토큰 | 메모리 (TokenManager) | 재발급 (자동) |
| 웹소켓 구독 목록, 마지막 체결 틱 | 메모리 (MarketStream / StrategyEngine.latestTrades) | 재구독 (자동) — 재접속 전 틱은 폴링이 메운다 |
| 시장 캘린더 | 메모리 캐시 (6h TTL) | 재조회 (자동) |
| 브라켓 (익절/손절 예약) | 메모리 (BracketMonitor) | **소실** |
| 비상정지 플래그, 연속 실패 카운터 | 메모리 (TradingGuard) | 초기화 (정지 해제됨) |
| 일일 누적 주문 금액 | 메모리 (RiskGuard) | **초기화** — 재시작 직후 하루 상한이 다시 열린다 |
| 전략 내부 상태 (진입 시각, 감쇠 카운터 등) | 메모리 (전략 필드) | **소실** — 전략이 서버 상태로 복원하는 패턴 권장 |

## 에러 처리 계층 (0.5.0+)

어댑터는 브로커별 에러를 타입화된 계층으로 매핑하고, 엔진은 타입별로 반응한다:

```
BrokerApiException (기반)
 |- AuthError               -> 어댑터가 토큰 재발급 후 재시도 (넥스트), 소진 시 틱 실패
 |- RateLimitError          -> 어댑터가 백오프 재시도, 소진 시 틱 스킵 (비상정지 카운트 제외)
 |- MarketClosedError       -> 틱 조용히 스킵 (비상정지 카운트 제외 - KRX 합성 캘린더의 공휴일 케이스 포함)
 |- InsufficientFundsError  -> 해당 시그널만 스킵
 |- InvalidOrderError       -> 시그널 실패 로그
 |- OrderNotFoundError      -> 호출부 판단
```

## 브로커 어댑터

`BrokerClient` 구현체는 `hermetix.broker` 값으로 선택된다 (@ConditionalOnProperty). 실측 기반 어댑터별 특성:

| | next | kis | kiwoom | nh ⚠️ | db ⚠️ | ls ⚠️ | toss ⚠️ 실전 전용 | kb ⚠️ 실전 전용 |
|---|---|---|---|---|---|---|---|---|
| 인증 | OAuth client_credentials, 토큰 12h (v1.3) | appkey/appsecret → 토큰 24h (발급 1회/분 제한) | appkey/secretkey → 토큰 (expires_dt) | appkey/secret → 토큰 24h (운영 호스트 전용 발급, 쿼리스트링) | appkey/secret → 토큰 24h (form, 발급 1분 1건) | appkey/secret → 토큰 (form, 익일 07시 만료) | client_id/secret → 토큰 24h (client 당 유효 토큰 1개, 재발급 시 이전 토큰 무효) | appKey/appSecret → 토큰 (dataHeader/dataBody 봉투) |
| 실시간 | 없음 (스펙 미제공) | 웹소켓 체결가 `H0STCNT0`·호가 `H0STASP0` ✅ 모의 실측 (2026-09), 주문통보 `H0STCNI9/0` ⚠️ 문서 기반(HTS ID·AES) | 웹소켓 체결 `0B`·호가 `0D` ✅ 모의 실측 (2026-09), 주문체결 `00` ⚠️ 문서 기반 | ⚠️ 문서 기반: 체결 `oc/nc/mc`·호가 `ob/nb/mb`(marketCd 별)·통보 `d2/d3`, 토큰은 메시지 헤더, 운영 7070·모의 17070(모의 시세는 "미제공" 표기) | ⚠️ 문서 기반: `S00` 체결·`S01` 호가(`tr_key` "J 005930")·`IS0/IS1` 통보, 접속 10초 내 첫 전송, 7070/17070 | ⚠️ 문서 기반: `S3_/K3_` 체결·`H1_/HA_` 호가(KOSPI·KOSDAQ 둘 다 구독)·`SC0~SC4` 통보, 9443/29443 | ⚠️ AsyncAPI 1.2.2 기반: 단일 배열 선언형 구독 `trade/orderbook:{kr,us}`·`personal:order`, Bearer 핸드셰이크, 60초 `PING` | 없음 — 2026-09 명세 95개 전부 REST, 실시간 언급 없음 |
| 레이트리밋 | 그룹별 초당 제한, 429 + `Retry-After` (어댑터 자동 재시도 없음 — 엔진이 다음 틱까지 대기) | 초당 제한 → 600ms 쓰로틀 + EGW00201 재시도 | TR당 초당 1회 → 1100ms 쓰로틀 + 재시도 | 초당 5회 → 250ms 쓰로틀 + 429 재시도 | 앱 20 TPS·잔고 2·예수금 1 TPS → 500ms 쓰로틀 + IGW00201 지수 백오프 | TR 별 1~10 TPS → 전역 500ms + 차트(t8410) 전용 1100ms 쓰로틀 | 429 + `Retry-After` → 200ms 쓰로틀 + 지수 백오프 | 5초당 200건(추정) → 100ms 쓰로틀 + 재시도 |
| 캔들 | 1m/1d (v1.3) | 1d (분봉 API 가 당일 한정이라 미지원) | 1d | 1d (currentDaily) | 1d (kr-chart/day) | 1d (t8410) | 1m/1d (`/candles`, 최대 200) | 1d (ivs11560, 시장구분 설정 필요) |
| 캘린더 | 서버 제공 (미국장) | KRX 합성 (공휴일 미반영) | KRX 합성 | KRX 합성 | KRX 합성 | KRX 합성 | KRX 합성 (미국 종목도 KRX 캘린더로 틱) | KRX 합성 |
| clientOrderId | 지원 (24h 멱등) | 미지원 (무시) | 미지원 (무시) | 미지원 | 미지원 | 미지원 | 지원 (`clientOrderId`, 없으면 UUID 생성) | 미지원 |
| 미체결 조회 | 서버 제공 | **서버 미제공 → 어댑터 메모리 추적** (체결은 보유수량 변화로 근사, 재시작 시 추적 소실) | 서버 제공 (ka10075) | dailyOrderExecution (당일, ny_cns_qty>0) | transaction-history (당일, MrcAbleQty>0) | t0425 (당일, ordrem>0) | `/orders?status=OPEN` (서버 제공) | ssqm2341 (당일, nccls_q>0) |
| 주문취소 | orderId 만으로 가능 | ODNO 단독 (지점번호 불필요 - 실측) | 미체결 조회로 종목코드 역참조 | org_mkt_orr_no + iem_cd (조회로 역참조) | OrgOrdNo + IsuNo + 잔량 (조회로 역참조) | CSPAT00801 OrgOrdNo + A접두 IsuNo + 잔량 | `/orders/{id}/cancel` — **새 orderId 발급**, 어댑터는 원주문 ID 로 PENDING_CANCEL 반환 | ssam1806 orgn_ordr_no(10자리 패딩) + 잔량 |
| 수량/금액 표기 | JSON 문자열 | 문자열 | 부호 접두(가격) / zero-padded(금액) — 어댑터가 정규화 | 숫자/문자 혼재 — 양쪽 허용 파서, iem_cd 12자리→6자리 정규화 | 문자열 추정 — 양쪽 허용 파서, IsuNo A접두 제거 | 숫자 추정 — 양쪽 허용 파서, expcode A접두 제거 | JSON 문자열, 등락·거래량 없음(null/0) | zero-padded 문자열 → trim, ISIN(KR7…)→6자리 |
| 응답 정규화 | v1.3 원시 응답(quotes `outcome`, 캔들 `time`, `cashAmount`, `averageBuyPrice`, 캘린더 `status`+`sessions[]`)을 공통 모델로 변환. 등락률·손익률 %→비율, KST 세션 시각→뉴욕 현지 HH:mm, 총평가=예수금+보유 평가금액(보유 조회 1회 추가) | KIS 응답 → 공통 모델 | 키움 응답 → 공통 모델 | Input_0/Output_n 봉투, HTTP 200 + rsp_cd 업무오류 | In/Out 봉투, HTTP 200 + rsp_cd 업무오류 | `<TR>InBlock`/`OutBlock` 봉투 + tr_cd 헤더, HTTP 200 + rsp_cd 업무오류 | `{result}` / `{error}` 봉투, 보유·주문 심볼 `KRX:`/`US:` 접두, 예수금 없음(KRW 매수가능금액), 체결은 종료 주문 execution 집계 | dataHeader/dataBody 봉투, processFlag != A 가 업무오류 |

⚠️ nh·db·ls·toss·kb 는 공식 SDK/문서 기반 구현으로 실측 전이다 — 각 클래스 KDoc 의 "문서로 확정하지 못한 점" 을 실측으로 확인해야 한다. toss·kb 는 모의투자 환경이 없어(`environments = {LIVE}`) 실계좌로만 검증할 수 있으므로 엔진의 실전 게이트(`hermetix.live.enabled`)와 `RiskGuard` 상한이 필수다.

새 어댑터 추가 절차: ① 모의서버 실측(토큰/시세/캔들/잔고/주문/에러 포맷)으로 `conformance/fixtures/<broker>.json` 작성 ② `BrokerClient` 구현 ③ **컨포먼스 킷 통과** (`BrokerConformance.verify`, 네 언어 공통 시나리오 — [conformance/README.md](../conformance/README.md)) ④ 오토컨피그에 @ConditionalOnProperty 등록 ⑤ env-gated 실서버 스모크 테스트.

### 레이트리밋 공용 부품 (RateLimiter)

어댑터마다 복붙하던 쓰로틀/백오프를 `RateLimiter(minIntervalMillis, maxRetries, backoffMillis)` 하나로 통합했다. `execute { }` 가 호출 간 최소 간격을 보장하고 `RateLimitError` 면 백오프 후 재시도한다. 서버가 `Retry-After` 를 주면(`RateLimitError.retryAfterSeconds`) 그 값을 30초 상한 내에서 따른다. 재시도가 소진되면 마지막 예외를 그대로 던지고, 엔진은 그때서야 틱을 건너뛴다.

| 어댑터 | 최소 간격 | 재시도 | 백오프 |
|---|---|---|---|
| next | 없음 | 2회 | `Retry-After` 또는 1s×n |
| kis | 모의 600ms / 실전 100ms | 3회 | 1s×n (EGW00201) |
| kiwoom | 1100ms (TR당 초당 1회) | 3회 | 1.1s×n |

Python `RateLimiter`, JS `RateLimiter`, Go `rateLimiter` 가 같은 의미다.

## 버전/호환 정책

- SPI(`TradingStrategy`/`StrategyContext`/`Signal`) 변경 = breaking → minor 버전 상승 (0.x 에서는 0.N+1.0)
- JitPack 이 git 태그를 빌드하므로 **태그 = 릴리즈**. 태그를 옮겨 달지 않는다 (JitPack 은 한 번 빌드한 버전을 캐시)
- Gradle Wrapper 는 반드시 커밋에 포함한다 (없으면 JitPack 이 구버전 Gradle 로 빌드 실패)

## 알려진 한계 요약

- 브라켓/전략 상태의 메모리 휘발성 (위 상태 지도 참조)
- 실시간 실측은 kis·kiwoom 체결가·호가뿐 — 그 둘의 주문통보와 nh·db·ls·toss 전 채널은 문서 기반(실측 전). next·kb 는 웹소켓이 없어 폴링 스냅샷이라 틱 사이의 변화를 못 본다
- 정규장 판정은 캘린더 API 기준 — 프리/애프터마켓 주문은 `regularHoursOnly=false` 로 가능하나 체결 규칙은 서버 정책을 따른다
- 단일 계좌 전제 — 전략 여러 개가 같은 심볼을 다루면 보유/미체결 판단이 겹친다 (전략 가이드에서 금지 권고)
