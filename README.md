<div align="center">

# Hermetix

**국내 증권사 오픈 API, 하나의 인터페이스로 — 모의투자로 검증하고, 설정 한 줄로 실전까지**

ccxt 가 거래소에 하는 일을 증권사에 합니다. 증권사는 ID 한 토큰으로 고르고, 전략은 한 번만 씁니다. 키는 언제나 당신의 기기에서만 쓰입니다.

[![JitPack](https://jitpack.io/v/tripleauth-dev/hermetix-securities.svg)](https://jitpack.io/#tripleauth-dev/hermetix-securities)
[![npm](https://img.shields.io/npm/v/hermetix.svg)](https://www.npmjs.com/package/hermetix)
[![PyPI](https://img.shields.io/pypi/v/hermetix.svg)](https://pypi.org/project/hermetix/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

[홈](https://hermetix-api-prod.tripleauth.com/) · [사용량 랭킹](https://hermetix-api-prod.tripleauth.com/rankings.html) · [API 현황](https://hermetix-api-prod.tripleauth.com/status.html) · [전략 작성 가이드](docs/strategy-guide.md) · [아키텍처](docs/architecture.md) · [로드맵](ROADMAP.md)

</div>

---

```python
import hermetix

client = hermetix.next(api_key="pk_test_…", api_secret="sk_test_…", account="acc_main")
quote = client.get_quotes(["AAPL"])[0]          # 같은 모양의 Quote — 어느 증권사든
client.get_candles("AAPL", CandleInterval.DAY_1, limit=30)
client.get_holdings()
```

`next` 를 `kis` 로 바꾸면 한국투자증권입니다. Kotlin·Python·JavaScript/TypeScript·Go 네 언어가 같은 규약을 씁니다 ([빠른 시작](#빠른-시작--연결-계층)).

## 지원 증권사

| | ID | 증권사 | 시장 | 캔들 | 실시간 | 모의 | 실전 | 상태 |
|:---:|---|---|---|---|:---:|:---:|:---:|:---:|
| <img src="https://www.google.com/s2/favicons?domain=nextsecurities.com&sz=64" width="28"/> | `next` | [넥스트증권](https://docs.nextsecurities.dev/) | 미국주식 | 1m · 1d | ❌ 스펙 없음 | ✅ | ✅ 키 프리픽스로 구분 | ✅ 검증 (공개 스펙 v1.3) |
| <img src="https://www.google.com/s2/favicons?domain=koreainvestment.com&sz=64" width="28"/> | `kis` | [한국투자증권](https://apiportal.koreainvestment.com/) | KRX 국내주식 | 1d | ✅ 체결가·호가 · ⚠️ 주문통보 | ✅ | ✅ 호스트·TR 자동 전환 | ✅ 검증 (모의) |
| <img src="https://www.google.com/s2/favicons?domain=kiwoom.com&sz=64" width="28"/> | `kiwoom` | [키움증권](https://openapi.kiwoom.com/) | KRX 국내주식 | 1d | ✅ 체결가·호가 · ⚠️ 주문통보 | ✅ | ✅ 호스트 자동 전환 | ✅ 검증 (모의) |
| <img src="https://www.google.com/s2/favicons?domain=nhqv.com&sz=64" width="28"/> | `nh` | [NH투자증권 NH PLUG](https://www.nhplug.com/) | KRX 국내주식 | 1d | ⚠️ 체결가·호가·주문통보 | ✅ 호스트 분리 | ✅ | ⚠️ 미검증 (문서 기반) |
| <img src="https://www.google.com/s2/favicons?domain=ls-sec.co.kr&sz=64" width="28"/> | `ls` | [LS증권](https://openapi.ls-sec.co.kr/) | KRX 국내주식 | 1d | ⚠️ 체결가·호가·주문통보 | ✅ 키로 구분 | ✅ | ⚠️ 미검증 (문서 기반) |
| <img src="https://www.google.com/s2/favicons?domain=dbsec.co.kr&sz=64" width="28"/> | `db` | [DB증권](https://openapi.dbsec.co.kr/) | KRX 국내주식 | 1d | ⚠️ 체결가·호가·주문통보 | ✅ 키로 구분 | ✅ | ⚠️ 미검증 (문서 기반) |
| <img src="https://www.google.com/s2/favicons?domain=tossinvest.com&sz=64" width="28"/> | `toss` | [토스증권](https://openapi.tossinvest.com/) | KRX · 미국주식 | 1m · 1d | ⚠️ 체결가·호가·주문통보 | ❌ 샌드박스 없음 | ✅ | ⚠️ 미검증 (실전 전용) |
| <img src="https://www.google.com/s2/favicons?domain=kbsec.com&sz=64" width="28"/> | `kb` | [KB증권](https://openapi.kbsec.com/) | KRX 국내주식 | 1d | ❌ 스펙 없음 (REST 전용) | ❌ "추후 제공" | ✅ 오픈베타 | ⚠️ 미검증 (실전 전용) |

✅ 검증 = 실서버 스모크(시세→캔들→계좌→주문)를 통과. ⚠️ 미검증 = 공식 문서·SDK 로 만든 어댑터로, 모의계좌 실측 전. ⚠️ 실전 전용 = 모의 환경이 없어 실계좌로만 쓸 수 있는 증권사 — 주문 금액 상한과 소액으로 시작하세요. 실시간 ✅ 는 모의 웹소켓 장중 실측, ⚠️ 는 구현은 됐지만 실측 전, ❌ 는 스펙에 웹소켓이 없어 폴링만 동작.

증권사별 설정·프로토콜·제약과 검증 상태의 자세한 뜻은 [증권사별 설정과 제약](docs/brokers.md)에, 각 증권사 API 가 실제로 어떻게 쓰이고 있는지는 [사용량 랭킹](https://hermetix-api-prod.tripleauth.com/rankings.html)과 [API 현황](https://hermetix-api-prod.tripleauth.com/status.html)에 있습니다. 2026-09 기준 REST 오픈 API 가 있는 국내 증권사는 모두 붙였습니다 ([조사 보고서](claudedocs/korean-broker-openapi-survey-2026-09.md)). 새 증권사는 [요청 이슈](../../issues/new?template=broker-request.md)로 알려주세요.

## 왜 Hermetix 인가

- **증권사 독립** — 같은 코드가 넥스트(미국)와 한국투자·키움·NH·LS·DB·토스·KB(KRX)에서 그대로 돕니다. 응답의 방언(부호 접두, zero-padding, TR-ID 체계)은 어댑터가 정규화합니다
- **네 언어, 같은 규약** — Kotlin 레퍼런스와 Python·JS·Go 네이티브 구현이 같은 골든 픽스처의 [컨포먼스 시나리오](conformance/README.md)를 통과합니다
- **전략 = 클래스 하나** — 인증, 시세·계좌 조회, 주문 실행, 체결 추적, 익절·손절 브라켓은 엔진이 처리합니다
- **안전 우선** — 매도 수량 자동 클램프(공매도 방지), 주문 멱등키, 연속 실패 시 비상정지, 주문 금액 상한, KRX 호가단위 자동 보정
- **모의 → 실전은 설정 한 줄** — `environment: live` 와 명시 동의(`hermetix.live.enabled`)가 있어야만 실전 주문이 나갑니다
- **키는 로컬에만** — 클라이언트 라이브러리입니다. 키와 주문은 당신의 기기에서 증권사로 직접 갑니다. 중간 서버가 없습니다

## 설치

| 언어 | 버전 | 의존성 | 설치 |
|---|---|---|---|
| [Python](python/README.md) | 0.11.1 | 0개 (stdlib, 3.10+) · 실시간은 `websockets` 선택 | `pip install hermetix` (실시간: `pip install 'hermetix[stream]'`) |
| [JavaScript/TypeScript](js/README.md) | 0.11.1 | decimal.js (Node 22+) | `npm install hermetix` |
| [Go](go/README.md) | 0.11.1 | shopspring/decimal · 웹소켓 1개 | `go get github.com/tripleauth-dev/hermetix-securities/go@v0.11.1` |
| Kotlin/JVM (레퍼런스) | 0.11.1 | Spring Boot | JitPack — 아래 |

```kotlin
// settings.gradle.kts
repositories { mavenCentral(); maven("https://jitpack.io") }

// build.gradle.kts
dependencies {
    implementation("com.github.tripleauth-dev.hermetix-securities:hermetix-engine:0.11.1")   // 전략 봇 (연결 계층 포함)
    // implementation("com.github.tripleauth-dev.hermetix-securities:hermetix-broker:0.11.1") // 연결 계층만
}
```

## 빠른 시작 — 연결 계층

봇이 필요 없다면 연결 계층만으로 통일 API 를 씁니다. 증권사는 **ID 한 토큰**으로 고르고, 자격 증명 키(`api_key`·`api_secret`·`account`)는 어느 증권사든 같습니다 ([브로커 팩토리 규약](docs/broker-factory.md) — 증권사별 `account`·`extra` 표 포함):

```python
import hermetix
client = hermetix.kis(api_key="…", api_secret="…", account="12345678")   # cano
quote = client.get_quotes(["005930"])[0]                                 # Quote: price, bid_price, ask_price, volume, change_rate
```
```ts
import hermetix from "hermetix";
const client = hermetix.kis({ apiKey: "…", apiSecret: "…", account: "12345678" });
const [quote] = await client.getQuotes(["005930"]);
```
```go
client, err := hermetix.Kis(hermetix.Credentials{APIKey: "…", APISecret: "…", Account: "12345678"})
quotes, _ := client.GetQuotes([]string{"005930"})
```
```kotlin
val client = Hermetix.kis(Credentials(apiKey = "…", apiSecret = "…", account = "12345678"))
client.getQuotes(listOf("005930")).quotes[0].price
```

증권사별 클래스(`KisClient`, `KisApiClient`, `NewKisClient` …)를 직접 만들어도 됩니다. 금액·수량은 모든 언어에서 Decimal 입니다. 웹소켓 스트림(`StreamingBrokerClient.openStream()`)도 연결 계층만으로 씁니다 — [실시간 스트림](#실시간-스트림).

## 빠른 시작 — 전략 봇

전략은 `spec`(감시 종목·캔들·호출 주기)과 `decide()`(시그널 반환) 두 가지만 채웁니다. 엔진이 정규장 중에만 호출하고, 시그널을 주문으로 바꾸고, 익절·손절과 비상정지를 대신합니다. Python·JS·Go 의 봇 예시는 각 언어 README 의 "5분 빠른 시작" 에 있습니다. 아래는 Kotlin(Spring Boot) 입니다.

> 처음이라면 [hermetix-strategy-template](https://github.com/tripleauth-dev/hermetix-strategy-template) 을 "Use this template" 으로 복제하는 게 가장 빠릅니다.

```yaml
# application.yml
hermetix:
  broker: next                   # next | kis | kiwoom | nh | ls | db | toss | kb — 이 한 줄로 증권사가 바뀝니다
  next:
    client-id: pk_test_...
    client-secret: sk_test_...
    account-id: acc_main
```

```kotlin
@Component
class MyStrategy : TradingStrategy {

    override val spec = StrategySpec(
        name = "my-first",
        symbols = listOf("AAPL"),
        candleInterval = CandleInterval.DAY_1,
        candleLimit = 20,
    )

    override fun decide(context: StrategyContext): List<Signal> {
        val price = context.quote("AAPL")?.price ?: return emptyList()
        val ma20 = context.candles("AAPL").map { it.close }
            .reduce(BigDecimal::add).divide(BigDecimal(20), 4, RoundingMode.HALF_EVEN)

        if (price > ma20 && !context.hasPosition("AAPL") && !context.hasOpenOrder("AAPL")) {
            return listOf(
                Signal.Buy(
                    symbol = "AAPL",
                    quantity = BigDecimal.ONE,
                    takeProfitPrice = price.multiply(BigDecimal("1.04")),  // 익절/손절은
                    stopLossPrice = price.multiply(BigDecimal("0.98")),    // 엔진이 자동 실행
                ),
            )
        }
        return emptyList()
    }
}
```

`@SpringBootApplication` 으로 실행하면 엔진이 전략 빈을 찾아 해당 시장의 정규장 시간에만 호출합니다. 증권사별 키 블록과 옵션(NH `market-cd`, KIS `hts-id`, KB `chart-market-clsf` …)은 [증권사별 설정과 제약](docs/brokers.md)에 있습니다.

## 실전투자로 전환

모의투자에서 검증한 봇을 실제 계좌로 옮길 때는 두 가지를 명시해야 합니다. 하나라도 빠지면 엔진이 기동을 거부합니다.

```yaml
hermetix:
  broker: kis
  kis:
    environment: live          # paper(기본) | live — 호스트·TR ID 가 자동으로 바뀝니다
    appkey: ${KIS_APPKEY:}
    appsecret: ${KIS_APPSECRET:}
    cano: ${KIS_CANO:}
  live:
    enabled: true              # 실전 명시 동의. 없으면 "LIVE 설정됨 - 기동하지 않음" 으로 멈춥니다
  risk:
    max-order-value: 1000000   # 주문 1건 상한 (증권사 통화). 넘는 시그널은 제출하지 않습니다
    max-daily-order-value: 5000000   # 하루(UTC) 누적 상한 — 매수·매도 합산
```

- 넥스트증권은 키 프리픽스가 환경을 결정합니다 (`pk_test_`=모의, `pk_live_`=실전). `environment` 와 키가 어긋나면 기동 시 실패합니다
- 주문 금액 상한은 모의투자에서도 설정하면 적용됩니다. 현재가를 알 수 없는 종목의 주문은 상한이 설정된 경우 거부됩니다
- 심볼에 시장 접두를 붙일 수 있습니다 (`KRX:005930`, `US:AAPL`). 접두 없는 심볼은 증권사 기본 시장으로 해석됩니다

## 동작 방식

```
전략 (TradingStrategy)          ← 당신이 작성하는 유일한 부분
    ↓ Signal (Buy/Sell/Cancel)
StrategyEngine                  ← 정규장 스케줄링, 컨텍스트 구성, 브라켓/비상정지
    ↓ BrokerClient 인터페이스
8개 증권사 어댑터               ← 인증, 레이트리밋, 방언 정규화, (웹소켓 스트림)
```

- 엔진은 `pollInterval` 주기로 전략을 호출합니다 — 해당 증권사 시장의 정규장에만 (next=미국장 ET, KRX 증권사=KST)
- 실시간이 필요한 전략은 폴링 대신 체결가 스트림으로 호출되고 호가창·주문통보도 받습니다 — [실시간 스트림](#실시간-스트림)
- 기동 시 검증: 전략의 캔들 주기를 증권사가 지원하는지, 환경(모의/실전)이 선언된 것인지, 실전이면 명시 동의가 있는지 — 하나라도 어긋나면 스케줄하지 않습니다
- `Signal.Sell` 은 보유 수량으로 자동 클램프됩니다 (공매도 방지). 주문 금액 상한을 넘는 시그널은 제출하지 않습니다
- 익절/손절(소프트웨어 브라켓)은 앱 메모리에서 관리됩니다 — 재시작 시 사라지므로 [전략 가이드](docs/strategy-guide.md)의 복원 패턴을 참고하세요
- 연속 실패가 임계치(기본 5회)에 도달하면 비상정지 — 미체결 전량 취소 후 주문 차단 (휴장·레이트리밋은 카운트 제외)
- 증권사별 요청 제한은 공용 `RateLimiter` 가 쓰로틀·백오프·`Retry-After` 로 흡수합니다. 에러는 `MarketClosedError`, `RateLimitError`, `InsufficientFundsError` 처럼 증권사와 무관하게 타입화돼 있습니다

## 실시간 스트림

증권사가 웹소켓을 제공하면 폴링 위에 세 채널(체결가 TRADES · 호가 ORDER_BOOK · 주문통보 ORDER_EVENTS)을 얹을 수 있습니다. 전략 코드는 그대로이고 `StrategySpec` 두 줄만 바뀝니다.

```kotlin
override val spec = StrategySpec(
    name = "scalp", symbols = listOf("005930"),
    trigger = TickTrigger.ON_TRADE,             // 체결가 틱마다 decide() 호출 (기본 POLL)
    minTickInterval = Duration.ofSeconds(1),    // 연속 호출 최소 간격 — 캔들·계좌 REST 폭주 방지
    orderBook = true,                           // context.orderBook("005930") 로 10단계 호가·잔량
)
```

- **체결가** — 틱이 몰리면 하나로 합치고, 스트림이 끊기면 `pollInterval` 폴링이 안전망으로 계속 돕니다
- **호가** — `orderBook = true` 전략에만 구독되며 틱을 촉발하지는 않습니다
- **주문통보** — 증권사가 제공하면 엔진이 자동 구독해 체결을 서버 조회 없이 브라켓에 반영합니다. 구독에 실패하면(예: KIS HTS ID 미설정) 경고만 남기고 폴링 판정을 유지합니다
- 스트림이 없는 증권사(next·kb)에서는 폴링으로 동작합니다

연결 계층만으로도 씁니다 (`StreamingBrokerClient`):

```kotlin
val stream = (client as StreamingBrokerClient).openStream()
stream.subscribeTrades(listOf("KRX:005930")) { tick -> println("${tick.symbol} ${tick.price} x${tick.quantity}") }
stream.subscribeOrderBook(listOf("005930")) { book -> println("ask1=${book.bestAsk} bid1=${book.bestBid}") }
stream.subscribeOrderEvents { event -> println("${event.type} ${event.orderId} ${event.quantity}@${event.price}") }
stream.connect()   // 끊기면 지수 백오프로 재접속하고 구독을 복원합니다
```

증권사별 채널 상태(✅ 실측 / ⚠️ 문서 기반 / ❌ 없음)와 프로토콜·제약(세션 수, 구독 상한, 토큰 만료 등)은 [증권사별 설정과 제약](docs/brokers.md#실시간-스트림--증권사별-상태와-프로토콜)에 있습니다.

## 사용량 데이터

SDK 는 **어느 증권사의 어떤 기능이 얼마나 쓰이는지**를 합산해 보내고, 그 데이터로 [사용량 랭킹](https://hermetix-api-prod.tripleauth.com/rankings.html)과 [API 현황](https://hermetix-api-prod.tripleauth.com/status.html)(증권사별 에러율·응답 시간)을 공개합니다. 기본 배포본은 항상 켜져 있고 끄는 설정은 없습니다. 대신 무엇을 보내는지 그대로 공개합니다 (정본: [docs/telemetry.md](docs/telemetry.md)).

| 보내는 것 | 보내지 않는 것 |
|---|---|
| 증권사 ID, 환경(모의/실전) | 종목·수량·가격·금액 |
| 호출 종류별 성공/에러 건수 (시간 단위 합계) | 주문번호·체결번호·계좌번호·고객 ID |
| 에러 분류(레이트리밋·인증·휴장 등), 응답 시간 p50/p95 | API 키·토큰·HTS ID |
| 실시간 채널별 구독 수·메시지 수·재접속 수 | 전략 이름·설정값 |
| SDK 언어·버전, 설치 단위 무작위 ID | IP 주소 (서버가 저장하지 않음), OS·호스트명 |

개별 호출은 보내지 않고 합산한 건수만 60초마다 한 번, 매매 경로와 분리된 백그라운드 스레드로 보냅니다. 전송 실패는 조용히 버려 주문이 늦어지거나 실패하는 일은 없습니다. 새 의존성 없이 각 언어의 내장 HTTP 클라이언트만 씁니다.

## 수익률 확인

봇을 띄우면 `GET /pnl`(총평가/현금/평가손익/종목별 손익 JSON)과 주기 PnL 로그가 자동 제공됩니다.

```yaml
hermetix:
  pnl:
    log-interval-minutes: 60
    initial-capital: 20000     # 설정하면 총수익률(return=%)도 계산
```

## 공식 전략

| 레포 | 전략 | 특징 |
|---|---|---|
| [hermetix-strategy-template](https://github.com/tripleauth-dev/hermetix-strategy-template) | 이동평균 예제 | **여기서 시작하세요** |
| [hermetix-larry-strategy](https://github.com/tripleauth-dev/hermetix-larry-strategy) | 변동성 돌파 | 캔들 분석 + 손절 브라켓 |
| [hermetix-trend-breakout-strategy](https://github.com/tripleauth-dev/hermetix-trend-breakout-strategy) | WMA 추세선 돌파 | 지표 계산 + 익절/손절 브라켓 |
| [hermetix-grid-strategy](https://github.com/tripleauth-dev/hermetix-grid-strategy) | 목표가 스캘핑 | 지정가/취소 컨트롤, KRX 호환 |
| [hermetix-dca-strategy](https://github.com/tripleauth-dev/hermetix-dca-strategy) | 무한매수법 분할 매수 | 거래일당 1회분, 목표 수익률 청산 |
| [hermetix-turtle-strategy](https://github.com/tripleauth-dev/hermetix-turtle-strategy) | 터틀 트레이딩 | Donchian 채널 돌파, 롱 온리 |

직접 만든 전략을 공유하려면 [전략 공유 이슈](../../issues/new?template=strategy-share.md)를 올려주세요.

<!-- 커뮤니티 전략 목록 -->

## 릴리즈

| 버전 | 내용 |
|---|---|
| 0.11.1 | 저장소 이동 — `tripleauth-dev/hermetix-securities`. Go 모듈 경로·JitPack 좌표가 새 이름으로 바뀌어 새 태그 필요 |
| 0.11.0 | **브로커 팩토리** `hermetix.next(...)` — 증권사 ID 한 토큰과 통일된 자격 증명으로 생성 ([규약](docs/broker-factory.md)). 사용량 텔레메트리 수신 엔드포인트 확정 (`https://hermetix-api-prod.tripleauth.com/v1/usage`) |
| 0.10.0 | nh·db·ls·toss 실시간 스트림(문서 기반), KB 는 웹소켓 없음 확정. **네 언어 한 줄 설치** — PyPI·npm 등록, Go `go/v0.10.0` 태그 |
| 0.9.0 | 호가·주문통보 채널, `StrategySpec.orderBook`, 브라켓·KIS 추적에 통보 반영 |
| 0.8.0 | 실시간 계층 1차 — `MarketStream` SPI, KIS·키움 체결가(모의 실측), `TickTrigger.ON_TRADE` |
| 0.7.0 | LS·토스·KB 어댑터 (문서 기반) |
| 0.6.0 | 거래 환경 paper/live, 실전 게이트, 주문 금액 상한, `MARKET:CODE` 심볼 |

태그 = 릴리즈(JitPack). Python·JS 는 같은 번호로 PyPI·npm 에, Go 는 `go/vX.Y.Z` 태그로 올라갑니다. 릴리즈 절차는 수동이며 CI 는 두지 않습니다.

## 문서

- 언어별 사용설명서 — [Python](python/README.md) · [JavaScript/TypeScript](js/README.md) · [Go](go/README.md)
- [브로커 팩토리 규약](docs/broker-factory.md) — `hermetix.next(...)` 생성 규약, 통일된 자격 증명 키와 증권사별 `extra`
- [증권사별 설정과 제약](docs/brokers.md) — 검증 상태의 뜻, 엔진 설정 YAML, 실시간 프로토콜과 제약
- [전략 작성 가이드](docs/strategy-guide.md) — SPI 레퍼런스(Kotlin), 패턴, 안전장치, 테스트, 트러블슈팅
- [아키텍처](docs/architecture.md) — 모듈 구조, 틱 파이프라인, 거래 환경·RiskGuard·심볼 규약, 어댑터 비교표
- [사용량 텔레메트리 계약](docs/telemetry.md) — 보내는 것/보내지 않는 것, 페이로드, 서명, 서버 처리
- [컨포먼스 킷](conformance/README.md) — 새 어댑터 검증 시나리오와 네 언어 공용 골든 픽스처
- [국내 증권사 오픈 API 조사 (2026-09)](claudedocs/korean-broker-openapi-survey-2026-09.md) — 어댑터 우선순위 근거
- [로드맵](ROADMAP.md) · [기여 가이드](CONTRIBUTING.md)

## 라이선스

[MIT](LICENSE)

> **면책**: 이 프로젝트는 학습·연구용 소프트웨어이며 투자 조언이 아닙니다. 실전투자(`environment: live`)로 발생하는 모든 손실의 책임은 사용자에게 있습니다 — 모의투자에서 충분히 검증하고, 주문 금액 상한을 설정한 뒤 소액으로 시작하세요. Hermetix 는 어떤 서버로도 당신의 API 키를 받지 않습니다. 각 증권사 로고는 해당 회사의 자산이며, 지원 서비스를 표시하기 위해서만 사용됩니다.
