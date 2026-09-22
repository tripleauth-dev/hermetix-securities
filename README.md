<div align="center">

# Hermetix

**국내 증권사 오픈 API, 하나의 인터페이스로**

증권사는 ID 한 토큰으로 고르고, 전략은 한 번만 씁니다. 모의투자로 검증하고 설정 한 줄로 실전에 갑니다.

[![JitPack](https://jitpack.io/v/tripleauth-dev/hermetix-securities.svg)](https://jitpack.io/#tripleauth-dev/hermetix-securities)
[![npm](https://img.shields.io/npm/v/hermetix.svg)](https://www.npmjs.com/package/hermetix)
[![PyPI](https://img.shields.io/pypi/v/hermetix.svg)](https://pypi.org/project/hermetix/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

[홈](https://hermetix-api-prod.tripleauth.com/) · [사용량 랭킹](https://hermetix-api-prod.tripleauth.com/rankings.html) · [API 현황](https://hermetix-api-prod.tripleauth.com/status.html) · [전략 작성 가이드](docs/strategy-guide.md) · [아키텍처](docs/architecture.md)

</div>

---

```python
import hermetix
from hermetix import CandleInterval

client = hermetix.next(api_key="pk_test_…", api_secret="sk_test_…", account="acc_main")

quote = client.get_quotes(["AAPL"])[0]
candles = client.get_candles("AAPL", CandleInterval.DAY_1, limit=30)
holdings = client.get_holdings()
```

`next` 를 `kis` 로 바꾸면 한국투자증권입니다. 응답 모델은 어느 증권사든 같습니다. Kotlin, Python, JavaScript/TypeScript, Go 네 언어가 같은 규약을 씁니다.

## 지원 증권사

| | ID | 증권사 | 시장 | 실시간 | 모의 | 실전 | 상태 |
|:---:|---|---|---|:---:|:---:|:---:|:---:|
| <img src="https://www.google.com/s2/favicons?domain=nextsecurities.com&sz=64" width="24"/> | `next` | [넥스트증권](https://docs.nextsecurities.dev/) | 미국주식 | ❌ | ✅ | ✅ | ✅ 검증 |
| <img src="https://www.google.com/s2/favicons?domain=koreainvestment.com&sz=64" width="24"/> | `kis` | [한국투자증권](https://apiportal.koreainvestment.com/) | KRX | ✅ | ✅ | ✅ | ✅ 검증 |
| <img src="https://www.google.com/s2/favicons?domain=kiwoom.com&sz=64" width="24"/> | `kiwoom` | [키움증권](https://openapi.kiwoom.com/) | KRX | ✅ | ✅ | ✅ | ✅ 검증 |
| <img src="https://www.google.com/s2/favicons?domain=nhqv.com&sz=64" width="24"/> | `nh` | [NH투자증권](https://www.nhplug.com/) | KRX | ⚠️ | ✅ | ✅ | ⚠️ 미검증 |
| <img src="https://www.google.com/s2/favicons?domain=ls-sec.co.kr&sz=64" width="24"/> | `ls` | [LS증권](https://openapi.ls-sec.co.kr/) | KRX | ⚠️ | ✅ | ✅ | ⚠️ 미검증 |
| <img src="https://www.google.com/s2/favicons?domain=dbsec.co.kr&sz=64" width="24"/> | `db` | [DB증권](https://openapi.dbsec.co.kr/) | KRX | ⚠️ | ✅ | ✅ | ⚠️ 미검증 |
| <img src="https://www.google.com/s2/favicons?domain=tossinvest.com&sz=64" width="24"/> | `toss` | [토스증권](https://openapi.tossinvest.com/) | KRX · 미국 | ⚠️ | ❌ | ✅ | ⚠️ 실전 전용 |
| <img src="https://www.google.com/s2/favicons?domain=kbsec.com&sz=64" width="24"/> | `kb` | [KB증권](https://openapi.kbsec.com/) | KRX | ❌ | ❌ | ✅ | ⚠️ 실전 전용 |

- ✅ 검증: 모의 서버에서 시세·캔들·계좌·주문 전 구간 스모크 통과
- ⚠️ 미검증: 공식 문서로 구현했고 실측 전. 실전 전용은 모의 환경이 없는 증권사
- 실시간 ✅ 는 체결가·호가 실측 완료, ⚠️ 는 구현만 됨, ❌ 는 스펙에 웹소켓 없음

캔들 주기, 증권사별 설정 키, 실시간 프로토콜과 제약은 [증권사별 설정과 제약](docs/brokers.md)에 있습니다. 각 증권사 API 의 실제 사용량과 에러율은 [사용량 랭킹](https://hermetix-api-prod.tripleauth.com/rankings.html)과 [API 현황](https://hermetix-api-prod.tripleauth.com/status.html)에서 볼 수 있습니다.

## 왜 Hermetix 인가

- **증권사 독립** — 같은 코드가 8개 증권사에서 그대로 돕니다
- **네 언어, 같은 규약** — Kotlin·Python·JS·Go 가 같은 [컨포먼스 시나리오](conformance/README.md)를 통과합니다
- **전략은 클래스 하나** — 인증, 조회, 주문, 체결 추적, 익절·손절은 엔진이 맡습니다
- **안전장치 내장** — 공매도 방지, 주문 멱등키, 비상정지, 주문 금액 상한, KRX 호가단위 보정
- **모의에서 실전으로 설정 한 줄** — 명시 동의 없이는 실전 주문이 나가지 않습니다
- **키는 로컬에만** — 키와 주문은 당신의 기기에서 증권사로 직접 갑니다

## 설치

| 언어 | 설치 |
|---|---|
| [Python](python/README.md) | `pip install hermetix` |
| [JavaScript/TypeScript](js/README.md) | `npm install hermetix` |
| [Go](go/README.md) | `go get github.com/tripleauth-dev/hermetix-securities/go@latest` |
| Kotlin/JVM | JitPack, 아래 |

```kotlin
repositories { mavenCentral(); maven("https://jitpack.io") }

dependencies {
    implementation("com.github.tripleauth-dev.hermetix-securities:hermetix-engine:0.11.2")
}
```

Python 실시간 스트림은 `pip install 'hermetix[stream]'` 입니다. 연결 계층만 쓰는 Kotlin 은 `hermetix-broker` 를 대신 넣습니다.

## 빠른 시작: 연결 계층

증권사는 ID 로 고르고, 자격 증명 키는 어느 증권사든 `api_key`·`api_secret`·`account` 세 개입니다. 증권사별 `account` 의 뜻과 추가 옵션은 [브로커 팩토리 규약](docs/broker-factory.md)에 있습니다.

```python
import hermetix
client = hermetix.kis(api_key="…", api_secret="…", account="12345678")
quote = client.get_quotes(["005930"])[0]
```
```ts
import hermetix from "hermetix";
const client = hermetix.kis({ apiKey: "…", apiSecret: "…", account: "12345678" });
const [quote] = await client.getQuotes(["005930"]);
```
```go
client, err := hermetix.Kis(hermetix.Credentials{APIKey: "…", APISecret: "…", Account: "12345678"})
quotes, err := client.GetQuotes([]string{"005930"})
```
```kotlin
val client = Hermetix.kis(Credentials(apiKey = "…", apiSecret = "…", account = "12345678"))
val quote = client.getQuotes(listOf("005930")).quotes[0]
```

금액과 수량은 모든 언어에서 Decimal 입니다. 증권사별 클래스를 직접 만들어도 됩니다.

## 빠른 시작: 전략 봇

전략은 `spec` 과 `decide()` 두 가지만 채웁니다. 엔진이 정규장에만 호출하고, 시그널을 주문으로 바꾸고, 익절·손절과 비상정지를 처리합니다.

처음이라면 [hermetix-strategy-template](https://github.com/tripleauth-dev/hermetix-strategy-template) 을 복제하세요. 네 언어 폴더가 있고 예제 전략과 테스트가 들어 있습니다. 아래는 Kotlin 이고, Python·JS·Go 예시는 각 언어 README 에 있습니다.

```yaml
# application.yml
hermetix:
  broker: next
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
                    takeProfitPrice = price.multiply(BigDecimal("1.04")),
                    stopLossPrice = price.multiply(BigDecimal("0.98")),
                ),
            )
        }
        return emptyList()
    }
}
```

`broker` 값을 `kis` 로 바꾸면 증권사가 바뀝니다. 증권사별 키 블록은 [증권사별 설정과 제약](docs/brokers.md)에 있습니다.

## 실전투자로 전환

두 가지를 명시해야 합니다. 하나라도 빠지면 엔진이 기동을 거부합니다.

```yaml
hermetix:
  broker: kis
  kis:
    environment: live
    appkey: ${KIS_APPKEY:}
    appsecret: ${KIS_APPSECRET:}
    cano: ${KIS_CANO:}
  live:
    enabled: true
  risk:
    max-order-value: 1000000
    max-daily-order-value: 5000000
```

- `environment: live` 로 호스트와 TR ID 가 실전으로 바뀝니다
- `live.enabled` 가 실전 명시 동의입니다
- `risk` 는 주문 1건과 하루 누적 금액 상한이며, 모의투자에서도 적용됩니다

넥스트증권은 키 프리픽스(`pk_test_`, `pk_live_`)가 환경을 결정합니다.

## 동작 방식

```
전략 (TradingStrategy)      ← 당신이 작성하는 유일한 부분
    ↓ Signal
StrategyEngine              ← 정규장 스케줄링, 브라켓, 비상정지
    ↓ BrokerClient
8개 증권사 어댑터           ← 인증, 레이트리밋, 응답 정규화, 웹소켓
```

- 엔진은 해당 시장의 정규장에만 전략을 호출합니다
- 기동 시 전략과 증권사의 호환성을 검증하고, 어긋나면 스케줄하지 않습니다
- 매도는 보유 수량으로 클램프되고, 연속 실패 5회면 미체결을 전량 취소하고 주문을 막습니다
- 익절·손절 브라켓은 메모리에 있어 재시작 시 사라집니다. 복원 패턴은 [전략 가이드](docs/strategy-guide.md)에 있습니다

## 실시간 스트림

증권사가 웹소켓을 제공하면 체결가·호가·주문통보 세 채널을 씁니다. 전략 코드는 그대로이고 `StrategySpec` 만 바뀝니다.

```kotlin
override val spec = StrategySpec(
    name = "scalp", symbols = listOf("005930"),
    trigger = TickTrigger.ON_TRADE,
    minTickInterval = Duration.ofSeconds(1),
    orderBook = true,
)
```

- 체결가 틱마다 `decide()` 가 호출되고, 스트림이 끊기면 폴링이 이어받습니다
- 주문통보는 엔진이 자동 구독해 체결을 브라켓에 바로 반영합니다
- 웹소켓이 없는 증권사(`next`, `kb`)는 폴링으로 동작합니다

연결 계층만으로도 씁니다.

```kotlin
val stream = (client as StreamingBrokerClient).openStream()
stream.subscribeTrades(listOf("005930")) { tick -> println("${tick.price} x${tick.quantity}") }
stream.subscribeOrderBook(listOf("005930")) { book -> println("${book.bestBid} / ${book.bestAsk}") }
stream.subscribeOrderEvents { event -> println("${event.type} ${event.orderId}") }
stream.connect()
```

증권사별 채널 상태와 프로토콜 제약은 [증권사별 설정과 제약](docs/brokers.md#실시간-스트림--증권사별-상태와-프로토콜)에 있습니다.

## 사용량 데이터

SDK 는 어느 증권사의 어떤 기능이 얼마나 쓰이는지를 합산해 보내고, 그 데이터로 [사용량 랭킹](https://hermetix-api-prod.tripleauth.com/rankings.html)과 [API 현황](https://hermetix-api-prod.tripleauth.com/status.html)을 공개합니다. 항상 켜져 있고 끄는 설정은 없습니다. 대신 보내는 것을 그대로 공개합니다.

| 보내는 것 | 보내지 않는 것 |
|---|---|
| 증권사 ID, 모의/실전 | 종목, 수량, 가격, 금액 |
| 기능별 성공·에러 건수, 에러 분류 | 주문번호, 계좌번호, 고객 ID |
| 응답 시간 p50·p95 | API 키, 토큰, HTS ID |
| 실시간 구독·메시지·재접속 수 | 전략 이름, 설정값 |
| SDK 언어·버전, 설치 단위 무작위 ID | IP, OS, 호스트명 |

합산값만 60초마다 별도 스레드로 보내며, 전송 실패는 무시합니다. 매매 경로에는 영향이 없습니다. 자세한 계약은 [docs/telemetry.md](docs/telemetry.md) 입니다.

## 수익률

봇을 띄우면 `GET /pnl` 과 주기 PnL 로그가 자동 제공됩니다.

```yaml
hermetix:
  pnl:
    log-interval-minutes: 60
    initial-capital: 20000
```

## 공식 전략

| 레포 | 전략 |
|---|---|
| [hermetix-strategy-template](https://github.com/tripleauth-dev/hermetix-strategy-template) | 이동평균 예제, 네 언어. 여기서 시작하세요 |
| [hermetix-larry-strategy](https://github.com/tripleauth-dev/hermetix-larry-strategy) | 변동성 돌파 |
| [hermetix-trend-breakout-strategy](https://github.com/tripleauth-dev/hermetix-trend-breakout-strategy) | WMA 추세선 돌파 |
| [hermetix-grid-strategy](https://github.com/tripleauth-dev/hermetix-grid-strategy) | 목표가 스캘핑 |
| [hermetix-dca-strategy](https://github.com/tripleauth-dev/hermetix-dca-strategy) | 무한매수법 분할 매수 |
| [hermetix-turtle-strategy](https://github.com/tripleauth-dev/hermetix-turtle-strategy) | 터틀 트레이딩 |

전략을 공유하려면 [전략 공유 이슈](../../issues/new?template=strategy-share.md)를 올려주세요.

<!-- 커뮤니티 전략 목록 -->

## 릴리즈

| 버전 | 내용 |
|---|---|
| 0.11.2 | 텔레메트리 수신 엔드포인트를 `service-api-prod.hermetix.dev` 로 변경 |
| 0.11.1 | 저장소를 `tripleauth-dev/hermetix-securities` 로 이동. Go 모듈 경로와 JitPack 좌표 변경 |
| 0.11.0 | 브로커 팩토리 `hermetix.next(...)`. 사용량 텔레메트리 수신 엔드포인트 확정 |
| 0.10.0 | nh·db·ls·toss 실시간 스트림. PyPI·npm 등록 |
| 0.9.0 | 호가·주문통보 채널 |
| 0.8.0 | 실시간 계층 1차. KIS·키움 체결가 |
| 0.7.0 | LS·토스·KB 어댑터 |
| 0.6.0 | 모의/실전 환경, 실전 게이트, 주문 금액 상한 |

태그가 릴리즈입니다. Python·JS 는 같은 번호로 PyPI·npm 에, Go 는 `go/vX.Y.Z` 태그로 올라갑니다.

## 문서

- 언어별 사용설명서: [Python](python/README.md) · [JavaScript/TypeScript](js/README.md) · [Go](go/README.md)
- [브로커 팩토리 규약](docs/broker-factory.md): `hermetix.next(...)` 생성 규약과 자격 증명 키
- [증권사별 설정과 제약](docs/brokers.md): 캔들 주기, 설정 키, 실시간 프로토콜
- [전략 작성 가이드](docs/strategy-guide.md): SPI 레퍼런스, 패턴, 안전장치, 테스트
- [아키텍처](docs/architecture.md): 모듈 구조, 틱 파이프라인, 심볼 규약
- [사용량 텔레메트리 계약](docs/telemetry.md)
- [컨포먼스 킷](conformance/README.md): 새 어댑터 검증 시나리오
- [국내 증권사 오픈 API 조사](claudedocs/korean-broker-openapi-survey-2026-09.md)
- [로드맵](ROADMAP.md) · [기여 가이드](CONTRIBUTING.md)

## 라이선스

[MIT](LICENSE)

> 이 프로젝트는 학습·연구용 소프트웨어이며 투자 조언이 아닙니다. 실전투자로 발생하는 손실의 책임은 사용자에게 있습니다. 모의투자에서 검증하고, 주문 금액 상한을 설정한 뒤 소액으로 시작하세요. 각 증권사 로고는 해당 회사의 자산입니다.
