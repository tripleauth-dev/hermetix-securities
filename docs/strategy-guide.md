# 전략 작성 가이드

이 문서는 hermetix-securities 위에서 전략을 작성하는 방법을 처음부터 끝까지 다룹니다. 빠른 시작은 [hermetix-strategy-template](https://github.com/tauthdev/hermetix-strategy-template) README 를, 이 문서는 그다음 단계의 상세 레퍼런스로 보세요.

## 1. 전략의 생명주기

엔진은 앱 기동 시 `TradingStrategy` 를 구현한 모든 스프링 빈을 찾아 각자의 `pollInterval` 주기로 `decide()` 를 호출합니다. `trigger = TickTrigger.ON_TRADE` 인 전략은 여기에 더해 브로커 체결가 스트림의 틱마다 호출됩니다 (폴링은 안전망으로 유지).

```
앱 기동 → 전략 빈 발견 → 스케줄 등록
매 틱:
  1. 비상정지 상태면 스킵
  2. 장시간 체크 (regularHoursOnly=true 면 브로커 시장의 정규장에만 진행 — next: 미국장 09:30–16:00 ET, kis/kiwoom: KRX 09:00–15:30 KST)
  3. StrategyContext 구성 (시세/캔들/계좌/보유/미체결 API 조회 — 스트림 틱이 모든 심볼을 덮으면 시세는 틱에서, orderBook=true 면 최신 호가창도 포함)
  4. 소프트웨어 브라켓 점검 (익절/손절 도달 시 자동 청산 — 전략 호출보다 우선)
  5. strategy.decide(context) 호출
  6. 반환된 Signal 목록을 순서대로 실행
```

전략 인스턴스는 재사용됩니다. 상태(마지막 판정 캔들, 진입 시각 등)는 클래스 필드에 보관하면 되지만, **앱 재시작 시 사라진다**는 점을 항상 설계에 반영하세요. 서버에서 다시 조회할 수 있는 것(보유/미체결)은 `StrategyContext` 를 기준으로 판단하는 것이 재시작에 안전합니다.

## 2. StrategySpec — 전략의 규격 선언

```kotlin
override val spec = StrategySpec(
    name = "my-strategy",              // 필수. clientOrderId 프리픽스 (영문/숫자/하이픈)
    symbols = listOf("AAPL", "TSLA"),  // 감시 종목. quotes/candles 가 이 종목들로 공급됨
    candleInterval = CandleInterval.DAY_1,   // 브로커별 지원 주기가 다르다 — next: 1m/1d, kis/kiwoom: 1d (BrokerCapabilities)
    candleLimit = 50,                  // 공급받을 캔들 개수
    pollInterval = Duration.ofSeconds(60),
    regularHoursOnly = true,           // false 면 폐장 중에도 호출됨 (주문은 체결 안 됨에 유의)
    trigger = TickTrigger.POLL,        // ON_TRADE 면 체결가 스트림 틱마다 호출 (kis/kiwoom 모의 실측, 0.8.0) — 폴링은 안전망으로 유지
    minTickInterval = Duration.ofSeconds(1), // ON_TRADE 에서 연속 호출 사이 최소 간격 (캔들·계좌 REST 폭주 방지)
    orderBook = false,                 // true 면 심볼 호가창 스트림을 구독해 context.orderBook(symbol) 로 공급 (kis/kiwoom)
)
```

- `trigger = ON_TRADE` 는 브로커가 `BrokerCapabilities.streams` 에 `TRADES` 를 선언한 경우에만 스트림을 붙입니다. 아니면 경고 로그 후 폴링으로 동작합니다
- 스트림 틱이 전략의 모든 심볼을 덮으면 `context.quote()` 는 REST 대신 마지막 체결 틱(가격·호가·누적거래량)으로 채워집니다. 캔들·계좌·미체결은 여전히 REST 라 틱마다 `2 + 심볼 수 + 2` 호출이 나갑니다 — 모의 서버 레이트리밋(kis 2건/s)을 생각해 `minTickInterval` 을 잡으세요

- `candleLimit` 은 필요한 만큼만: 매 틱 심볼마다 캔들 API 를 호출하므로 크면 느려집니다
- 캔들이 필요 없는 전략도 `candleLimit` 최소값(2 정도)을 두세요

## 3. StrategyContext — 판단 재료

| 필드/메서드 | 내용 | 비고 |
|---|---|---|
| `now` | 엔진 기준 현재 시각 | 테스트 주입 가능하도록 `ZonedDateTime.now()` 대신 이걸 쓰세요 |
| `quote(symbol)` | 현재가 스냅샷 | `price`, `bidPrice`/`askPrice`(장 마감 시 null), 당일 누적 `volume`. 시세가 없는 종목(넥스트 `NOT_FOUND`/`NO_DATA`)은 null |
| `candles(symbol)` | 캔들 리스트 (과거→최신) | **마지막 캔들은 진행 중**(미완성)입니다. 완성 캔들 기준 전략은 `dropLast(1)` 하세요 |
| `holding(symbol)` / `hasPosition(symbol)` | 보유 포지션 | `avgEntryPrice` 는 계좌 전체 평균 매입 단가 |
| `openOrders(symbol)` / `hasOpenOrder(symbol)` | 미체결 주문 | 취소하려면 `orderId` 를 `Signal.Cancel` 로 |
| `buyingPower` | 주문 가능 현금 | 수량 계산의 기준 |
| `orderBook(symbol)` | 최신 호가창 (10단계) | `orderBook = true` 전략에만, 스트림이 한 번이라도 준 심볼만. `bestAsk`/`bestBid`/`asks`/`bids`/총잔량. 없으면 null (0.9.0, kis·kiwoom 실측, nh·db·ls·toss 문서 기반) |
| `account` | 계좌 정보 | `cash`, `portfolioValue` |

심볼 조회 메서드(`quote`/`candles`/`holding`/`hasPosition`/`openOrders`/`orderBook`)는 `MARKET:CODE` 접두 유무를 무시하고 코드로 맞춥니다. `KRX:005930` 으로 감시하면서 서버가 `005930` 으로 주는 보유/미체결을 그대로 찾을 수 있습니다.

## 4. Signal — 의사결정 표현

### 매수

```kotlin
Signal.Buy(
    symbol = "AAPL",
    quantity = BigDecimal("5"),
    orderType = OrderType.MARKET,      // 또는 LIMIT (+ limitPrice)
    timeInForce = TimeInForce.DAY,     // DAY: 당일 소멸 / GTC: 체결까지 유지
    takeProfitPrice = BigDecimal("320"),  // 선택: 체결 후 자동 익절
    stopLossPrice = BigDecimal("290"),    // 선택: 체결 후 자동 손절
)
```

`takeProfitPrice`/`stopLossPrice` 를 지정하면 코어의 **소프트웨어 브라켓**이 체결을 추적하다가 현재가 도달 시 시장가로 자동 청산합니다. 주의:

- 브라켓 상태는 메모리에만 있습니다 (재시작 시 소실)
- 청산은 시장가라 급변동 시 슬리피지가 있습니다
- 진입 주문이 취소/거부되면 브라켓도 자동 폐기됩니다

### 매도

```kotlin
Signal.Sell(symbol = "AAPL", quantity = BigDecimal("5"))  // 시장가
Signal.Sell(symbol = "AAPL", quantity = qty, orderType = OrderType.LIMIT, limitPrice = target, timeInForce = TimeInForce.GTC)
```

매도 수량은 **보유 수량으로 자동 클램프**됩니다 (공매도 방지). 보유가 없으면 조용히 스킵되고 경고 로그만 남습니다.

### 취소

```kotlin
Signal.Cancel(orderId = order.orderId)
```

취소 후 재주문 패턴은 "이번 틱에 Cancel → 다음 틱에 새 주문" 이 안전합니다 (취소 완료를 서버 상태로 확인한 뒤 진행).

## 5. 안전장치

- **비상정지(TradingGuard)**: 틱 처리 중 연속 실패가 `hermetix.engine.max-consecutive-failures`(기본 5)에 도달하면 미체결 전량 취소 후 모든 주문이 차단됩니다. 해제는 `TradingGuard.resume()` (빈 주입 후 호출) 또는 앱 재시작
- **주문 금액 상한(RiskGuard)**: `hermetix.risk.max-order-value`(1건) / `max-daily-order-value`(하루 UTC 누적, 매수·매도 합산)를 넘는 시그널은 제출되지 않고 경고 로그만 남습니다. 추정 금액은 수량 × (지정가 또는 현재가)이며, 현재가를 모르면 상한이 설정된 경우 거부합니다. 누적치는 메모리에만 있습니다
- **실전 게이트**: 브로커가 `environment: live` 면 `hermetix.live.enabled=true` 가 없는 한 엔진이 전략을 스케줄하지 않습니다. 실전 전환 시 상한 설정을 함께 권장합니다
- **clientOrderId 멱등성**: 지원 브로커(next)에서 모든 주문에 `{전략이름}-{uuid}` 가 부여되어 24시간 내 중복 제출이 방지됩니다
- **주의 — 여러 전략이 같은 심볼을 다루면 안 됩니다.** 보유/미체결 판단이 심볼 단위라 서로의 포지션을 침범합니다. 전략마다 다른 종목을 배정하세요

## 6. 자주 쓰는 패턴

**완성 캔들 기준 1회 판정** (larry, trend-breakout 방식):

```kotlin
private var lastEvaluated: Instant? = null

val completed = context.candles(symbol).dropLast(1)
val target = completed.last()
if (lastEvaluated == target.timestamp) return emptyList()
lastEvaluated = target.timestamp
```

**예산 기반 수량 계산**:

```kotlin
val quantity = context.buyingPower
    .multiply(budgetRatio)
    .divide(price, 0, RoundingMode.DOWN)   // 정수 주식 수
if (quantity < BigDecimal.ONE) return emptyList()
```

**재시작 안전한 만료 청산**:

```kotlin
// 진입 시각을 모르면(재시작) 발견 시점부터 다시 센다
val openedAt = entryAt ?: context.now.also { entryAt = it }
if (Duration.between(openedAt, context.now) >= expireDuration) { /* Sell */ }
```

**다중 종목**: 전략 클래스를 종목 파라미터를 받는 형태로 만들고 `@Configuration` 에서 종목별 빈을 등록하면 각자 독립 스케줄로 돌아갑니다. `spec.name` 은 반드시 서로 다르게.

## 7. 테스트

전략은 순수 함수에 가깝게 (`decide(context) → signals`) 설계되어 있어 API 목킹 없이 테스트할 수 있습니다. `StrategyContext` 를 직접 생성해 넣으세요:

```kotlin
val context = StrategyContext(
    now = ZonedDateTime.now(),
    quotes = mapOf("AAPL" to Quote(...)),
    candles = mapOf("AAPL" to candleFixtures),
    account = AccountResponse(...),
    holdings = emptyMap(),
    openOrders = emptyList(),
    buyingPower = BigDecimal("10000"),
)
assertThat(strategy.decide(context)).hasSize(1)
```

실전 예시는 공식 전략 레포들의 테스트를 참고하세요: [next-larry-strategy](https://github.com/tauthdev/next-larry-strategy), [next-trend-breakout-strategy](https://github.com/tauthdev/next-trend-breakout-strategy), [next-grid-strategy](https://github.com/tauthdev/next-grid-strategy)

## 8. 트러블슈팅

| 증상 | 원인/해결 |
|---|---|
| 엔진이 전략을 안 찾음 | `@Component` 누락 또는 컴포넌트 스캔 범위 밖. "등록된 TradingStrategy 빈이 없습니다" 로그 확인 |
| 틱이 전혀 안 돎 | 브로커 시장의 정규장 시간이 아님 (`regularHoursOnly = false` 로 폐장 테스트 가능) |
| 401 반복 | 키 오류. `NEXT_CLIENT_ID`/`NEXT_CLIENT_SECRET` 확인 |
| 주문이 423 | 서버 킬 스위치 활성 상태 (문서상 — 현재 모의 서버엔 미배포) |
| 갑자기 모든 주문 스킵 | 비상정지 발동. 로그에서 "TRADING HALTED" 검색, 원인 해결 후 재시작 |
| BUY 인데 체결 수량이 0 | 지정가가 시장과 멀면 미체결이 정상. `GET /v1/orders` 로 상태 확인 |
