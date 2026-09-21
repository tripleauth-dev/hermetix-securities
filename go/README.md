# Hermetix Go

증권사 모의투자 통합 트레이딩 프레임워크 — Go 구현. 여덟 개 증권사(넥스트·한국투자·키움·NH·DB·LS·토스·KB)를 하나의 `BrokerClient` 인터페이스로 다루고, 전략 하나를 브로커 교체 없이 어디서나 돌립니다.

버전 **0.11.0** (태그 `go/v0.11.0`) · Go **1.26+** (`go.mod` 기준) · 의존성 `github.com/shopspring/decimal`, `github.com/coder/websocket`(실시간 스트림) · MIT

**금액에 float 를 절대 섞지 마세요.** 가격·수량은 전부 `decimal.Decimal` 입니다.

## 설치

```bash
go get github.com/tripleauth-dev/hermetix-securities/go@v0.11.0
```

```go
import (
    "github.com/shopspring/decimal"
    hermetix "github.com/tripleauth-dev/hermetix-securities/go"
)

// 브로커 ID 한 토큰만 바꾸면 증권사가 바뀝니다 (docs/broker-factory.md)
client, err := hermetix.Next(hermetix.Credentials{APIKey: "pk_test_…", APISecret: "sk_test_…", Account: "acc_main"})
```

모노레포의 `go/` 하위 모듈이라 태그가 `go/v0.11.0` 형식입니다. 패키지 이름은 `hermetix` 로 별칭을 두는 것을 권장합니다.

## 5분 빠른 시작 — 전략 봇

`Strategy` 인터페이스 두 메서드(`Spec`, `Decide`)만 구현하면 엔진이 정규장 중 `PollInterval` 마다 호출하고, 시그널을 주문으로 바꾸고, 익절·손절·비상정지를 대신 처리합니다.

```go
package main

import (
    "os"

    "github.com/shopspring/decimal"
    hermetix "github.com/tripleauth-dev/hermetix-securities/go"
)

type MyStrategy struct{}

func (s MyStrategy) Spec() hermetix.StrategySpec {
    return hermetix.StrategySpec{
        Name:           "my-first",              // clientOrderId 프리픽스 (영문/숫자/하이픈)
        Symbols:        []string{"AAPL"},        // 감시 종목 — quotes/candles 가 이 종목으로 공급됨
        CandleInterval: hermetix.Day1,           // 브로커별 지원 주기 (next 1m/1d, 국내 1d)
        CandleLimit:    20,
    }
}

func (s MyStrategy) Decide(ctx *hermetix.StrategyContext) ([]hermetix.Signal, error) {
    q, ok := ctx.Quote("AAPL")
    if !ok || ctx.HasPosition("AAPL") || ctx.HasOpenOrder("AAPL") {
        return nil, nil // 할 일 없음
    }
    tp := q.Price.Mul(decimal.NewFromFloat(1.04))
    sl := q.Price.Mul(decimal.NewFromFloat(0.98))
    return []hermetix.Signal{hermetix.BuySignal{
        Symbol:          "AAPL",
        Quantity:        decimal.NewFromInt(1),
        TakeProfitPrice: &tp, // 체결 후 이 가격에 닿으면 엔진이 자동 청산 (소프트웨어 브라켓)
        StopLossPrice:   &sl,
    }}, nil
}

func main() {
    broker := hermetix.NewNextClient(os.Getenv("NEXT_CLIENT_ID"), os.Getenv("NEXT_CLIENT_SECRET"))
    hermetix.NewStrategyEngine(broker, []hermetix.Strategy{MyStrategy{}}).Run() // 블로킹, Ctrl-C 로 종료
}
```

```bash
NEXT_CLIENT_ID=pk_test_... NEXT_CLIENT_SECRET=sk_test_... go run .
```

- `Decide` 는 해당 브로커 시장의 **정규장에만** 호출됩니다 (next 미국장 ET, 국내 브로커 KRX 09:00–15:30 KST). 폐장 테스트는 `RegularHoursOnly` 를 `false` 로.
- `StrategyContext` 는 호출 시점 스냅샷입니다: `Quote`, `CandlesOf`(과거→최신), `Holding`/`HasPosition`, `OpenOrdersOf`/`HasOpenOrder`, `BuyingPower`, `Account`, `OrderBook`(호가 구독 시).
- 시그널은 `BuySignal`(익절·손절 선택), `SellSignal`(보유 수량으로 자동 클램프, 공매도 불가), `CancelSignal{OrderID}` 세 가지입니다.
- 전략 안에서 브로커 API 를 직접 부르거나 고루틴을 만들지 마세요. 상태는 전략 구조체 필드에 두되 재시작 시 사라집니다.

## 연결 계층만 쓰기

봇 없이 시세 수집·대시보드·알림용으로 `BrokerClient` 만 써도 됩니다. 모든 어댑터가 같은 메서드를 제공합니다.

```go
// 자격 증명은 모든 브로커가 같은 모양 — APIKey / APISecret / Account (+ Environment, Extra). 증권사를 바꾸려면 Kis → Kiwoom 처럼 함수 이름만 바꿉니다
broker, err := hermetix.Kis(hermetix.Credentials{APIKey: os.Getenv("KIS_APPKEY"), APISecret: os.Getenv("KIS_APPSECRET"), Account: os.Getenv("KIS_CANO")})
if err != nil {
    panic(err) // 필수 값 누락, 모르는 Extra 키, PAPER/LIVE 외 환경
}
// ID 문자열로 고르려면: broker, err := hermetix.Client("kis", creds)   (반환은 BrokerClient 인터페이스)
// 기존 생성자 hermetix.NewKisClient(appkey, appsecret, cano) 와 Set* 체이닝도 그대로 됩니다

quotes, err := broker.GetQuotes([]string{"005930", "000660"})
if err != nil {
    panic(err)
}
fmt.Println(quotes[0].Price, quotes[0].ChangeRate)

candles, _ := broker.GetCandles("005930", hermetix.Day1, 30) // 과거 → 최신
holdings, _ := broker.GetHoldings()
power, _ := broker.GetBuyingPower()
fmt.Println(len(candles), len(holdings), power)

limit := decimal.NewFromInt(70000)
order, err := broker.CreateOrder(hermetix.CreateOrderRequest{
    Symbol: "005930", Side: hermetix.Buy, OrderType: hermetix.Limit,
    Quantity: decimal.NewFromInt(1), LimitPrice: &limit,
})
if err == nil {
    _, _ = broker.CancelOrder(order.OrderID)
}
```

- 심볼은 `MARKET:CODE` 접두를 허용합니다 (`KRX:005930`, `US:AAPL`). 접두 없는 심볼은 브로커 기본 시장으로 해석되고, 응답은 요청 표기 그대로 돌아옵니다.
- 인증(토큰 발급·갱신)과 레이트리밋(쓰로틀·백오프)은 어댑터 안에서 처리합니다. 실패는 `error` 로 돌아오고, `errors.As` 로 `*hermetix.RateLimitError`·`*hermetix.MarketClosedError` 등 타입을 구분할 수 있습니다.
- KRX 지정가는 `hermetix.KrxTickRound(price)` 로 호가단위에 맞추세요.

## 브로커 설정

| ID | 생성자 | 설정 메서드 | 모의 / 실전 | 상태 |
|---|---|---|---|---|
| `next` 넥스트증권 (미국주식) | `NewNextClient(clientID, clientSecret)` | `SetEnvironment(env) error` — 키 프리픽스(`pk_test_`/`pk_live_`)와 어긋나면 에러, `SetBaseURL` | 키로 구분 | ✅ 검증 (공개 스펙 v1.3) |
| `kis` 한국투자증권 | `NewKisClient(appkey, appsecret, cano)` | `SetEnvironment`(호스트·TR ID 자동 전환), `SetWSURL`, `SetHTSID`(주문 통보용), `SetThrottle`, `SetBaseURL` | 모의 ✅ 실전 ✅ | ✅ 검증 (모의) |
| `kiwoom` 키움증권 | `NewKiwoomClient(appkey, secretkey)` | `SetEnvironment`(호스트 전환), `SetWSURL`, `SetThrottle`, `SetBaseURL` | 모의 ✅ 실전 ✅ | ✅ 검증 (모의) |
| `nh` NH투자증권 NH PLUG | `NewNhClient(appKey, appSecret, accountNo)` — 계좌 비우면 모의(03) 계좌 자동 선택 | `SetEnvironment`(moapi/api 호스트), `SetWSURL`, `SetAuthURL`, `SetThrottle`, `SetBaseURL` | 모의 ✅ 실전 ✅ | ⚠️ 미검증 (문서 기반) |
| `db` DB증권 | `NewDbClient(appKey, appSecret)` | `SetEnvironment`, `SetWSURL`, `SetMacAddress`, `SetThrottle`, `SetBaseURL` | 키로 구분 | ⚠️ 미검증 (문서 기반) |
| `ls` LS증권 | `NewLsClient(appKey, appSecret)` | `SetEnvironment`, `SetWSURL`, `SetMacAddress`, `SetExchGubun`, `SetThrottle`, `SetChartThrottle`, `SetBaseURL` | 키로 구분 | ⚠️ 미검증 (문서 기반) |
| `toss` 토스증권 (KRX·미국) | `NewTossClient(clientID, clientSecret, accountSeq)` — 계좌 비우면 첫 위탁계좌 | `SetWSURL`, `SetThrottle`, `SetBaseURL` (환경은 항상 Live) | 실전만 (샌드박스 없음) | ⚠️ 미검증 (실전 전용) |
| `kb` KB증권 오픈베타 | `NewKbClient(appKey, appSecret)` | `SetExcgClsf`, `SetSorOrderCcd`, `SetChartMarketClsf`(차트 시장 0 KOSPI/1 KOSDAQ), `SetThrottle`, `SetBaseURL` (환경은 항상 Live) | 실전만 ("추후 제공") | ⚠️ 미검증 (실전 전용) |

설정 메서드는 체이닝됩니다 (`SetEnvironment` 는 next 만 `error` 를 돌려줍니다). 브로커 전환은 생성자 한 줄 교체입니다.

```go
var broker hermetix.BrokerClient
switch os.Getenv("HERMETIX_BROKER") {
case "kis":
    broker = hermetix.NewKisClient(os.Getenv("KIS_APPKEY"), os.Getenv("KIS_APPSECRET"), os.Getenv("KIS_CANO"))
case "kiwoom":
    broker = hermetix.NewKiwoomClient(os.Getenv("KIWOOM_APPKEY"), os.Getenv("KIWOOM_SECRETKEY"))
case "nh":
    broker = hermetix.NewNhClient(os.Getenv("NH_APP_KEY"), os.Getenv("NH_APP_SECRET"), os.Getenv("NH_ACCOUNT_NO"))
case "db":
    broker = hermetix.NewDbClient(os.Getenv("DB_APP_KEY"), os.Getenv("DB_APP_SECRET")).SetMacAddress(os.Getenv("DB_MAC_ADDRESS"))
case "ls":
    broker = hermetix.NewLsClient(os.Getenv("LS_APP_KEY"), os.Getenv("LS_APP_SECRET"))
case "toss":
    broker = hermetix.NewTossClient(os.Getenv("TOSS_CLIENT_ID"), os.Getenv("TOSS_CLIENT_SECRET"), os.Getenv("TOSS_ACCOUNT_SEQ"))
case "kb":
    broker = hermetix.NewKbClient(os.Getenv("KB_APP_KEY"), os.Getenv("KB_APP_SECRET"))
default:
    broker = hermetix.NewNextClient(os.Getenv("NEXT_CLIENT_ID"), os.Getenv("NEXT_CLIENT_SECRET"))
}
```

브로커가 지원하는 기능은 `broker.Capabilities()` 에 코드로 선언돼 있습니다 (캔들 주기, 지원 환경·시장, 멱등키, 실시간 채널). 엔진은 기동 시 전략의 캔들 주기·환경을 이 선언과 대조해 어긋나면 스케줄하지 않고 로그로 알립니다.

## 실전투자로 전환

모의에서 검증한 뒤 실제 계좌로 옮길 때는 환경과 명시 동의를 함께 지정합니다. 하나라도 빠지면 엔진이 전략을 스케줄하지 않습니다.

```go
broker := hermetix.NewKisClient(os.Getenv("KIS_APPKEY"), os.Getenv("KIS_APPSECRET"), os.Getenv("KIS_CANO")).
    SetEnvironment(hermetix.Live) // 호스트·TR ID 자동 전환

maxOrder := decimal.NewFromInt(1_000_000)
maxDaily := decimal.NewFromInt(5_000_000)
hermetix.NewStrategyEngineWithOptions(broker, []hermetix.Strategy{MyStrategy{}}, hermetix.EngineOptions{
    LiveTradingEnabled:     true,      // 실전 명시 동의 — 없으면 Live 브로커는 기동 거부
    MaxOrderValue:          &maxOrder, // 주문 1건 추정 금액 상한 (브로커 통화)
    MaxDailyOrderValue:     &maxDaily, // 하루(UTC) 누적 상한 — 매수·매도 합산, 메모리 보관(재시작 시 초기화)
    MaxConsecutiveFailures: 5,         // 연속 실패 시 비상정지 (미체결 전량 취소 + 신규 주문 차단)
}).Run()
```

- 넥스트증권은 `SetEnvironment(hermetix.Live)` 가 `pk_live_` 키가 아니면 `error` 를 돌려줍니다 (실전 키를 모의로 착각하는 사고 방지).
- 토스·KB 는 모의투자가 없어 생성 즉시 Live 입니다. `LiveTradingEnabled` 와 주문 상한 없이는 엔진이 돌지 않으니 소액으로 시작하세요.
- KIS·키움 실전은 호스트·TR ID 전환만 구현돼 있고 실계좌 스모크는 아직입니다.
- 키는 항상 당신의 기기에서만 쓰입니다. Hermetix 는 어떤 서버로도 키를 보내지 않습니다.

## 실시간 스트림

웹소켓 스트림은 폴링 위에 얹는 선택 계층입니다. 전략 코드는 그대로 두고 `StrategySpec` 만 바꿉니다.

### 체결가 트리거

```go
func (s ScalpStrategy) Spec() hermetix.StrategySpec {
    return hermetix.StrategySpec{
        Name: "scalp", Symbols: []string{"005930"},
        Trigger:         hermetix.TriggerOnTrade, // 체결 틱마다 Decide 호출
        MinTickInterval: 2 * time.Second,          // 연속 호출 최소 간격 (기본 1s)
        PollInterval:    60 * time.Second,         // 스트림이 끊겼을 때의 안전망 주기
    }
}
```

- 몰려온 틱은 하나로 합치고, `MinTickInterval` 은 직전 호출이 끝난 뒤부터 잽니다. 스트림이 끊기면 `PollInterval` 폴링이 계속 돌고 스트림은 뒤에서 재접속합니다.
- 틱이 전략의 모든 심볼을 덮으면 `ctx.Quote()` 는 REST 대신 마지막 체결 틱으로 채워집니다. 캔들·계좌·미체결은 여전히 REST 라 틱마다 `1 + 심볼 수 + 3` 호출이 나갑니다. 모의 서버 레이트리밋(kis 2건/s)을 생각해 간격을 잡으세요.
- 스트림을 선언하지 않은 브로커(`next`, `kb`)에서 `TriggerOnTrade` 를 쓰면 경고 로그 후 폴링으로 동작합니다.

### 호가창

```go
func (s BookStrategy) Spec() hermetix.StrategySpec {
    return hermetix.StrategySpec{Name: "book", Symbols: []string{"005930"}, OrderBook: true}
}

func (s BookStrategy) Decide(ctx *hermetix.StrategyContext) ([]hermetix.Signal, error) {
    book, ok := ctx.OrderBook("005930") // KRX:005930 으로 조회해도 같은 결과
    if !ok {
        return nil, nil // 아직 호가가 안 옴
    }
    ask, _ := book.BestAsk()
    bid, _ := book.BestBid()
    fmt.Println(ask.Price, ask.Quantity, bid.Price, bid.Quantity, len(book.Asks), book.TotalAskQuantity)
    return nil, nil
}
```

`OrderBook: true` 면 엔진이 심볼의 10단계 호가창을 구독해 최신 스냅샷을 컨텍스트로 넣습니다. 호가 틱은 전략을 촉발하지 않습니다 (촉발은 체결가 트리거의 몫).

### 주문 통보

브로커가 주문 통보 채널을 선언하면 엔진이 **자동으로** 구독합니다. 진입 주문의 체결 통보가 오면 서버 조회 없이 브라켓을 즉시 활성화하고(취소·거부면 폐기), KIS 모의처럼 주문 조회가 없는 어댑터는 `OrderEventApplier` 로 메모리 추적을 즉시 확정합니다.

```go
broker := hermetix.NewKisClient(os.Getenv("KIS_APPKEY"), os.Getenv("KIS_APPSECRET"), os.Getenv("KIS_CANO")).
    SetHTSID(os.Getenv("KIS_HTS_ID")) // KIS 주문 통보는 HTS ID 로 구독 (없으면 경고 후 폴링 판정)
```

KIS 통보 프레임은 AES 암호문이며 구독 응답의 key/iv 로 복호화합니다. 구독에 실패해도(HTS ID 미설정 등) 엔진은 기동하고 체결 판정을 폴링으로 계속합니다.

### 엔진 없이 스트림만 쓰기

```go
client := hermetix.NewKiwoomClient(os.Getenv("KIWOOM_APPKEY"), os.Getenv("KIWOOM_SECRETKEY"))
stream := client.OpenStream()
defer stream.Close() // Close 뒤에는 재접속하지 않는다

stream.SubscribeTrades([]string{"KRX:005930", "000660"}, func(tick hermetix.TradeTick) {
    fmt.Println(tick.Symbol, tick.Price, tick.Quantity, tick.Timestamp) // 심볼은 요청 표기 그대로
})
if err := stream.SubscribeOrderBook([]string{"005930"}, func(book hermetix.OrderBookTick) {
    ask, _ := book.BestAsk()
    fmt.Println(book.Symbol, ask.Price, ask.Quantity)
}); err != nil {
    fmt.Println("호가 미지원:", err)
}
if err := stream.SubscribeOrderEvents(func(ev hermetix.OrderEvent) {
    fmt.Println(ev.Type, ev.OrderID, ev.Symbol, ev.Quantity, ev.Price)
}); err != nil {
    fmt.Println("주문 통보 미지원:", err)
}
stream.Connect() // 즉시 반환. 구독은 연결 전에 등록해도 연결되는 순간 전송된다
time.Sleep(time.Minute)
```

- 리스너는 스트림 고루틴에서 호출됩니다. 오래 걸리는 일은 채널로 넘기세요. 리스너가 panic 해도 스트림은 로그만 남기고 계속 갑니다.
- 연결이 끊기면 지수 백오프(1s→2s→…30s)로 재접속하고 구독을 다시 보냅니다. `IsConnected()` 로 상태를 볼 수 있습니다.
- 지원하지 않는 채널은 `SubscribeOrderBook`/`SubscribeOrderEvents` 가 `error` 를 돌려주고, `SubscribeTrades` 는 선언 여부와 무관하게 등록만 합니다. 선언은 `client.Capabilities().HasStream(hermetix.StreamTrades)` 로 확인하세요.

### 브로커별 실시간 상태

| ID | 체결가 | 호가 | 주문 통보 | 비고 |
|---|---|---|---|---|
| `kis` | ✅ `H0STCNT0` | ✅ `H0STASP0` | ⚠️ `H0STCNI9`/`H0STCNI0` (HTS ID, AES) | 체결가·호가는 2026-09 모의 장중 실측. 접속키는 접속마다 `ApprovalKey()` 로 발급 |
| `kiwoom` | ✅ `0B` | ✅ `0D` | ⚠️ `00` | 체결·호가 실측. 통보는 일반 주문 가능한 모의 계좌가 있어야 실측 가능 |
| `nh` | ⚠️ `oc`/`nc`/`mc` | ⚠️ `ob`/`nb`/`mb` | ⚠️ `d2`/`d3` | 채널은 시장 코드(KRX/NXT/통합)에 따라 갈림(기본 KRX). 포털은 **모의 시세를 "미제공"** 으로 표기(통보만 올 수 있음). 세션당 등록 10건(SDK 실측)/30건(공식), 앱키당 세션 2개. 운영 서버가 중간 CA 를 안 보내 JVM/Go 트러스트 문제가 있을 수 있음 |
| `db` | ⚠️ `S00` | ⚠️ `S01` | ⚠️ `IS0`/`IS1` | 접속 후 10초 안에 첫 전송 필요(엔진은 즉시 구독), 계좌당 세션 2개·종목 50개, 연결 6회/분. 통보는 해제 메시지가 없어 세션 종료가 곧 해제 |
| `ls` | ⚠️ `S3_`+`K3_` | ⚠️ `H1_`+`HA_` | ⚠️ `SC0`~`SC4` | 서버가 시장을 판별하지 않아 KOSPI·KOSDAQ TR 을 **둘 다 등록**(등록 수 2배). 토큰은 익일 07:00 만료 → 재접속 시 새 토큰. 세션·등록 한도 미문서 |
| `toss` | ⚠️ `trade:kr`/`us` | ⚠️ `orderbook:kr`/`us` | ⚠️ `personal:order` | 실전 전용. 구독 집합을 배열 하나로 선언(변경은 200ms 동안 합침), 계정당 연결 2개(3번째가 오면 가장 오래된 것 종료), 구독 100개, 선언 5회/초, 180초 무송신 시 끊겨 60초 `PING`. 체결량은 누적 스냅샷의 차이, 체결 틱에 누적거래량·등락 없음 |
| `next`, `kb` | ❌ | ❌ | ❌ | 웹소켓 스펙 없음 — 폴링만 |

✅ 모의 웹소켓 장중 실측 · ⚠️ 구현·테스트는 됐지만 실측 전(공식 문서·SDK·AsyncAPI 기반). 프레임 샘플과 기대값은 [`conformance/fixtures/*.json`](../conformance/fixtures) 의 `stream` 섹션(`measured` 플래그)에 있습니다.

## 수익률

```go
initial := decimal.NewFromInt(10_000_000)
report, err := hermetix.Pnl(broker, &initial) // initialCapital 이 nil 이면 수익률 없이 평가액만
if err == nil {
    fmt.Println(report.PortfolioValue, report.TotalUnrealizedPnl, report.TotalReturnRate)
}
```

`PnlReport` 는 계좌 현금·평가액·보유 평가금액 합·미실현 손익 합·총수익률(시작 자금 대비)·보유 목록을 담습니다.

## 검증

```bash
cd go
go test ./...                 # 오프라인 — 골든 픽스처 재생 (컨포먼스·파서·엔진·스트림)
go test -race ./...           # 스트림·엔진 동시성 검사
go run ./cmd/smoke next       # 실서버 스모크 (next | kis | kiwoom)
```

| 스모크 | 환경변수 |
|---|---|
| `next` | `NEXT_CLIENT_ID`, `NEXT_CLIENT_SECRET` |
| `kis` | `KIS_APPKEY`, `KIS_APPSECRET`, `KIS_CANO` |
| `kiwoom` | `KIWOOM_APPKEY`, `KIWOOM_SECRETKEY` |

스모크는 시세 → 캔들 → 캘린더 → 계좌 → 주문(체결되지 않을 지정가) → 취소 전 구간을 실서버에서 돌립니다. 모의 서버라도 실제 주문이 접수되니 장중에만 실행하세요.

새 어댑터는 `hermetix.VerifyBrokerConformance(broker, hermetix.ConformanceScenario{...})` 로 공통 규약을 검증합니다. 네 언어가 같은 [골든 픽스처](../conformance/README.md)를 재생하므로 포팅 간 동작이 일치합니다.

⚠️ 미검증 브로커(nh·db·ls·toss·kb 와 kis·kiwoom 의 주문 통보)는 메인테이너가 계좌를 갖고 있지 않아 실측을 못 했습니다. 해당 증권사 계좌가 있다면 스모크를 돌려 [새 브로커 요청 이슈](https://github.com/tripleauth-dev/hermetix-securities/issues/new?template=broker-request.md)로 결과(응답 프레임)를 알려 주세요. 실측 프레임으로 픽스처를 교체하고 ✅ 로 승격합니다.

## 공식 전략 예제

Kotlin 전략 레포 3종과 같은 로직이 `examples` 패키지에 있습니다 (임포트해서 바로 쓸 수 있음).

| 이름 | 생성자 | 내용 |
|---|---|---|
| `larry` | `examples.NewLarryStrategy()` | 래리 윌리엄스 변동성 돌파 |
| `trend` | `examples.NewTrendBreakoutStrategy()` | 추세선 돌파 |
| `grid` | `examples.NewGridStrategy()` | 그리드 매매 |

```bash
HERMETIX_BROKER=next NEXT_CLIENT_ID=... NEXT_CLIENT_SECRET=... go run ./cmd/example larry
HERMETIX_BROKER=kis  KIS_APPKEY=... KIS_APPSECRET=... KIS_CANO=...  go run ./cmd/example grid   # KRX 호환
```

```go
import "github.com/tripleauth-dev/hermetix-securities/go/examples"

strategy := examples.NewGridStrategy()
strategy.Symbols = []string{"005930"}
hermetix.NewStrategyEngine(broker, []hermetix.Strategy{strategy}).Run()
```

## 알려진 한계

- 브라켓(익절/손절 예약)·일일 누적 주문 금액·전략 내부 상태는 메모리에만 있어 재시작 시 사라집니다. 전략이 보유 포지션으로 복원하는 패턴을 권장합니다.
- KIS 모의 서버는 미체결·체결 조회를 제공하지 않아 주문을 어댑터가 메모리로 추적하고 체결은 보유 수량 변화(또는 주문 통보)로 판정합니다. 재시작 후 잔여 미체결은 보이지 않습니다.
- 국내 브로커의 캘린더는 KRX 정규장을 합성합니다 (공휴일은 개장일로 보이지만 주문은 서버가 거부).
- 국내 브로커 캔들은 일봉만, 넥스트·토스는 1분봉·일봉입니다.
- 단일 계좌 전제 — 전략 여러 개가 같은 심볼을 다루면 보유·미체결 판단이 겹칩니다.
- 체결 틱 사이의 호가 변화는 호가 스트림을 켜지 않으면 보지 못하며, 폴링 전용 브로커(next·kb)는 틱 주기 사이의 변화를 못 봅니다.

## 사용량 텔레메트리

SDK 는 **어느 증권사가 얼마나 쓰이는지**를 시간 단위로 합산해 `hermetix-service` 로 보내고, 그 데이터로 증권사 사용량 랭킹을 공개합니다. 기본 배포본은 항상 켜져 있고 끄는 설정은 없습니다 — 대신 보내는 내용을 여기와 [계약 문서](../docs/telemetry.md)에 그대로 공개합니다.

| 보내는 것 | 보내지 않는 것 |
|---|---|
| 브로커 ID, 환경(모의/실전), 호출 종류별 성공·에러 건수(레이트리밋·인증·휴장 등 분류), 응답 시간 p50·p95, 스트림 채널별 구독·메시지·재접속 수, SDK 언어·버전, 설치 단위 무작위 ID(`~/.hermetix/installation-id`) | 종목, 수량·가격·금액, 주문번호·계좌번호, API 키·토큰·HTS ID, 전략 이름, IP(서버가 저장하지 않음) |

- 매매 경로와 분리돼 있습니다. 카운터는 메모리, 전송은 60초마다 백그라운드 고루틴. 실패·타임아웃은 조용히 버리고 재전송하지 않습니다
- Go 에는 종료 훅이 없으므로 프로세스가 끝나기 전에 `hermetix.FlushTelemetry()` 를 부르면 남은 버킷이 나갑니다. `StrategyEngine.Stop()` 이 자동으로 부릅니다
- 테스트에서는 `hermetix.TelemetryTransport` 를 바꿔 전송을 가로챌 수 있습니다
- 모든 전송은 계약의 요청 서명(`X-Hermetix-Key-Id`·`X-Hermetix-Timestamp`·`X-Hermetix-Signature`, HMAC-SHA256)을 싣습니다 — 공개 SDK 라 비밀은 아니며 스팸·스캐너를 거르는 문턱입니다 ([계약 문서](../docs/telemetry.md) "요청 서명")

## 문서

- [아키텍처](../docs/architecture.md) — 틱 파이프라인, 실시간 계층, 어댑터 비교표, 상태 지도
- [전략 작성 가이드](../docs/strategy-guide.md) — Kotlin 기준이지만 Spec·Context·Signal 의미는 동일
- [컨포먼스 킷](../conformance/README.md) — 골든 픽스처와 어댑터 추가 절차
- [로드맵](../ROADMAP.md) · [증권사 오픈 API 조사](../claudedocs/korean-broker-openapi-survey-2026-09.md)
