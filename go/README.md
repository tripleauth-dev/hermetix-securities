# Hermetix Go

국내 증권사 오픈 API 를 하나의 `BrokerClient` 인터페이스로 다루는 Go 구현입니다. 넥스트·한국투자·키움·NH·LS·DB·토스·KB 여덟 곳을 같은 코드로 씁니다.

가격과 수량은 전부 `decimal.Decimal` 입니다. float 를 섞지 마세요.

## 설치

```bash
go get github.com/tripleauth-dev/hermetix-securities/go@latest
```

```go
import (
    "github.com/shopspring/decimal"
    hermetix "github.com/tripleauth-dev/hermetix-securities/go"
)
```

모노레포의 `go/` 하위 모듈이라 태그는 `go/vX.Y.Z` 형식입니다. Go 1.26 이상, 의존성은 `shopspring/decimal` 과 `coder/websocket` 둘입니다.

## 증권사 고르기

증권사는 함수 이름으로 고르고, 자격 증명은 어느 증권사든 `Credentials` 한 모양입니다.

```go
client, err := hermetix.Next(hermetix.Credentials{
    APIKey:    os.Getenv("NEXT_CLIENT_ID"),
    APISecret: os.Getenv("NEXT_CLIENT_SECRET"),
    Account:   "acc_main",
})
if err != nil {
    log.Fatal(err)
}
```

`Next` 를 `Kis` 로 바꾸면 한국투자증권입니다. 문자열 ID 로 고르려면 `hermetix.Client("kis", creds)` 를 쓰고, 목록은 `hermetix.Brokers` 입니다.

- `Environment` 를 비우면 증권사 기본값입니다. 토스·KB 는 항상 실전입니다
- 필수 값 누락, 모르는 `Extra` 키, 잘못된 환경은 `error` 로 돌아옵니다
- 증권사별 `Account` 의 뜻과 `Extra` 키는 [브로커 팩토리 규약](../docs/broker-factory.md)에 있습니다

`hermetix.NewKisClient(appkey, appsecret, cano)` 같은 기존 생성자와 `Set*` 체이닝도 그대로 됩니다.

## 5분 빠른 시작: 전략 봇

처음이라면 [hermetix-strategy-template/go](https://github.com/tripleauth-dev/hermetix-strategy-template/tree/main/go) 을 복제하세요. 아래 예제 전략과 테스트가 들어 있습니다.

`Strategy` 인터페이스의 `Spec` 과 `Decide` 두 메서드만 구현합니다. 엔진이 정규장에만 호출하고, 시그널을 주문으로 바꾸고, 익절·손절과 비상정지를 처리합니다.

```go
package main

import (
    "log"
    "os"

    "github.com/shopspring/decimal"
    hermetix "github.com/tripleauth-dev/hermetix-securities/go"
)

type MyStrategy struct{}

func (s MyStrategy) Spec() hermetix.StrategySpec {
    return hermetix.StrategySpec{
        Name:           "my-first",
        Symbols:        []string{"AAPL"},
        CandleInterval: hermetix.Day1,
        CandleLimit:    20,
    }
}

func (s MyStrategy) Decide(ctx *hermetix.StrategyContext) ([]hermetix.Signal, error) {
    q, ok := ctx.Quote("AAPL")
    if !ok || ctx.HasPosition("AAPL") || ctx.HasOpenOrder("AAPL") {
        return nil, nil
    }
    tp := q.Price.Mul(decimal.NewFromFloat(1.04))
    sl := q.Price.Mul(decimal.NewFromFloat(0.98))
    return []hermetix.Signal{hermetix.BuySignal{
        Symbol:          "AAPL",
        Quantity:        decimal.NewFromInt(1),
        TakeProfitPrice: &tp,
        StopLossPrice:   &sl,
    }}, nil
}

func main() {
    broker, err := hermetix.Next(hermetix.Credentials{
        APIKey:    os.Getenv("NEXT_CLIENT_ID"),
        APISecret: os.Getenv("NEXT_CLIENT_SECRET"),
    })
    if err != nil {
        log.Fatal(err)
    }
    hermetix.NewStrategyEngine(broker, []hermetix.Strategy{MyStrategy{}}).Run()
}
```

```bash
NEXT_CLIENT_ID=pk_test_... NEXT_CLIENT_SECRET=sk_test_... go run .
```

- `Run()` 은 블로킹이며 Ctrl-C 로 끝납니다
- `StrategyContext` 는 호출 시점 스냅샷입니다. `Quote`, `CandlesOf`, `Holding`, `HasPosition`, `OpenOrdersOf`, `HasOpenOrder`, `OrderBook` 로 읽습니다
- 시그널은 `BuySignal`, `SellSignal`, `CancelSignal` 세 가지이며 매도는 보유 수량으로 클램프됩니다
- 폐장 중에 테스트하려면 `RegularHoursOnly` 에 `false` 포인터를 넣습니다
- 전략 안에서 증권사 API 를 직접 부르거나 고루틴을 만들지 마세요

## 연결 계층만 쓰기

봇 없이 시세 수집이나 대시보드에 `BrokerClient` 만 써도 됩니다.

```go
broker, err := hermetix.Kis(hermetix.Credentials{
    APIKey:    os.Getenv("KIS_APPKEY"),
    APISecret: os.Getenv("KIS_APPSECRET"),
    Account:   os.Getenv("KIS_CANO"),
})
if err != nil {
    log.Fatal(err)
}

quotes, err := broker.GetQuotes([]string{"005930", "000660"})
candles, err := broker.GetCandles("005930", hermetix.Day1, 30)
holdings, err := broker.GetHoldings()
power, err := broker.GetBuyingPower()

limit := hermetix.KrxTickRound(decimal.NewFromInt(70000))
order, err := broker.CreateOrder(hermetix.CreateOrderRequest{
    Symbol:     "005930",
    Side:       hermetix.Buy,
    OrderType:  hermetix.Limit,
    Quantity:   decimal.NewFromInt(1),
    LimitPrice: &limit,
})
canceled, err := broker.CancelOrder(order.OrderID)
```

- 캔들은 과거에서 최신 순입니다
- 심볼에 `KRX:005930`, `US:AAPL` 처럼 시장 접두를 붙일 수 있습니다
- 인증과 레이트리밋은 어댑터가 처리합니다. 에러 종류는 `errors.As` 로 `*hermetix.RateLimitError`, `*hermetix.MarketClosedError` 등을 구분합니다
- KRX 지정가는 `hermetix.KrxTickRound` 로 호가단위에 맞춥니다
- 증권사가 지원하는 기능은 `broker.Capabilities()` 에 선언돼 있습니다

## 실전투자로 전환

환경과 명시 동의를 함께 지정합니다. 하나라도 빠지면 엔진이 전략을 스케줄하지 않습니다.

```go
broker, err := hermetix.Kis(hermetix.Credentials{
    APIKey:      os.Getenv("KIS_APPKEY"),
    APISecret:   os.Getenv("KIS_APPSECRET"),
    Account:     os.Getenv("KIS_CANO"),
    Environment: hermetix.Live,
})

maxOrder := decimal.NewFromInt(1_000_000)
maxDaily := decimal.NewFromInt(5_000_000)
hermetix.NewStrategyEngineWithOptions(broker, []hermetix.Strategy{MyStrategy{}}, hermetix.EngineOptions{
    LiveTradingEnabled:     true,
    MaxOrderValue:          &maxOrder,
    MaxDailyOrderValue:     &maxDaily,
    MaxConsecutiveFailures: 5,
}).Run()
```

- `Environment: hermetix.Live` 로 호스트와 TR ID 가 실전으로 바뀝니다
- `LiveTradingEnabled` 가 실전 명시 동의입니다
- `MaxOrderValue` 와 `MaxDailyOrderValue` 는 주문 1건과 하루 누적 금액 상한입니다. 모의투자에서도 적용됩니다
- `MaxConsecutiveFailures` 회 연속 실패하면 미체결을 전량 취소하고 주문을 막습니다

넥스트증권은 `pk_live_` 키가 아닌데 `Live` 를 지정하면 에러입니다. 토스·KB 는 모의 환경이 없으니 소액으로 시작하세요.

## 실시간 스트림

증권사가 웹소켓을 제공하면 체결가·호가·주문통보 세 채널을 씁니다. 전략 코드는 그대로이고 `StrategySpec` 만 바뀝니다.

```go
func (s ScalpStrategy) Spec() hermetix.StrategySpec {
    return hermetix.StrategySpec{
        Name:            "scalp",
        Symbols:         []string{"005930"},
        Trigger:         hermetix.TriggerOnTrade,
        MinTickInterval: 2 * time.Second,
        OrderBook:       true,
    }
}
```

- `TriggerOnTrade` 는 체결가 틱마다 `Decide` 를 호출하고, 스트림이 끊기면 `PollInterval` 폴링이 이어받습니다
- `MinTickInterval` 은 직전 호출이 끝난 뒤부터 잽니다. 캔들·계좌는 여전히 REST 라 모의 서버 레이트리밋을 생각해 잡으세요
- `OrderBook: true` 면 `ctx.OrderBook(symbol)` 로 10단계 호가창을 읽습니다. 호가 틱은 전략을 촉발하지 않습니다
- 주문통보는 엔진이 자동 구독해 체결을 브라켓에 바로 반영합니다. KIS 는 `Extra: {"hts_id": ...}` 가 있어야 구독됩니다
- 웹소켓이 없는 증권사(`next`, `kb`)는 폴링으로 동작합니다

엔진 없이 스트림만 쓸 수도 있습니다.

```go
client, err := hermetix.Kiwoom(hermetix.Credentials{
    APIKey:    os.Getenv("KIWOOM_APPKEY"),
    APISecret: os.Getenv("KIWOOM_SECRETKEY"),
})
if err != nil {
    log.Fatal(err)
}
stream := client.OpenStream()
defer stream.Close()

stream.SubscribeTrades([]string{"005930", "000660"}, func(tick hermetix.TradeTick) {
    fmt.Println(tick.Symbol, tick.Price, tick.Quantity)
})
if err := stream.SubscribeOrderBook([]string{"005930"}, func(book hermetix.OrderBookTick) {
    ask, _ := book.BestAsk()
    bid, _ := book.BestBid()
    fmt.Println(book.Symbol, bid.Price, ask.Price)
}); err != nil {
    log.Println("호가 미지원:", err)
}
if err := stream.SubscribeOrderEvents(func(ev hermetix.OrderEvent) {
    fmt.Println(ev.Type, ev.OrderID, ev.Quantity, ev.Price)
}); err != nil {
    log.Println("주문통보 미지원:", err)
}
stream.Connect()
time.Sleep(time.Minute)
```

- `Connect()` 는 즉시 반환하고, 구독은 연결 전에 등록해도 됩니다
- 리스너는 스트림 고루틴에서 호출되니 오래 걸리는 일은 채널로 넘기세요
- 끊기면 지수 백오프로 재접속하고 구독을 다시 보냅니다. `Close()` 뒤에는 재접속하지 않습니다
- 채널 지원 여부는 `client.Capabilities().HasStream(hermetix.StreamTrades)` 로 확인합니다

증권사별 채널 상태와 프로토콜 제약은 [증권사별 설정과 제약](../docs/brokers.md#실시간-스트림--증권사별-상태와-프로토콜)에 있습니다.

## 수익률

```go
initial := decimal.NewFromInt(10_000_000)
report, err := hermetix.Pnl(broker, &initial)
fmt.Println(report.PortfolioValue, report.TotalUnrealizedPnl, report.TotalReturnRate)
```

`initialCapital` 이 `nil` 이면 수익률 없이 평가액만 계산합니다.

## 사용량 텔레메트리

SDK 는 어느 증권사의 어떤 기능이 얼마나 쓰이는지를 합산해 보냅니다. 항상 켜져 있고 끄는 설정은 없습니다. 보내는 항목과 보내지 않는 항목은 [루트 README](../README.md#사용량-데이터)와 [계약 문서](../docs/telemetry.md)에 있습니다.

- 전송은 60초마다 백그라운드 고루틴에서 나가고 실패는 무시합니다
- 프로세스가 끝나기 전에 `hermetix.FlushTelemetry()` 를 부르면 남은 버킷이 나갑니다. `StrategyEngine.Stop()` 이 자동으로 부릅니다
- 테스트에서는 `hermetix.TelemetryTransport` 를 바꿔 전송을 가로챕니다

## 검증

```bash
go test ./...
go test -race ./...
go run ./cmd/smoke next
```

| 스모크 | 환경변수 |
|---|---|
| `next` | `NEXT_CLIENT_ID`, `NEXT_CLIENT_SECRET` |
| `kis` | `KIS_APPKEY`, `KIS_APPSECRET`, `KIS_CANO` |
| `kiwoom` | `KIWOOM_APPKEY`, `KIWOOM_SECRETKEY` |

- `go test` 는 오프라인이며 골든 픽스처로 어댑터·파서·엔진·스트림을 검증합니다
- 스모크는 시세부터 주문·취소까지 실서버에서 돌리므로 장중에만 실행하세요
- 새 어댑터는 `hermetix.VerifyBrokerConformance(broker, scenario)` 로 공통 규약을 검증합니다

미검증 증권사(nh·db·ls·toss·kb)의 계좌가 있다면 스모크 결과를 [새 증권사 요청 이슈](https://github.com/tripleauth-dev/hermetix-securities/issues/new?template=broker-request.md)로 알려주세요.

## 공식 전략 예제

`examples` 패키지에 Kotlin 전략 레포와 같은 로직이 들어 있습니다.

| 이름 | 생성자 | 내용 |
|---|---|---|
| `larry` | `examples.NewLarryStrategy()` | 변동성 돌파 |
| `trend` | `examples.NewTrendBreakoutStrategy()` | 추세선 돌파 |
| `grid` | `examples.NewGridStrategy()` | 그리드 매매 |

```bash
HERMETIX_BROKER=next NEXT_CLIENT_ID=... NEXT_CLIENT_SECRET=... go run ./cmd/example larry
HERMETIX_BROKER=kis KIS_APPKEY=... KIS_APPSECRET=... KIS_CANO=... go run ./cmd/example grid
```

```go
import "github.com/tripleauth-dev/hermetix-securities/go/examples"

strategy := examples.NewGridStrategy()
strategy.Symbols = []string{"005930"}
hermetix.NewStrategyEngine(broker, []hermetix.Strategy{strategy}).Run()
```

## 알려진 한계

- 브라켓, 일일 누적 주문 금액, 전략 내부 상태는 메모리에만 있어 재시작 시 사라집니다
- KIS 모의 서버는 주문 조회가 없어 어댑터가 메모리로 추적하고, 체결은 보유 수량 변화나 주문통보로 판정합니다
- 국내 증권사 캘린더는 KRX 정규장을 합성하므로 공휴일도 개장일로 보입니다
- 국내 증권사 캔들은 일봉만, 넥스트·토스는 1분봉과 일봉입니다
- 전략 여러 개가 같은 심볼을 다루면 보유·미체결 판단이 겹칩니다

## 문서

- [브로커 팩토리 규약](../docs/broker-factory.md): 자격 증명 키와 증권사별 `Extra`
- [증권사별 설정과 제약](../docs/brokers.md): 캔들 주기, 실시간 프로토콜
- [전략 작성 가이드](../docs/strategy-guide.md): Kotlin 기준이지만 Spec·Context·Signal 의미는 같습니다
- [아키텍처](../docs/architecture.md)
- [컨포먼스 킷](../conformance/README.md)
- [로드맵](../ROADMAP.md) · [증권사 오픈 API 조사](../claudedocs/korean-broker-openapi-survey-2026-09.md)
