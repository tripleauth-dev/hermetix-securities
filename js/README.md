# Hermetix JavaScript/TypeScript

[![npm](https://img.shields.io/npm/v/hermetix.svg)](https://www.npmjs.com/package/hermetix)

국내 증권사 오픈 API 를 하나의 인터페이스로 쓰는 Node.js 구현입니다. 전략 한 클래스가 8개 증권사에서 같은 코드로 돕니다.

버전 0.11.3 · Node 22 이상 · 런타임 의존성은 `decimal.js` 하나 · MIT

## 설치

```bash
npm install hermetix
```

ESM 패키지이고 TypeScript 타입 정의가 들어 있습니다. 실시간 웹소켓은 Node 22 내장 `WebSocket` 을 씁니다.

금액과 수량은 모두 `Decimal` 입니다. `number` 를 섞지 마세요.

## 증권사 고르기

증권사는 ID 로 고르고, 자격 증명은 `{ apiKey, apiSecret, account?, environment? }` 한 모양입니다. `hermetix.next` 를 `hermetix.kis` 로 바꾸면 증권사가 바뀝니다.

```ts
import hermetix from "hermetix";

const next = hermetix.next({ apiKey: "pk_test_…", apiSecret: "sk_test_…", account: "acc_main" });
const kis = hermetix.kis({ apiKey: "…", apiSecret: "…", account: "12345678" });
const kiwoom = hermetix.kiwoom({ apiKey: "…", apiSecret: "…" });
```

설정값으로 고를 때는 `client("kis", creds)` 를 씁니다. 증권사별 `account` 의 뜻과 `hts_id` 같은 추가 옵션은 [브로커 팩토리 규약](../docs/broker-factory.md)에 있습니다. `new KisClient(appkey, appsecret, cano)` 처럼 클래스를 직접 만들어도 됩니다.

## 5분 빠른 시작: 전략 봇

처음이라면 [hermetix-strategy-template/js](https://github.com/tripleauth-dev/hermetix-strategy-template/tree/main/js) 을 복제하세요. 아래 예제 전략과 테스트가 들어 있습니다.

전략은 `Strategy` 인터페이스 하나입니다. `spec` 으로 감시 종목과 호출 주기를 선언하고 `decide` 에서 시그널을 돌려주면, 주문 제출과 익절·손절과 비상정지는 엔진이 맡습니다.

```ts
import hermetix, { Decimal, StrategyEngine, buy } from "hermetix";
import type { Signal, Strategy, StrategyContext, StrategySpec } from "hermetix";

class MyFirstStrategy implements Strategy {
  readonly spec: StrategySpec = {
    name: "my-first",
    symbols: ["005930"],
    candleInterval: "1d",
    candleLimit: 20,
    pollIntervalSeconds: 60,
  };

  decide(ctx: StrategyContext): Signal[] {
    const q = ctx.quote("005930");
    if (!q) return [];
    const closes = ctx.candlesOf("005930").map((c) => c.close);
    if (closes.length < 20) return [];
    const ma20 = closes.reduce((a, b) => a.plus(b), new Decimal(0)).div(closes.length);

    if (q.price.gt(ma20) && !ctx.hasPosition("005930") && !ctx.hasOpenOrder("005930")) {
      return [buy("005930", new Decimal(1), {
        takeProfitPrice: q.price.mul("1.04"),
        stopLossPrice: q.price.mul("0.98"),
      })];
    }
    return [];
  }
}

const broker = hermetix.kis({
  apiKey: process.env.KIS_APPKEY!,
  apiSecret: process.env.KIS_APPSECRET!,
  account: process.env.KIS_CANO!,
});
await new StrategyEngine(broker, [new MyFirstStrategy()]).run();
```

- `run()` 은 블로킹 루프이고 정규장에만 전략을 호출합니다
- 기동 시 전략의 캔들 주기와 증권사 환경을 검사하고, 어긋나면 그 전략은 스케줄하지 않습니다
- 시그널은 `buy(symbol, qty, opts)`, `sell(symbol, qty, opts)`, `cancel(orderId)` 입니다
- `sell` 은 보유 수량으로 클램프되고, `decide` 는 `Promise<Signal[]>` 을 돌려줘도 됩니다
- `name` 은 clientOrderId 프리픽스라 영문·숫자·하이픈만 씁니다

키는 환경변수로 넣으세요. 키는 당신의 기기에서 증권사로 직접 갑니다.

## 연결 계층만 쓰기

봇 없이 시세 수집이나 대시보드에 클라이언트만 쓸 수 있습니다. 8개 어댑터가 같은 `BrokerClient` 를 구현합니다.

```ts
import hermetix, { Decimal, pnlReport } from "hermetix";

const client = hermetix.kiwoom({ apiKey: process.env.KIWOOM_APPKEY!, apiSecret: process.env.KIWOOM_SECRETKEY! });

const [quote] = await client.getQuotes(["005930"]);
const candles = await client.getCandles("005930", "1d", 30);
const account = await client.getAccount();
const holdings = await client.getHoldings();
const power = await client.getBuyingPower();

const order = await client.createOrder({
  symbol: "005930", side: "BUY", orderType: "LIMIT", quantity: new Decimal(1), limitPrice: new Decimal(60000),
});
const status = (await client.getOrder(order.orderId)).status;
await client.cancelOrder(order.orderId);

const report = await pnlReport(client, new Decimal(10_000_000));
```

- 캔들은 과거에서 최신 순이고, `changeRate` 는 비율입니다 (-0.0347 은 -3.47%)
- 실패는 `BrokerApiError` 계층으로 throw 됩니다: `RateLimitError`, `MarketClosedError`, `InsufficientFundsError`, `InvalidOrderError`, `AuthError`, `OrderNotFoundError`
- `pnlReport` 는 총평가·예수금·미실현손익·보유 목록을 담고, 시작 자금을 주면 총수익률도 계산합니다
- 심볼은 `MARKET:CODE` 접두를 허용합니다 (`KRX:005930`, `US:AAPL`)

증권사별 지원 기능과 검증 상태는 [증권사별 설정과 제약](../docs/brokers.md), 생성자 인자 전체는 각 클래스의 시그니처를 보세요.

## 실전투자로 전환

환경과 명시 동의를 함께 지정합니다. 하나라도 빠지면 엔진이 전략을 스케줄하지 않습니다.

```ts
import hermetix, { Decimal, StrategyEngine } from "hermetix";
import type { Strategy } from "hermetix";

declare const strategy: Strategy;

const broker = hermetix.kis({
  apiKey: process.env.KIS_APPKEY!,
  apiSecret: process.env.KIS_APPSECRET!,
  account: process.env.KIS_CANO!,
  environment: "LIVE",
});
await new StrategyEngine(broker, [strategy], 5, {
  liveTradingEnabled: true,
  maxOrderValue: new Decimal(1_000_000),
  maxDailyOrderValue: new Decimal(5_000_000),
}).run();
```

- `environment: "LIVE"` 로 호스트와 TR ID 가 실전으로 바뀝니다
- 세 번째 인자는 연속 실패 임계치입니다. 도달하면 미체결을 전량 취소하고 주문을 막습니다
- `maxOrderValue` 는 주문 1건, `maxDailyOrderValue` 는 하루 누적 상한이고 모의에서도 적용됩니다
- 토스·KB 는 모의 환경이 없어 기본값이 `"LIVE"` 입니다

## 실시간 스트림

kis·kiwoom·nh·db·ls·toss 는 체결가, 호가, 주문통보 세 채널을 웹소켓으로 줍니다. next·kb 는 웹소켓이 없어 폴링만 됩니다.

### 체결가 트리거

`spec.trigger = "ON_TRADE"` 로 두면 체결 틱마다 전략을 호출합니다.

```ts
import hermetix, { StrategyEngine } from "hermetix";
import type { Signal, Strategy, StrategyContext, StrategySpec } from "hermetix";

class ScalpStrategy implements Strategy {
  readonly spec: StrategySpec = {
    name: "scalp",
    symbols: ["005930"],
    trigger: "ON_TRADE",
    minTickIntervalMs: 1000,
    pollIntervalSeconds: 60,
  };
  decide(ctx: StrategyContext): Signal[] {
    const q = ctx.quote("005930");
    return q ? [] : [];
  }
}

const broker = hermetix.kis({ apiKey: process.env.KIS_APPKEY!, apiSecret: process.env.KIS_APPSECRET!, account: process.env.KIS_CANO! });
await new StrategyEngine(broker, [new ScalpStrategy()]).run();
```

- 몰려온 틱은 하나로 합치고 `minTickIntervalMs` 보다 촘촘히는 부르지 않습니다
- 캔들·계좌·보유·미체결은 여전히 REST 라 틱마다 호출이 나갑니다. 모의 서버 레이트리밋을 생각해 간격을 잡으세요
- 스트림이 끊기면 지수 백오프로 재접속하고, 그동안 `pollIntervalSeconds` 폴링이 이어받습니다

### 호가창

`spec.orderBook = true` 면 호가창을 구독해 `ctx.orderBook(symbol)` 로 줍니다. 호가는 전략 호출을 촉발하지 않습니다.

```ts
import { bestAsk, bestBid } from "hermetix";
import type { Signal, Strategy, StrategyContext, StrategySpec } from "hermetix";

class BookStrategy implements Strategy {
  readonly spec: StrategySpec = { name: "book", symbols: ["005930"], trigger: "ON_TRADE", orderBook: true };
  decide(ctx: StrategyContext): Signal[] {
    const book = ctx.orderBook("005930");
    if (!book) return [];
    const ask = bestAsk(book), bid = bestBid(book);
    if (ask && bid && ask.quantity.gt(bid.quantity.mul(3))) return [];
    return [];
  }
}
```

### 주문통보

증권사가 주문통보 채널을 선언하면 엔진이 자동 구독해 체결을 브라켓에 바로 반영합니다. KIS 는 HTS ID 가 필요하고, 없으면 경고만 남기고 폴링 판정을 유지합니다.

```ts
import hermetix from "hermetix";

const broker = hermetix.kis({
  apiKey: process.env.KIS_APPKEY!,
  apiSecret: process.env.KIS_APPSECRET!,
  account: process.env.KIS_CANO!,
  hts_id: process.env.KIS_HTS_ID!,
});
```

### 엔진 없이 직접 구독

```ts
import hermetix, { bestAsk, bestBid } from "hermetix";

const broker = hermetix.toss({ apiKey: process.env.TOSS_CLIENT_ID!, apiSecret: process.env.TOSS_CLIENT_SECRET! });
const stream = broker.openStream();
stream.subscribeTrades(["KRX:005930", "US:AAPL"], (t) => console.log(t.symbol, t.price.toString()));
stream.subscribeOrderBook(["KRX:005930"], (b) => console.log(bestAsk(b)?.price.toString(), bestBid(b)?.price.toString()));
stream.subscribeOrderEvents((e) => console.log(e.type, e.orderId));
stream.connect();
stream.close();
```

구독은 연결 전에 등록해도 되고 재접속 시 복원됩니다. 리스너가 던진 예외는 스트림이 삼키고 로그만 남깁니다.

증권사별 채널의 실측 여부와 세션 한도 같은 제약은 [증권사별 설정과 제약](../docs/brokers.md#실시간-스트림--증권사별-상태와-프로토콜)에 있습니다.

## 사용량 텔레메트리

SDK 는 어느 증권사의 어떤 기능이 얼마나 쓰이는지를 합산해 60초마다 보냅니다. 끄는 설정은 없고, 보내는 것과 보내지 않는 것은 [루트 README](../README.md#사용량-데이터)와 [계약](../docs/telemetry.md)에 그대로 공개돼 있습니다.

테스트에서는 `UsageTelemetry.transport` 를 바꾸고 `UsageTelemetry.flushNow()` 로 페이로드를 확인할 수 있습니다.

## 검증

```bash
cd js && npm install
npm test
```

테스트는 오프라인입니다. 골든 픽스처 재생, 가짜 웹소켓 서버, 엔진 테스트를 돌립니다. 새 어댑터는 `verifyBrokerConformance(broker, scenario)` 로 같은 시나리오를 검사할 수 있습니다.

실서버 스모크는 수동이고 모의 계좌에서만 쓰세요. 체결되지 않을 지정가로 1주 매수 후 즉시 취소합니다.

```bash
npm run build
node dist/tests/smoke.js next
node dist/tests/smoke.js kis
node dist/tests/smoke.js kiwoom
```

| 증권사 | 환경변수 |
|---|---|
| `next` | `NEXT_CLIENT_ID`, `NEXT_CLIENT_SECRET` |
| `kis` | `KIS_APPKEY`, `KIS_APPSECRET`, `KIS_CANO`, 주문통보 실측 시 `KIS_HTS_ID` |
| `kiwoom` | `KIWOOM_APPKEY`, `KIWOOM_SECRETKEY` |

미검증 증권사(nh·db·ls·toss·kb)는 계좌가 있는 사용자의 실측 제보로 승격합니다. [새 증권사 요청 이슈](https://github.com/tripleauth-dev/hermetix-securities/issues/new?template=broker-request.md)로 알려주세요.

## 예제 전략

`examples/` 에 Kotlin 공식 전략과 같은 로직 3종이 있습니다. `HERMETIX_BROKER` 로 증권사를 고릅니다.

```bash
npm run build
HERMETIX_BROKER=next NEXT_CLIENT_ID=… NEXT_CLIENT_SECRET=… node dist/examples/larry.js
HERMETIX_BROKER=kis KIS_APPKEY=… KIS_APPSECRET=… KIS_CANO=… node dist/examples/grid.js
HERMETIX_BROKER=kiwoom KIWOOM_APPKEY=… KIWOOM_SECRETKEY=… node dist/examples/trendBreakout.js
```

## 알려진 한계

- 브라켓, 전략 내부 상태, 일일 주문 누적치는 메모리에만 있어 재시작 시 사라집니다
- KIS 모의 서버는 미체결·체결 조회가 없어 어댑터가 주문을 메모리로 추적하고, 재시작 후 남은 미체결은 보이지 않습니다
- KRX 캘린더는 정규장을 합성하므로 공휴일도 개장일로 보이지만 주문은 서버가 거부합니다
- 전략 여러 개가 같은 계좌의 같은 심볼을 다루면 보유·미체결 판단이 겹칩니다

## 문서

- [루트 README](../README.md): 지원 증권사 표, 설계 원칙
- [브로커 팩토리 규약](../docs/broker-factory.md): 자격 증명 키와 증권사별 옵션
- [증권사별 설정과 제약](../docs/brokers.md): 캔들 주기, 실시간 프로토콜
- [전략 작성 가이드](../docs/strategy-guide.md): StrategySpec·StrategyContext·Signal 상세
- [아키텍처](../docs/architecture.md): 틱 파이프라인, 실시간 계층
- [컨포먼스 킷](../conformance/README.md): 골든 픽스처와 새 어댑터 절차
