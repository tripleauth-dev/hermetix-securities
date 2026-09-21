# Hermetix JavaScript/TypeScript

[![npm](https://img.shields.io/npm/v/hermetix.svg)](https://www.npmjs.com/package/hermetix)

증권사 모의투자·실전투자를 하나의 인터페이스로 통일한 트레이딩 프레임워크 — Node.js 구현입니다. 전략 한 클래스만 쓰면 넥스트·한국투자·키움 등 8개 증권사에서 같은 코드로 돌아갑니다.

버전 0.11.1 · Node 22+ · 런타임 의존성은 `decimal.js` 하나(실시간 웹소켓은 Node 22 내장 `WebSocket`) · MIT

## 설치

```bash
npm install hermetix        # ESM 패키지, TypeScript 타입 정의 포함
```

`decimal.js` 는 JS 의 부동소수점(0.1+0.2≠0.3)으로 돈을 계산하지 않기 위한 필수 선택입니다. **금액·수량에 `number` 를 절대 섞지 마세요** — 라이브러리의 모든 가격·수량은 `Decimal` 입니다.

저장소를 받아 개발할 때:

```bash
cd js && npm install
npm run build               # tsc → dist/
```

## 5분 빠른 시작 — 전략 봇

전략은 `Strategy` 인터페이스 하나입니다. `spec` 으로 감시 종목과 호출 주기를 선언하고, `decide` 에서 시그널을 돌려주면 주문 제출·익절/손절·비상정지는 엔진이 맡습니다.

```ts
import hermetix, { Decimal, StrategyEngine, buy } from "hermetix";
import type { Signal, Strategy, StrategyContext, StrategySpec } from "hermetix";

class MyFirstStrategy implements Strategy {
  readonly spec: StrategySpec = {
    name: "my-first",             // clientOrderId 프리픽스 — 영문/숫자/하이픈
    symbols: ["005930"],          // 감시 종목 (접두 없으면 브로커 기본 시장 — kis 는 KRX)
    candleInterval: "1d",         // kis/kiwoom 은 일봉만, next/toss 는 1m 도 가능
    candleLimit: 20,
    pollIntervalSeconds: 60,      // 전략 호출 주기
    regularHoursOnly: true,       // 정규장에만 호출 (기본값)
  };

  decide(ctx: StrategyContext): Signal[] {
    const q = ctx.quote("005930");
    if (!q) return [];
    const closes = ctx.candlesOf("005930").map((c) => c.close);
    if (closes.length < 20) return [];
    const ma20 = closes.reduce((a, b) => a.plus(b), new Decimal(0)).div(closes.length);

    if (q.price.gt(ma20) && !ctx.hasPosition("005930") && !ctx.hasOpenOrder("005930")) {
      return [buy("005930", new Decimal(1), {
        takeProfitPrice: q.price.mul("1.04"),   // 도달 시 엔진이 시장가 청산
        stopLossPrice: q.price.mul("0.98"),
      })];
    }
    return [];
  }
}

const broker = hermetix.kis({ apiKey: process.env.KIS_APPKEY!, apiSecret: process.env.KIS_APPSECRET!, account: process.env.KIS_CANO! });
await new StrategyEngine(broker, [new MyFirstStrategy()]).run();   // Ctrl+C 로 종료
```

- 키는 환경변수로 넣으세요. 키는 항상 당신의 기기에서만 쓰이고, Hermetix 는 어떤 서버로도 키를 보내지 않습니다
- `run()` 은 블로킹 루프입니다. 기동 시 브로커 환경(모의/실전)과 전략의 캔들 주기 지원 여부를 검사해 어긋나면 그 전략은 스케줄하지 않고 로그로 알립니다
- 브로커 전환은 브로커 ID 한 토큰입니다 (`hermetix.next({...})`, `hermetix.kiwoom({...})`). 자격 증명은 모든 브로커가 `{ apiKey, apiSecret, account?, environment? }` 한 모양이고 브로커별 선택 항목은 `hts_id` 같은 snake_case 키로 함께 넘깁니다 — 표는 [docs/broker-factory.md](../docs/broker-factory.md). 전략 코드는 그대로입니다
- 시그널: `buy(symbol, qty, opts)` / `sell(symbol, qty, opts)` / `cancel(orderId)`. `sell` 은 보유 수량으로 자동 클램프됩니다(공매도 방지). `decide` 는 `Promise<Signal[]>` 을 돌려줘도 됩니다

## 연결 계층만 쓰기

봇 없이 시세 수집·대시보드·알림에 브로커 클라이언트만 쓸 수 있습니다. 8개 어댑터가 같은 `BrokerClient` 를 구현합니다. `hermetix.<브로커>({...})` 로 만들고, 설정값으로 고르려면 `client("kiwoom", {...})` 를 씁니다. 기존처럼 `new KiwoomClient(appkey, secretkey)` 로 직접 만들어도 됩니다.

```ts
import hermetix, { Decimal, pnlReport } from "hermetix";

const client = hermetix.kiwoom({ apiKey: process.env.KIWOOM_APPKEY!, apiSecret: process.env.KIWOOM_SECRETKEY! });

const [quote] = await client.getQuotes(["005930"]);
console.log(quote.price.toString(), quote.changeRate?.toString());        // 등락률은 비율 (-0.0347 = -3.47%)

const candles = await client.getCandles("005930", "1d", 30);             // 과거 → 최신 순
const account = await client.getAccount();
const holdings = await client.getHoldings();
const power = await client.getBuyingPower();
console.log(candles.length, account.cash.toString(), holdings.length, power.toString());

const order = await client.createOrder({
  symbol: "005930", side: "BUY", orderType: "LIMIT", quantity: new Decimal(1), limitPrice: new Decimal(60000),
});
console.log((await client.getOrder(order.orderId)).status);
await client.cancelOrder(order.orderId);

console.log(await pnlReport(client));                                      // 총평가·미실현손익 요약
```

브로커별 방언(부호 접두 가격, zero-padded 금액, 다른 응답 봉투)은 어댑터가 정규화하므로 호출 쪽 코드는 동일합니다. 실패는 `BrokerApiError` 계층(`RateLimitError`·`MarketClosedError`·`InsufficientFundsError`·`InvalidOrderError`·`AuthError`·`OrderNotFoundError`)으로 throw 됩니다.

## 브로커 설정

생성자는 위치 인자입니다. 필수 인자 뒤의 선택 인자는 기본값으로 두고 필요한 것만 채우세요.

| ID | 클래스 | 생성자 인자 (순서대로) | 모의 / 실전 | 검증 상태 |
|---|---|---|---|---|
| `next` | `NextClient` | `clientId, clientSecret, accountId="acc_main", baseUrl, environment="PAPER"` | 키 프리픽스로 결정 (`pk_test_` 모의 / `pk_live_` 실전) | ✅ 검증 (공개 스펙 v1.3) |
| `kis` | `KisClient` | `appkey, appsecret, cano, acntPrdtCd="01", baseUrl="", throttleMs=0, environment="PAPER", wsUrl="", htsId=""` | 모의 / 실전 (호스트·TR ID 자동 전환) | ✅ 검증 (모의) |
| `kiwoom` | `KiwoomClient` | `appkey, secretkey, baseUrl="", throttleMs=1100, environment="PAPER", wsUrl=""` | 모의 / 실전 (호스트 자동 전환) | ✅ 검증 (모의) |
| `nh` | `NhClient` | `appKey, appSecret, accountNo="", baseUrl="", authUrl, marketCd="KRX", orderMarketCd="KRX", throttleMs=250, environment="PAPER", wsUrl=""` | 모의 / 실전 (호스트 분리). `accountNo` 비우면 모의(03) 계좌 자동 선택 | ⚠️ 미검증 (문서 기반) |
| `db` | `DbClient` | `appKey, appSecret, baseUrl, macAddress="", marketDivCode="J", throttleMs=500, environment="PAPER", wsUrl=""` | 모의투자용 키면 모의, 실전 키면 실전 (REST 호스트 동일) | ⚠️ 미검증 (문서 기반) |
| `ls` | `LsClient` | `appKey, appSecret, baseUrl, macAddress="", exchGubun="", throttleMs=500, chartThrottleMs=1100, environment="PAPER", wsUrl=""` | 모의투자용 키면 모의, 실전 키면 실전 (REST 호스트 동일) | ⚠️ 미검증 (문서 기반) |
| `toss` | `TossClient` | `clientId, clientSecret, accountSeq="", baseUrl, throttleMs=200, environment="LIVE", wsUrl` | **실전 전용** (샌드박스 없음). `accountSeq` 비우면 첫 위탁계좌 | ⚠️ 미검증 (실전 전용) |
| `kb` | `KbClient` | `appKey, appSecret, baseUrl, excgClsf="1", sorOrderCcd="K", chartMarketClsf="0", throttleMs=100, environment="LIVE"` | **실전 전용** 오픈베타 (모의 "추후 제공") | ⚠️ 미검증 (실전 전용) |

- `wsUrl`(kis·kiwoom·nh·db·ls·toss)은 실시간 웹소켓 주소이고 비우면 환경에 맞는 기본값을 씁니다. `htsId`(kis)는 주문 통보 구독 키입니다 — 아래 실시간 절 참고
- 심볼은 `MARKET:CODE` 접두를 허용합니다 (`KRX:005930`, `US:AAPL`). 토스만 두 시장(KRX·US)을 한 계좌로 다루고, 나머지는 접두 없는 심볼을 브로커 기본 시장으로 해석합니다
- ✅ 검증 = 실서버 스모크(시세→캔들→계좌→주문)를 통과한 환경. ⚠️ 미검증 = 공식 SDK·문서에서 역추적한 구현으로, 네 언어 컨포먼스 테스트는 통과했지만 실측 전이라 스펙 해석 오류가 있을 수 있습니다. 상태는 [루트 README 지원 증권사 표](../README.md#지원-증권사)와 같습니다

## 실전투자로 전환

모의에서 검증한 뒤 실제 계좌로 옮길 때는 환경과 명시 동의를 함께 지정합니다. 하나라도 빠지면 엔진이 전략을 스케줄하지 않습니다 — "모의인 줄 알고 실전 키를 넣은" 사고를 막는 장치입니다.

```ts
import { Decimal, KisClient, StrategyEngine } from "hermetix";
import type { Strategy } from "hermetix";

declare const strategy: Strategy;

const broker = new KisClient(process.env.KIS_APPKEY!, process.env.KIS_APPSECRET!, process.env.KIS_CANO!, "01", "", 0, "LIVE");
await new StrategyEngine(broker, [strategy], 5, {
  liveTradingEnabled: true,                       // 실전 명시 동의
  maxOrderValue: new Decimal(1_000_000),          // 주문 1건 추정 금액(수량 × 가격) 상한 — 브로커 통화
  maxDailyOrderValue: new Decimal(5_000_000),     // 하루(UTC) 누적 주문 금액 상한 — 매수·매도 합산
}).run();
```

- 세 번째 인자 `5` 는 연속 실패 임계치입니다. 도달하면 비상정지(미체결 전량 취소 + 신규 주문 차단). 휴장·레이트리밋은 실패로 세지 않습니다
- 주문 금액 상한을 넘는 시그널은 제출하지 않고 경고만 남깁니다. 누적치는 메모리에만 있어 재시작 시 초기화됩니다
- 토스·KB 는 모의투자가 없어 생성자 기본값이 `"LIVE"` 입니다. `liveTradingEnabled` 와 상한 없이는 기동하지 않으니 소액으로 시작하세요
- KIS·키움 실전은 호스트·TR ID 전환만 구현돼 있고 실계좌 스모크는 아직입니다

## 실시간 스트림

kis·kiwoom·nh·db·ls·toss 는 웹소켓 스트림(`StreamingBrokerClient`)을 제공합니다. 채널은 세 가지 — 체결가(`TRADES`)·10단계 호가(`ORDER_BOOK`)·내 주문 통보(`ORDER_EVENTS`) — 이고 브로커가 `capabilities.streams` 로 선언합니다. next·kb 는 공개 스펙에 웹소켓이 없어 폴링만 됩니다.

### 체결가 트리거

`spec.trigger = "ON_TRADE"` 로 선언하면 체결 틱마다 전략을 호출합니다. 전략 코드는 바뀌지 않습니다.

```ts
import hermetix, { StrategyEngine } from "hermetix";
import type { Signal, Strategy, StrategyContext, StrategySpec } from "hermetix";

class ScalpStrategy implements Strategy {
  readonly spec: StrategySpec = {
    name: "scalp",
    symbols: ["005930"],
    trigger: "ON_TRADE",          // 체결 틱마다 호출
    minTickIntervalMs: 1000,      // 연속 호출 사이 최소 간격 (기본 1000)
    pollIntervalSeconds: 60,      // 스트림이 끊겼을 때의 안전망 주기
  };
  decide(ctx: StrategyContext): Signal[] {
    const q = ctx.quote("005930");   // 마지막 체결 틱 (가격·호가·누적거래량)
    return q ? [] : [];
  }
}

const broker = hermetix.kis({ apiKey: process.env.KIS_APPKEY!, apiSecret: process.env.KIS_APPSECRET!, account: process.env.KIS_CANO! });
await new StrategyEngine(broker, [new ScalpStrategy()]).run();
```

- 몰려온 틱은 하나로 합치고 `minTickIntervalMs` 보다 촘촘히는 부르지 않습니다. 틱이 tick 실행 도중 도착하면 종료 후 간격이 지나고 한 번 더 돕니다
- 스트림 틱이 전략의 모든 심볼을 덮으면 현재가 REST 호출을 건너뜁니다. 캔들·계좌·보유·미체결·매수가능은 여전히 REST 라 틱마다 `1 + 심볼 수 + 3` 호출이 나갑니다 — 모의 서버 레이트리밋(kis 초당 2건)을 생각해 간격을 잡으세요
- 스트림이 끊기면 지수 백오프(1s→30s)로 재접속하고 구독을 복원하며, 그동안 폴링이 안전망으로 돕니다. 스트림을 선언하지 않은 브로커에서는 경고 후 폴링으로 동작합니다

### 호가창

`spec.orderBook = true` 면 심볼 호가창을 구독해 `ctx.orderBook(symbol)` 로 공급합니다. 호가는 전략 호출을 촉발하지 않습니다.

```ts
import { bestAsk, bestBid } from "hermetix";
import type { Signal, Strategy, StrategyContext, StrategySpec } from "hermetix";

class BookStrategy implements Strategy {
  readonly spec: StrategySpec = { name: "book", symbols: ["005930"], trigger: "ON_TRADE", orderBook: true };
  decide(ctx: StrategyContext): Signal[] {
    const book = ctx.orderBook("005930");          // 최우선부터 10단계 asks/bids, 스트림이 아직 안 줬으면 undefined
    if (!book) return [];
    const ask = bestAsk(book), bid = bestBid(book);
    if (ask && bid && ask.quantity.gt(bid.quantity.mul(3))) return [];   // 매도 잔량 우세 → 진입 보류
    return [];
  }
}
```

### 주문 통보

브로커가 `ORDER_EVENTS` 를 선언하면 엔진이 자동 구독합니다. 진입 주문 체결을 서버 조회 없이 브라켓에 반영하고(`BracketMonitor.onOrderEvent`), KIS 모의처럼 주문 조회가 없는 어댑터의 메모리 추적도 즉시 확정합니다(`applyOrderEvent`). 구독에 실패하면 경고만 남기고 체결 판정은 폴링으로 계속합니다.

```ts
import { KisClient } from "hermetix";

// KIS 는 HTS ID 로 구독합니다 (9번째 인자). 통보 프레임은 AES-256-CBC 암호문이며 구독 응답의 key/iv 로 복호화합니다 (Node 내장 crypto)
const broker = new KisClient(process.env.KIS_APPKEY!, process.env.KIS_APPSECRET!, process.env.KIS_CANO!, "01", "", 0, "PAPER", "", process.env.KIS_HTS_ID!);
```

### 엔진 없이 직접 구독

```ts
import { TossClient, bestAsk, bestBid } from "hermetix";

const broker = new TossClient(process.env.TOSS_CLIENT_ID!, process.env.TOSS_CLIENT_SECRET!);   // 또는 KisClient / NhClient / DbClient / LsClient
const stream = broker.openStream();
stream.subscribeTrades(["KRX:005930", "US:AAPL"], (t) => console.log(t.symbol, t.price.toString(), t.quantity.toString()));
stream.subscribeOrderBook(["KRX:005930"], (b) => console.log(bestAsk(b)?.price.toString(), bestBid(b)?.price.toString()));
stream.subscribeOrderEvents((e) => console.log(e.type, e.orderId, e.quantity?.toString(), e.price?.toString()));
stream.connect();                                  // 구독은 연결 전에 등록해도 되고, 재접속 시 자동 복원
// ...
stream.close();                                    // 이후 재접속하지 않음
```

리스너는 이벤트 루프에서 호출되고, 리스너가 던진 예외는 스트림이 삼키고 로그만 남깁니다. 심볼은 구독할 때 쓴 표기 그대로 돌아옵니다(`KRX:005930` 으로 구독하면 `KRX:005930`).

### 브로커별 실시간 상태와 제약

| 브로커 | 체결가 | 호가 | 주문 통보 | 제약·비고 |
|---|---|---|---|---|
| `kis` | ✅ 실측 `H0STCNT0` | ✅ 실측 `H0STASP0` | ⚠️ 문서 기반 `H0STCNI9/0` | 접속키(`/oauth2/Approval`)로 구독, 모의 `ws://ops…:31000`. 통보는 `htsId` 필수 |
| `kiwoom` | ✅ 실측 `0B` | ✅ 실측 `0D` | ⚠️ 문서 기반 `00` | REST 토큰으로 LOGIN 후 REG. 모의 `wss://mockapi…:10000/api/dostk/websocket` |
| `nh` | ⚠️ 문서 기반 | ⚠️ 문서 기반 | ⚠️ 문서 기반 `d2`+`d3` | 채널은 `marketCd` 로 결정 (KRX `oc/ob`, NXT `nc/nb`, UNT `mc/mb`). 모의 서버는 시세 "미제공" 표기라 통보만 올 수 있음. 세션당 등록 10건(SDK 실측)/30건(공식), 앱키당 세션 2개 |
| `db` | ⚠️ 문서 기반 `S00` | ⚠️ 문서 기반 `S01` | ⚠️ 문서 기반 `IS0`+`IS1` | 접속 후 10초 안에 첫 전송(엔진은 즉시 구독). 계좌당 세션 2개·종목 50개. 통보 등록은 해제 없음 |
| `ls` | ⚠️ 문서 기반 `S3_`+`K3_` | ⚠️ 문서 기반 `H1_`+`HA_` | ⚠️ 문서 기반 `SC0`~`SC4` | 서버가 시장을 판별하지 않아 KOSPI·KOSDAQ TR 을 둘 다 등록(종목당 2건). 토큰 익일 07:00 만료 |
| `toss` | ⚠️ AsyncAPI 기반 | ⚠️ AsyncAPI 기반 | ⚠️ AsyncAPI 기반 `personal:order` | **실전 전용**. 배열 하나가 구독 집합 전체(선언형). 계정당 연결 2개·구독 100개·선언 5회/초. 180초 무송신 시 서버가 끊어 60초마다 텍스트 `PING`. 핸드셰이크 `Authorization: Bearer` 는 Node 22 내장 WebSocket(undici)의 `headers` 옵션으로 싣습니다. 체결 틱에 누적거래량·등락 없음 |
| `next`, `kb` | ❌ | ❌ | ❌ | 공개 스펙에 웹소켓 없음 — 폴링만 |

✅ 실측 = 2026-09 모의 웹소켓 장중 실측 통과. ⚠️ 문서 기반 = 공식 문서·SDK·AsyncAPI 예시로 구현했고 픽스처 파서 테스트는 통과했지만 실측 전(`conformance/fixtures/*.json#stream` 의 `measured: false`). 각 스트림 클래스 상단 주석에 프로토콜 근거와 미확정 항목을 적었습니다.

## 수익률

```ts
import { Decimal, NextClient, pnlReport } from "hermetix";

const client = new NextClient(process.env.NEXT_CLIENT_ID!, process.env.NEXT_CLIENT_SECRET!);
const report = await pnlReport(client, new Decimal(10000));   // 시작 자금을 주면 totalReturnRate 계산
console.log(report.portfolioValue.toString(), report.totalUnrealizedPnl.toString(), report.totalReturnRate?.toString());
```

`PnlReport` 는 총평가·예수금·보유 평가금액·미실현손익·(선택) 총수익률과 보유 목록을 담습니다.

## 사용량 텔레메트리

SDK 는 **어느 증권사가 얼마나 쓰이는지**를 시간 단위로 합산해 60초마다 `hermetix-service` 로 보냅니다. 이 데이터로 증권사 사용량 랭킹을 공개합니다. 끄는 설정은 없고(항상 켜짐), 대신 보내는 내용을 그대로 공개합니다 — 계약은 [docs/telemetry.md](../docs/telemetry.md).

| 보내는 것 | 보내지 않는 것 |
|---|---|
| 브로커 ID, 환경(모의/실전), 호출 종류별 성공·에러 건수(에러는 분류만), 응답 시간 p50·p95, 실시간 채널별 구독·메시지·재접속 수, SDK 언어·버전, 설치 단위 무작위 ID(`~/.hermetix/installation-id`) | 종목·수량·가격·금액, 주문번호·계좌번호, API 키·토큰·HTS ID, 전략 이름, IP(서버가 저장하지 않음), OS·호스트명 |

전송은 매매 경로와 분리된 unref 타이머에서 일어나고, 실패는 조용히 버립니다(재시도·큐 없음). 모든 요청은 계약의 공유 키로 HMAC-SHA256 서명(`X-Hermetix-Signature`)을 실어 보냅니다 — 스팸·스캐너를 거르는 문턱이며, 공개 SDK 라 비밀 인증은 아닙니다([계약 "요청 서명"](../docs/telemetry.md)). 새 의존성은 없습니다(`fetch`). 테스트에서는 `UsageTelemetry.transport` 를 교체하고 `UsageTelemetry.flushNow()`/`drain()` 으로 페이로드를 확인할 수 있습니다.

## 검증

```bash
cd js && npm install
npm test                                  # 오프라인: 골든 픽스처 재생 + 가짜 웹소켓 서버 + 엔진 테스트
```

- 컨포먼스: 8개 어댑터가 `conformance/fixtures/{broker}.json` 을 가짜 HTTP 로 재생해 공통 시나리오(시세→캔들→캘린더→계좌→보유→매수가능→주문→조회→취소→체결)를 통과합니다. 새 어댑터를 만들면 `verifyBrokerConformance(broker, scenario)` 로 같은 검사를 돌릴 수 있습니다
- 실시간: 픽스처의 `stream` 섹션(체결가·호가·주문통보 프레임)으로 파서를, 로컬 `ws` 서버로 구독·재접속·에코 흐름을 검증합니다

실서버 스모크(수동, 키는 환경변수):

```bash
node dist/tests/smoke.js next      # 시세→캔들→캘린더→계좌→보유→매수가능→주문→취소→PnL
node dist/tests/smoke.js kis
node dist/tests/smoke.js kiwoom
```

| 브로커 | 환경변수 |
|---|---|
| `next` | `NEXT_CLIENT_ID`, `NEXT_CLIENT_SECRET` |
| `kis` | `KIS_APPKEY`, `KIS_APPSECRET`, `KIS_CANO` (+ 주문 통보 실측 시 `KIS_HTS_ID`) |
| `kiwoom` | `KIWOOM_APPKEY`, `KIWOOM_SECRETKEY` |

스모크는 체결되지 않을 지정가(현재가의 80%)로 1주 매수 후 즉시 취소합니다. 모의 계좌에서만 쓰세요.

⚠️ 미검증 브로커(nh·db·ls·toss·kb)와 문서 기반 스트림은 해당 증권사 계좌가 있는 사용자의 실측 제보로 승격합니다. 계좌가 있으시면 [새 브로커 요청 이슈](https://github.com/tripleauth-dev/hermetix-securities/issues/new?template=broker-request.md)로 실측 결과를 알려 주세요 — 픽스처를 실측 프레임으로 교체하고 README 상태를 올립니다.

## 공식 전략 예제

`examples/` 에 Kotlin 전략 레포 3종과 같은 로직이 있습니다. `HERMETIX_BROKER` 로 브로커를 고릅니다(next·kis·kiwoom).

```bash
npm run build
HERMETIX_BROKER=next NEXT_CLIENT_ID=... NEXT_CLIENT_SECRET=... node dist/examples/larry.js          # 래리 윌리엄스 변동성 돌파
HERMETIX_BROKER=kis  KIS_APPKEY=... KIS_APPSECRET=... KIS_CANO=... node dist/examples/grid.js       # 목표가 스캘핑 (캔들 미사용, KRX 호환)
HERMETIX_BROKER=kiwoom KIWOOM_APPKEY=... KIWOOM_SECRETKEY=... node dist/examples/trendBreakout.js  # 추세 돌파
```

## 알려진 한계

- 브라켓(익절/손절)·전략 내부 상태·일일 주문 누적치는 메모리에만 있어 재시작 시 사라집니다. 전략이 기동 시 보유 포지션을 다시 점검하는 패턴을 권장합니다
- KIS 모의 서버는 미체결·체결 조회를 제공하지 않아 어댑터가 주문을 메모리로 추적하고 체결은 보유 수량 변화로 근사합니다(주문 통보를 구독하면 즉시 확정). 재시작 후 남은 미체결은 보이지 않습니다
- kis·kiwoom·nh·db·ls·kb 는 일봉만, next·toss 는 1분봉·일봉을 지원합니다. KRX 캘린더는 정규장(평일 09:00–15:30 KST)을 합성하므로 공휴일은 개장일로 보이지만 주문은 서버가 거부합니다
- 전략 여러 개가 같은 계좌의 같은 심볼을 다루면 보유·미체결 판단이 겹칩니다. 전략당 심볼을 나누세요
- 실시간은 체결가·호가·주문통보 세 채널뿐이며 next·kb 는 웹소켓이 없습니다. 문서 기반 스트림의 프로토콜 세부(응답 프레임 형태, 세션 한도)는 실측 전입니다

## 문서

- [루트 README](../README.md) — 지원 브로커 표, 설계 원칙, 로드맵
- [아키텍처](../docs/architecture.md) — 틱 파이프라인, 실시간 계층, 어댑터 비교표, 상태 지도 (Kotlin 기준이지만 동작은 동일)
- [전략 작성 가이드](../docs/strategy-guide.md) — StrategySpec·StrategyContext·Signal 상세, 자주 쓰는 패턴, 트러블슈팅
- [컨포먼스 킷](../conformance/README.md) — 골든 픽스처 형식과 새 어댑터 추가 절차
- [브로커 조사 보고서](../claudedocs/korean-broker-openapi-survey-2026-09.md) — 국내 증권사 오픈 API 실태 (2026-09)
