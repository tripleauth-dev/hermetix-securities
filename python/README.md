# Hermetix Python

[![PyPI](https://img.shields.io/pypi/v/hermetix.svg)](https://pypi.org/project/hermetix/)

국내 증권사 오픈 API 를 하나의 인터페이스로 쓰는 Hermetix 의 Python 구현입니다. Kotlin 레퍼런스와 같은 증권사 어댑터 8종, 같은 전략 규약, 같은 안전장치를 제공합니다.

| 항목 | 값 |
|---|---|
| 버전 | 0.11.1 |
| Python | 3.10 이상 |
| 런타임 의존성 | 없음 |
| 실시간 스트림 | `websockets`, `cryptography` (선택 설치) |
| 라이선스 | MIT |

금액과 수량은 전부 `Decimal` 입니다. float 를 섞지 마세요.

## 설치

```bash
pip install hermetix
pip install 'hermetix[stream]'
pip install -e './python[dev]'
```

첫 줄이 코어, 둘째 줄이 실시간 스트림 포함, 셋째 줄이 저장소를 클론해 개발할 때입니다.

## 증권사 고르기

증권사는 ID 로 고르고, 자격 증명 키는 어느 증권사든 `api_key`, `api_secret`, `account`, `environment` 네 가지입니다. 증권사별 추가 옵션은 키워드 인자로 넘깁니다.

```python
import hermetix

client = hermetix.next(api_key="pk_test_...", api_secret="sk_test_...", account="acc_main")
client = hermetix.kis(api_key="...", api_secret="...", account="12345678", hts_id="my-hts-id")
client = hermetix.kiwoom(api_key="...", api_secret="...", environment="LIVE")
client = hermetix.client("toss", api_key="...", api_secret="...", account="1")
```

| 증권사 | `account` | 추가 옵션 |
|---|---|---|
| `next` | account_id, 기본 `acc_main` | `base_url` |
| `kis` | cano, 필수 | `acnt_prdt_cd` `custtype` `hts_id` `base_url` `ws_url` `throttle_seconds` |
| `kiwoom` | 없음 | `base_url` `ws_url` `throttle_seconds` |
| `nh` | account_no | `auth_url` `market_cd` `order_market_cd` `base_url` `ws_url` `throttle_seconds` |
| `ls` | 없음 | `mac_address` `exch_gubun` `chart_throttle_seconds` `base_url` `ws_url` `throttle_seconds` |
| `db` | 없음 | `mac_address` `market_div_code` `base_url` `ws_url` `throttle_seconds` |
| `toss` | account_seq | `base_url` `ws_url` `throttle_seconds` |
| `kb` | 없음 | `excg_clsf` `sor_order_ccd` `chart_market_clsf` `base_url` `throttle_seconds` |

`hermetix.brokers` 가 ID 목록입니다. 모르는 옵션, 빈 키, kis 의 빈 `account` 는 `ValueError` 입니다. `KisClient(appkey=..., appsecret=..., cano=...)` 처럼 클래스를 직접 만들어도 됩니다. 규약은 [브로커 팩토리 규약](../docs/broker-factory.md), 각 옵션의 뜻은 [증권사별 설정과 제약](../docs/brokers.md)에 있습니다.

## 5분 빠른 시작: 전략 봇

처음이라면 [hermetix-strategy-template/python](https://github.com/tripleauth-dev/hermetix-strategy-template/tree/main/python) 을 복제하세요. 아래 예제 전략과 테스트가 들어 있습니다.

전략은 `Strategy` 를 상속해 `spec` 과 `decide()` 만 채웁니다. 엔진이 정규장에만 `decide()` 를 부르고, 시그널을 주문으로 바꾸고, 익절과 손절을 대신 실행합니다.

```python
import os
from decimal import Decimal

import hermetix
from hermetix import Buy, CandleInterval, Strategy, StrategyEngine, StrategySpec


class Ma20Strategy(Strategy):
    spec = StrategySpec(
        name="ma20",
        symbols=["005930"],
        candle_interval=CandleInterval.DAY_1,
        candle_limit=20,
        poll_interval_seconds=60,
    )

    def decide(self, ctx):
        quote = ctx.quote("005930")
        candles = ctx.candles_of("005930")
        if not quote or len(candles) < 20:
            return []
        if ctx.has_position("005930") or ctx.has_open_order("005930"):
            return []
        ma20 = sum(c.close for c in candles) / 20
        if quote.price > ma20:
            return [Buy("005930", Decimal(1),
                        take_profit_price=quote.price * Decimal("1.04"),
                        stop_loss_price=quote.price * Decimal("0.98"))]
        return []


broker = hermetix.kis(api_key=os.environ["KIS_APPKEY"], api_secret=os.environ["KIS_APPSECRET"],
                      account=os.environ["KIS_CANO"])
StrategyEngine(broker, [Ma20Strategy()]).run()
```

- `run()` 은 블로킹 루프이고 Ctrl+C 로 끝납니다
- `hermetix.kis` 를 `hermetix.next` 로 바꾸면 증권사가 바뀌고 전략 코드는 그대로입니다
- KRX 증권사는 일봉만 지원하고 넥스트와 토스는 1분봉도 됩니다. 비호환 조합은 기동 시 걸러냅니다
- 매도는 보유 수량으로 클램프되고, 연속 실패 5회면 미체결을 전량 취소하고 주문을 막습니다
- 키는 환경변수로 주입하세요. 키는 당신의 기기에서만 쓰입니다

## 연결 계층만 쓰기

봇 없이 시세 수집이나 대시보드에 쓸 때는 클라이언트를 직접 호출합니다. 8개 증권사가 같은 메서드와 같은 모델을 돌려줍니다.

```python
import os
from decimal import Decimal

import hermetix
from hermetix import CandleInterval, CreateOrderRequest, OrderSide, OrderType

client = hermetix.kiwoom(api_key=os.environ["KIWOOM_APPKEY"], api_secret=os.environ["KIWOOM_SECRETKEY"])

quote = client.get_quotes(["005930"])[0]
candles = client.get_candles("005930", CandleInterval.DAY_1, limit=30)
account = client.get_account()
holdings = client.get_holdings()
buying_power = client.get_buying_power()

order = client.create_order(CreateOrderRequest(
    symbol="005930", side=OrderSide.BUY, order_type=OrderType.LIMIT,
    quantity=Decimal(1), limit_price=quote.bid_price or quote.price,
))
print(client.get_order(order.order_id).status)
client.cancel_order(order.order_id)
open_orders = [o for o in client.get_orders() if o.status.is_open]
fills = client.get_fills()
calendar = client.get_calendar()
```

`Quote` 는 `price`, `bid_price`, `ask_price`, `volume`, `change_rate` 를, `Holding` 은 `symbol`, `quantity`, `avg_entry_price` 를 가집니다. 캔들은 과거에서 최신 순입니다.

실패는 `hermetix.errors` 의 타입으로 옵니다. `RateLimitError`, `MarketClosedError`, `InsufficientFundsError`, `InvalidOrderError`, `AuthError`, `OrderNotFoundError`, 그 외 `BrokerApiError` 입니다.

## 실전투자로 전환

환경과 명시 동의를 함께 지정합니다. 하나라도 빠지면 엔진이 전략을 스케줄하지 않습니다.

```python
import os
from decimal import Decimal

import hermetix
from hermetix import StrategyEngine

broker = hermetix.kis(api_key=os.environ["KIS_APPKEY"], api_secret=os.environ["KIS_APPSECRET"],
                      account=os.environ["KIS_CANO"], environment="LIVE")
engine = StrategyEngine(
    broker, [],
    live_trading_enabled=True,
    max_order_value=Decimal(1_000_000),
    max_daily_order_value=Decimal(5_000_000),
)
```

- `environment="LIVE"` 로 호스트와 TR ID 가 실전으로 바뀝니다
- `live_trading_enabled` 가 실전 명시 동의입니다
- `max_order_value` 는 주문 1건, `max_daily_order_value` 는 하루 누적 상한이며 모의투자에서도 적용됩니다
- 토스와 KB 는 모의투자가 없어 실계좌로만 쓸 수 있으니 상한을 더 작게 잡으세요
- 넥스트증권은 키 프리픽스가 환경을 결정하며, `environment` 와 어긋나면 `ValueError` 입니다

## 실시간 스트림

`pip install 'hermetix[stream]'` 이 필요합니다. `websockets` 가 없으면 `open_stream().connect()` 가 `ImportError` 를 냅니다.

### 체결가 트리거

`trigger` 만 바꾸면 체결 틱마다 `decide()` 가 호출됩니다.

```python
from hermetix import Strategy, StrategySpec, TickTrigger


class Scalper(Strategy):
    spec = StrategySpec(
        name="scalp", symbols=["005930"],
        trigger=TickTrigger.ON_TRADE,
        min_tick_interval_seconds=1.0,
        poll_interval_seconds=60,
        order_book=True,
    )

    def decide(self, ctx):
        book = ctx.order_book("005930")
        if book and book.best_ask and book.best_bid:
            spread = book.best_ask.price - book.best_bid.price
        return []
```

- 몰려온 틱은 하나로 합치고, `min_tick_interval_seconds` 는 실행 직전에 다시 확인합니다
- 스트림이 끊기면 재접속하는 동안 `poll_interval_seconds` 폴링이 이어받습니다
- 틱이 모든 심볼을 덮으면 `ctx.quote()` 는 REST 대신 마지막 체결 틱입니다. 캔들·계좌·미체결은 여전히 REST 라 틱당 여러 호출이 나갑니다
- `order_book=True` 면 `ctx.order_book(symbol)` 로 10단계 호가창을 받습니다. `asks`, `bids`, `best_ask`, `best_bid` 가 있고 각 단계는 `price` 와 `quantity` 입니다
- 주문통보는 엔진이 자동 구독해 체결을 브라켓에 바로 반영합니다. KIS 는 `hts_id` 가 필요합니다
- 웹소켓이 없는 증권사(`next`, `kb`)는 폴링으로 동작합니다

### 엔진 없이 직접 구독

```python
import os
import time

import hermetix

client = hermetix.kis(api_key=os.environ["KIS_APPKEY"], api_secret=os.environ["KIS_APPSECRET"],
                      account=os.environ["KIS_CANO"], hts_id=os.environ.get("KIS_HTS_ID", ""))

stream = client.open_stream()
stream.subscribe_trades(["005930", "000660"], lambda t: print("체결", t.symbol, t.price, t.quantity))
stream.subscribe_order_book(["005930"], lambda b: print("호가", b.best_ask, b.best_bid))
if os.environ.get("KIS_HTS_ID"):
    stream.subscribe_order_events(lambda e: print("주문", e.type, e.order_id, e.quantity, e.price))
stream.connect()
try:
    time.sleep(60)
finally:
    stream.close()
```

`connect()` 는 즉시 반환하고 백그라운드 스레드에서 접속과 재접속을 처리합니다. 리스너는 그 스레드에서 불리므로 오래 걸리는 일은 넣지 마세요. 리스너 예외는 로그만 남깁니다.

증권사별 채널 상태와 프로토콜 제약은 [증권사별 설정과 제약](../docs/brokers.md#실시간-스트림--증권사별-상태와-프로토콜)에 있습니다. 프레임 파서는 `hermetix.brokers.*_stream` 의 `parse_*` 함수이며 네 언어가 같은 골든 픽스처로 검증합니다.

## 수익률

```python
from decimal import Decimal

from hermetix import pnl_report

report = pnl_report(broker, initial_capital=Decimal(20_000_000))
print(report["portfolio_value"], report["total_unrealized_pnl"], report["total_return_rate"])
```

`cash`, `portfolio_value`, `total_market_value`, `total_unrealized_pnl`, `total_return_rate`, `holdings` 를 돌려줍니다.

## 사용량 텔레메트리

SDK 는 어느 증권사의 어떤 기능이 얼마나 쓰이는지를 합산해 보내고, 그 데이터로 [사용량 랭킹](https://hermetix-api-prod.tripleauth.com/rankings.html)을 공개합니다. 항상 켜져 있고 표준 라이브러리만 씁니다. 보내는 것과 보내지 않는 것은 [루트 README](../README.md#사용량-데이터)와 [계약](../docs/telemetry.md)에 있습니다.

## 검증

```bash
pip install -e './python[dev]'
pytest python/tests/
python python/tests/smoke.py kis
```

`pytest` 는 오프라인입니다. 스모크는 실서버에 붙으며 `next`, `kis`, `kiwoom` 중 하나를 인자로 받습니다.

| 증권사 | 환경변수 |
|---|---|
| `next` | `NEXT_CLIENT_ID`, `NEXT_CLIENT_SECRET` |
| `kis` | `KIS_APPKEY`, `KIS_APPSECRET`, `KIS_CANO` |
| `kiwoom` | `KIWOOM_APPKEY`, `KIWOOM_SECRETKEY` |

새 어댑터는 `verify_broker_conformance(client, ConformanceScenario(symbol, limit_price, quantity))` 로 표준 시나리오를 통과시키세요. 미검증 증권사의 실측 응답은 [새 증권사 요청 이슈](../.github/ISSUE_TEMPLATE/broker-request.md)로 보내주시면 픽스처를 교체합니다.

## 예제

`examples/` 의 단일 파일 전략입니다. `HERMETIX_BROKER` 로 증권사(`next`, `kis`, `kiwoom`)를 고르고 위 환경변수로 키를 줍니다.

| 파일 | 전략 |
|---|---|
| `examples/larry.py` | 변동성 돌파 |
| `examples/trend_breakout.py` | WMA 추세선 돌파 |
| `examples/grid.py` | 목표가 스캘핑 |

```bash
HERMETIX_BROKER=kis KIS_APPKEY=... KIS_APPSECRET=... KIS_CANO=... python examples/grid.py
```

## 알려진 한계

- 브라켓, KIS 메모리 주문 추적, 일일 누적 주문 금액은 메모리에만 있어 재시작 시 사라집니다
- KRX 캘린더는 평일 정규장을 합성한 것이라 공휴일이 개장일로 보입니다. 그날은 `MarketClosedError` 로 조용히 넘어갑니다
- 단일 계좌 전제라 여러 전략이 같은 심볼을 다루면 보유·미체결 판단이 겹칩니다
- 문서 기반 어댑터(`nh`, `db`, `ls`, `toss`, `kb`)와 모든 주문통보는 스펙 해석 오류가 있을 수 있습니다

## 문서

- [루트 README](../README.md): 지원 증권사, Kotlin 사용법
- [브로커 팩토리 규약](../docs/broker-factory.md)
- [증권사별 설정과 제약](../docs/brokers.md)
- [전략 작성 가이드](../docs/strategy-guide.md)
- [아키텍처](../docs/architecture.md)
- [컨포먼스 킷](../conformance/README.md)
- [로드맵](../ROADMAP.md)
