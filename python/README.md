# Hermetix Python

[![PyPI](https://img.shields.io/pypi/v/hermetix.svg)](https://pypi.org/project/hermetix/)

국내외 증권사 모의투자·실전투자를 하나의 인터페이스로 통일한 트레이딩 프레임워크의 Python 구현입니다. Kotlin 레퍼런스와 같은 브로커 어댑터 8종, 같은 전략 규약, 같은 안전장치를 제공하며 같은 골든 픽스처로 동작 일치를 검증합니다.

| 항목 | 값 |
|---|---|
| 버전 | 0.11.1 |
| Python | 3.10 이상 |
| 런타임 의존성 | 0개 (표준 라이브러리만). 실시간 스트림만 선택 설치 `pip install 'hermetix[stream]'` → `websockets` + `cryptography` |
| 라이선스 | MIT |

금액·수량은 전부 `Decimal` 입니다. float 를 섞지 마세요.

## 설치

```bash
pip install hermetix                 # 코어 (REST 폴링 봇·연결 계층)
pip install 'hermetix[stream]'       # + 웹소켓 실시간 스트림 (websockets, cryptography)
pip install -e './python[dev]'       # 저장소를 클론해 개발할 때 (pytest 포함)
```

## 브로커 고르기 — `hermetix.next(...)`

ccxt 의 `ccxt.binance(...)` 처럼 **브로커 ID 한 토큰만 바꾸면** 증권사가 바뀝니다. 자격 증명은 어느 브로커든 `api_key`·`api_secret`·`account`·`environment` 네 가지 모양이고, 브로커별 선택 항목은 키워드 인자(`extra`)로 넘깁니다. 규약은 [docs/broker-factory.md](../docs/broker-factory.md).

```python
import hermetix

client = hermetix.next(api_key="pk_test_...", api_secret="sk_test_...", account="acc_main")
client = hermetix.kis(api_key=..., api_secret=..., account="12345678", hts_id="my-hts-id")  # 주문 통보용 hts_id 는 extra
client = hermetix.kiwoom(api_key=..., api_secret=..., environment="LIVE")
client = hermetix.client("toss", api_key=..., api_secret=..., account="1")                  # ID 문자열로 고를 때
hermetix.brokers   # ('next', 'kis', 'kiwoom', 'nh', 'ls', 'db', 'toss', 'kb')
```

| 브로커 | `account` | extra |
|---|---|---|
| next | account_id (기본 `acc_main`) | `base_url` |
| kis | cano (필수) | `acnt_prdt_cd` `custtype` `hts_id` `base_url` `ws_url` `throttle_seconds` |
| kiwoom | — | `base_url` `ws_url` `throttle_seconds` |
| nh | account_no | `auth_url` `market_cd` `order_market_cd` + 공통 |
| ls | — | `mac_address` `exch_gubun` `chart_throttle_seconds` + 공통 |
| db | — | `mac_address` `market_div_code` + 공통 |
| toss | account_seq | 공통 |
| kb | — | `excg_clsf` `sor_order_ccd` `chart_market_clsf` `base_url` `throttle_seconds` |

모르는 extra 키·빈 `api_key`/`api_secret`·kis 의 빈 `account` 는 `ValueError` 입니다. 기존 클래스 직접 생성(`KisClient(appkey=..., appsecret=..., cano=...)`)도 그대로 됩니다.

## 5분 빠른 시작 — 전략 봇

전략은 `Strategy` 를 상속해 `spec` 과 `decide()` 만 채우면 됩니다. 엔진이 정규장 중에만 주기적으로 스냅샷을 만들어 `decide()` 를 부르고, 돌려준 시그널을 주문으로 바꿉니다. 익절·손절은 `Buy` 에 가격만 적으면 엔진이 대신 청산합니다.

```python
import os
from decimal import Decimal

import hermetix
from hermetix import Buy, CandleInterval, Strategy, StrategyEngine, StrategySpec


class Ma20Strategy(Strategy):
    spec = StrategySpec(
        name="ma20",                       # clientOrderId 프리픽스 — 영문·숫자·하이픈
        symbols=["005930"],                # 감시 종목 (접두 없으면 브로커 기본 시장 = KRX)
        candle_interval=CandleInterval.DAY_1,
        candle_limit=20,
        poll_interval_seconds=60,          # 전략 호출 주기
        regular_hours_only=True,           # 정규장(09:00–15:30 KST)에만 호출
    )

    def decide(self, ctx):
        q = ctx.quote("005930")
        candles = ctx.candles_of("005930")
        if not q or len(candles) < 20 or ctx.has_position("005930") or ctx.has_open_order("005930"):
            return []
        ma20 = sum(c.close for c in candles) / 20
        if q.price > ma20:
            return [Buy("005930", Decimal(1),
                        take_profit_price=q.price * Decimal("1.04"),   # 익절 — 엔진이 자동 매도
                        stop_loss_price=q.price * Decimal("0.98"))]    # 손절
        return []


broker = hermetix.kis(api_key=os.environ["KIS_APPKEY"], api_secret=os.environ["KIS_APPSECRET"],
                      account=os.environ["KIS_CANO"])                   # 모의투자 기본. kis → next/kiwoom/… 만 바꾸면 증권사 전환
StrategyEngine(broker, [Ma20Strategy()]).run()                         # 블로킹 루프, Ctrl+C 로 종료
```

- 키는 코드에 적지 말고 환경변수로 주입하세요. 키는 항상 당신의 기기에서만 쓰이며 Hermetix 는 어떤 서버로도 키를 보내지 않습니다.
- 브로커 전환은 클라이언트 교체 한 줄입니다 (아래 [브로커 설정 표](#브로커-설정-표)). 전략 코드는 그대로입니다.
- KRX 브로커는 일봉(`DAY_1`)만 지원하고 넥스트·토스는 `MIN_1` 도 됩니다. 비호환 조합은 엔진이 기동 시 걸러내고 이유를 로그에 남깁니다.
- `Sell` 은 보유 수량으로 자동 클램프되고(공매도 방지), 연속 실패가 5회에 이르면 비상정지(미체결 전량 취소 + 신규 주문 차단)됩니다.

## 연결 계층만 쓰기

봇 없이 시세 수집·대시보드·알림에 쓸 때는 브로커 클라이언트를 직접 호출합니다. 8개 브로커가 같은 메서드와 같은 모델을 돌려줍니다.

```python
import os
from decimal import Decimal

import hermetix
from hermetix import CandleInterval, CreateOrderRequest, OrderSide, OrderType

client = hermetix.kiwoom(api_key=os.environ["KIWOOM_APPKEY"], api_secret=os.environ["KIWOOM_SECRETKEY"])
# 다른 증권사는 kiwoom → kis / next / nh / ls / db / toss / kb 만 바꾼다. 기존 KiwoomClient(appkey=..., secretkey=...) 도 된다

quote = client.get_quotes(["005930"])[0]                       # Quote: price, bid_price, ask_price, volume, change_rate
candles = client.get_candles("005930", CandleInterval.DAY_1, limit=30)   # 과거 → 최신
account = client.get_account()                                 # cash, portfolio_value, currency
holdings = client.get_holdings()                               # [Holding(symbol, quantity, avg_entry_price, ...)]
buying_power = client.get_buying_power()                       # Decimal

order = client.create_order(CreateOrderRequest(
    symbol="005930", side=OrderSide.BUY, order_type=OrderType.LIMIT,
    quantity=Decimal(1), limit_price=quote.bid_price or quote.price,
))
print(client.get_order(order.order_id).status)                 # OrderStatus.SUBMITTED / FILLED / ...
client.cancel_order(order.order_id)
open_orders = [o for o in client.get_orders() if o.status.is_open]
fills = client.get_fills()
calendar = client.get_calendar()                               # [MarketDay(date, open, regular, timezone)]
```

실패는 `hermetix.errors` 의 타입으로 옵니다: `RateLimitError`(어댑터가 백오프 재시도 후 소진 시), `MarketClosedError`, `InsufficientFundsError`, `InvalidOrderError`, `AuthError`, `OrderNotFoundError`, 그 외 `BrokerApiError`.

## 브로커 설정 표

| ID | 클래스 | 필수 인자 | 주요 선택 인자 | 모의 / 실전 | 상태 |
|---|---|---|---|---|---|
| `next` | `NextClient` | `client_id`, `client_secret` | `account_id`, `base_url`, `environment` (키 프리픽스 `pk_test_`/`pk_live_` 와 일치해야 함) | ✅ / ✅ | ✅ 검증 (공개 스펙 v1.3) |
| `kis` | `KisClient` | `appkey`, `appsecret`, `cano` | `acnt_prdt_cd`, `custtype`, `environment`, `throttle_seconds`, `ws_url`, `hts_id` (주문 통보 구독용) | ✅ / ✅ 호스트·TR 자동 전환 | ✅ 검증 (모의) |
| `kiwoom` | `KiwoomClient` | `appkey`, `secretkey` | `environment`, `throttle_seconds`, `ws_url` | ✅ / ✅ 호스트 자동 전환 | ✅ 검증 (모의) |
| `nh` | `NhClient` | `app_key`, `app_secret` | `account_no` (비우면 모의 계좌 자동 선택), `market_cd` (`KRX`/`NXT`/`UNT` — 실시간 채널도 따라감), `order_market_cd`, `environment`, `ws_url` | ✅ / ✅ | ⚠️ 문서 기반 미검증 |
| `db` | `DbClient` | `app_key`, `app_secret` | `mac_address`, `market_div_code`, `environment` (모의 키면 모의, 호스트 동일), `ws_url` | ✅ / ✅ | ⚠️ 문서 기반 미검증 |
| `ls` | `LsClient` | `app_key`, `app_secret` | `mac_address`, `exch_gubun`, `chart_throttle_seconds`, `environment`, `ws_url` | ✅ / ✅ | ⚠️ 문서 기반 미검증 |
| `toss` | `TossClient` | `client_id`, `client_secret` | `account_seq` (비우면 첫 위탁계좌), `ws_url` | ❌ 샌드박스 없음 / ✅ | ⚠️ 실전 전용, 미검증 |
| `kb` | `KbClient` | `app_key`, `app_secret` | `excg_clsf`, `sor_order_ccd`, `chart_market_clsf` (차트 종목 시장 0 KOSPI / 1 KOSDAQ) | ❌ "추후 제공" / ✅ 오픈베타 | ⚠️ 실전 전용, 미검증 |

- ✅ 검증 = 모의 서버 스모크(시세→캔들→계좌→주문 전 구간) 통과. ⚠️ 미검증 = 공식 SDK·문서에서 역추적한 구현으로, 컨포먼스 픽스처는 통과했지만 실측 전이라 스펙 해석 오류가 있을 수 있습니다.
- `environment` 기본값은 `TradingEnvironment.PAPER`, 토스·KB 만 `LIVE` 입니다. `ws_url` 은 비우면 환경에 맞는 기본 주소를 씁니다.
- 심볼에 시장 접두를 붙일 수 있습니다 (`KRX:005930`, `US:AAPL`). 접두 없는 심볼은 브로커 기본 시장으로 해석되고, 컨텍스트 조회는 접두 유무를 무시합니다.

## 실전투자로 전환

모의에서 검증한 뒤 실계좌로 옮길 때는 환경과 명시 동의를 함께 지정합니다. 하나라도 빠지면 엔진이 전략을 스케줄하지 않습니다 — "모의인 줄 알고 실전 키를 넣은" 사고를 막는 장치입니다.

```python
import os
from decimal import Decimal

from hermetix import KisClient, StrategyEngine, TradingEnvironment

broker = KisClient(appkey=os.environ["KIS_APPKEY"], appsecret=os.environ["KIS_APPSECRET"],
                   cano=os.environ["KIS_CANO"], environment=TradingEnvironment.LIVE)  # 호스트·TR ID 자동 전환
engine = StrategyEngine(
    broker, [],                                  # 여기에 전략 인스턴스
    live_trading_enabled=True,                   # 실전 명시 동의
    max_order_value=Decimal(1_000_000),          # 주문 1건 추정 금액 상한 (브로커 통화)
    max_daily_order_value=Decimal(5_000_000),    # 하루(UTC) 누적 상한 — 매수·매도 합산
)
```

- 처음에는 반드시 소액 상한으로 시작하세요. 상한을 넘는 시그널은 제출되지 않고 경고만 남습니다.
- 토스·KB 는 모의투자가 없어 실계좌로만 쓸 수 있습니다. 두 어댑터의 REST·실시간 모두 실측 전이므로 더욱 작은 상한이 필요합니다.
- 넥스트증권은 키 프리픽스가 환경을 결정합니다. `environment` 와 어긋나면 생성 시 `ValueError` 입니다.

## 실시간 스트림

`pip install 'hermetix[stream]'` 이 필요합니다. 코어는 여전히 의존성 0이고, `websockets` 가 없으면 `open_stream().connect()` 가 설치 안내와 함께 `ImportError` 를 냅니다.

### 체결가 트리거

전략 코드는 그대로 두고 `trigger` 만 바꾸면 체결 틱마다 `decide()` 가 호출됩니다.

```python
from hermetix import Strategy, StrategySpec, TickTrigger


class Scalper(Strategy):
    spec = StrategySpec(
        name="scalp", symbols=["005930"],
        trigger=TickTrigger.ON_TRADE,        # 체결 틱마다 호출
        min_tick_interval_seconds=1.0,       # 연속 호출 사이 최소 간격 — 캔들·계좌 REST 폭주 방지
        poll_interval_seconds=60,            # 스트림이 끊겼을 때의 안전망 주기
        order_book=True,                     # 호가창 스트림도 구독 → ctx.order_book(symbol)
    )

    def decide(self, ctx):
        book = ctx.order_book("005930")
        if book and book.best_ask and book.best_bid:
            spread = book.best_ask.price - book.best_bid.price
        return []
```

- 몰려온 틱은 하나로 합쳐지고, 최소 간격은 실행 직전에 다시 확인합니다. 스트림이 끊기면 자동 재접속하는 동안 폴링이 계속 돕니다.
- 스트림 틱이 전략의 모든 심볼을 덮으면 `ctx.quote()` 는 REST 대신 마지막 체결 틱(가격·호가·누적거래량)입니다. 캔들·계좌·미체결은 여전히 REST 라 틱당 `1 + 심볼 수 + 3` 호출이 나갑니다. 모의 서버 레이트리밋(KIS 2건/s)을 생각해 `min_tick_interval_seconds` 를 잡으세요.
- 스트림을 선언하지 않은 브로커(`next`, `kb`)에서 `ON_TRADE` 를 쓰면 경고 후 폴링으로 동작합니다.

### 호가창

`order_book=True` 전략은 심볼의 10단계 호가창을 `ctx.order_book(symbol)` 로 받습니다. `OrderBookTick` 은 `asks`/`bids`(최우선부터, `OrderBookLevel(price, quantity)`), `best_ask`/`best_bid`, `total_ask_quantity`/`total_bid_quantity` 를 가집니다. 호가는 틱을 촉발하지 않고 스냅샷만 갱신합니다.

### 주문 통보

브로커가 주문 통보 채널을 제공하면 엔진이 자동 구독합니다. 진입 주문 체결이 서버 조회 없이 즉시 브라켓에 반영되고(부분 체결 누적), 취소·거부는 브라켓을 폐기합니다. KIS 모의처럼 주문 조회가 없는 어댑터는 메모리 추적도 통보로 즉시 확정합니다. KIS 는 `KisClient(..., hts_id="HTS아이디")` 가 필요하고(통보 프레임이 AES 암호문이라 `cryptography` 사용), 비우면 경고 후 폴링 판정으로 동작합니다.

### 엔진 없이 직접 구독

```python
import os
import time

from hermetix import KisClient

client = KisClient(appkey=os.environ["KIS_APPKEY"], appsecret=os.environ["KIS_APPSECRET"],
                   cano=os.environ["KIS_CANO"], hts_id=os.environ.get("KIS_HTS_ID", ""))

stream = client.open_stream()                                             # MarketStream
stream.subscribe_trades(["KRX:005930", "000660"], lambda t: print("체결", t.symbol, t.price, t.quantity))
stream.subscribe_order_book(["005930"], lambda b: print("호가", b.best_ask, b.best_bid))
if client.capabilities and os.environ.get("KIS_HTS_ID"):
    stream.subscribe_order_events(lambda e: print("주문", e.type, e.order_id, e.quantity, e.price))
stream.connect()                                                          # 즉시 반환, 백그라운드 스레드에서 접속·재접속
try:
    time.sleep(60)
finally:
    stream.close()                                                        # 이후 재접속하지 않음
```

리스너는 스트림 스레드에서 호출되므로 오래 걸리는 일은 넣지 마세요. 리스너 예외는 로그만 남기고 연결은 유지됩니다. 틱의 심볼은 구독 요청 표기 그대로 돌아옵니다(`KRX:005930` 으로 구독하면 `KRX:005930`).

### 브로커별 실시간 상태와 제약

| 브로커 | 체결가 | 호가 | 주문 통보 | 제약·참고 |
|---|---|---|---|---|
| `kis` | ✅ 모의 실측 | ✅ 모의 실측 | ⚠️ 문서 기반 | 통보는 `hts_id` 로 구독, 프레임 AES 복호화(`cryptography`). 모의 `ws://ops…:31000`, 실전 `:21000` |
| `kiwoom` | ✅ 모의 실측 | ✅ 모의 실측 | ⚠️ 문서 기반 | REST 토큰으로 LOGIN. 모의 `wss://mockapi…:10000`, 실전 `wss://api…:10000` |
| `nh` | ⚠️ 문서 기반 | ⚠️ 문서 기반 | ⚠️ 문서 기반 | `market_cd` 에 따라 채널이 갈림(KRX `oc/ob`, NXT `nc/nb`, UNT `mc/mb`). 모의 서버는 시세 채널이 "미제공" 표기라 통보만 올 수 있음. 세션당 등록 10건(SDK 실측)~30건(공식), 앱키당 세션 2개 |
| `db` | ⚠️ 문서 기반 | ⚠️ 문서 기반 | ⚠️ 문서 기반 | 접속 후 10초 안에 첫 구독 전송(어댑터가 자동). 계좌당 세션 2개, 종목 50개, 접속 6회/분 |
| `ls` | ⚠️ 문서 기반 | ⚠️ 문서 기반 | ⚠️ 문서 기반 | 서버가 시장을 판별하지 않아 KOSPI·KOSDAQ TR 을 종목마다 둘 다 등록(등록 수 2배). 토큰은 익일 07:00 만료 |
| `toss` | ⚠️ AsyncAPI 기반 | ⚠️ AsyncAPI 기반 | ⚠️ AsyncAPI 기반 | 실전 전용. 계정당 연결 2개(3번째가 오면 가장 오래된 것 종료), 구독 100개, 선언 5회/초, 180초 무송신 시 끊김(60초 `PING` 자동). 체결 틱에 누적거래량·등락 없음 |
| `next`, `kb` | ❌ | ❌ | ❌ | 웹소켓 스펙이 없어 폴링만 (넥스트 공개 스펙 v1.3, KB 오픈베타 명세 2026-09) |

프레임 파서는 `hermetix.brokers.{kis,kiwoom,nh,db,ls,toss}_stream` 의 `parse_*` 함수이며, 골든 픽스처 `conformance/fixtures/*.json#stream` 으로 네 언어가 같은 값을 내는지 검증합니다. `measured: false` 인 픽스처는 문서 예시에서 재구성한 값입니다.

## 수익률

```python
from decimal import Decimal

from hermetix import pnl_report

report = pnl_report(broker, initial_capital=Decimal(20_000_000))   # dict
print(report["portfolio_value"], report["total_unrealized_pnl"], report["total_return_rate"])
```

`cash`, `portfolio_value`, `total_market_value`, `total_unrealized_pnl`, `total_return_rate`(초기 자금을 줬을 때), `holdings` 를 돌려줍니다.

## 사용량 텔레메트리

SDK 는 **어느 증권사가 얼마나 쓰이는지**를 시간 단위로 합산해 `hermetix-service` 로 보내고, 그 데이터로 증권사 사용량 랭킹을 공개합니다. 항상 켜져 있으며(끄는 설정 없음) 표준 라이브러리만 씁니다. 계약 전문은 [docs/telemetry.md](../docs/telemetry.md).

| 보내는 것 | 보내지 않는 것 |
|---|---|
| 브로커 ID, 환경(모의/실전), 호출 종류별 성공·에러 건수(시간 버킷 합계), 에러 분류, 응답 시간 p50·p95, 실시간 채널별 구독·메시지·재접속 수, SDK 버전, 설치 단위 무작위 ID | 종목, 수량·가격·금액, 주문번호·계좌번호, API 키·토큰·HTS ID, 전략 이름, IP(서버가 저장하지 않음), OS·호스트명 |

- 60초마다 한 번 묶어서 보내고(첫 전송은 시작 60초 후), 실패는 조용히 버립니다. 매매 경로를 막지 않습니다
- 어댑터의 공개 메서드는 `BrokerClient` 가 자동으로 계측하고, 토큰 발급은 `auth`, 스트림은 구독·메시지·재접속 수만 셉니다
- 모든 전송은 HMAC-SHA256 요청 서명(`X-Hermetix-Key-Id`/`Timestamp`/`Signature`)을 싣습니다 — 공개 키라 비밀은 아니고 스팸·스캐너를 거르는 문턱입니다 ([계약](../docs/telemetry.md) "요청 서명")


## 검증

```bash
pip install -e './python[dev]'
pytest python/tests/                                 # 오프라인 — 골든 픽스처 재생, 가짜 웹소켓 서버, 엔진 트리거
python python/tests/smoke.py next|kis|kiwoom         # 실서버 스모크 (환경변수로 키 주입)
```

스모크가 읽는 환경변수:

| 브로커 | 환경변수 |
|---|---|
| `next` | `NEXT_CLIENT_ID`, `NEXT_CLIENT_SECRET` |
| `kis` | `KIS_APPKEY`, `KIS_APPSECRET`, `KIS_CANO` (주문 통보까지 보려면 코드에서 `hts_id` 지정) |
| `kiwoom` | `KIWOOM_APPKEY`, `KIWOOM_SECRETKEY` |

- 새 어댑터를 만들 때는 `verify_broker_conformance(client, ConformanceScenario(symbol, limit_price, quantity))` 로 표준 시나리오를 통과시키세요. 네 언어가 같은 픽스처를 씁니다 ([컨포먼스 킷](../conformance/README.md)).
- ⚠️ 미검증 브로커의 실측은 해당 증권사 계좌를 가진 사용자의 제보로 진행합니다. 실측 응답을 [새 브로커 요청 이슈](../.github/ISSUE_TEMPLATE/broker-request.md)로 보내주시면 픽스처를 실측값으로 교체하고 상태를 승격합니다.

## 공식 전략 예제 (examples/)

Kotlin 전략 레포 3종과 동일 로직의 실행 가능한 단일 파일입니다. `HERMETIX_BROKER` 로 브로커(`next`/`kis`/`kiwoom`)를 고르고 위 환경변수로 키를 줍니다.

| 파일 | 전략 | 실행 |
|---|---|---|
| `examples/larry.py` | 변동성 돌파 | `HERMETIX_BROKER=next NEXT_CLIENT_ID=... NEXT_CLIENT_SECRET=... python examples/larry.py` |
| `examples/trend_breakout.py` | WMA 추세선 돌파 | 동일 |
| `examples/grid.py` | 목표가 스캘핑 (캔들 미사용, KRX 호환) | `HERMETIX_BROKER=kis KIS_APPKEY=... KIS_APPSECRET=... KIS_CANO=... python examples/grid.py` |

## 알려진 한계

- 브라켓(익절/손절 예약), KIS 메모리 주문 추적, 일일 누적 주문 금액은 앱 메모리에만 있어 재시작 시 사라집니다. 전략이 보유 포지션을 서버에서 다시 점검하는 패턴을 권장합니다.
- KRX 브로커의 캘린더는 정규장(평일 09:00–15:30 KST)을 합성한 것이라 공휴일이 개장일로 보입니다. 주문은 서버가 거부하므로 안전에는 문제 없지만 그날은 틱마다 `MarketClosedError` 로 조용히 넘어갑니다.
- 단일 계좌 전제입니다. 전략 여러 개가 같은 심볼을 다루면 보유·미체결 판단이 겹칩니다.
- ⚠️ 문서 기반 어댑터(nh·db·ls·toss·kb, 그리고 모든 브로커의 주문 통보)는 스펙 해석 오류가 있을 수 있습니다. 실계좌라면 반드시 소액 상한과 함께 쓰세요.
- 키는 사용자 기기에서만 쓰입니다. Hermetix 가 운영하는 서버는 없고, 키를 받아 대신 호출하는 구조는 채택하지 않습니다.

## 문서

- [루트 README](../README.md) — 지원 브로커 표, Kotlin 사용법
- [아키텍처](../docs/architecture.md) — 틱 파이프라인, 실시간 계층, 어댑터 비교표, 상태 지도
- [전략 작성 가이드](../docs/strategy-guide.md) — StrategySpec·StrategyContext·Signal 상세, 자주 쓰는 패턴
- [컨포먼스 킷](../conformance/README.md) — 골든 픽스처와 검증 시나리오
- [로드맵](../ROADMAP.md)
