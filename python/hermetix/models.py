"""공통 도메인 모델 (Kotlin hermetix-broker 의 dto 와 동일 의미).

금액/수량은 전부 Decimal 이다 - float 를 절대 섞지 말 것.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
import re
from enum import Enum


class TradingEnvironment(Enum):
    """거래 환경. PAPER=모의투자(기본), LIVE=실전투자 — 엔진은 live_trading_enabled=True 없이는 LIVE 를 기동하지 않는다.
    키는 항상 사용자 기기에서만 쓰인다."""
    PAPER = "PAPER"
    LIVE = "LIVE"


_MARKET_PREFIX = re.compile(r"^([A-Z]{2,6}):(.+)$")


def parse_symbol(symbol: str) -> tuple[str | None, str]:
    """`MARKET:CODE` 표기를 (market, code) 로 나눈다. 접두가 없으면 (None, symbol)."""
    m = _MARKET_PREFIX.match(symbol)
    return (m.group(1), m.group(2)) if m else (None, symbol)


def symbol_code(symbol: str) -> str:
    """접두를 뗀 브로커 심볼 코드"""
    return parse_symbol(symbol)[1]


def symbols_match(a: str, b: str) -> bool:
    """코드가 같고, 둘 다 시장을 명시했다면 시장도 같아야 한다"""
    ma, ca = parse_symbol(a)
    mb, cb = parse_symbol(b)
    return ca == cb and (ma is None or mb is None or ma == mb)


class CandleInterval(Enum):
    MIN_1 = "1m"
    MIN_5 = "5m"
    HOUR_1 = "1h"
    DAY_1 = "1d"


class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class TimeInForce(Enum):
    DAY = "DAY"
    GTC = "GTC"


class OrderStatus(Enum):
    """주문 상태 (넥스트증권 공개 스펙 v1.3 부록 D 7종 + UNKNOWN 폴백).

    PENDING_CANCEL 은 취소 접수 후 미확정 상태 — 원주문이 체결될 수 있으므로 OPEN 으로 분류한다.
    """
    SUBMITTED = "SUBMITTED"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    PENDING_CANCEL = "PENDING_CANCEL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    UNKNOWN = "UNKNOWN"

    @property
    def is_open(self) -> bool:
        return self in (OrderStatus.SUBMITTED, OrderStatus.PARTIALLY_FILLED, OrderStatus.PENDING_CANCEL)


@dataclass(frozen=True)
class Quote:
    symbol: str
    price: Decimal
    bid_price: Decimal | None
    ask_price: Decimal | None
    volume: int
    change: Decimal | None
    change_rate: Decimal | None
    timestamp: datetime


@dataclass(frozen=True)
class Candle:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int


@dataclass(frozen=True)
class SessionHours:
    start: str  # "09:30"
    end: str    # "16:00"


@dataclass(frozen=True)
class MarketDay:
    date: str  # "2026-08-05"
    open: bool
    regular: SessionHours | None
    timezone: str
    holiday: str | None = None


@dataclass(frozen=True)
class Account:
    account_id: str
    currency: str
    cash: Decimal
    portfolio_value: Decimal
    status: str = "ACTIVE"
    name: str | None = None


@dataclass(frozen=True)
class Holding:
    symbol: str
    quantity: Decimal
    avg_entry_price: Decimal
    current_price: Decimal | None = None
    market_value: Decimal | None = None
    unrealized_pnl: Decimal | None = None
    unrealized_pnl_rate: Decimal | None = None
    # 종목 거래 통화 — avg_entry_price·current_price·unrealized_pnl 의 단위. None 이면 계좌 통화.
    # market_value 는 항상 계좌 통화 (계좌 총평가에 더하는 값)
    currency: str | None = None


@dataclass(frozen=True)
class CreateOrderRequest:
    symbol: str
    side: OrderSide
    order_type: OrderType
    quantity: Decimal
    limit_price: Decimal | None = None
    time_in_force: TimeInForce = TimeInForce.DAY
    client_order_id: str | None = None


@dataclass(frozen=True)
class Order:
    order_id: str
    status: OrderStatus
    symbol: str | None = None
    side: OrderSide | None = None
    order_type: OrderType | None = None
    quantity: Decimal | None = None
    limit_price: Decimal | None = None
    filled_quantity: Decimal | None = None
    avg_fill_price: Decimal | None = None
    client_order_id: str | None = None
    submitted_at: datetime | None = None
    canceled_at: datetime | None = None


@dataclass(frozen=True)
class Fill:
    fill_id: str | None
    order_id: str | None
    symbol: str | None
    side: OrderSide | None
    quantity: Decimal | None
    price: Decimal | None


class StreamChannel(Enum):
    """브로커가 제공하는 실시간 스트림 채널. BrokerCapabilities.streams 로 선언한다."""
    TRADES = "TRADES"              # 체결가 - 체결이 일어날 때마다 TradeTick
    ORDER_BOOK = "ORDER_BOOK"      # 호가 - 호가창이 바뀔 때마다 OrderBookTick (10단계)
    ORDER_EVENTS = "ORDER_EVENTS"  # 내 주문의 접수·체결·취소·거부 통보 - OrderEvent


@dataclass(frozen=True)
class TradeTick:
    """체결 1건. 브로커 프레임을 공통 모델로 정규화한 것.

    - symbol 은 구독 요청 표기 그대로 돌려준다 (KRX:005930 으로 구독하면 KRX:005930)
    - quantity 는 이 체결의 수량, cumulative_volume 은 당일 누적 거래량
    - 호가·등락은 프레임에 있으면 채우고 없으면 None
    """
    symbol: str
    price: Decimal
    quantity: Decimal
    timestamp: datetime
    bid_price: Decimal | None = None
    ask_price: Decimal | None = None
    cumulative_volume: int | None = None
    change: Decimal | None = None
    change_rate: Decimal | None = None

    def to_quote(self) -> Quote:
        """스트림 틱을 REST 현재가와 같은 모양으로 - 엔진이 quotes 호출을 아낄 때 쓴다"""
        return Quote(symbol=self.symbol, price=self.price, bid_price=self.bid_price, ask_price=self.ask_price,
                     volume=self.cumulative_volume or 0, change=self.change, change_rate=self.change_rate,
                     timestamp=self.timestamp)


@dataclass(frozen=True)
class OrderBookLevel:
    """호가 한 단계"""
    price: Decimal
    quantity: Decimal


@dataclass(frozen=True)
class OrderBookTick:
    """호가창 스냅샷. asks/bids 는 최우선(1호가)부터 순서대로, 브로커가 주는 만큼(보통 10단계). 심볼은 구독 요청 표기 그대로."""
    symbol: str
    timestamp: datetime
    asks: list[OrderBookLevel]
    bids: list[OrderBookLevel]
    total_ask_quantity: Decimal | None = None
    total_bid_quantity: Decimal | None = None

    @property
    def best_ask(self) -> OrderBookLevel | None:
        return self.asks[0] if self.asks else None

    @property
    def best_bid(self) -> OrderBookLevel | None:
        return self.bids[0] if self.bids else None


class OrderEventType(Enum):
    ACCEPTED = "ACCEPTED"   # 주문 접수
    FILLED = "FILLED"       # 체결 (부분 체결 포함 - OrderEvent.quantity 가 이번 체결량)
    CANCELED = "CANCELED"   # 취소 확인
    MODIFIED = "MODIFIED"   # 정정 확인
    REJECTED = "REJECTED"   # 거부


def normalize_order_id(order_id: str) -> str:
    """앞자리 0 패딩 차이를 무시한 주문번호 (KIS 통보 10자리 vs REST ODNO 7자리 등)"""
    return order_id.strip().lstrip("0") or "0"


@dataclass(frozen=True)
class OrderEvent:
    """내 주문 통보 1건.

    - order_id 는 브로커 주문번호. 브로커에 따라 REST 응답과 자릿수(0 패딩)가 다를 수 있어 비교는 order_id_matches 로 한다
    - quantity/price 는 이벤트 종류에 따라 체결량·체결가(FILLED) 또는 주문량·주문가(그 외)
    - remaining_quantity 는 브로커가 주는 경우만 (키움 902). KIS 통보에는 없다
    """
    order_id: str
    type: OrderEventType
    timestamp: datetime
    symbol: str | None = None
    side: OrderSide | None = None
    quantity: Decimal | None = None
    price: Decimal | None = None
    remaining_quantity: Decimal | None = None
    original_order_id: str | None = None
    reason: str | None = None

    def order_id_matches(self, other: str) -> bool:
        return normalize_order_id(self.order_id) == normalize_order_id(other)


@dataclass(frozen=True)
class BrokerCapabilities:
    """브로커가 지원하는 기능의 코드 선언. 실측으로 확인한 것만 True 로 선언한다."""
    broker_id: str
    market: str          # 기본 시장 "US" | "KRX" — 접두 없는 심볼은 이 시장으로 해석
    currency: str
    candle_intervals: frozenset[CandleInterval]
    client_order_id: bool
    native_bracket: bool
    fractional_shares: bool
    server_open_orders: bool = True
    # 지원 거래 환경. 실전(LIVE)은 실측으로 확인한 어댑터만 선언
    environments: frozenset[TradingEnvironment] = frozenset({TradingEnvironment.PAPER})
    # 한 계좌로 다룰 수 있는 시장 목록 (MARKET:CODE 접두 허용 값). None 이면 {market}
    markets: frozenset[str] | None = None
    # 실시간 스트림 채널. 비어있으면 폴링만. 선언한 어댑터는 StreamingBrokerClient 를 구현해야 한다
    streams: frozenset[StreamChannel] = frozenset()

    def __post_init__(self):
        if self.markets is None:
            object.__setattr__(self, "markets", frozenset({self.market}))

    def symbol_code(self, symbol: str) -> str:
        """심볼의 시장 접두가 지원 시장인지 확인하고 브로커 코드를 돌려준다. 미지원이면 ValueError."""
        market, code = parse_symbol(symbol)
        if market is not None and market not in self.markets:
            raise ValueError(f"브로커 '{self.broker_id}' 는 시장 '{market}' 을 지원하지 않습니다 (지원: {sorted(self.markets)}): {symbol}")
        return code
