"""Hermetix - 증권사 모의투자 통합 트레이딩 프레임워크 (Python).

빠른 시작:

    from decimal import Decimal
    from hermetix import NextClient, Strategy, StrategySpec, StrategyEngine, Buy, CandleInterval

    class MyStrategy(Strategy):
        spec = StrategySpec(name="my-first", symbols=["AAPL"])

        def decide(self, ctx):
            q = ctx.quote("AAPL")
            if q and not ctx.has_position("AAPL") and not ctx.has_open_order("AAPL"):
                return [Buy("AAPL", Decimal(1),
                            take_profit_price=q.price * Decimal("1.04"),
                            stop_loss_price=q.price * Decimal("0.98"))]
            return []

    broker = NextClient(client_id="pk_test_...", client_secret="sk_test_...")
    StrategyEngine(broker, [MyStrategy()]).run()

브로커 전환은 브로커 ID 한 토큰 (docs/broker-factory.md — 네 언어 공통 규약):

    import hermetix
    broker = hermetix.next(api_key="pk_test_...", api_secret="sk_test_...", account="acc_main")
    broker = hermetix.kis(api_key=..., api_secret=..., account=...)   # 한국투자 모의
    broker = hermetix.kiwoom(api_key=..., api_secret=...)             # 키움 모의
    broker = hermetix.client("toss", api_key=..., api_secret=..., account=...)

기존 클래스 직접 생성(KisClient(appkey=..., appsecret=..., cano=...))도 그대로 된다.
``hermetix.next`` 는 내장 ``next`` 와 이름이 같으니 ``from hermetix import next`` 대신 ``hermetix.next(...)`` 로 쓴다.
"""
from .broker import (BrokerClient, MarketStream, OrderBookListener, OrderEventListener, RateLimiter, StreamingBrokerClient,
                     TradeListener)
from .testing import ConformanceReport, ConformanceScenario, verify_broker_conformance
from .brokers.db import DbClient
from .brokers.kb import KbClient
from .brokers.kis import KisClient
from .brokers.ls import LsClient
from .brokers.nh import NhClient
from .brokers.toss import TossClient
from .brokers.kiwoom import KiwoomClient
from .brokers.next import NextClient
from .brokers.db_stream import DbMarketStream
from .brokers.kis_stream import KisMarketStream
from .brokers.kiwoom_stream import KiwoomMarketStream
from .brokers.ls_stream import LsMarketStream
from .brokers.nh_stream import NhMarketStream
from .brokers.toss_stream import TossMarketStream
from .engine import BracketMonitor, MarketCalendar, OrderExecutor, RiskGuard, StrategyEngine, TradingGuard, pnl_report
from . import factory as _factory
from .factory import brokers, client, db, kb, kis, kiwoom, ls, nh, toss
next = _factory.next  # noqa: A001 — 브로커 ID 가 곧 함수 이름 (hermetix.next 로 쓴다)
from .errors import (
    AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError,
    MarketClosedError, OrderNotFoundError, RateLimitError,
)
from .models import (
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, OrderBookLevel, OrderBookTick, OrderEvent, OrderEventType, OrderSide, OrderStatus,
    OrderType, Quote, StreamChannel, TimeInForce, TradeTick, TradingEnvironment, normalize_order_id, parse_symbol, symbol_code,
    symbols_match,
)
from .strategy import Buy, Cancel, Sell, Signal, Strategy, StrategyContext, StrategySpec, TickTrigger

__version__ = "0.11.3"

__all__ = [
    "brokers", "client", "next", "kis", "kiwoom", "nh", "ls", "db", "toss", "kb",
    "BrokerClient", "RateLimiter", "MarketStream", "StreamingBrokerClient", "TradeListener", "NextClient", "KisClient", "KiwoomClient", "NhClient", "DbClient",
    "LsClient", "TossClient", "KbClient",
    "ConformanceReport", "ConformanceScenario", "verify_broker_conformance",
    "StrategyEngine", "TradingGuard", "BracketMonitor", "OrderExecutor", "RiskGuard", "MarketCalendar", "pnl_report",
    "TradingEnvironment", "parse_symbol", "symbol_code", "symbols_match",
    "Strategy", "StrategySpec", "StrategyContext", "Signal", "Buy", "Sell", "Cancel", "TickTrigger",
    "StreamChannel", "TradeTick", "OrderBookLevel", "OrderBookTick", "OrderEvent", "OrderEventType", "normalize_order_id",
    "OrderBookListener", "OrderEventListener",
    "KisMarketStream", "KiwoomMarketStream", "NhMarketStream", "DbMarketStream", "LsMarketStream", "TossMarketStream",
    "Account", "BrokerCapabilities", "Candle", "CandleInterval", "CreateOrderRequest",
    "Fill", "Holding", "MarketDay", "Order", "OrderSide", "OrderStatus", "OrderType", "Quote", "TimeInForce",
    "BrokerApiError", "AuthError", "RateLimitError", "MarketClosedError",
    "InsufficientFundsError", "InvalidOrderError", "OrderNotFoundError",
]
