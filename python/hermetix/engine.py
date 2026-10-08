"""전략 실행 엔진 (Kotlin hermetix-engine 과 동일 의미).

매 틱: 장시간 확인 -> 스냅샷 구성 -> 브라켓 점검 -> 전략 호출 -> 시그널 실행.
틱은 단일 스레드에서 순차 실행된다 (같은 계좌 공유 - 동시 주문 경합 차단).
"""
from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from .broker import BrokerClient, MarketStream, StreamingBrokerClient
from .errors import InsufficientFundsError, MarketClosedError, RateLimitError
from .models import (CreateOrderRequest, Order, OrderBookTick, OrderEvent, OrderEventType, OrderSide, OrderStatus, OrderType,
                     StreamChannel, TradeTick, TradingEnvironment)
from .strategy import Buy, Cancel, Sell, Signal, Strategy, StrategyContext, TickTrigger

logger = logging.getLogger("hermetix")


class MarketCalendar:
    """개장 판단 - 브로커 캘린더를 6시간 캐시."""

    def __init__(self, broker: BrokerClient):
        self._broker = broker
        self._cache: list | None = None
        self._cached_at = 0.0

    def is_regular_open(self, now: datetime | None = None) -> bool:
        days = self._calendar()
        if not days:
            return False
        zone = ZoneInfo(days[0].timezone)
        local = (now or datetime.now(timezone.utc)).astimezone(zone)
        today = next((d for d in days if d.date == local.date().isoformat()), None)
        if today is None or not today.open or today.regular is None:
            return False
        hhmm = local.strftime("%H:%M")
        return today.regular.start <= hhmm < today.regular.end

    def _calendar(self):
        if self._cache is None or time.time() - self._cached_at > 6 * 3600:
            self._cache = self._broker.get_calendar()
            self._cached_at = time.time()
            logger.info("market calendar refreshed / days=%d", len(self._cache))
        return self._cache


class TradingGuard:
    """비상정지 - 연속 실패 임계치 도달 시 미체결 전량 취소 + 신규 주문 차단."""

    def __init__(self, broker: BrokerClient, max_consecutive_failures: int = 5):
        self._broker = broker
        self._max = max_consecutive_failures
        self._failures = 0
        self._halted = False
        self._lock = threading.Lock()

    @property
    def is_halted(self) -> bool:
        return self._halted

    def record_success(self) -> None:
        self._failures = 0

    def record_failure(self, cause: Exception) -> None:
        with self._lock:
            self._failures += 1
            logger.warning("engine failure %d/%d - %s", self._failures, self._max, cause)
            if self._failures >= self._max and not self._halted:
                self.halt(f"연속 실패 {self._failures}회")

    def halt(self, reason: str) -> None:
        if self._halted:
            return
        self._halted = True
        logger.error("TRADING HALTED / %s - 미체결 전량 취소", reason)
        try:
            for order in self._broker.get_orders():
                if order.status.is_open:
                    try:
                        self._broker.cancel_order(order.order_id)
                        logger.info("halt-cancel ok / %s", order.order_id)
                    except Exception as e:  # noqa: BLE001 - 취소 실패해도 나머지 진행
                        logger.error("halt-cancel failed / %s: %s", order.order_id, e)
        except Exception as e:  # noqa: BLE001
            logger.error("halt: open order lookup failed: %s", e)

    def resume(self) -> None:
        self._failures = 0
        self._halted = False
        logger.info("trading resumed")


class RiskGuard:
    """주문 금액 상한 — 실전투자에서 봇 폭주 손실 규모를 제한한다 (모의에서도 설정하면 적용).

    - max_order_value: 주문 1건의 추정 금액(수량 × 지정가 또는 현재가) 상한
    - max_daily_order_value: 하루(UTC) 누적 주문 금액 상한 — 매수·매도 합산
    추정 금액을 알 수 없으면(현재가 없음) 상한이 설정된 경우 거부한다. 누적치는 메모리에만 있다.
    """

    def __init__(self, max_order_value: Decimal | None = None, max_daily_order_value: Decimal | None = None,
                 now: "callable | None" = None):
        self._max_order = max_order_value
        self._max_daily = max_daily_order_value
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._day = self._now().date()
        self._daily_total = Decimal(0)
        self._lock = threading.Lock()

    @property
    def is_active(self) -> bool:
        return self._max_order is not None or self._max_daily is not None

    def try_reserve(self, symbol: str, quantity: Decimal, price: Decimal | None) -> str | None:
        """허용하면 일일 누적에 반영하고 None, 거부하면 사유. 누적은 제출 전에 잡는다 (보수적)."""
        if not self.is_active:
            return None
        if price is None or price <= 0:
            return f"가격을 알 수 없어 주문 금액 상한을 검증할 수 없습니다 (symbol={symbol})"
        value = quantity * price
        if self._max_order is not None and value > self._max_order:
            return f"주문 금액 {value} 이(가) 1건 상한 {self._max_order} 을(를) 초과합니다 (symbol={symbol})"
        with self._lock:
            self._roll_day()
            if self._max_daily is not None and self._daily_total + value > self._max_daily:
                return (f"일일 누적 주문 금액 {self._daily_total + value} 이(가) 상한 {self._max_daily} 을(를) "
                        f"초과합니다 (오늘 누적={self._daily_total})")
            self._daily_total += value
        return None

    def daily_total(self) -> Decimal:
        with self._lock:
            self._roll_day()
            return self._daily_total

    def _roll_day(self) -> None:
        today = self._now().date()
        if today != self._day:
            self._day = today
            self._daily_total = Decimal(0)


@dataclass(frozen=True)
class _Bracket:
    entry_order_id: str
    symbol: str
    quantity: Decimal
    take_profit: Decimal | None
    stop_loss: Decimal | None
    active: bool = False
    filled_quantity: Decimal = Decimal(0)


class BracketMonitor:
    """소프트웨어 익절/손절. 상태는 메모리에만 있다 (재시작 시 소실)."""

    def __init__(self, broker: BrokerClient):
        self._broker = broker
        self._brackets: dict[str, _Bracket] = {}

    def register(self, entry_order_id: str, symbol: str, quantity: Decimal,
                 take_profit: Decimal | None, stop_loss: Decimal | None) -> None:
        if take_profit is None and stop_loss is None:
            return
        self._brackets[entry_order_id] = _Bracket(entry_order_id, symbol, quantity, take_profit, stop_loss)
        logger.info("bracket registered / %s %s qty=%s tp=%s sl=%s",
                    entry_order_id, symbol, quantity, take_profit, stop_loss)

    def check(self, ctx: StrategyContext) -> list[Sell]:
        signals: list[Sell] = []
        for bracket in list(self._brackets.values()):
            if not bracket.active:
                bracket = self._resolve_entry(bracket)
                if bracket is None or not bracket.active:
                    continue

            quote = ctx.quote(bracket.symbol)
            if quote is None:
                continue
            tp_hit = bracket.take_profit is not None and quote.price >= bracket.take_profit
            sl_hit = bracket.stop_loss is not None and quote.price <= bracket.stop_loss
            if not (tp_hit or sl_hit):
                continue

            del self._brackets[bracket.entry_order_id]
            held = ctx.holding(bracket.symbol)
            qty = min(bracket.quantity, held.quantity if held else Decimal(0))
            if qty <= 0:
                logger.warning("bracket hit but no holdings / %s", bracket.symbol)
                continue
            logger.info("bracket %s / %s price=%s", "TAKE-PROFIT" if tp_hit else "STOP-LOSS",
                        bracket.symbol, quote.price)
            signals.append(Sell(symbol=bracket.symbol, quantity=qty))
        return signals

    def _resolve_entry(self, bracket: _Bracket) -> _Bracket | None:
        try:
            order = self._broker.get_order(bracket.entry_order_id)
        except Exception as e:  # noqa: BLE001
            logger.warning("bracket entry lookup failed / %s: %s", bracket.entry_order_id, e)
            return bracket
        if order.status == OrderStatus.FILLED:
            updated = replace(bracket, active=True)
            self._brackets[bracket.entry_order_id] = updated
            logger.info("bracket activated / entry filled %s", bracket.entry_order_id)
            return updated
        if not order.status.is_open:
            del self._brackets[bracket.entry_order_id]
            logger.info("bracket dropped / entry %s %s", order.status.value, bracket.entry_order_id)
            return None
        return bracket

    def on_order_event(self, event: OrderEvent) -> None:
        """주문 통보로 진입 주문 상태를 바로 반영한다 (서버 조회 없이). 체결은 누적해 주문 수량을 채우면 활성화,
        취소·거부는 폐기. 통보가 없는 브로커에서는 기존처럼 틱마다 _resolve_entry 가 조회한다."""
        bracket = next((b for b in self._brackets.values() if event.order_id_matches(b.entry_order_id)), None)
        if bracket is None:
            return
        if event.type == OrderEventType.FILLED:
            filled = bracket.filled_quantity + (event.quantity or Decimal(0))
            active = bracket.active or filled >= bracket.quantity
            self._brackets[bracket.entry_order_id] = replace(bracket, filled_quantity=filled, active=active)
            if active and not bracket.active:
                logger.info("bracket activated / entry filled by event %s", bracket.entry_order_id)
        elif event.type in (OrderEventType.CANCELED, OrderEventType.REJECTED):
            del self._brackets[bracket.entry_order_id]
            logger.info("bracket dropped / entry %s by event %s", event.type.value, bracket.entry_order_id)

    @property
    def active_count(self) -> int:
        return len(self._brackets)


class OrderExecutor:
    """Signal -> 주문 실행. 매도 클램프(공매도 방지), 주문 금액 상한(RiskGuard), 멱등키(지원 브로커만)."""

    def __init__(self, broker: BrokerClient, brackets: BracketMonitor, guard: TradingGuard,
                 risk: RiskGuard | None = None):
        self._broker = broker
        self._brackets = brackets
        self._guard = guard
        self._risk = risk or RiskGuard()

    def execute(self, strategy_name: str, signals: list[Signal], ctx: StrategyContext) -> None:
        for signal in signals:
            if self._guard.is_halted:
                logger.warning("[%s] halted - signal skipped: %s", strategy_name, signal)
                continue
            try:
                if isinstance(signal, Buy):
                    self._buy(strategy_name, signal, ctx)
                elif isinstance(signal, Sell):
                    self._sell(strategy_name, signal, ctx)
                elif isinstance(signal, Cancel):
                    order = self._broker.cancel_order(signal.order_id)
                    logger.info("[%s] CANCEL / %s -> %s", strategy_name, signal.order_id, order.status.value)
            except InsufficientFundsError:
                logger.warning("[%s] 주문가능금액 부족으로 시그널 스킵: %s", strategy_name, signal)
            except Exception as e:  # noqa: BLE001 - 시그널 단위 격리
                logger.error("[%s] signal 실행 실패 %s: %s", strategy_name, signal, e)

    def _within_risk(self, strategy_name: str, symbol: str, quantity: Decimal,
                     limit_price: Decimal | None, ctx: StrategyContext) -> bool:
        """추정 금액 = 수량 × (지정가 or 현재가). 상한을 넘으면 경고 후 False"""
        quote = ctx.quote(symbol)
        price = limit_price if limit_price is not None else (quote.price if quote else None)
        rejection = self._risk.try_reserve(symbol, quantity, price)
        if rejection is None:
            return True
        logger.warning("[%s] 주문 금액 상한으로 시그널 스킵: %s", strategy_name, rejection)
        return False

    def _buy(self, strategy_name: str, signal: Buy, ctx: StrategyContext) -> None:
        if not self._within_risk(strategy_name, signal.symbol, signal.quantity, signal.limit_price, ctx):
            return
        order = self._broker.create_order(CreateOrderRequest(
            symbol=signal.symbol, side=OrderSide.BUY, order_type=signal.order_type,
            quantity=signal.quantity, limit_price=signal.limit_price,
            time_in_force=signal.time_in_force,
            client_order_id=self._client_order_id(strategy_name),
        ))
        logger.info("[%s] BUY 접수 / %s qty=%s type=%s orderId=%s",
                    strategy_name, signal.symbol, signal.quantity, signal.order_type.value, order.order_id)
        self._brackets.register(order.order_id, signal.symbol, signal.quantity,
                                signal.take_profit_price, signal.stop_loss_price)

    def _sell(self, strategy_name: str, signal: Sell, ctx: StrategyContext) -> None:
        held = ctx.holding(signal.symbol)
        qty = min(signal.quantity, held.quantity if held else Decimal(0))
        if qty <= 0:
            logger.warning("[%s] SELL 스킵 / %s 보유 수량 없음", strategy_name, signal.symbol)
            return
        if not self._within_risk(strategy_name, signal.symbol, qty, signal.limit_price, ctx):
            return
        order = self._broker.create_order(CreateOrderRequest(
            symbol=signal.symbol, side=OrderSide.SELL, order_type=signal.order_type,
            quantity=qty, limit_price=signal.limit_price, time_in_force=signal.time_in_force,
            client_order_id=self._client_order_id(strategy_name),
        ))
        logger.info("[%s] SELL 접수 / %s qty=%s orderId=%s", strategy_name, signal.symbol, qty, order.order_id)

    def _client_order_id(self, strategy_name: str) -> str | None:
        if not self._broker.capabilities.client_order_id:
            return None
        return f"{strategy_name}-{uuid.uuid4().hex[:8]}"


class StrategyEngine:
    """등록된 전략들을 각자의 poll_interval 로 순차 호출한다.

    실시간 트리거(TickTrigger.ON_TRADE): 브로커가 StreamingBrokerClient 이고 TRADES 채널을 선언하면
    전략 심볼의 체결가 스트림을 구독하고, 틱마다 run 루프를 깨워 같은 스레드에서 tick 을 돌린다.
    대기 중인 틱이 있으면 합치고, 직전 틱 종료 후 min_tick_interval_seconds 가 지나야 다음을 돌린다 (실행 직전에 재확인).
    스트림 틱이 모든 심볼을 덮으면 quotes REST 호출 대신 마지막 틱을 현재가로 쓴다. 폴링은 안전망으로 계속 돈다.
    """

    def __init__(self, broker: BrokerClient, strategies: list[Strategy],
                 max_consecutive_failures: int = 5, *,
                 live_trading_enabled: bool = False,
                 max_order_value: Decimal | None = None,
                 max_daily_order_value: Decimal | None = None):
        """live_trading_enabled: 브로커가 LIVE 환경이면 True 여야 스케줄한다 (실전 명시 동의).
        max_order_value / max_daily_order_value: 주문 금액 상한 (RiskGuard, 브로커 통화)."""
        self.broker = broker
        self.guard = TradingGuard(broker, max_consecutive_failures)
        self.brackets = BracketMonitor(broker)
        self.risk = RiskGuard(max_order_value, max_daily_order_value)
        self.executor = OrderExecutor(broker, self.brackets, self.guard, self.risk)
        self.calendar = MarketCalendar(broker)
        self._stop = threading.Event()
        self._wake = threading.Event()           # 스트림 틱이 run 루프를 깨운다
        self._stream: MarketStream | None = None
        self.latest_trades: dict[str, TradeTick] = {}   # 심볼(요청 표기) -> 마지막 체결 틱
        self.latest_order_books: dict[str, OrderBookTick] = {}  # 심볼(요청 표기) -> 마지막 호가창 (spec.order_book 전략만)
        self._order_events_attached = False
        self._pending: dict[str, bool] = {}              # 전략 이름 -> 대기 중인 스트림 틱 (합치기용)
        self._last_tick_ended_at: dict[str, float] = {}  # 전략 이름 -> 직전 tick 종료 시각 (monotonic)

        caps = broker.capabilities
        environment = broker.environment
        self.strategies = []
        if environment not in caps.environments:
            logger.error("브로커 '%s' 는 %s 환경을 지원하지 않습니다 (지원: %s) - 엔진을 시작하지 않습니다",
                         caps.broker_id, environment.value, [e.value for e in caps.environments])
            return
        if environment == TradingEnvironment.LIVE and not live_trading_enabled:
            logger.error("브로커 '%s' 가 실전투자(LIVE)로 설정돼 있지만 live_trading_enabled=True 가 없습니다 - "
                         "엔진을 시작하지 않습니다. 실제 돈으로 거래하려면 명시하세요.", caps.broker_id)
            return
        if environment == TradingEnvironment.LIVE:
            logger.warning("***** 실전투자(LIVE) 모드 - 주문이 실제 계좌에서 체결됩니다. 주문 금액 상한 설정을 권장합니다 *****")
        for strategy in strategies:
            # capability 검증 - 미지원 조합은 스케줄하지 않는다 (fail-fast)
            if strategy.spec.candle_interval not in caps.candle_intervals:
                logger.error(
                    "[%s] 스케줄 제외: 브로커 '%s' 는 %s 캔들을 지원하지 않습니다 (지원: %s)",
                    strategy.spec.name, caps.broker_id, strategy.spec.candle_interval.value,
                    [i.value for i in caps.candle_intervals])
                continue
            self.strategies.append(strategy)
        logger.info("broker=%s environment=%s market=%s / strategies=%s",
                    caps.broker_id, environment.value, caps.market, [s.spec.name for s in self.strategies])
        for strategy in self.strategies:
            if strategy.spec.trigger == TickTrigger.ON_TRADE:
                self._attach_stream(strategy)
            if strategy.spec.order_book:
                self._attach_order_book(strategy)
        self._attach_order_events()

    @property
    def stream_connected(self) -> bool:
        """스트림이 열려 있고 로그인까지 끝났는지 - 상태 확인용"""
        return self._stream is not None and self._stream.is_connected

    def _stream_for(self, channel: StreamChannel) -> MarketStream | None:
        """브로커가 채널을 제공하면 공유 스트림을 (필요 시 열어) 돌려주고, 아니면 None"""
        broker = self.broker
        if not isinstance(broker, StreamingBrokerClient) or channel not in broker.capabilities.streams:
            return None
        if self._stream is None:
            self._stream = broker.open_stream()
            self._stream.connect()
        return self._stream

    def _attach_stream(self, strategy: Strategy) -> None:
        """ON_TRADE 전략을 체결가 스트림에 붙인다. 브로커가 지원하지 않으면 경고만 남기고 폴링으로 둔다"""
        spec = strategy.spec
        stream = self._stream_for(StreamChannel.TRADES)
        if stream is None:
            logger.warning("[%s] trigger=ON_TRADE 이지만 브로커 '%s' 는 체결가 스트림을 제공하지 않습니다 - "
                           "poll_interval=%ss 폴링으로 동작합니다", spec.name, self.broker.capabilities.broker_id,
                           spec.poll_interval_seconds)
            return

        def on_trade(tick: TradeTick, strategy=strategy) -> None:
            self.latest_trades[tick.symbol] = tick
            self.request_tick(strategy)

        stream.subscribe_trades(spec.symbols, on_trade)
        logger.info("[%s] trade stream attached / symbols=%s min_tick_interval=%ss",
                    spec.name, spec.symbols, spec.min_tick_interval_seconds)

    def _attach_order_book(self, strategy: Strategy) -> None:
        """spec.order_book 전략의 심볼 호가창을 구독해 컨텍스트로 공급한다. 틱을 촉발하지는 않는다"""
        spec = strategy.spec
        stream = self._stream_for(StreamChannel.ORDER_BOOK)
        if stream is None:
            logger.warning("[%s] order_book=True 이지만 브로커 '%s' 는 호가 스트림을 제공하지 않습니다 - "
                           "컨텍스트의 order_book 은 비어 있습니다", spec.name, self.broker.capabilities.broker_id)
            return

        def on_book(tick: OrderBookTick) -> None:
            self.latest_order_books[tick.symbol] = tick

        stream.subscribe_order_book(spec.symbols, on_book)
        logger.info("[%s] order book stream attached / symbols=%s", spec.name, spec.symbols)

    def _attach_order_events(self) -> None:
        """주문 통보를 구독해 어댑터 추적(apply_order_event)과 브라켓(on_order_event)에 반영한다.
        브로커가 제공하면 항상 붙인다 - 구독 실패(KIS HTS ID 미설정 등)는 경고만 남기고 폴링 판정으로 둔다"""
        if self._order_events_attached:
            return
        broker = self.broker
        if not isinstance(broker, StreamingBrokerClient):
            return
        stream = self._stream_for(StreamChannel.ORDER_EVENTS)
        if stream is None:
            return
        try:
            stream.subscribe_order_events(lambda event: self._on_order_event(broker, event))
            self._order_events_attached = True
            logger.info("order event stream attached")
        except Exception as e:  # noqa: BLE001
            logger.warning("주문 통보 스트림을 구독하지 못했습니다 - 체결 판정은 폴링으로 계속합니다: %s", e)

    def _on_order_event(self, broker: StreamingBrokerClient, event: OrderEvent) -> None:
        logger.info("order event / %s order=%s %s %s qty=%s price=%s", event.type.value, event.order_id,
                    event.symbol or "", event.side.value if event.side else "", event.quantity, event.price)
        try:
            broker.apply_order_event(event)
        except Exception:  # noqa: BLE001
            logger.exception("apply_order_event 실패 / %s", event.order_id)
        try:
            self.brackets.on_order_event(event)
        except Exception:  # noqa: BLE001
            logger.exception("bracket on_order_event 실패 / %s", event.order_id)

    def request_tick(self, strategy: Strategy) -> None:
        """스트림 틱으로 tick 을 요청한다. 이미 대기 중이면 합친다. 스트림 스레드에서 호출되므로 플래그만 세우고 루프를 깨운다"""
        self._pending[strategy.spec.name] = True
        self._wake.set()

    def _remaining_interval(self, strategy: Strategy) -> float:
        """직전 tick 종료 후 min_tick_interval 까지 남은 초 (0 이면 바로 실행 가능)"""
        last = self._last_tick_ended_at.get(strategy.spec.name)
        if last is None:
            return 0.0
        return max(0.0, last + strategy.spec.min_tick_interval_seconds - time.monotonic())

    def run(self) -> None:
        """블로킹 실행 루프. stop() 또는 KeyboardInterrupt 로 종료.

        폴링 스케줄은 그대로 두고(안전망), 스트림 틱이 요청한 전략은 최소 간격이 지났을 때 같은 스레드에서 추가로 돌린다.
        """
        next_run = {s.spec.name: 0.0 for s in self.strategies}
        try:
            while not self._stop.is_set():
                self._wake.clear()  # 이 아래에서 도착하는 틱은 다음 대기를 즉시 깨운다
                now = time.monotonic()
                wait = 1.0
                for strategy in self.strategies:
                    name = strategy.spec.name
                    if now >= next_run[name]:
                        self.tick(strategy)
                        next_run[name] = time.monotonic() + strategy.spec.poll_interval_seconds
                    if self._pending.get(name):
                        # 최소 간격은 실행 직전에 확인한다 - tick 도중 도착한 틱은 종료 시각이 갱신된 뒤에야 판정할 수 있다
                        remaining = self._remaining_interval(strategy)
                        if remaining > 0:
                            wait = min(wait, remaining)
                        else:
                            self._pending.pop(name, None)
                            self.tick(strategy)
                            wait = 0.0  # 방금 돌린 전략에 또 틱이 쌓였을 수 있다 - 바로 재판정
                if wait > 0:
                    self._wake.wait(wait)
        except KeyboardInterrupt:
            logger.info("interrupted - engine stopping")

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        stream = self._stream
        if stream is not None:
            try:
                stream.close()
            except Exception:  # noqa: BLE001
                pass

    def tick(self, strategy: Strategy) -> None:
        spec = strategy.spec
        try:
            if self.guard.is_halted:
                return
            if spec.regular_hours_only and not self.calendar.is_regular_open():
                logger.debug("[%s] market closed - tick skipped", spec.name)
                return

            ctx = self._build_context(strategy)

            bracket_signals = self.brackets.check(ctx)  # 익절/손절이 전략 판단보다 우선
            if bracket_signals:
                self.executor.execute(spec.name, list(bracket_signals), ctx)

            signals = strategy.decide(ctx)
            if signals:
                self.executor.execute(spec.name, signals, ctx)

            self.guard.record_success()
        except MarketClosedError as e:
            logger.debug("[%s] market closed - %s", spec.name, e)  # 휴장 - 실패 아님
        except RateLimitError as e:
            logger.warning("[%s] rate limited - %s", spec.name, e)  # 다음 틱에 회복
        except Exception as e:  # noqa: BLE001
            logger.exception("[%s] tick failed", spec.name)
            self.guard.record_failure(e)
        finally:
            self._last_tick_ended_at[spec.name] = time.monotonic()

    def _stream_quotes(self, strategy: Strategy) -> dict | None:
        """ON_TRADE 전략의 모든 심볼에 스트림 틱이 있으면 그것을 현재가로 쓴다 (REST quotes 1회 절약). 하나라도 없으면 None -> REST"""
        spec = strategy.spec
        if spec.trigger != TickTrigger.ON_TRADE or self._stream is None:
            return None
        quotes = {}
        for symbol in spec.symbols:
            tick = self.latest_trades.get(symbol)
            if tick is None:
                return None
            quotes[symbol] = tick.to_quote()
        return quotes

    def _build_context(self, strategy: Strategy) -> StrategyContext:
        spec = strategy.spec
        quotes = self._stream_quotes(strategy)
        if quotes is None:
            quotes = {q.symbol: q for q in self.broker.get_quotes(spec.symbols)}
        return StrategyContext(
            now=datetime.now(timezone.utc),
            quotes=quotes,
            candles={s: self.broker.get_candles(s, spec.candle_interval, spec.candle_limit)
                     for s in spec.symbols},
            account=self.broker.get_account(),
            holdings={h.symbol: h for h in self.broker.get_holdings()},
            open_orders=[o for o in self.broker.get_orders() if o.status.is_open],
            buying_power=self.broker.get_buying_power(),
            order_books={s: self.latest_order_books[s] for s in spec.symbols if s in self.latest_order_books}
            if spec.order_book else {},
        )


def pnl_report(broker: BrokerClient, initial_capital: Decimal | None = None) -> dict:
    """계좌 수익률 리포트 (Kotlin PnlService 대응)."""
    account = broker.get_account()
    holdings = broker.get_holdings()
    total_mv = sum((h.market_value or Decimal(0)) for h in holdings)
    # 평가손익 합계는 계좌 통화 종목만 - 외화 종목 손익(currency 가 다름)은 단위가 달라 각 holding 에만 있다
    total_pnl = sum((h.unrealized_pnl or Decimal(0)) for h in holdings if h.currency in (None, account.currency))
    total_return = None
    if initial_capital and initial_capital > 0:
        total_return = (account.portfolio_value - initial_capital) / initial_capital
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "account_id": account.account_id,
        "currency": account.currency,
        "cash": account.cash,
        "portfolio_value": account.portfolio_value,
        "total_market_value": total_mv,
        "total_unrealized_pnl": total_pnl,
        "total_return_rate": total_return,
        "holdings": holdings,
    }
