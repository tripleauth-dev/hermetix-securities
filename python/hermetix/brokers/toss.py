"""토스증권 Open API 어댑터 (REST v1.2.15).

⚠️ 실전 전용 · 실측 전 — 토스증권은 모의투자 샌드박스가 없다. 공식 OpenAPI 문서로 구현했고 실계좌 소액 검증 전까지 "미검증".
반드시 live_trading_enabled=True 와 주문 금액 상한을 설정하고 소액으로 시작하라.

규약: /api/v1/… + Authorization: Bearer. 계좌·자산·주문 API 는 X-Tossinvest-Account: {accountSeq} 헤더 필수.
성공 {"result": …}, 에러 {"error": {requestId, code, message, data}}. client 당 유효 토큰 1개(재발급 시 이전 토큰 무효).
한 계좌로 KRX·미국을 다룬다 → 보유·주문 심볼은 KRX:005930 / US:AAPL 로 접두를 붙여 돌려준다.
공통 모델과의 차이: 시세에 등락·거래량 없음, 예수금 없음(KRW 매수가능금액으로 대체), 체결 엔드포인트 없음(종료 주문 execution 집계),
취소·정정은 새 orderId 발급(원주문 ID 유지 + PENDING_CANCEL), 캘린더는 KRX 합성.
"""
from __future__ import annotations

import threading
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from ..broker import MarketStream, RateLimiter, StreamingBrokerClient, _Http, krx_calendar, krx_tick_round
from ..errors import (
    AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, MarketClosedError, OrderNotFoundError, RateLimitError,
)
from ..models import (
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest, StreamChannel,
    Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote, TradingEnvironment, parse_symbol,
)
from ..tokens import FAILURE_COOLDOWN_SECONDS, TokenManager
from .toss_stream import TossMarketStream

_STATUS = {
    "PENDING": OrderStatus.SUBMITTED, "PENDING_REPLACE": OrderStatus.SUBMITTED, "PENDING_CANCEL": OrderStatus.PENDING_CANCEL,
    "PARTIAL_FILLED": OrderStatus.PARTIALLY_FILLED, "FILLED": OrderStatus.FILLED,
    "CANCELED": OrderStatus.CANCELED, "REPLACED": OrderStatus.CANCELED,
    "REJECTED": OrderStatus.REJECTED, "CANCEL_REJECTED": OrderStatus.REJECTED, "REPLACE_REJECTED": OrderStatus.REJECTED,
}


def _d(value) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def _ts(value) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


class TossClient(StreamingBrokerClient):

    # 실시간 웹소켓 (AsyncAPI 1.2.2, 실측 전) - 계정당 연결 2개(3번째가 오면 가장 오래된 것 종료), 구독 100개, 선언 5회/초,
    # 180초 무송신 시 서버가 끊음 -> 60초 PING, 토큰은 핸드셰이크에서만 검사, 재접속 시 REST 토큰 재사용(재발급하면 이전 토큰 무효)
    WS_URL = "wss://openapi-ws.tossinvest.com/ws/v1"

    capabilities = BrokerCapabilities(
        broker_id="toss", market="KRX", currency="KRW",
        candle_intervals=frozenset({CandleInterval.MIN_1, CandleInterval.DAY_1}),
        client_order_id=True, native_bracket=False, fractional_shares=False, server_open_orders=True,
        environments=frozenset({TradingEnvironment.LIVE}),  # 샌드박스 없음
        markets=frozenset({"KRX", "US"}),
        # AsyncAPI 1.2.2 기반, 실측 전 - trade/orderbook:{kr,us}·personal:order 선언형 구독
        streams=frozenset({StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS}),
    )

    def __init__(self, client_id: str, client_secret: str, account_seq: str = "", base_url: str = "https://openapi.tossinvest.com",
                 throttle_seconds: float = 0.2, environment: TradingEnvironment = TradingEnvironment.LIVE, ws_url: str = WS_URL):
        """account_seq 를 비우면 GET /api/v1/accounts 의 첫 BROKERAGE 계좌를 쓴다."""
        self.environment = environment
        self._ws_url = ws_url
        self._client_id = client_id
        self._client_secret = client_secret
        self._account_seq = account_seq
        self._http = _Http(base_url)
        self._limiter = RateLimiter(throttle_seconds, max_retries=3, backoff=lambda attempt: float(1 << (attempt - 1)))
        self._tokens = TokenManager("toss", client_id, self._issue_token, 60)
        self._account_lock = threading.Lock()

    # ------------------------------------------------------------------ market

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        requested = {self.capabilities.symbol_code(s): s for s in symbols}
        result = self._call("GET", "/api/v1/prices", query={"symbols": ",".join(requested)})
        quotes = []
        for q in result or []:
            price = _d(q.get("lastPrice"))
            if price is None:
                continue
            quotes.append(Quote(symbol=requested.get(q.get("symbol"), q.get("symbol")), price=price, bid_price=None, ask_price=None,
                                volume=0, change=None, change_rate=None,  # /prices 에 등락·거래량 없음
                                timestamp=_ts(q.get("timestamp")) or datetime.now(timezone.utc)))
        return quotes

    def get_candles(self, symbol: str, interval: CandleInterval, limit: int | None = None) -> list[Candle]:
        if interval not in self.capabilities.candle_intervals:
            raise ValueError(f"토스 어댑터는 1m/1d 캔들만 지원합니다 ({interval.value})")
        count = max(1, min(limit or 100, 200))
        result = self._call("GET", "/api/v1/candles", query={"symbol": self.capabilities.symbol_code(symbol), "interval": interval.value, "count": str(count)})
        candles = []
        for c in (result or {}).get("candles") or []:
            ts = _ts(c.get("timestamp"))
            if ts is None:
                continue
            candles.append(Candle(timestamp=ts, open=_d(c.get("openPrice")) or Decimal(0), high=_d(c.get("highPrice")) or Decimal(0),
                                  low=_d(c.get("lowPrice")) or Decimal(0), close=_d(c.get("closePrice")) or Decimal(0),
                                  volume=int(_d(c.get("volume")) or 0)))
        candles.sort(key=lambda c: c.timestamp)  # 최신순 → 과거→최신
        return candles

    def get_calendar(self) -> list[MarketDay]:
        return krx_calendar()

    # ----------------------------------------------------------------- account

    def get_account(self) -> Account:
        cash = self._buying_power("KRW")
        market_value = sum((h.market_value or Decimal(0) for h in self.get_holdings()), Decimal(0))
        return Account(account_id=self._account(), currency="KRW", cash=cash, portfolio_value=cash + market_value)

    def get_holdings(self) -> list[Holding]:
        result = self._call("GET", "/api/v1/holdings", account=True) or {}
        holdings = []
        for h in result.get("items") or []:
            qty = _d(h.get("quantity"))
            if qty is None or qty <= 0:
                continue
            market = "US" if h.get("marketCountry") == "US" else "KRX"
            holdings.append(Holding(
                symbol=f"{market}:{h.get('symbol')}", quantity=qty,
                avg_entry_price=_d(h.get("averagePurchasePrice")) or Decimal(0), current_price=_d(h.get("lastPrice")),
                market_value=_d(((h.get("marketValue") or {}).get("amount"))) if not isinstance((h.get("marketValue") or {}).get("amount"), dict) else _d((h.get("marketValue") or {}).get("amount", {}).get("krw")),
                unrealized_pnl=_d(((h.get("profitLoss") or {}).get("amount"))) if not isinstance((h.get("profitLoss") or {}).get("amount"), dict) else _d((h.get("profitLoss") or {}).get("amount", {}).get("krw")),
                unrealized_pnl_rate=_d((h.get("profitLoss") or {}).get("rate")),  # 이미 소수 비율
            ))
        return holdings

    def get_buying_power(self) -> Decimal:
        return self._buying_power("KRW")

    # ------------------------------------------------------------------ orders

    def create_order(self, request: CreateOrderRequest) -> Order:
        market, _ = parse_symbol(request.symbol)
        code = self.capabilities.symbol_code(request.symbol)
        is_krx = (market or "KRX") == "KRX"
        is_limit = request.order_type == OrderType.LIMIT
        body = {"clientOrderId": request.client_order_id or str(uuid.uuid4()), "symbol": code, "side": request.side.value,
                "orderType": request.order_type.value, "timeInForce": "DAY", "quantity": str(request.quantity), "confirmHighValueOrder": False}
        if is_limit:
            body["price"] = str(krx_tick_round(request.limit_price)) if is_krx else str(request.limit_price)
        result = self._call("POST", "/api/v1/orders", json_body=body, account=True) or {}
        order_id = str(result.get("orderId", "")).strip()
        if not order_id:
            raise BrokerApiError(200, None, "토스 주문 응답에 orderId 가 없습니다")
        return Order(order_id=order_id, status=OrderStatus.SUBMITTED, symbol=f"{'KRX' if is_krx else 'US'}:{code}", side=request.side,
                     order_type=request.order_type, quantity=request.quantity, limit_price=request.limit_price,
                     filled_quantity=Decimal(0), client_order_id=result.get("clientOrderId") or request.client_order_id,
                     submitted_at=datetime.now(timezone.utc))

    def get_orders(self) -> list[Order]:
        result = self._call("GET", "/api/v1/orders", query={"status": "OPEN"}, account=True) or {}
        return [self._to_order(o) for o in result.get("orders") or []]

    def get_order(self, order_id: str) -> Order:
        return self._to_order(self._call("GET", f"/api/v1/orders/{order_id}", account=True) or {})

    def cancel_order(self, order_id: str) -> Order:
        self._call("POST", f"/api/v1/orders/{order_id}/cancel", json_body={}, account=True)  # 새 orderId 발급 — 원주문 ID 유지
        return Order(order_id=order_id, status=OrderStatus.PENDING_CANCEL, canceled_at=datetime.now(timezone.utc))

    def get_fills(self) -> list[Fill]:
        result = self._call("GET", "/api/v1/orders", query={"status": "CLOSED"}, account=True) or {}
        fills = []
        for o in result.get("orders") or []:
            ex = o.get("execution") or {}
            qty = _d(ex.get("filledQuantity"))
            if qty is None or qty <= 0:
                continue
            side = o.get("side")
            fills.append(Fill(fill_id=None, order_id=o.get("orderId"), symbol=f"{'US' if o.get('currency') == 'USD' else 'KRX'}:{o.get('symbol')}",
                              side=OrderSide(side) if side in ("BUY", "SELL") else None, quantity=qty, price=_d(ex.get("averageFilledPrice"))))
        return fills

    # ---------------------------------------------------------------- internal

    @staticmethod
    def _to_order(o: dict) -> Order:
        ex = o.get("execution") or {}
        side, otype = o.get("side"), o.get("orderType")
        return Order(order_id=str(o.get("orderId", "")), status=_STATUS.get(str(o.get("status")), OrderStatus.UNKNOWN),
                     symbol=f"{'US' if o.get('currency') == 'USD' else 'KRX'}:{o.get('symbol')}",
                     side=OrderSide(side) if side in ("BUY", "SELL") else None,
                     order_type=OrderType(otype) if otype in ("MARKET", "LIMIT") else None,
                     quantity=_d(o.get("quantity")), limit_price=_d(o.get("price")), filled_quantity=_d(ex.get("filledQuantity")) or Decimal(0),
                     avg_fill_price=_d(ex.get("averageFilledPrice")), client_order_id=o.get("clientOrderId"),
                     submitted_at=_ts(o.get("orderedAt")), canceled_at=_ts(o.get("canceledAt")))

    def _buying_power(self, currency: str) -> Decimal:
        return _d((self._call("GET", "/api/v1/buying-power", query={"currency": currency}, account=True) or {}).get("cashBuyingPower")) or Decimal(0)

    def _account(self) -> str:
        if self._account_seq:
            return self._account_seq
        with self._account_lock:
            if self._account_seq:
                return self._account_seq
            accounts = self._call("GET", "/api/v1/accounts") or []
            picked = next((a for a in accounts if a.get("accountType") == "BROKERAGE"), accounts[0] if accounts else None)
            if picked is None:
                raise BrokerApiError(200, None, "토스 계좌 목록이 비어 있습니다")
            self._account_seq = str(picked.get("accountSeq"))
            return self._account_seq

    def _call(self, method: str, path: str, *, query: dict | None = None, json_body: dict | None = None, account: bool = False):
        return self._limiter.execute(
            lambda: self._tokens.call(lambda token: self._call_once(method, path, query, json_body, account, token)), f"toss {path}")

    def _call_once(self, method: str, path: str, query, json_body, account: bool, token: str):
        headers = {"Authorization": f"Bearer {token}"}
        if account:
            headers["X-Tossinvest-Account"] = self._account()
        status, parsed = self._http.request(method, path, headers=headers, query=query, json_body=json_body)
        if status < 200 or status >= 300:
            error = parsed.get("error") or {}
            code = str(error.get("code", ""))
            msg = f"Toss({path}) [{code}] {error.get('message', '')} requestId={error.get('requestId', '')}".strip()
            retry_after = (getattr(self._http, "last_headers", None) or {}).get("retry-after")
            try:
                retry_after = float(retry_after) if retry_after else None
            except ValueError:
                retry_after = None
            if status == 429:
                raise RateLimitError(status, code, msg, retry_after)
            if status == 401:
                raise AuthError(status, code, msg)
            if code == "order-not-found":
                raise OrderNotFoundError(code, msg)
            if code == "insufficient-buying-power":
                raise InsufficientFundsError(status, code, msg)
            if code == "order-hours-closed":
                raise MarketClosedError(status, code, msg)
            if status in (400, 409, 422):
                raise InvalidOrderError(status, code, msg)
            raise BrokerApiError(status, code, msg)
        return parsed.get("result")

    # ------------------------------------------------------------------ stream

    def open_stream(self) -> MarketStream:
        return TossMarketStream(self._ws_url, self._get_token, self._account, usage=self._usage)

    def _get_token(self) -> str:
        return self._tokens.get()

    def _issue_token(self) -> tuple[str, float]:
        with self._usage.measure("auth"):
            self._limiter.throttle.wait()
            status, body = self._http.request("POST", "/oauth2/token", form_body={
                "grant_type": "client_credentials", "client_id": self._client_id, "client_secret": self._client_secret})
            if status != 200 or not body.get("access_token"):
                error = body.get("error")
                code = error.get("code") if isinstance(error, dict) else error
                msg = f"토스 토큰 발급 실패({code}): {body.get('error_description') or (error.get('message') if isinstance(error, dict) else '') or ''}"
                if status == 429:
                    retry_after = (getattr(self._http, "last_headers", None) or {}).get("retry-after")
                    raise RateLimitError(status, code, msg, _float_or_none(retry_after) or FAILURE_COOLDOWN_SECONDS)
                raise AuthError(status, code, msg)
            return body["access_token"], time.time() + float(body.get("expires_in", 86400))


def _float_or_none(value) -> float | None:
    try:
        return float(value) if value else None
    except ValueError:
        return None
