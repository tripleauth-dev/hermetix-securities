"""넥스트증권 모의투자 어댑터 (미국주식) — 공개 스펙 v1.3 기준.

v1.3 응답(quotes outcome, 캔들 time, 계좌 cashAmount, 보유 averageBuyPrice, 캘린더 status+sessions[] …)을
브로커 중립 공통 모델(Quote/Candle/Holding/Order …)로 정규화한다. 공통 모델은 전략이 보는 타입이므로
서버 스펙 변경은 이 어댑터 안에서만 흡수한다 (KIS/키움 어댑터와 같은 방식).

- OAuth client_credentials, 토큰 12h(expires_in=43200). 401 시 1회 재발급-재시도
- 공통 헤더(v1.3): X-Request-Id 는 토큰 발급 외 전 API 필수(누락 시 400 request-id-required),
  계좌·자산·주문 API 는 X-Next-Account-Id 필수(구 X-Nextsecurities-Account 에서 개명)
- 시각은 ISO 8601 · KST (오프셋 생략 시 KST). 등락률·손익률은 % 단위 → 공통 모델 규약(비율)로 /100
- 에러 엔벨로프: {"error": {type, code, message, ...}}. 토큰 발급 400/401 만 OAuth 표준 {error, error_description}
- 고급 주문(STOP/BRACKET)/kill-switch/modify/cancel-all 은 /v2 로 제공 — 이 어댑터는 아직 미연동
"""
from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from ..broker import BrokerClient, RateLimiter, _Http
from ..errors import (
    AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError,
    MarketClosedError, OrderNotFoundError, RateLimitError,
)
from ..models import (
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote,
    SessionHours, TradingEnvironment,
)
from ..tokens import FAILURE_COOLDOWN_SECONDS, TokenManager

ACCOUNT_HEADER = "X-Next-Account-Id"
REQUEST_ID_HEADER = "X-Request-Id"
_REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,64}$")

KST = ZoneInfo("Asia/Seoul")
NEW_YORK = ZoneInfo("America/New_York")
_MARKET = "US"


def _float_or_none(value) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def new_request_id() -> str:
    """v1.3 규칙: 영숫자·점·밑줄·하이픈만, 최대 64자. hmx- 프리픽스 + UUID = 40자."""
    rid = f"hmx-{uuid.uuid4()}"
    assert _REQUEST_ID_PATTERN.match(rid)
    return rid


def _d(value) -> Decimal | None:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _pct(value) -> Decimal | None:
    """% 단위 → 비율 (3.3333 → 0.033333)"""
    rate = _d(value)
    return None if rate is None else rate / Decimal(100)


def _ts(value: str | None) -> datetime | None:
    """v1.3 시각: ISO 8601, 오프셋 생략 시 KST"""
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=KST)
    return parsed


def _ny_clock(value: str | None) -> str | None:
    """KST ISO 시각 → 뉴욕 현지 HH:MM (공통 캘린더 모델은 현지 타임존 + HH:MM)"""
    parsed = _ts(value)
    return None if parsed is None else parsed.astimezone(NEW_YORK).strftime("%H:%M")


class NextClient(BrokerClient):

    capabilities = BrokerCapabilities(
        broker_id="next",
        market="US",
        currency="USD",
        candle_intervals=frozenset({CandleInterval.MIN_1, CandleInterval.DAY_1}),  # v1.3: 1m · 1d
        client_order_id=True,
        native_bracket=False,  # 서버 /v2/orders/advanced(BRACKET) 연동 전까지 소프트웨어 브라켓
        fractional_shares=False,  # v1.3 주문 수량은 정수만
        server_open_orders=True,
        environments=frozenset({TradingEnvironment.PAPER, TradingEnvironment.LIVE}),  # 키 프리픽스로 결정
    )

    def __init__(self, client_id: str, client_secret: str, account_id: str = "acc_main",
                 base_url: str = "https://openapi.nextsecurities.dev",
                 environment: TradingEnvironment = TradingEnvironment.PAPER):
        # 환경은 키 프리픽스가 결정한다 (pk_test_=모의, pk_live_=실전) — 설정과 어긋나면 기동 실패
        expected = "pk_live_" if environment == TradingEnvironment.LIVE else "pk_test_"
        if client_id.startswith("pk_") and not client_id.startswith(expected):
            raise ValueError(f"environment={environment.value} 인데 client_id 가 '{expected}' 로 시작하지 않습니다 "
                             "(모의=pk_test_, 실전=pk_live_). 키와 환경 설정을 맞추세요.")
        self.environment = environment
        self._client_id = client_id
        self._client_secret = client_secret
        self._account_id = account_id
        self._http = _Http(base_url)
        # 429 는 Retry-After 만큼 기다렸다가 최대 2회 재시도. 쓰로틀은 없다 (초당 한도가 넉넉함)
        self._limiter = RateLimiter(0, max_retries=2, backoff=lambda attempt: 1.0 * attempt)
        self._tokens = TokenManager("next", client_id, self._issue_token, 60)

    # ------------------------------------------------------------------ market

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        requested = {self.capabilities.symbol_code(s): s for s in symbols}
        body = self._get("/v1/market/quotes", query={"symbols": ",".join(requested)})
        quotes = []
        for q in body.get("quotes", []):
            # NOT_FOUND / NO_DATA 는 개별 종목의 정상 결과 — 가격이 없으므로 제외한다 (ctx.quote() 가 None)
            if q.get("outcome") != "OK" or q.get("price") is None:
                continue
            quotes.append(Quote(
                symbol=requested.get(q["symbol"], q["symbol"]),  # 요청받은 표기(시장 접두 포함)로
                price=_d(q["price"]),
                bid_price=_d(q.get("bidPrice")),
                ask_price=_d(q.get("askPrice")),
                volume=int(q.get("volume") or 0),
                change=_d(q.get("change")),
                change_rate=_pct(q.get("changeRate")),
                timestamp=_ts(q.get("lastTradeAt")) or _ts(q.get("requestedAt")) or datetime.now(timezone.utc),
            ))
        return quotes

    def get_candles(self, symbol: str, interval: CandleInterval, limit: int | None = None) -> list[Candle]:
        query = {"symbol": self.capabilities.symbol_code(symbol), "interval": interval.value}
        if limit is not None:
            query["limit"] = str(limit)
        body = self._get("/v1/market/candles", query=query)
        return [
            Candle(
                timestamp=_ts(c["time"]),
                open=_d(c["open"]), high=_d(c["high"]), low=_d(c["low"]), close=_d(c["close"]),
                volume=int(c.get("volume") or 0),
            )
            for c in body.get("candles", [])
        ]

    def get_calendar(self) -> list[MarketDay]:
        """v1.3: date 는 거래소 현지 일자, 세션 시각은 KST → 뉴욕 현지 HH:MM 으로 바꿔 담는다."""
        body = self._get("/v1/market/calendar")
        days = []
        for d in body.get("calendar", []):
            regular = next((s for s in d.get("sessions") or [] if s.get("type") == "REGULAR"), None)
            start = _ny_clock(regular.get("open")) if regular else None
            end = _ny_clock(regular.get("close")) if regular else None
            is_open = d.get("status") in ("OPEN", "HALF_DAY") and start is not None and end is not None
            days.append(MarketDay(
                date=d["date"],
                open=is_open,
                regular=SessionHours(start, end) if is_open else None,
                timezone="America/New_York",
                holiday=d.get("holidayName"),
            ))
        return days

    # ----------------------------------------------------------------- account

    def get_account(self) -> Account:
        """v1.3 계좌 응답은 예수금(cashAmount)만 준다. 총평가는 예수금 + 보유 평가금액 합 (보유 조회 1회 추가)."""
        body = self._get("/v1/account", account=True)
        cash = _d(body["cashAmount"])
        market_value = sum((h.market_value or Decimal(0) for h in self.get_holdings()), Decimal(0))
        return Account(
            account_id=body["accountId"],
            currency=body.get("currency") or "USD",
            cash=cash,
            portfolio_value=cash + market_value,
            status="ACTIVE",
            name=None,
        )

    def get_holdings(self) -> list[Holding]:
        body = self._get("/v1/account/holdings", account=True)
        return [
            Holding(
                symbol=h["symbol"],
                quantity=_d(h["quantity"]),
                avg_entry_price=_d(h["averageBuyPrice"]),
                current_price=_d(h.get("currentPrice")),
                market_value=_d(h.get("evaluationAmount")),
                unrealized_pnl=_d(h.get("evaluationPnl")),
                unrealized_pnl_rate=_pct(h.get("evaluationPnlRate")),
            )
            for h in body.get("holdings", [])
        ]

    def get_buying_power(self) -> Decimal:
        return _d(self._get("/v1/account/buying-power", account=True)["buyingPower"])

    # ------------------------------------------------------------------ orders

    def create_order(self, request: CreateOrderRequest) -> Order:
        payload = {
            # v1.3: clientOrderId(멱등키)·market 필수 — 호출자가 안 주면 어댑터가 UUID 를 만든다
            "clientOrderId": request.client_order_id or str(uuid.uuid4()),
            "market": _MARKET,
            "symbol": self.capabilities.symbol_code(request.symbol),
            "side": request.side.value,
            "orderType": request.order_type.value,
            "quantity": str(request.quantity),
            "timeInForce": request.time_in_force.value,
        }
        if request.limit_price is not None:
            payload["limitPrice"] = str(request.limit_price)
        return self._order(self._post("/v1/orders", payload, account=True))

    def get_orders(self) -> list[Order]:
        body = self._get("/v1/orders", account=True)
        return [self._order(o) for o in body.get("orders", [])]

    def get_order(self, order_id: str) -> Order:
        return self._order(self._get(f"/v1/orders/{order_id}", account=True))

    def cancel_order(self, order_id: str) -> Order:
        return self._order(self._request("DELETE", f"/v1/orders/{order_id}", account=True))

    def get_fills(self) -> list[Fill]:
        body = self._get("/v1/orders/fills", account=True)
        return [
            Fill(
                fill_id=None,  # v1.3: 원장이 체결 ID 를 발급하지 않는다
                order_id=f.get("orderId"), symbol=f.get("symbol"),
                side=OrderSide(f["side"]) if f.get("side") in ("BUY", "SELL") else None,
                quantity=_d(f.get("quantity")), price=_d(f.get("price")),
            )
            for f in body.get("fills", [])
        ]

    # ---------------------------------------------------------------- internal

    @staticmethod
    def _order(body: dict) -> Order:
        """생성/상세/취소 응답 공통 — 생성·취소는 orderId/status/requestedAt 만 온다."""
        status = body.get("status", "UNKNOWN")
        order_type = (body.get("orderType") or "").upper()
        return Order(
            order_id=body["orderId"],
            status=OrderStatus(status) if status in OrderStatus.__members__ else OrderStatus.UNKNOWN,
            symbol=body.get("symbol"),
            side=OrderSide(body["side"]) if body.get("side") in ("BUY", "SELL") else None,
            order_type=OrderType(order_type) if order_type in ("MARKET", "LIMIT") else None,
            quantity=_d(body.get("quantity")),
            limit_price=_d(body.get("limitPrice")),
            filled_quantity=_d(body.get("filledQuantity")),
            avg_fill_price=_d(body.get("avgFillPrice")),
            client_order_id=body.get("requestId"),  # v1.3 상세 조회의 clientOrderId 필드명
            submitted_at=_ts(body.get("requestedAt")),
            canceled_at=None,
        )

    def _get(self, path: str, *, query: dict | None = None, account: bool = False) -> dict:
        return self._request("GET", path, query=query, account=account)

    def _post(self, path: str, payload: dict, *, account: bool = False) -> dict:
        return self._request("POST", path, json_body=payload, account=account)

    def _request(self, method: str, path: str, *, query: dict | None = None,
                 json_body: dict | None = None, account: bool = False) -> dict:
        def call(token: str) -> dict:
            headers = {
                "Authorization": f"Bearer {token}",
                REQUEST_ID_HEADER: new_request_id(),
            }
            if account and self._account_id:
                headers[ACCOUNT_HEADER] = self._account_id
            status, body = self._http.request(method, path, headers=headers, query=query, json_body=json_body)
            if status < 200 or status >= 300:
                retry_after = (getattr(self._http, "last_headers", None) or {}).get("retry-after")
                raise self._map_error(status, body.get("error") or {}, _float_or_none(retry_after))
            return body

        # 토큰 거부(만료) 시 1회 재발급 후 재시도는 TokenManager.call
        return self._limiter.execute(lambda: self._tokens.call(call), "next")

    @staticmethod
    def _map_error(status: int, error: dict, retry_after_seconds: float | None = None) -> BrokerApiError:
        """v1.3 에러 type ↔ HTTP: validation(400) authentication(401) permission(403) not_found(404)
        conflict(409) business_rule(422) locked(423, 킬스위치 trading-halted) rate_limit(429) server(5xx)"""
        code = error.get("code")
        error_type = error.get("type")
        message = f"Next({code}) {error.get('message', '')} requestId={error.get('requestId')}"
        if status == 401 or error_type == "authentication":
            return AuthError(status, code, message)
        if status == 429:
            return RateLimitError(status, code, message, retry_after_seconds)
        if error_type == "permission":
            # 조회전용 키(insufficient-scope)·계좌 불일치 — 자금 부족으로 오인하지 않는다
            return BrokerApiError(status, code, message)
        if code == "order-not-found":
            return OrderNotFoundError(code, message)
        if code and "insufficient" in code:
            return InsufficientFundsError(status, code, message)
        if code == "trading-halted" or (code and "market-closed" in code):
            return MarketClosedError(status, code, message)
        if error_type in ("validation", "business_rule"):
            return InvalidOrderError(status, code, message)
        return BrokerApiError(status, code, message)

    def _get_token(self) -> str:
        return self._tokens.get()

    def _issue_token(self) -> tuple[str, float]:
        with self._usage.measure("auth"):
            status, body = self._http.request(
                "POST", "/v1/oauth/token",
                headers={REQUEST_ID_HEADER: new_request_id()},
                form_body={"grant_type": "client_credentials",
                           "client_id": self._client_id, "client_secret": self._client_secret},
            )
            if status != 200 or "access_token" not in body:
                retry_after = (getattr(self._http, "last_headers", None) or {}).get("retry-after")
                raise self._token_error(status, body, _float_or_none(retry_after))
            return body["access_token"], time.time() + float(body.get("expires_in", 43200))

    @staticmethod
    def _token_error(status: int, body: dict, retry_after_seconds: float | None = None) -> BrokerApiError:
        error = body.get("error")
        # 토큰 발급 400/401 은 OAuth 표준 형식: {"error": "invalid_client", "error_description": "..."}
        if isinstance(error, str):
            return AuthError(status, error, f"Next 토큰 발급 실패({error}): {body.get('error_description') or ''}")
        # 429/5xx 는 플랫폼 엔벨로프: {"error": {"code": .., "message": ..}}
        error = error or {}
        message = f"Next 토큰 발급 실패({error.get('code')}): {error.get('message') or ''}"
        if status == 429:
            return RateLimitError(status, error.get("code"), message, retry_after_seconds or FAILURE_COOLDOWN_SECONDS)
        return AuthError(status, error.get("code"), message)
