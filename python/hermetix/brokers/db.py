"""DB증권 REST OpenAPI 어댑터.

⚠️ 문서 기반 구현 (실측 전) — 공식 SDK(DBsecurities/dbsec-open-api) 소스·예제 docstring 과 포털 문서에서
엔드포인트·필드명·에러 코드를 역추적했다. 모의서버 실측 전까지 상태는 "미검증".

규약 (SDK 기준):
- 모든 API 는 POST, 본문 {"In": {...}}, 응답 rsp_cd/rsp_msg + Out(객체 또는 배열) / Out1. TR 코드는 문서용, 경로로 식별
- 헤더 authorization: Bearer + cont_yn/cont_key (+ 법인만 mac_address). appkey 헤더 없음
- 토큰 POST /oauth2/token 은 form-urlencoded(appsecretkey), 24h, 발급 1분 1건(초과 시 403 + IGW00201 → RateLimitError).
  발급 토큰은 ~/.hermetix/tokens/db-<키 해시>.json 에 저장해 프로세스 간 재사용하고, 발급 실패 후 60초는 서버에 다시 묻지 않는다
- 운영/모의 같은 호스트 — 모의 키로만 분기. HTTP 200 + rsp_cd != 00000 이 업무 오류(이때 Out 없음)
- 앱 20 TPS 이지만 잔고·체결 2 TPS, 예수금 1 TPS → 500ms 쓰로틀 + IGW00201 지수 백오프

미확인(실측 필요): 응답 숫자의 JSON 타입, IsuNo 의 A 접두 여부, 일봉 정렬(최신일 우선 추정), PrdyVrss 부호 여부
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from ..broker import KST, MarketStream, RateLimiter, StreamingBrokerClient, _Http, krx_calendar, krx_tick_round
from ..errors import (
    AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, MarketClosedError, OrderNotFoundError, RateLimitError,
)
from ..models import (
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote, StreamChannel, TradingEnvironment,
)
from ..tokens import FAILURE_COOLDOWN_SECONDS, TokenManager
from .db_stream import DbMarketStream

_AUTH_CODES = {"IGW00121", "IGW00122", "IGW00123", "IGW40342"}
_MARKET_CLOSED_CODES = {"2611", "3589", "3590", "3563"}
_INSUFFICIENT_CODES = {"1584", "2714", "2752", "M100"}
_INVALID_ORDER_CODES = {"2706", "3180", "3181"}
_ORDER_NOT_FOUND_CODES = {"3056", "3416"}

# 토큰 발급은 1분 1건 (초과 시 HTTP 403 + IGW00201) - 쿨다운·파일 캐시는 TokenManager
TOKEN_REFRESH_MARGIN_SECONDS = 600.0


def _d(value) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except Exception:  # noqa: BLE001
        return None


def _pct(value) -> Decimal | None:
    rate = _d(value)
    return None if rate is None else rate / Decimal(100)


def normalize_code(raw) -> str:
    """계좌·주문계 IsuNo 는 A005930 형태일 수 있다 → 6자리 코드"""
    text = str(raw or "").strip()
    return text[1:] if len(text) == 7 and text[0] == "A" else text


class DbClient(StreamingBrokerClient):

    # 실시간 웹소켓 - 운영 7070 / 모의 17070 (호스트 공유, 포트로 분리). 접속 후 10초 내 첫 전송, 계좌당 세션 2개·종목 50개, 연결 6회/분 (실측 전)
    PAPER_WS_URL = "wss://openapi.dbsec.co.kr:17070/websocket"
    LIVE_WS_URL = "wss://openapi.dbsec.co.kr:7070/websocket"

    capabilities = BrokerCapabilities(
        broker_id="db",
        market="KRX",
        currency="KRW",
        candle_intervals=frozenset({CandleInterval.DAY_1}),
        client_order_id=False,
        native_bracket=False,
        fractional_shares=False,
        server_open_orders=True,
        environments=frozenset({TradingEnvironment.PAPER, TradingEnvironment.LIVE}),
        # 문서 기반, 실측 전 - S00 체결·S01 호가·IS0/IS1 주문 통보
        streams=frozenset({StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS}),
    )

    def __init__(self, app_key: str, app_secret: str, base_url: str = "https://openapi.dbsec.co.kr:8443",
                 mac_address: str = "", market_div_code: str = "J", throttle_seconds: float = 0.5,
                 environment: TradingEnvironment = TradingEnvironment.PAPER, ws_url: str = ""):
        """운영/모의는 같은 호스트 — 모의투자용 키로만 분기된다. environment 는 엔진의 실전 게이트용 선언이다.
        ws_url 을 비우면 실시간 웹소켓은 모의 :17070 / 운영 :7070 (/websocket)."""
        self.environment = environment
        self._ws_url = ws_url or (self.LIVE_WS_URL if environment == TradingEnvironment.LIVE else self.PAPER_WS_URL)
        self._app_key = app_key
        self._app_secret = app_secret
        self._mac_address = mac_address
        self._market_div_code = market_div_code
        self._http = _Http(base_url)
        self._limiter = RateLimiter(throttle_seconds, max_retries=4, backoff=lambda attempt: float(1 << (attempt - 1)))
        self._tokens = TokenManager("db", app_key, self._issue_token, TOKEN_REFRESH_MARGIN_SECONDS)

    # ------------------------------------------------------------------ market

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        quotes = []
        for symbol in symbols:
            out = self._call("/api/v1/quote/kr-stock/inquiry/price",
                             {"InputCondMrktDivCode": self._market_div_code,
                              "InputIscd1": self.capabilities.symbol_code(symbol)}).get("Out") or {}
            bid, ask = _d(out.get("Bidp1")), _d(out.get("Askp1"))
            quotes.append(Quote(
                symbol=symbol, price=_d(out.get("Prpr")) or Decimal(0),
                bid_price=bid if bid and bid > 0 else None, ask_price=ask if ask and ask > 0 else None,
                volume=int(_d(out.get("AcmlVol")) or 0), change=_d(out.get("PrdyVrss")), change_rate=_pct(out.get("PrdyCtrt")),
                timestamp=datetime.now(timezone.utc),
            ))
        return quotes

    def get_candles(self, symbol: str, interval: CandleInterval, limit: int | None = None) -> list[Candle]:
        if interval != CandleInterval.DAY_1:
            raise ValueError("DB 어댑터는 일봉(DAY_1)만 지원합니다.")
        count = limit or 30
        today = datetime.now(KST)
        start = today - timedelta(days=count * 16 // 10 + 10)  # 휴장일 여유
        rows = self._call("/api/v1/quote/kr-chart/day", {
            "InputOrgAdjPrc": "1", "InputCondMrktDivCode": self._market_div_code,
            "InputIscd1": self.capabilities.symbol_code(symbol),
            "InputDate1": start.strftime("%Y%m%d"), "InputDate2": today.strftime("%Y%m%d")}).get("Out") or []
        candles = []
        for r in rows:
            date = str(r.get("Date", "")).strip()
            if len(date) != 8:
                continue
            candles.append(Candle(
                timestamp=datetime.strptime(date, "%Y%m%d").replace(tzinfo=KST),
                open=_d(r.get("Oprc")) or Decimal(0), high=_d(r.get("Hprc")) or Decimal(0),
                low=_d(r.get("Lprc")) or Decimal(0), close=_d(r.get("Prpr")) or Decimal(0),
                volume=int(_d(r.get("CntgVol")) or _d(r.get("AcmlVol")) or 0),  # 2024 샘플은 AcmlVol
            ))
        candles.sort(key=lambda c: c.timestamp)
        return candles[-count:]

    def get_calendar(self) -> list[MarketDay]:
        return krx_calendar()

    # ----------------------------------------------------------------- account

    def get_account(self) -> Account:
        out = self._balance().get("Out") or {}
        portfolio = _d(out.get("DpsastAmt")) or Decimal(0)  # 예탁자산 = 현금 + 평가
        cash = _d(out.get("Dps2"))
        if cash is None:
            cash = portfolio - (_d(out.get("TotEvalAmt")) or Decimal(0))
        return Account(account_id=f"db-{self.environment.value.lower()}", currency="KRW", cash=cash, portfolio_value=portfolio)

    def get_holdings(self) -> list[Holding]:
        holdings = []
        for row in self._balance().get("Out1") or []:
            qty = _d(row.get("BalQty0")) or _d(row.get("BalQty"))
            if qty is None or qty <= 0:
                continue
            holdings.append(Holding(
                symbol=normalize_code(row.get("IsuNo")), quantity=qty,
                avg_entry_price=_d(row.get("ExecPrc")) or Decimal(0), current_price=_d(row.get("NowPrc")),
                market_value=_d(row.get("EvalAmt")), unrealized_pnl=_d(row.get("EvalPnlAmt")),
                unrealized_pnl_rate=_pct(row.get("Ernrat")),
            ))
        return holdings

    def get_buying_power(self) -> Decimal:
        out = self._call("/api/v1/trading/kr-stock/inquiry/acnt-deposit", {}).get("Out1") or {}
        return _d(out.get("DpsBalAmt")) or _d(out.get("WthdwAbleAmt")) or Decimal(0)

    # ------------------------------------------------------------------ orders

    def create_order(self, request: CreateOrderRequest) -> Order:
        code = self.capabilities.symbol_code(request.symbol)
        is_limit = request.order_type == OrderType.LIMIT
        out = self._call("/api/v1/trading/kr-stock/order", {
            "IsuNo": code, "TrchNo": 1, "OrdQty": int(request.quantity),
            "OrdPrc": int(krx_tick_round(request.limit_price)) if is_limit else 0,
            "BnsTpCode": "2" if request.side == OrderSide.BUY else "1",
            "OrdprcPtnCode": "00" if is_limit else "03",
            "MgntrnCode": "000", "LoanDt": "00000000", "OrdCndiTpCode": "0",
        }).get("Out") or {}
        order_id = str(out.get("OrdNo", "")).strip()
        if not order_id:
            raise BrokerApiError(200, None, "DB 주문 응답에 OrdNo 가 없습니다")
        return Order(order_id=order_id, status=OrderStatus.SUBMITTED, symbol=code, side=request.side,
                     order_type=request.order_type, quantity=request.quantity, limit_price=request.limit_price,
                     filled_quantity=Decimal(0), client_order_id=request.client_order_id,
                     submitted_at=datetime.now(timezone.utc))

    def get_orders(self) -> list[Order]:
        return [o for o in (self._to_order(r) for r in self._history_rows()) if o.status.is_open]

    def get_order(self, order_id: str) -> Order:
        for row in self._history_rows():
            if _same_no(row.get("OrdNo"), order_id):
                return self._to_order(row)
        return Order(order_id=order_id, status=OrderStatus.CANCELED)

    def cancel_order(self, order_id: str) -> Order:
        row = next((r for r in self._history_rows() if _same_no(r.get("OrdNo"), order_id)), None)
        if row is None:
            raise OrderNotFoundError("order-not-found", f"DB 당일 주문에서 찾을 수 없습니다: {order_id}")
        self._call("/api/v1/trading/kr-stock/order-cancel", {
            "OrgOrdNo": int(order_id.lstrip("0") or "0"), "IsuNo": normalize_code(row.get("IsuNo")),
            "OrdQty": int(_remaining(row))})
        return Order(order_id=order_id, status=OrderStatus.PENDING_CANCEL, canceled_at=datetime.now(timezone.utc))

    def get_fills(self) -> list[Fill]:
        fills = []
        for row in self._history_rows():
            qty = _d(row.get("AllExecQty"))
            if qty is None or qty <= 0:
                continue
            fills.append(Fill(fill_id=None, order_id=str(row.get("OrdNo", "")).strip(), symbol=normalize_code(row.get("IsuNo")),
                              side=_side_of(row), quantity=qty, price=_d(row.get("AvrExecPrc"))))
        return fills

    # ---------------------------------------------------------------- internal

    def _balance(self) -> dict:
        return self._call("/api/v1/trading/kr-stock/inquiry/balance", {"QryTpCode0": "0"})

    def _history_rows(self) -> list[dict]:
        return self._call("/api/v1/trading/kr-stock/inquiry/transaction-history", {
            "SorTpYn": "2", "ExecYn": "0", "TrdMktCode": "0", "BnsTpCode": "0", "IsuTpCode": "0", "QryTp": "0"}).get("Out1") or []

    @staticmethod
    def _to_order(row: dict) -> Order:
        ord_qty = _d(row.get("OrdQty")) or Decimal(0)
        filled = _d(row.get("AllExecQty")) or Decimal(0)
        remaining = _remaining(row)
        trx = str(row.get("OrdTrxPtnCode", "")).strip()
        if trx == "9":
            status = OrderStatus.PENDING_CANCEL
        elif trx == "8":
            status = OrderStatus.CANCELED
        elif remaining > 0 and filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        elif remaining > 0:
            status = OrderStatus.SUBMITTED
        elif filled > 0 and filled >= ord_qty:
            status = OrderStatus.FILLED
        elif filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        else:
            status = OrderStatus.CANCELED
        price = _d(row.get("OrdPrc"))
        avg = _d(row.get("AvrExecPrc"))
        return Order(order_id=str(row.get("OrdNo", "")).strip(), status=status, symbol=normalize_code(row.get("IsuNo")),
                     side=_side_of(row), order_type=OrderType.MARKET if str(row.get("OrdprcPtnCode", "")) == "03" else OrderType.LIMIT,
                     quantity=ord_qty, limit_price=price if price and price > 0 else None,
                     filled_quantity=filled, avg_fill_price=avg if avg and avg > 0 else None)

    def _call(self, path: str, body: dict) -> dict:
        return self._limiter.execute(lambda: self._tokens.call(lambda token: self._call_once(path, body, token)), f"DB {path}")

    def _call_once(self, path: str, body: dict, token: str) -> dict:
        headers = {"authorization": f"Bearer {token}", "cont_yn": "N", "cont_key": ""}
        if self._mac_address:
            headers["mac_address"] = self._mac_address
        status, parsed = self._http.request("POST", path, headers=headers, json_body={"In": body})
        code = str(parsed.get("rsp_cd", "")).strip()
        rsp_msg = str(parsed.get("rsp_msg", ""))
        msg = f"DB({path}) [{code}] {rsp_msg}".strip()
        if status < 200 or status >= 300 or code != "00000":
            retry_after = (getattr(self._http, "last_headers", None) or {}).get("retry-after")
            try:
                retry_after = float(retry_after) if retry_after else None
            except ValueError:
                retry_after = None
            if code == "IGW00201" or status == 429:
                raise RateLimitError(status, code, msg, retry_after)
            if code in _AUTH_CODES or status == 401:
                raise AuthError(status, code, msg)
            if code in _MARKET_CLOSED_CODES:
                raise MarketClosedError(status, code, msg)
            if code in _INSUFFICIENT_CODES or "부족" in rsp_msg:
                raise InsufficientFundsError(status, code, msg)
            if code in _INVALID_ORDER_CODES:
                raise InvalidOrderError(status, code, msg)
            if code in _ORDER_NOT_FOUND_CODES:
                raise OrderNotFoundError(code, msg)
            raise BrokerApiError(status, code, msg)
        return parsed

    # ------------------------------------------------------------------ stream

    def open_stream(self) -> MarketStream:
        return DbMarketStream(self._ws_url, self._get_token, usage=self._usage)

    def _get_token(self) -> str:
        return self._tokens.get()

    def _issue_token(self) -> tuple[str, float]:
        with self._usage.measure("auth"):
            self._limiter.throttle.wait()
            status, body = self._http.request(
                "POST", "/oauth2/token",
                form_body={"grant_type": "client_credentials", "appkey": self._app_key,
                           "appsecretkey": self._app_secret, "scope": "oob"})  # JSON/appsecret 은 IGW00133
            if status != 200 or not body.get("access_token"):
                code = body.get("rsp_cd") or body.get("error")
                detail = body.get("rsp_msg") or body.get("error_description") or ""
                if code == "IGW00201" or status == 429:
                    raise RateLimitError(status, code, f"DB 토큰 발급 유량 초과({code}): {detail} (발급은 1분당 1회 제한)",
                                         FAILURE_COOLDOWN_SECONDS)
                raise AuthError(status, code, f"DB 토큰 발급 실패({code}): {detail}")
            return body["access_token"], time.time() + float(body.get("expires_in", 86400))


def _remaining(row: dict) -> Decimal:
    r = _d(row.get("MrcAbleQty"))
    if r is not None:
        return r
    return (_d(row.get("OrdQty")) or Decimal(0)) - (_d(row.get("AllExecQty")) or Decimal(0)) - (_d(row.get("MrcQty")) or Decimal(0))


def _side_of(row: dict) -> OrderSide:
    return OrderSide.BUY if str(row.get("BnsTpCode", "")).strip() == "2" else OrderSide.SELL


def _same_no(a, b) -> bool:
    return str(a or "").strip().lstrip("0") == str(b or "").strip().lstrip("0")
