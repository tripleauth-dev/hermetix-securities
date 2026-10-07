"""LS증권(구 이베스트투자증권) OPEN API 어댑터.

⚠️ 문서 기반 구현 (실측 전) — 공식 포털 TR 문서와 커뮤니티 카탈로그(krsec, LsApiHelper, k-ebest-im, 공식 샘플)에서
엔드포인트·TR 코드·필드명을 역추적했다. 모의서버 실측 전까지 상태는 "미검증".

규약: 모든 API 는 POST, 경로는 기능군(/stock/market-data, /stock/chart, /stock/accno, /stock/order)이고 TR 은 tr_cd 헤더로 고른다.
본문 {"<TR>InBlock": {...}} 또는 {"<TR>InBlock1": {...}}, 응답 rsp_cd("00000" 성공)/rsp_msg + OutBlock 들. 계좌번호는 토큰에 바인딩.
실전/모의 같은 호스트(모의 appkey 로 라우팅). 모의 주문은 IsuNo 에 A 접두 필수 → 항상 A+코드. HTTP 200 + rsp_cd != 00000 이 업무 오류.
TR 별 TPS(시세 3, 차트 1, 계좌 2, 예수금 1, 주문 10) → 전역 0.5s + 차트 전용 1.1s 쓰로틀.
미확인: 잔고 expcode 의 A 접두, sign 코드 의미(4·5 하락 가정), 응답 숫자 타입, 장 마감 코드(메시지 판단), medosu 표기
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
from .ls_stream import LsMarketStream
from .nh import _d, _pct

_AUTH_CODES = {"IGW00121", "IGW00123"}
_FALLING_SIGNS = {"4", "5"}


def normalize_code(raw) -> str:
    """계좌 TR 의 종목코드는 A005930 형태(추정) → 6자리 코드"""
    text = str(raw or "").strip()
    return text[1:] if len(text) == 7 and text[0] == "A" else text


class LsClient(StreamingBrokerClient):

    # 실시간 웹소켓 - 실전 9443 / 모의 29443 (/websocket). 토큰은 익일 07:00 만료, 세션·등록 한도 미문서 (실측 전)
    PAPER_WS_URL = "wss://openapi.ls-sec.co.kr:29443/websocket"
    LIVE_WS_URL = "wss://openapi.ls-sec.co.kr:9443/websocket"

    capabilities = BrokerCapabilities(
        broker_id="ls", market="KRX", currency="KRW",
        candle_intervals=frozenset({CandleInterval.DAY_1}),
        client_order_id=False, native_bracket=False, fractional_shares=False, server_open_orders=True,
        environments=frozenset({TradingEnvironment.PAPER, TradingEnvironment.LIVE}),
        # 문서 기반, 실측 전 - S3_/K3_ 체결·H1_/HA_ 호가·SC0~SC4 주문 통보
        streams=frozenset({StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS}),
    )

    def __init__(self, app_key: str, app_secret: str, base_url: str = "https://openapi.ls-sec.co.kr:8080",
                 mac_address: str = "", exch_gubun: str = "", throttle_seconds: float = 0.5,
                 chart_throttle_seconds: float = 1.1, environment: TradingEnvironment = TradingEnvironment.PAPER,
                 ws_url: str = ""):
        self.environment = environment
        self._ws_url = ws_url or (self.LIVE_WS_URL if environment == TradingEnvironment.LIVE else self.PAPER_WS_URL)
        self._app_key = app_key
        self._app_secret = app_secret
        self._mac_address = mac_address
        self._exch_gubun = exch_gubun
        self._http = _Http(base_url)
        self._limiter = RateLimiter(throttle_seconds, max_retries=3, backoff=lambda attempt: 1.0 * attempt)
        self._chart_limiter = RateLimiter(chart_throttle_seconds, max_retries=0)  # 차트 TR 초당 1건
        self._tokens = TokenManager("ls", app_key, self._issue_token, 600)

    # ------------------------------------------------------------------ market

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        quotes = []
        for symbol in symbols:
            out = self._call("/stock/market-data", "t1102", "t1102InBlock",
                             {"shcode": self.capabilities.symbol_code(symbol), "exchgubun": self._exch_gubun}).get("t1102OutBlock") or {}
            falling = str(out.get("sign", "")) in _FALLING_SIGNS
            change, rate = _d(out.get("change")), _d(out.get("diff"))
            if falling:
                change = -change if change is not None and change > 0 else change
                rate = -rate if rate is not None and rate > 0 else rate
            quotes.append(Quote(symbol=symbol, price=_d(out.get("price")) or Decimal(0), bid_price=None, ask_price=None,
                                volume=int(_d(out.get("volume")) or 0), change=change,
                                change_rate=(rate / Decimal(100)) if rate is not None else None, timestamp=datetime.now(timezone.utc)))
        return quotes

    def get_candles(self, symbol: str, interval: CandleInterval, limit: int | None = None) -> list[Candle]:
        if interval != CandleInterval.DAY_1:
            raise ValueError("LS 어댑터는 일봉(DAY_1)만 지원합니다.")
        count = limit or 30
        today = datetime.now(KST)
        start = today - timedelta(days=count * 16 // 10 + 10)
        body = self._chart_limiter.execute(lambda: self._call("/stock/chart", "t8410", "t8410InBlock", {
            "shcode": self.capabilities.symbol_code(symbol), "gubun": "2", "qrycnt": count,
            "sdate": start.strftime("%Y%m%d"), "edate": today.strftime("%Y%m%d"), "cts_date": "", "comp_yn": "N", "sujung": "Y"}), "LS t8410")
        candles = []
        for r in body.get("t8410OutBlock1") or []:
            date = str(r.get("date", "")).strip()
            if len(date) != 8:
                continue
            candles.append(Candle(timestamp=datetime.strptime(date, "%Y%m%d").replace(tzinfo=KST),
                                  open=_d(r.get("open")) or Decimal(0), high=_d(r.get("high")) or Decimal(0),
                                  low=_d(r.get("low")) or Decimal(0), close=_d(r.get("close")) or Decimal(0),
                                  volume=int(_d(r.get("jdiff_vol")) or 0)))
        candles.sort(key=lambda c: c.timestamp)
        return candles[-count:]

    def get_calendar(self) -> list[MarketDay]:
        return krx_calendar()

    # ----------------------------------------------------------------- account

    def get_account(self) -> Account:
        s = self._balance().get("t0424OutBlock") or {}
        cash = _d(s.get("sunamt1")) or Decimal(0)  # 추정 D2 예수금
        portfolio = _d(s.get("sunamt"))
        if portfolio is None or portfolio <= 0:
            portfolio = cash + (_d(s.get("tappamt")) or Decimal(0))
        return Account(account_id=f"ls-{self.environment.value.lower()}", currency="KRW", cash=cash, portfolio_value=portfolio)

    def get_holdings(self) -> list[Holding]:
        holdings = []
        for row in self._balance().get("t0424OutBlock1") or []:
            qty = _d(row.get("janqty"))
            if qty is None or qty <= 0:
                continue
            holdings.append(Holding(symbol=normalize_code(row.get("expcode")), quantity=qty,
                                    avg_entry_price=_d(row.get("pamt")) or Decimal(0), current_price=_d(row.get("price")),
                                    market_value=_d(row.get("appamt")), unrealized_pnl=_d(row.get("dtsunik")),
                                    unrealized_pnl_rate=_pct(row.get("sunikrt"))))
        return holdings

    def get_buying_power(self) -> Decimal:
        out = self._call("/stock/accno", "CSPAQ12200", "CSPAQ12200InBlock1", {"BalCreTp": "0"}).get("CSPAQ12200OutBlock2") or {}
        return _d(out.get("MnyOrdAbleAmt")) or _d(out.get("Dps")) or Decimal(0)

    # ------------------------------------------------------------------ orders

    def create_order(self, request: CreateOrderRequest) -> Order:
        code = self.capabilities.symbol_code(request.symbol)
        is_limit = request.order_type == OrderType.LIMIT
        out = self._call("/stock/order", "CSPAT00601", "CSPAT00601InBlock1", {
            "IsuNo": f"A{code}", "OrdQty": int(request.quantity),
            "OrdPrc": int(krx_tick_round(request.limit_price)) if is_limit else 0,
            "BnsTpCode": "2" if request.side == OrderSide.BUY else "1", "OrdprcPtnCode": "00" if is_limit else "03",
            "MgntrnCode": "000", "LoanDt": "", "OrdCndiTpCode": "0"}).get("CSPAT00601OutBlock2") or {}
        order_id = str(out.get("OrdNo", "")).strip()
        if not order_id or order_id == "0":
            raise BrokerApiError(200, None, "LS 주문 응답에 OrdNo 가 없습니다")
        return Order(order_id=order_id, status=OrderStatus.SUBMITTED, symbol=code, side=request.side, order_type=request.order_type,
                     quantity=request.quantity, limit_price=request.limit_price, filled_quantity=Decimal(0),
                     client_order_id=request.client_order_id, submitted_at=datetime.now(timezone.utc))

    def get_orders(self) -> list[Order]:
        return [o for o in (self._to_order(r) for r in self._order_rows()) if o.status.is_open]

    def get_order(self, order_id: str) -> Order:
        for row in self._order_rows():
            if _same_no(row.get("ordno"), order_id):
                return self._to_order(row)
        return Order(order_id=order_id, status=OrderStatus.CANCELED)

    def cancel_order(self, order_id: str) -> Order:
        row = next((r for r in self._order_rows() if _same_no(r.get("ordno"), order_id)), None)
        if row is None:
            raise OrderNotFoundError("order-not-found", f"LS 당일 주문에서 찾을 수 없습니다: {order_id}")
        remaining = _d(row.get("ordrem"))
        if remaining is None:
            remaining = (_d(row.get("qty")) or Decimal(0)) - (_d(row.get("cheqty")) or Decimal(0))
        self._call("/stock/order", "CSPAT00801", "CSPAT00801InBlock1", {
            "OrgOrdNo": int(order_id.lstrip("0") or "0"), "IsuNo": f"A{normalize_code(row.get('expcode'))}", "OrdQty": int(remaining)})
        return Order(order_id=order_id, status=OrderStatus.PENDING_CANCEL, canceled_at=datetime.now(timezone.utc))

    def get_fills(self) -> list[Fill]:
        fills = []
        for row in self._order_rows():
            qty = _d(row.get("cheqty"))
            if qty is None or qty <= 0:
                continue
            fills.append(Fill(fill_id=None, order_id=str(row.get("ordno", "")).strip(), symbol=normalize_code(row.get("expcode")),
                              side=_side_of(row), quantity=qty, price=_d(row.get("cheprice"))))
        return fills

    # ---------------------------------------------------------------- internal

    def _balance(self) -> dict:
        return self._call("/stock/accno", "t0424", "t0424InBlock", {"prcgb": "1", "chegb": "2", "dangb": "0", "charge": "1", "cts_expcode": ""})

    def _order_rows(self) -> list[dict]:
        return self._call("/stock/accno", "t0425", "t0425InBlock",
                          {"expcode": "", "chegb": "0", "medosu": "0", "sortgb": "1", "cts_ordno": ""}).get("t0425OutBlock1") or []

    @staticmethod
    def _to_order(row: dict) -> Order:
        qty = _d(row.get("qty")) or Decimal(0)
        filled = _d(row.get("cheqty")) or Decimal(0)
        remaining = _d(row.get("ordrem"))
        if remaining is None:
            remaining = qty - filled
        text = str(row.get("status", ""))
        if "취소" in text and remaining > 0:
            status = OrderStatus.PENDING_CANCEL
        elif remaining > 0 and filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        elif remaining > 0:
            status = OrderStatus.SUBMITTED
        elif filled > 0 and filled >= qty:
            status = OrderStatus.FILLED
        elif filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        elif "거부" in text:
            status = OrderStatus.REJECTED
        else:
            status = OrderStatus.CANCELED
        price, avg = _d(row.get("price")), _d(row.get("cheprice"))
        return Order(order_id=str(row.get("ordno", "")).strip(), status=status, symbol=normalize_code(row.get("expcode")), side=_side_of(row),
                     order_type=OrderType.MARKET if str(row.get("hogagb", "")) == "03" else OrderType.LIMIT,
                     quantity=qty, limit_price=price if price and price > 0 else None,
                     filled_quantity=filled, avg_fill_price=avg if avg and avg > 0 else None)

    def _call(self, path: str, tr_cd: str, in_block: str, body: dict) -> dict:
        return self._limiter.execute(lambda: self._tokens.call(lambda token: self._call_once(path, tr_cd, in_block, body, token)),
                                     f"LS {tr_cd}")

    def _call_once(self, path: str, tr_cd: str, in_block: str, body: dict, token: str) -> dict:
        headers = {"authorization": f"Bearer {token}", "tr_cd": tr_cd, "tr_cont": "N", "tr_cont_key": ""}
        if self._mac_address:
            headers["mac_address"] = self._mac_address
        status, parsed = self._http.request("POST", path, headers=headers, json_body={in_block: body})
        code = str(parsed.get("rsp_cd") or parsed.get("error_code") or "").strip()
        rsp_msg = str(parsed.get("rsp_msg") or parsed.get("error_description") or "")
        msg = f"LS({tr_cd}) [{code}] {rsp_msg}".strip()
        if status < 200 or status >= 300 or ("rsp_cd" in parsed and code != "00000"):
            retry_after = (getattr(self._http, "last_headers", None) or {}).get("retry-after")
            try:
                retry_after = float(retry_after) if retry_after else None
            except ValueError:
                retry_after = None
            if code == "IGW00201" or status == 429:
                raise RateLimitError(status, code, msg, retry_after)
            if code in _AUTH_CODES or status == 401:
                raise AuthError(status, code, msg)
            if any(k in rsp_msg for k in ("장종료", "장운영", "장 마감", "장마감")):
                raise MarketClosedError(status, code, msg)
            if "부족" in rsp_msg:
                raise InsufficientFundsError(status, code, msg)
            if "호가" in rsp_msg or "단위" in rsp_msg:
                raise InvalidOrderError(status, code, msg)
            if "주문번호" in rsp_msg or "원주문" in rsp_msg:
                raise OrderNotFoundError(code, msg)
            raise BrokerApiError(status, code, msg)
        return parsed

    # ------------------------------------------------------------------ stream

    def open_stream(self) -> MarketStream:
        return LsMarketStream(self._ws_url, self._get_token, usage=self._usage)

    def _get_token(self) -> str:
        return self._tokens.get()

    def _issue_token(self) -> tuple[str, float]:
        with self._usage.measure("auth"):
            self._limiter.throttle.wait()
            status, body = self._http.request("POST", "/oauth2/token", form_body={
                "grant_type": "client_credentials", "appkey": self._app_key, "appsecretkey": self._app_secret, "scope": "oob"})
            if status != 200 or not body.get("access_token"):
                code = body.get("error_code") or body.get("rsp_cd")
                detail = body.get("error_description") or body.get("rsp_msg") or ""
                if code == "IGW00201" or status == 429:
                    raise RateLimitError(status, code, f"LS 토큰 발급 유량 초과({code}): {detail}", FAILURE_COOLDOWN_SECONDS)
                raise AuthError(status, code, f"LS 토큰 발급 실패({code}): {detail}")
            return body["access_token"], time.time() + float(body.get("expires_in", 86400))


def _side_of(row: dict) -> OrderSide:
    text = str(row.get("medosu", "")).strip()
    return OrderSide.BUY if "매수" in text or text == "2" else OrderSide.SELL


def _same_no(a, b) -> bool:
    return str(a or "").strip().lstrip("0") == str(b or "").strip().lstrip("0")
