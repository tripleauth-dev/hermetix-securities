"""NH투자증권 NH PLUG(나무 PLUG) REST OpenAPI 어댑터.

⚠️ 문서 기반 구현 (실측 전) — 공식 Python SDK(PLUG-OpenAPI/nhplug-sdk, MIT)와 포털 OpenAPI 문서(2026-09-08)에서
엔드포인트·필드명·에러 코드를 역추적했다. 모의서버 실측 전까지 상태는 "미검증".

규약 (SDK 기준):
- 모든 API 는 POST, 본문 {"Input_0": {...}}, 응답 rsp_cd/rsp_msg + Output_0(+Output_1). TR 헤더 없이 경로로 식별
- 인증 헤더 authorization: Bearer + x-client-id / x-client-secret
- 토큰은 운영 호스트 /oauth2/token 에서 쿼리스트링 파라미터로 발급(24h), 모의·운영 양쪽에 사용
- 모의/운영은 호스트로만 구분 (moapi / api). 계좌 acct_type 은 환경과 맞아야 한다 (모의 03, 운영 01)
- HTTP 200 이어도 업무 오류 가능 — rsp_cd ∈ {00000,00166,00221,13578} 또는 rsp_msg 에 "완료" 면 성공 (SDK 판정식)
- 초당 5회 한도 → 250ms 쓰로틀 + 429(IGW4290x, Retry-After) 재시도

미확인(실측 필요): mkt_orr_no(주문 응답)와 itg_orr_no(체결 조회)의 동일 여부, ost_cns_dit 코드 의미, 등락률·수익률 단위(% 추정), 응답 숫자 타입
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from decimal import Decimal

from ..broker import KST, MarketStream, RateLimiter, StreamingBrokerClient, _Http, krx_calendar, krx_tick_round
from ..errors import AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, OrderNotFoundError, RateLimitError
from ..models import (
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote, StreamChannel, TradingEnvironment,
)
from ..tokens import FAILURE_COOLDOWN_SECONDS, TokenManager
from .nh_stream import NhMarketStream

_SUCCESS_CODES = {"00000", "00166", "00221", "13578", "00165", "00218"}
_FALLING_SIGNS = {"4", "5", "8", "9"}


def _d(value) -> Decimal | None:
    """숫자/문자열 어느 쪽으로 와도 파싱 — 문서상 와이어 타입이 API 마다 다르다"""
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
    """계좌·주문 API 의 iem_cd 는 길이 12(선행 0)일 수 있고 A 접두가 붙을 수 있다 → 6자리 코드"""
    text = str(raw or "").strip().removeprefix("A")
    return text[-6:] if len(text) > 6 and text.isdigit() else text


def _parse_date(raw) -> datetime | None:
    text = str(raw or "").strip()
    for fmt, t in (("%Y%m%d", text), ("%Y-%m-%d", text), ("%Y/%m/%d", "20" + text if len(text) == 8 and "/" in text else "")):
        if t:
            try:
                return datetime.strptime(t, fmt).replace(tzinfo=KST)
            except ValueError:
                continue
    return None


class NhClient(StreamingBrokerClient):

    PAPER_URL = "https://moapi.nhplug.com:8443"
    LIVE_URL = "https://api.nhplug.com:8443"
    AUTH_URL = "https://api.nhplug.com:8443"
    # 실시간 웹소켓 - 경로 /websocket 필수. 모의(17070)는 포털 가이드에 시세 채널 "미제공" 표기라 통보만 올 수 있다 (실측 전)
    PAPER_WS_URL = "wss://moapi.nhplug.com:17070/websocket"
    LIVE_WS_URL = "wss://api.nhplug.com:7070/websocket"

    capabilities = BrokerCapabilities(
        broker_id="nh",
        market="KRX",
        currency="KRW",
        candle_intervals=frozenset({CandleInterval.DAY_1}),
        client_order_id=False,
        native_bracket=False,
        fractional_shares=False,
        server_open_orders=True,
        environments=frozenset({TradingEnvironment.PAPER, TradingEnvironment.LIVE}),
        # 문서 기반, 실측 전 - 체결 oc/nc/mc·호가 ob/nb/mb (market_cd 별)·통보 d2/d3
        streams=frozenset({StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS}),
    )

    def __init__(self, app_key: str, app_secret: str, account_no: str = "",
                 base_url: str = "", auth_url: str = AUTH_URL, market_cd: str = "KRX", order_market_cd: str = "KRX",
                 throttle_seconds: float = 0.25, environment: TradingEnvironment = TradingEnvironment.PAPER,
                 ws_url: str = ""):
        """account_no 를 비우면 /n2/acctinfo 에서 환경에 맞는 acct_type(모의 03 / 운영 01)의 첫 계좌를 고른다.
        ws_url 을 비우면 실시간 웹소켓은 모의 moapi:17070 / 운영 api:7070 (/websocket). 세션당 등록 10건(SDK 실측)/30건(공식 문구),
        앱키당 세션 2개. 운영 WS 서버가 중간 CA 를 보내지 않아 TLS 검증이 실패할 수 있다."""
        self.environment = environment
        self._ws_url = ws_url or (self.LIVE_WS_URL if environment == TradingEnvironment.LIVE else self.PAPER_WS_URL)
        self._app_key = app_key
        self._app_secret = app_secret
        self._account_no = account_no
        self._market_cd = market_cd
        self._order_market_cd = order_market_cd
        self._http = _Http(base_url or (self.LIVE_URL if environment == TradingEnvironment.LIVE else self.PAPER_URL))
        self._auth_http = _Http(auth_url)
        self._limiter = RateLimiter(throttle_seconds, max_retries=3, backoff=lambda attempt: 1.0 * attempt)
        self._tokens = TokenManager("nh", app_key, self._issue_token, 300)
        self._account_lock = threading.Lock()

    # ------------------------------------------------------------------ market

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        quotes = []
        for symbol in symbols:
            out = self._call("/krstock/quote/v1/currentPrice",
                             {"market_cd": self._market_cd, "iem_cd": self.capabilities.symbol_code(symbol)}).get("Output_0") or {}
            sign = str(out.get("prdy_vrss_sign", ""))
            change = _d(out.get("prdy_vrss"))
            rate = _d(out.get("prdy_ctrt"))
            if sign in _FALLING_SIGNS:
                change = -change if change is not None and change > 0 else change
                rate = -rate if rate is not None and rate > 0 else rate
            bid, ask = _d(out.get("bidp")), _d(out.get("askp"))
            quotes.append(Quote(
                symbol=symbol, price=_d(out.get("stck_prpr")) or Decimal(0),
                bid_price=bid if bid and bid > 0 else None, ask_price=ask if ask and ask > 0 else None,
                volume=int(_d(out.get("acml_vol")) or 0), change=change,
                change_rate=(rate / Decimal(100)) if rate is not None else None,
                timestamp=datetime.now(timezone.utc),
            ))
        return quotes

    def get_candles(self, symbol: str, interval: CandleInterval, limit: int | None = None) -> list[Candle]:
        if interval != CandleInterval.DAY_1:
            raise ValueError("NH 어댑터는 일봉(DAY_1)만 지원합니다.")
        count = limit or 30
        rows = self._call("/krstock/quote/v1/currentDaily",
                          {"market_cd": self._market_cd, "iem_cd": self.capabilities.symbol_code(symbol),
                           "array_cnt": str(count)}).get("Output_0") or []
        candles = []
        for r in rows:
            ts = _parse_date(r.get("bsop_date"))
            if ts is None:
                continue
            candles.append(Candle(timestamp=ts, open=_d(r.get("stck_oprc")) or Decimal(0), high=_d(r.get("stck_hgpr")) or Decimal(0),
                                  low=_d(r.get("stck_lwpr")) or Decimal(0), close=_d(r.get("stck_clpr")) or Decimal(0),
                                  volume=int(_d(r.get("acml_vol")) or 0)))
        candles.sort(key=lambda c: c.timestamp)  # 문서상 최신일 우선 → 과거→최신
        return candles[-count:]

    def get_calendar(self) -> list[MarketDay]:
        return krx_calendar()

    # ----------------------------------------------------------------- account

    def get_account(self) -> Account:
        summary = self._balance().get("Output_0") or {}
        cash = _d(summary.get("dca")) or Decimal(0)
        portfolio = _d(summary.get("tot_aet_amt"))
        if portfolio is None or portfolio <= 0:
            portfolio = cash + (_d(summary.get("tot_eal_amt")) or Decimal(0))
        return Account(account_id=self._account(), currency="KRW", cash=cash, portfolio_value=portfolio)

    def get_holdings(self) -> list[Holding]:
        holdings = []
        for row in self._balance().get("Output_1") or []:
            qty = _d(row.get("itg_bnc_qty"))
            if qty is None or qty <= 0:
                continue
            holdings.append(Holding(
                symbol=normalize_code(row.get("iem_cd")), quantity=qty,
                avg_entry_price=_d(row.get("phs_pr")) or Decimal(0), current_price=_d(row.get("now_pr")),
                market_value=_d(row.get("eal_amt")), unrealized_pnl=_d(row.get("eal_pls_amt")),
                unrealized_pnl_rate=_pct(row.get("pft_rt")),
            ))
        return holdings

    def get_buying_power(self) -> Decimal:
        summary = self._balance().get("Output_0") or {}
        return _d(summary.get("orr_pbl_amt")) or _d(summary.get("dca")) or Decimal(0)

    # ------------------------------------------------------------------ orders

    def create_order(self, request: CreateOrderRequest) -> Order:
        code = self.capabilities.symbol_code(request.symbol)
        is_limit = request.order_type == OrderType.LIMIT
        path = "/krstock/order/v1/cashBuy" if request.side == OrderSide.BUY else "/krstock/order/v1/cashSell"
        body = {"act_no": self._account(), "iem_cd": code, "orr_qty": int(request.quantity),
                "nmn_pr_tp_cd": "01" if is_limit else "05", "orr_cnd_dit_cd": "00", "ssl_nmn_pr_dit_cd": "00",
                "rmt_mkt_cd": self._order_market_cd, "sor_mkt_sli_yn": "N"}
        if is_limit:
            body["orr_pr"] = int(krx_tick_round(request.limit_price))  # KRX 호가단위 보정
        out = self._call(path, body).get("Output_0") or {}
        order_id = str(out.get("mkt_orr_no", "")).strip()
        if not order_id:
            raise BrokerApiError(200, None, "NH 주문 응답에 mkt_orr_no 가 없습니다")
        return Order(order_id=order_id, status=OrderStatus.SUBMITTED, symbol=code, side=request.side,
                     order_type=request.order_type, quantity=request.quantity, limit_price=request.limit_price,
                     filled_quantity=Decimal(0), client_order_id=request.client_order_id,
                     submitted_at=datetime.now(timezone.utc))

    def get_orders(self) -> list[Order]:
        return [o for o in (self._to_order(r) for r in self._execution_rows()) if o.status.is_open]

    def get_order(self, order_id: str) -> Order:
        for row in self._execution_rows():
            if _same_no(row.get("itg_orr_no"), order_id):
                return self._to_order(row)
        return Order(order_id=order_id, status=OrderStatus.CANCELED)  # 당일 조회에 없으면 종료로 간주

    def cancel_order(self, order_id: str) -> Order:
        row = next((r for r in self._execution_rows() if _same_no(r.get("itg_orr_no"), order_id)), None)
        if row is None:
            raise OrderNotFoundError("order-not-found", f"NH 당일 주문에서 찾을 수 없습니다: {order_id}")
        self._call("/krstock/order/v1/cancel", {
            "act_no": self._account(), "org_mkt_orr_no": int(order_id.lstrip("0") or "0"),
            "all_pat_dit_cd": "1", "iem_cd": normalize_code(row.get("iem_cd")),
        })
        return Order(order_id=order_id, status=OrderStatus.CANCELED, canceled_at=datetime.now(timezone.utc))

    def get_fills(self) -> list[Fill]:
        fills = []
        for row in self._execution_rows():
            qty = _d(row.get("tot_cns_qty"))
            if qty is None or qty <= 0:
                continue
            fills.append(Fill(fill_id=None, order_id=str(row.get("itg_orr_no", "")).strip(),
                              symbol=normalize_code(row.get("iem_cd")), side=_side_of(row),
                              quantity=qty, price=_d(row.get("cns_avg_uit_pr"))))
        return fills

    # ---------------------------------------------------------------- internal

    def _balance(self) -> dict:
        return self._call("/krstock/inquiry/v1/balance", {
            "act_no": self._account(), "bnc_bse_cd": "5", "ltg_aot_dit_cd": "9", "aet_bse": "2", "qut_dit_cd": self._market_cd})

    def _execution_rows(self) -> list[dict]:
        """당일 주문·체결 전체(ost_cns_dit=0) — 문서마다 체결구분 코드 의미가 달라 전체를 받아 ny_cns_qty 로 가른다"""
        return self._call("/krstock/inquiry/v1/dailyOrderExecution", {
            "orr_dt": datetime.now(KST).strftime("%Y%m%d"), "act_no": self._account(),
            "ost_cns_dit": "0", "orr_mkt_cd": "00"}).get("Output_1") or []

    @staticmethod
    def _to_order(row: dict) -> Order:
        ord_qty = _d(row.get("orr_qty")) or Decimal(0)
        filled = _d(row.get("tot_cns_qty")) or Decimal(0)
        remaining = _d(row.get("ny_cns_qty"))
        if remaining is None:
            remaining = ord_qty - filled
        canceled = _d(row.get("can_qty")) or Decimal(0)
        reject = str(row.get("orr_rjt_rsn_cd_nm") or "").strip()
        if remaining > 0 and filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        elif remaining > 0:
            status = OrderStatus.SUBMITTED
        elif filled > 0 and filled >= ord_qty:
            status = OrderStatus.FILLED
        elif filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        elif canceled > 0:
            status = OrderStatus.CANCELED
        elif reject:
            status = OrderStatus.REJECTED
        else:
            status = OrderStatus.CANCELED
        price = _d(row.get("orr_pr"))
        avg = _d(row.get("cns_avg_uit_pr"))
        return Order(order_id=str(row.get("itg_orr_no", "")).strip(), status=status,
                     symbol=normalize_code(row.get("iem_cd")), side=_side_of(row),
                     order_type=OrderType.MARKET if "시장가" in str(row.get("nmn_pr_tp_cd_nm", "")) else OrderType.LIMIT,
                     quantity=ord_qty, limit_price=price if price and price > 0 else None,
                     filled_quantity=filled, avg_fill_price=avg if avg and avg > 0 else None)

    def _account(self) -> str:
        if self._account_no:
            return self._account_no
        with self._account_lock:
            if self._account_no:
                return self._account_no
            expected = "01" if self.environment == TradingEnvironment.LIVE else "03"
            accounts = self._call("/n2/acctinfo", {}).get("Output_0") or []
            picked = next((a for a in accounts if str(a.get("acct_type")) == expected), None)
            if picked is None:
                raise BrokerApiError(200, None, f"NH 계좌 목록에 {self.environment.value} 용 계좌(acct_type={expected})가 없습니다: {accounts}")
            self._account_no = str(picked["acct_no"])
            return self._account_no

    def _call(self, path: str, body: dict) -> dict:
        return self._limiter.execute(lambda: self._tokens.call(lambda token: self._call_once(path, body, token)), f"NH {path}")

    def _call_once(self, path: str, body: dict, token: str) -> dict:
        headers = {"authorization": f"Bearer {token}", "x-client-id": self._app_key, "x-client-secret": self._app_secret}
        status, parsed = self._http.request("POST", path, headers=headers, json_body={"Input_0": body})
        rsp_cd = str(parsed.get("rsp_cd", "")).strip()
        rsp_msg = str(parsed.get("rsp_msg", ""))
        gw = str(parsed.get("code") or (parsed.get("error") or {}).get("code") or rsp_cd) if isinstance(parsed.get("error"), dict) or parsed.get("code") else rsp_cd
        msg = f"NH({path}) [{gw}] {rsp_msg or parsed.get('message', '')}".strip()
        if status < 200 or status >= 300:
            retry_after = (getattr(self._http, "last_headers", None) or {}).get("retry-after")
            try:
                retry_after = float(retry_after) if retry_after else None
            except ValueError:
                retry_after = None
            if status == 429 or gw.startswith("IGW429"):
                raise RateLimitError(status, gw, msg, retry_after)
            if status == 401 or gw.startswith("IGW4004") or gw.startswith("IGW4003") or gw == "IGW40051":
                raise AuthError(status, gw, msg)
            if status == 400:
                raise InvalidOrderError(status, gw, msg)
            raise BrokerApiError(status, gw, msg)
        if rsp_cd not in _SUCCESS_CODES and "완료" not in rsp_msg:
            if "부족" in rsp_msg:
                raise InsufficientFundsError(status, rsp_cd, msg)
            raise BrokerApiError(status, rsp_cd, msg)
        return parsed

    # ------------------------------------------------------------------ stream

    def open_stream(self) -> MarketStream:
        return NhMarketStream(self._ws_url, self._get_token, market_cd=self._market_cd, account_no=self._account_no, usage=self._usage)

    def _get_token(self) -> str:
        return self._tokens.get()

    def _issue_token(self) -> tuple[str, float]:
        with self._usage.measure("auth"):
            self._limiter.throttle.wait()
            # SDK 규약: 파라미터는 쿼리스트링, 본문 없음, content-type 은 form-urlencoded
            status, body = self._auth_http.request(
                "POST", "/oauth2/token", headers={"Content-Type": "application/x-www-form-urlencoded"},
                query={"appkey": self._app_key, "appsecretkey": self._app_secret, "grant_type": "client_credentials", "scope": "oob"})
            if status != 200 or not body.get("access_token"):
                code = str(body.get("code") or body.get("rsp_cd") or "")
                msg = f"NH 토큰 발급 실패({code}): {body.get('message') or body.get('rsp_msg') or ''}"
                if status == 429 or code.startswith("IGW429"):
                    raise RateLimitError(status, code, msg, FAILURE_COOLDOWN_SECONDS)
                raise AuthError(status, code or None, msg)
            return body["access_token"], time.time() + float(body.get("expires_in", 86400))


def _side_of(row: dict) -> OrderSide:
    return OrderSide.BUY if "매수" in str(row.get("sby_dit_cd_nm", "")) else OrderSide.SELL


def _same_no(a, b) -> bool:
    return str(a or "").strip().lstrip("0") == str(b or "").strip().lstrip("0")
