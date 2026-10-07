"""KB증권 Open API(개인 오픈베타) 어댑터.

⚠️ 실전 전용 · 실측 전 — 모의투자가 없는 운영 단일 환경. 포털 공개 명세(TR 별 입출력 필드·샘플)와 공식 GitHub 예제로 구현했고
실계좌 소액 검증 전까지 "미검증". 오픈베타라 스펙이 바뀔 수 있다.

규약: 모든 API 는 POST /api/v1/{tr}, 본문·응답 모두 {"dataHeader", "dataBody"} 봉투. 헤더 Authorization: bearer + appKey.
성공은 dataHeader.processFlag == "A"(HTTP 200 이어도 "B" 면 업무 오류). 숫자는 zero-padded, 문자열은 공백 패딩 → trim.
계좌번호 필드 없음(appKey 바인딩 추정). 게이트웨이 5초당 200건(추정) → 0.1s 쓰로틀.
미확인: bdy_cmpr_ccd 부호 코드(4·5 하락 가정), 체결 조회 레코드 이름(Record1 가정), 표준코드(KR7005930003)→6자리 환산, 차트 시장구분(KOSPI 기본)
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal

from ..broker import KST, BrokerClient, RateLimiter, _Http, krx_calendar, krx_tick_round
from ..errors import (
    AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, MarketClosedError, OrderNotFoundError, RateLimitError,
)
from ..models import (
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote, TradingEnvironment,
)
from ..tokens import FAILURE_COOLDOWN_SECONDS, TokenManager
from .nh import _d, _pct

_FALLING_SIGNS = {"4", "5"}


def normalize_code(raw) -> str:
    """잔고 A005930 → 005930, 체결 조회 표준코드 KR7005930003(ISIN) → 4~9번째 자리"""
    text = str(raw or "").strip()
    if len(text) == 7 and text[0] == "A":
        return text[1:]
    if len(text) == 12 and text.startswith("KR"):
        return text[3:9]
    return text


class KbClient(BrokerClient):

    capabilities = BrokerCapabilities(
        broker_id="kb", market="KRX", currency="KRW",
        candle_intervals=frozenset({CandleInterval.DAY_1}),
        client_order_id=False, native_bracket=False, fractional_shares=False, server_open_orders=True,
        environments=frozenset({TradingEnvironment.LIVE}),  # 모의투자 "추후 제공 예정"
    )

    def __init__(self, app_key: str, app_secret: str, base_url: str = "https://developer.kbsec.com:32484",
                 excg_clsf: str = "1", sor_order_ccd: str = "K", chart_market_clsf: str = "0",
                 throttle_seconds: float = 0.1, environment: TradingEnvironment = TradingEnvironment.LIVE):
        """chart_market_clsf: 통합차트 시장구분 0 KOSPI / 1 KOSDAQ — 차트 TR 이 종목의 시장을 요구한다."""
        self.environment = environment
        self._app_key = app_key
        self._app_secret = app_secret
        self._excg_clsf = excg_clsf
        self._sor_order_ccd = sor_order_ccd
        self._chart_market_clsf = chart_market_clsf
        self._http = _Http(base_url)
        self._limiter = RateLimiter(throttle_seconds, max_retries=3, backoff=lambda attempt: 1.0 * attempt)
        self._tokens = TokenManager("kb", app_key, self._issue_token, 300)

    # ------------------------------------------------------------------ market

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        quotes = []
        for symbol in symbols:
            out = self._call("/api/v1/ivu10140", {"excg_clsf": self._excg_clsf, "shrt_cd": self.capabilities.symbol_code(symbol)})
            falling = _t(out.get("bdy_cmpr_ccd")) in _FALLING_SIGNS
            change, rate = _d(out.get("bdy_cmpr")), _d(out.get("up_dwn_r_p2"))
            if falling:
                change = -change if change is not None and change > 0 else change
                rate = -rate if rate is not None and rate > 0 else rate
            bid, ask = _d(out.get("b_sq1_askprc")), _d(out.get("s_sq1_askprc"))
            quotes.append(Quote(symbol=symbol, price=_d(out.get("now_prc")) or Decimal(0),
                                bid_price=bid if bid and bid > 0 else None, ask_price=ask if ask and ask > 0 else None,
                                volume=int(_d(out.get("acml_vlm")) or 0), change=change,
                                change_rate=(rate / Decimal(100)) if rate is not None else None, timestamp=datetime.now(timezone.utc)))
        return quotes

    def get_candles(self, symbol: str, interval: CandleInterval, limit: int | None = None) -> list[Candle]:
        if interval != CandleInterval.DAY_1:
            raise ValueError("KB 어댑터는 일봉(DAY_1)만 지원합니다.")
        count = limit or 30
        out = self._call("/api/v1/ivs11560", {
            "chrt_clsf": "D", "inq_clsf": "2", "strt_dy": datetime.now(KST).strftime("%Y%m%d"),
            "is_cd": self.capabilities.symbol_code(symbol), "minute_tck_indx": "일", "info_ccd": "1",
            "mkt_clsf": self._chart_market_clsf, "inq_cnt": str(count)})
        candles = []
        for r in out.get("out2") or []:
            date = _t(r.get("dt"))
            if len(date) != 8:
                continue
            candles.append(Candle(timestamp=datetime.strptime(date, "%Y%m%d").replace(tzinfo=KST),
                                  open=_d(r.get("opn_prc_p2")) or Decimal(0), high=_d(r.get("hgh_prc_p2")) or Decimal(0),
                                  low=_d(r.get("lw_prc_p2")) or Decimal(0), close=_d(r.get("cls_prc_p2")) or Decimal(0),
                                  volume=int(_d(r.get("vlm")) or 0)))
        candles.sort(key=lambda c: c.timestamp)
        return candles[-count:]

    def get_calendar(self) -> list[MarketDay]:
        return krx_calendar()

    # ----------------------------------------------------------------- account

    def get_account(self) -> Account:
        out = self._balance()
        cash = _d(out.get("dy_tfnd")) or Decimal(0)
        portfolio = _d(out.get("nt_asts_val_amt"))
        if portfolio is None or portfolio <= 0:
            portfolio = cash + (_d(out.get("val_amt_sum")) or Decimal(0))
        return Account(account_id="kb-live", currency="KRW", cash=cash, portfolio_value=portfolio)

    def get_holdings(self) -> list[Holding]:
        holdings = []
        for row in self._balance().get("Record1") or []:
            qty = max((q for q in (_d(row.get("ec_q")), _d(row.get("hld_q"))) if q is not None), default=None)
            if qty is None or qty <= 0:
                continue
            holdings.append(Holding(symbol=normalize_code(row.get("is_cd")), quantity=qty,
                                    avg_entry_price=_d(row.get("byng_avr_prc")) or Decimal(0), current_price=_d(row.get("now_prc")),
                                    market_value=_d(row.get("val_amt")), unrealized_pnl=_d(row.get("val_pl")),
                                    unrealized_pnl_rate=_pct(row.get("val_yld"))))
        return holdings

    def get_buying_power(self) -> Decimal:
        out = self._call("/api/v1/ssqm1802", {"bnd_mktio_ccd": "1", "is_no": ""})
        return _d(out.get("ordr_psbl_csh")) or _d(out.get("ordr_psbl_tl_amt")) or Decimal(0)

    # ------------------------------------------------------------------ orders

    def create_order(self, request: CreateOrderRequest) -> Order:
        code = self.capabilities.symbol_code(request.symbol)
        is_limit = request.order_type == OrderType.LIMIT
        path = "/api/v1/ssam1802" if request.side == OrderSide.BUY else "/api/v1/ssam1801"
        out = self._call(path, self._order_body("2" if request.side == OrderSide.BUY else "1", code, str(int(request.quantity)),
                                                str(int(krx_tick_round(request.limit_price))) if is_limit else "0", "00" if is_limit else "03"))
        order_id = _t(out.get("ordr_no"))
        if not order_id.lstrip("0"):
            raise BrokerApiError(200, None, f"KB 주문 응답에 ordr_no 가 없습니다: {_t(out.get('o_msg'))}")
        return Order(order_id=order_id, status=OrderStatus.SUBMITTED, symbol=code, side=request.side, order_type=request.order_type,
                     quantity=request.quantity, limit_price=request.limit_price, filled_quantity=Decimal(0),
                     client_order_id=request.client_order_id, submitted_at=datetime.now(timezone.utc))

    def get_orders(self) -> list[Order]:
        return [o for o in (self._to_order(r) for r in self._order_rows()) if o.status.is_open]

    def get_order(self, order_id: str) -> Order:
        for row in self._order_rows():
            if _same_no(row.get("ordr_no"), order_id):
                return self._to_order(row)
        return Order(order_id=order_id, status=OrderStatus.CANCELED)

    def cancel_order(self, order_id: str) -> Order:
        row = next((r for r in self._order_rows() if _same_no(r.get("ordr_no"), order_id)), None)
        if row is None:
            raise OrderNotFoundError("order-not-found", f"KB 당일 주문에서 찾을 수 없습니다: {order_id}")
        remaining = _d(row.get("nccls_q"))
        if remaining is None:
            remaining = (_d(row.get("ordr_q")) or Decimal(0)) - (_d(row.get("tl_ccls_q")) or Decimal(0))
        body = self._order_body("4", normalize_code(row.get("stnd_is_no") or row.get("stnd_is_cd")), str(int(remaining)), "0", "00")
        body.update({"crct_clsf": "2", "orgn_ordr_no": order_id.rjust(10, "0")})
        self._call("/api/v1/ssam1806", body)
        return Order(order_id=order_id, status=OrderStatus.PENDING_CANCEL, canceled_at=datetime.now(timezone.utc))

    def get_fills(self) -> list[Fill]:
        fills = []
        for row in self._order_rows():
            qty = _d(row.get("tl_ccls_q"))
            if qty is None or qty <= 0:
                continue
            price = _d(row.get("ccls_uprc"))
            fills.append(Fill(fill_id=None, order_id=_t(row.get("ordr_no")), symbol=normalize_code(row.get("stnd_is_no") or row.get("stnd_is_cd")),
                              side=_side_of(row), quantity=qty, price=price if price and price > 0 else None))
        return fills

    # ---------------------------------------------------------------- internal

    def _order_body(self, jb_clsf: str, code: str, qty: str, price: str, ordr_ccd: str) -> dict:
        return {"mkt_tm_clsf": "1", "ordr_jb_clsf": jb_clsf, "s_clsf": "", "is_cd": code, "ordr_q": qty, "ordr_uprc": price,
                "ordr_ccd": ordr_ccd, "crdt_typ_cd": "00", "ln_dt": "", "crct_clsf": "", "orgn_ordr_no": "", "gtc_ccd": "",
                "ordr_mng_no": "", "spclz_ordr_ccd": "", "acct_cd": "", "sor_ordr_ccd": self._sor_order_ccd, "stpd_prc": ""}

    def _balance(self) -> dict:
        return self._call("/api/v1/ssqm2952", {"excg_mktpr_ccd": ""})

    def _order_rows(self) -> list[dict]:
        return self._call("/api/v1/ssqm2341", {
            "inq_clsf": "9", "ccls_clsf": "0", "ordr_dt": datetime.now(KST).strftime("%Y%m%d"), "is_cd": "", "ordr_no": "",
            "mthr_ordr_no": "", "orgn_ordr_no": "", "s_ccls_amt": "", "b_ccls_amt": "", "s_ccls_q": "", "b_ccls_q": "",
            "ac_nm": "", "is_nm": "", "cn_clsf": "", "nxt_key": ""}).get("Record1") or []

    @staticmethod
    def _to_order(row: dict) -> Order:
        qty = _d(row.get("ordr_q")) or Decimal(0)
        filled = _d(row.get("tl_ccls_q")) or Decimal(0)
        remaining = _d(row.get("nccls_q"))
        if remaining is None:
            remaining = qty - filled
        cancel_text, reject = _t(row.get("crct_cncl_ccd")), _t(row.get("rfsl_rsn_nm"))
        if "취소" in cancel_text and remaining > 0:
            status = OrderStatus.PENDING_CANCEL
        elif remaining > 0 and filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        elif remaining > 0:
            status = OrderStatus.SUBMITTED
        elif filled > 0 and filled >= qty:
            status = OrderStatus.FILLED
        elif filled > 0:
            status = OrderStatus.PARTIALLY_FILLED
        elif reject:
            status = OrderStatus.REJECTED
        else:
            status = OrderStatus.CANCELED
        price, avg = _d(row.get("ordr_uprc")), _d(row.get("ccls_uprc"))
        return Order(order_id=_t(row.get("ordr_no")), status=status, symbol=normalize_code(row.get("stnd_is_no") or row.get("stnd_is_cd")),
                     side=_side_of(row), order_type=OrderType.MARKET if _t(row.get("ordr_ccd")).lstrip("0") == "3" else OrderType.LIMIT,
                     quantity=qty, limit_price=price if price and price > 0 else None,
                     filled_quantity=filled, avg_fill_price=avg if avg and avg > 0 else None)

    def _call(self, path: str, body: dict) -> dict:
        return self._limiter.execute(lambda: self._tokens.call(lambda token: self._call_once(path, body, token)), f"KB {path}")

    def _call_once(self, path: str, body: dict, token: str) -> dict:
        headers = {"Authorization": f"bearer {token}", "appKey": self._app_key}
        status, parsed = self._http.request("POST", path, headers=headers,
                                            json_body={"dataHeader": {"ipAddr": "", "macAddr": ""}, "dataBody": body})
        header = parsed.get("dataHeader") or {}
        data = parsed.get("dataBody") or {}
        flag = _t(header.get("processFlag"))
        code = _t(header.get("processCode")) or _t(header.get("resultCode"))
        message = next((m for m in (_t(header.get("processMessage")), _t(header.get("resultMessage")), _t(data.get("o_msg"))) if m), "")
        msg = f"KB({path}) [{code}] {message}".strip()
        if status < 200 or status >= 300 or (flag and flag != "A"):
            retry_after = (getattr(self._http, "last_headers", None) or {}).get("retry-after")
            try:
                retry_after = float(retry_after) if retry_after else None
            except ValueError:
                retry_after = None
            if status == 429 or "한도" in message or "초과" in message:
                raise RateLimitError(status, code, msg, retry_after)
            if status in (401, 403) or "토큰" in message or "인증" in message:
                raise AuthError(status, code, msg)
            if any(k in message for k in ("장종료", "장운영", "장마감", "휴장")):
                raise MarketClosedError(status, code, msg)
            if "부족" in message:
                raise InsufficientFundsError(status, code, msg)
            if any(k in message for k in ("호가", "수량", "단위")):
                raise InvalidOrderError(status, code, msg)
            if "주문번호" in message or "원주문" in message:
                raise OrderNotFoundError(code, msg)
            raise BrokerApiError(status, code, msg)
        return data

    def _get_token(self) -> str:
        return self._tokens.get()

    def _issue_token(self) -> tuple[str, float]:
        with self._usage.measure("auth"):
            self._limiter.throttle.wait()
            status, body = self._http.request("POST", "/oauth2/token", json_body={
                "dataHeader": {"ipAddr": "", "macAddr": ""},
                "dataBody": {"appKey": self._app_key, "appSecret": self._app_secret, "grantType": "client_credentials"}})
            data = body.get("dataBody") or {}
            if status != 200 or not data.get("access_token"):
                h = body.get("dataHeader") or {}
                code = _t(h.get("processCode")) or None
                msg = f"KB 토큰 발급 실패({_t(h.get('resultCode'))}): {_t(h.get('processMessage')) or _t(h.get('resultMessage'))}"
                if status == 429:
                    raise RateLimitError(status, code, msg, FAILURE_COOLDOWN_SECONDS)
                raise AuthError(status, code, msg)
            return data["access_token"], time.time() + float(data.get("expires_in", 86400))


def _t(value) -> str:
    return str(value or "").strip()


def _side_of(row: dict) -> OrderSide:
    return OrderSide.BUY if "매수" in _t(row.get("trd_dl_ccd_nm")) else OrderSide.SELL


def _same_no(a, b) -> bool:
    return _t(a).lstrip("0") == _t(b).lstrip("0")
