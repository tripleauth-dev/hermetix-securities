"""키움증권 REST 모의투자 어댑터 (KRX 국내주식).

실측 기반 (mockapi.kiwoom.com, 2026-08):
- 모든 호출은 POST + api-id 헤더(TR)로 라우팅
- 응답 엔벨로프: return_code(0=성공) / return_msg
- 가격 필드에 등락 방향 부호가 붙는다 (cur_prc "-239500" = 하락 중인 239,500원) -> 절대값
- 금액 필드는 zero-padded 문자열 ("000000100000000" = 1억)
- TR당 초당 1회 유량 제한 -> 1.1s 쓰로틀 + 백오프 재시도
- 캔들은 일봉(DAY_1)만 지원 / 모의투자는 KRX 만
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from decimal import Decimal

from ..broker import KST, MarketStream, RateLimiter, StreamingBrokerClient, _Http, krx_calendar, krx_tick_round
from ..errors import AuthError, BrokerApiError, MarketClosedError, OrderNotFoundError, RateLimitError
from ..models import (
    TradingEnvironment,
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote, StreamChannel,
)
from ..tokens import FAILURE_COOLDOWN_SECONDS, TokenManager
from .kiwoom_stream import KiwoomMarketStream


def _signed(value, default: str = "0") -> Decimal:
    """등락 부호 접두 필드 ("-239500", "+1200") - 부호 유지 파싱."""
    text = str(value).strip() if value not in (None, "") else default
    return Decimal(text.lstrip("+") or default)


def _signed_or_none(value) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value).strip().lstrip("+"))
    except Exception:
        return None


def _padded(value, default: str = "0") -> Decimal:
    """zero-padded 금액 ("000000100000000") 파싱."""
    text = str(value).strip() if value not in (None, "") else default
    return Decimal(text or default)


class KiwoomClient(StreamingBrokerClient):

    capabilities = BrokerCapabilities(
        broker_id="kiwoom",
        market="KRX",
        currency="KRW",
        candle_intervals=frozenset({CandleInterval.DAY_1}),
        client_order_id=False,
        native_bracket=False,
        fractional_shares=False,
        server_open_orders=True,  # ka10075 미체결 조회 제공
        environments=frozenset({TradingEnvironment.PAPER, TradingEnvironment.LIVE}),
        # 0B 주식체결·0D 호가 - 2026-09 모의 실측. 00 주문체결 - 문서 기반 (통보 프레임 실측 전)
        streams=frozenset({StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS}),
    )

    PAPER_URL = "https://mockapi.kiwoom.com"
    LIVE_URL = "https://api.kiwoom.com"
    PAPER_WS_URL = "wss://mockapi.kiwoom.com:10000/api/dostk/websocket"
    LIVE_WS_URL = "wss://api.kiwoom.com:10000/api/dostk/websocket"

    def __init__(self, appkey: str, secretkey: str,
                 base_url: str = "", throttle_seconds: float = 1.1,
                 environment: TradingEnvironment = TradingEnvironment.PAPER,
                 ws_url: str = ""):
        """base_url 을 비우면 환경에 따라 결정(모의 mockapi / 실전 api.kiwoom.com). TR ID 는 공통.
        ws_url 을 비우면 실시간 웹소켓은 모의 mockapi:10000 / 실전 api:10000."""
        self.environment = environment
        self._ws_url = ws_url or (self.LIVE_WS_URL if environment == TradingEnvironment.LIVE else self.PAPER_WS_URL)
        self._appkey = appkey
        self._secretkey = secretkey
        self._http = _Http(base_url or (self.LIVE_URL if environment == TradingEnvironment.LIVE else self.PAPER_URL))
        # TR 당 초당 1회 유량 제한 - 쓰로틀 + 백오프 재시도
        self._limiter = RateLimiter(throttle_seconds, max_retries=3, backoff=lambda attempt: 1.1 * attempt)
        self._tokens = TokenManager("kiwoom", appkey, self._issue_token, 300)

    # ------------------------------------------------------------------ market

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        quotes = []
        for symbol in symbols:
            node = self._call("/api/dostk/stkinfo", "ka10001", {"stk_cd": self.capabilities.symbol_code(symbol)})
            rate = _signed_or_none(node.get("flu_rt"))
            quotes.append(Quote(
                symbol=symbol,
                price=abs(_signed(node.get("cur_prc"))),
                bid_price=None, ask_price=None,
                volume=int(abs(_signed(node.get("trde_qty")))),
                change=_signed_or_none(node.get("pred_pre")),
                change_rate=(rate / 100) if rate is not None else None,  # % -> 비율
                timestamp=datetime.now(timezone.utc),
            ))
        return quotes

    def get_candles(self, symbol: str, interval: CandleInterval, limit: int | None = None) -> list[Candle]:
        if interval != CandleInterval.DAY_1:
            raise ValueError("키움 어댑터는 일봉(DAY_1)만 지원합니다")
        rows = self._call("/api/dostk/chart", "ka10081",
                          {"stk_cd": self.capabilities.symbol_code(symbol), "base_dt": datetime.now(KST).strftime("%Y%m%d"),
                           "upd_stkpc_tp": "1"}).get("stk_dt_pole_chart_qry", [])
        candles = [
            Candle(
                timestamp=datetime.strptime(r["dt"], "%Y%m%d").replace(tzinfo=KST),
                open=abs(_signed(r.get("open_pric"))), high=abs(_signed(r.get("high_pric"))),
                low=abs(_signed(r.get("low_pric"))), close=abs(_signed(r.get("cur_prc"))),
                volume=int(abs(_signed(r.get("trde_qty")))),
            )
            for r in rows if r.get("dt")
        ]
        candles.sort(key=lambda c: c.timestamp)  # 키움 최신순 -> 과거→최신
        return candles[-limit:] if limit else candles

    def get_calendar(self) -> list[MarketDay]:
        return krx_calendar()

    # ----------------------------------------------------------------- account

    def get_account(self) -> Account:
        deposit = self._call("/api/dostk/acnt", "kt00001", {"qry_tp": "3"})
        balance = self._balance()
        cash = _padded(deposit.get("entr"))
        portfolio = _padded(balance.get("prsm_dpst_aset_amt"))
        if portfolio <= 0:
            portfolio = cash + _padded(balance.get("tot_evlt_amt"))
        return Account(account_id="kiwoom-mock", currency="KRW", cash=cash, portfolio_value=portfolio)

    def get_holdings(self) -> list[Holding]:
        holdings = []
        for row in self._balance().get("acnt_evlt_remn_indv_tot") or []:
            qty = _padded(row.get("rmnd_qty"))
            if qty <= 0:
                continue
            rate = _signed_or_none(row.get("prft_rt"))
            holdings.append(Holding(
                symbol=str(row.get("stk_cd", "")).removeprefix("A"),
                quantity=qty,
                avg_entry_price=_padded(row.get("pur_pric")),
                current_price=abs(_signed(row.get("cur_prc"))) if row.get("cur_prc") else None,
                market_value=_padded(row.get("evlt_amt")) if row.get("evlt_amt") else None,
                unrealized_pnl=_signed_or_none(row.get("evltv_prft")),
                unrealized_pnl_rate=(rate / 100) if rate is not None else None,
            ))
        return holdings

    def get_buying_power(self) -> Decimal:
        deposit = self._call("/api/dostk/acnt", "kt00001", {"qry_tp": "3"})
        power = _padded(deposit.get("ord_alow_amt"))
        return power if power > 0 else _padded(deposit.get("entr"))

    # ------------------------------------------------------------------ orders

    def create_order(self, request: CreateOrderRequest) -> Order:
        api_id = "kt10000" if request.side == OrderSide.BUY else "kt10001"
        is_limit = request.order_type == OrderType.LIMIT
        node = self._call("/api/dostk/ordr", api_id, {
            "dmst_stex_tp": "KRX",
            "stk_cd": self.capabilities.symbol_code(request.symbol),
            "ord_qty": str(request.quantity),
            "ord_uv": str(krx_tick_round(request.limit_price)) if is_limit else "",
            "trde_tp": "0" if is_limit else "3",
            "cond_uv": "",
        })
        return Order(
            order_id=str(node.get("ord_no", "")),
            status=OrderStatus.SUBMITTED,
            symbol=self.capabilities.symbol_code(request.symbol), side=request.side, order_type=request.order_type,
            quantity=request.quantity, limit_price=request.limit_price,
            filled_quantity=Decimal(0), submitted_at=datetime.now(timezone.utc),
        )

    def get_orders(self) -> list[Order]:
        return [self._open_order(row) for row in self._open_rows()]

    def get_order(self, order_id: str) -> Order:
        for row in self._open_rows():
            if str(row.get("ord_no", "")).lstrip("0") == order_id.lstrip("0"):
                return self._open_order(row)
        for row in self._fill_rows():
            if str(row.get("ord_no", "")).lstrip("0") == order_id.lstrip("0"):
                return Order(
                    order_id=order_id, status=OrderStatus.FILLED,
                    symbol=str(row.get("stk_cd", "")).removeprefix("A"),
                    filled_quantity=_padded(row.get("cntr_qty")),
                    avg_fill_price=abs(_signed(row.get("cntr_pric"))) if row.get("cntr_pric") else None,
                )
        return Order(order_id=order_id, status=OrderStatus.CANCELED)

    def cancel_order(self, order_id: str) -> Order:
        row = next((r for r in self._open_rows()
                    if str(r.get("ord_no", "")).lstrip("0") == order_id.lstrip("0")), None)
        if row is None:
            raise OrderNotFoundError("order-not-found", f"키움 미체결 주문을 찾을 수 없습니다: {order_id}")
        self._call("/api/dostk/ordr", "kt10003", {
            "dmst_stex_tp": "KRX", "orig_ord_no": order_id,
            "stk_cd": str(row.get("stk_cd", "")).removeprefix("A"), "cncl_qty": "0",
        })
        return Order(order_id=order_id, status=OrderStatus.CANCELED, canceled_at=datetime.now(timezone.utc))

    def get_fills(self) -> list[Fill]:
        return [
            Fill(
                fill_id=str(r.get("ord_no", "")), order_id=str(r.get("ord_no", "")),
                symbol=str(r.get("stk_cd", "")).removeprefix("A"),
                side=OrderSide.BUY if "매수" in str(r.get("io_tp_nm", "")) else OrderSide.SELL,
                quantity=_padded(r.get("cntr_qty")),
                price=abs(_signed(r.get("cntr_pric"))) if r.get("cntr_pric") else None,
            )
            for r in self._fill_rows()
        ]

    # ---------------------------------------------------------------- internal

    @staticmethod
    def _open_order(row: dict) -> Order:
        ord_qty = _padded(row.get("ord_qty"))
        remaining = _padded(row.get("oso_qty")) if row.get("oso_qty") else ord_qty
        filled = ord_qty - remaining
        return Order(
            order_id=str(row.get("ord_no", "")),
            status=OrderStatus.PARTIALLY_FILLED if filled > 0 else OrderStatus.SUBMITTED,
            symbol=str(row.get("stk_cd", "")).removeprefix("A"),
            side=OrderSide.BUY if "매수" in str(row.get("io_tp_nm", "")) else OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=ord_qty,
            limit_price=abs(_signed(row.get("ord_pric"))) if row.get("ord_pric") else None,
            filled_quantity=filled,
        )

    def _open_rows(self) -> list[dict]:
        return self._call("/api/dostk/acnt", "ka10075",
                          {"all_stk_tp": "0", "trde_tp": "0", "stk_cd": "", "stex_tp": "0"}).get("oso", [])

    def _fill_rows(self) -> list[dict]:
        return self._call("/api/dostk/acnt", "ka10076",
                          {"stk_cd": "", "qry_tp": "0", "sell_tp": "0", "ord_no": "", "stex_tp": "0"}).get("cntr", [])

    def _balance(self) -> dict:
        return self._call("/api/dostk/acnt", "kt00018", {"qry_tp": "1", "dmst_stex_tp": "KRX"})

    def _call(self, path: str, api_id: str, body: dict) -> dict:
        return self._limiter.execute(lambda: self._tokens.call(lambda token: self._call_once(path, api_id, body, token)),
                                     f"키움 {api_id}")

    def _call_once(self, path: str, api_id: str, body: dict, token: str) -> dict:
        status, parsed = self._http.request(
            "POST", path,
            headers={"authorization": f"Bearer {token}", "api-id": api_id},
            json_body=body)

        if status < 200 or status >= 300 or parsed.get("return_code") != 0:
            code = str(parsed.get("return_code"))
            msg = f"키움({api_id}) {parsed.get('return_msg', '')}".strip()
            if "요청 개수를 초과" in msg:
                raise RateLimitError(status, code, msg)
            if "장종료" in msg or "RC4058" in msg:
                raise MarketClosedError(status, code, msg)
            if status == 401 or "8005" in msg:  # 8005: Token이 유효하지 않습니다
                raise AuthError(status, code, msg)
            raise BrokerApiError(status, code, msg)
        return parsed

    # ------------------------------------------------------------------ stream

    def open_stream(self) -> MarketStream:
        """웹소켓 로그인은 REST 접근토큰을 그대로 쓴다 - 만료 시 재접속 때 _get_token 이 갱신한다"""
        return KiwoomMarketStream(self._ws_url, self._get_token, usage=self._usage)

    def _get_token(self) -> str:
        return self._tokens.get()

    def _issue_token(self) -> tuple[str, float]:
        with self._usage.measure("auth"):
            self._limiter.throttle.wait()
            status, body = self._http.request(
                "POST", "/oauth2/token",
                json_body={"grant_type": "client_credentials",
                           "appkey": self._appkey, "secretkey": self._secretkey})
            if status != 200 or body.get("return_code") != 0 or not body.get("token"):
                code = str(body.get("return_code"))
                msg = f"키움 토큰 발급 실패: {body.get('return_msg', '')}"
                if status == 429 or "요청 개수를 초과" in msg:
                    raise RateLimitError(status, code, msg, FAILURE_COOLDOWN_SECONDS)
                raise AuthError(status, code, msg)
            # expires_dt: yyyyMMddHHmmss (KST)
            try:
                expires_at = datetime.strptime(body["expires_dt"], "%Y%m%d%H%M%S").replace(tzinfo=KST).timestamp()
            except (KeyError, ValueError):
                expires_at = time.time() + 86400
            return body["token"], expires_at
