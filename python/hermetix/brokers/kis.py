"""한국투자증권(KIS) 모의투자 어댑터 (KRX 국내주식).

실측 기반 (openapivts.koreainvestment.com:29443, 2026-08):
- 응답 엔벨로프: rt_cd("0"=성공) / msg_cd / msg1 / output*
- 초당 요청 제한 -> 0.6s 쓰로틀 + EGW00201 백오프 재시도
- 토큰 발급은 1분당 1회 제한(초과 시 EGW00133 → RateLimitError). 토큰은 24h, 파일 캐시로 프로세스 간 재사용 (TokenManager)
- **모의 서버는 미체결/체결 주문 조회를 제공하지 않는다** (일별주문체결 TR 이
  항상 빈 목록) -> 주문은 어댑터가 메모리 추적, 체결은 보유 수량 변화로 근사.
  앱 재시작 시 추적이 끊긴다 (재시작 후 잔여 미체결 주의)
- 주문 취소는 지점번호 없이 ODNO 만으로 동작 (실측 검증)
- 캔들은 일봉(DAY_1)만 지원 - 분봉 API 가 당일 데이터만 제공
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from ..broker import KST, MarketStream, RateLimiter, StreamingBrokerClient, _Http, krx_calendar, krx_tick_round
from ..errors import AuthError, BrokerApiError, MarketClosedError, OrderNotFoundError, RateLimitError
from ..models import (
    TradingEnvironment,
    Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
    Fill, Holding, MarketDay, Order, OrderEvent, OrderEventType, OrderSide, OrderStatus, Quote, StreamChannel,
)
from ..tokens import FAILURE_COOLDOWN_SECONDS, TokenManager
from .kis_stream import KisMarketStream

logger = logging.getLogger("hermetix")

# 업무 호출이 토큰을 거부 - EGW00121 유효하지 않은 token, EGW00123 기간이 만료된 token (HTTP 500 으로 온다)
_TOKEN_REJECTED_CODES = {"EGW00121", "EGW00123"}


def _d(value, default: str = "0") -> Decimal:
    text = str(value).strip() if value not in (None, "") else default
    return Decimal(text or default)


def _d_or_none(value) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value).strip())
    except Exception:
        return None


@dataclass(frozen=True)
class _Tracked:
    order: Order
    baseline_qty: Decimal
    day: date


class KisClient(StreamingBrokerClient):

    capabilities = BrokerCapabilities(
        broker_id="kis",
        market="KRX",
        currency="KRW",
        candle_intervals=frozenset({CandleInterval.DAY_1}),
        client_order_id=False,
        native_bracket=False,
        fractional_shares=False,
        server_open_orders=False,  # 모의 서버가 주문 조회 미제공 - 어댑터 내부 추적
        environments=frozenset({TradingEnvironment.PAPER, TradingEnvironment.LIVE}),
        # H0STCNT0 체결가·H0STASP0 호가 - 2026-09 모의 실측. H0STCNI9 주문 통보 - 문서 기반 (HTS ID 필요)
        streams=frozenset({StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS}),
    )

    def _tr(self, suffix: str) -> str:
        """계좌 TR ID — 모의 V, 실전 T 프리픽스 (예: _tr("TTC0802U") → VTTC0802U / TTTC0802U)"""
        return ("T" if self.environment == TradingEnvironment.LIVE else "V") + suffix

    PAPER_URL = "https://openapivts.koreainvestment.com:29443"
    LIVE_URL = "https://openapi.koreainvestment.com:9443"
    PAPER_WS_URL = "ws://ops.koreainvestment.com:31000"
    LIVE_WS_URL = "ws://ops.koreainvestment.com:21000"

    def __init__(self, appkey: str, appsecret: str, cano: str, acnt_prdt_cd: str = "01",
                 custtype: str = "P", base_url: str = "",
                 throttle_seconds: float = 0.0,
                 environment: TradingEnvironment = TradingEnvironment.PAPER,
                 ws_url: str = "", hts_id: str = ""):
        """base_url 을 비우면 환경에 따라 결정(모의 openapivts:29443 / 실전 openapi:9443).
        throttle_seconds 0 이면 자동 — 모의 0.6(초당 2건), 실전 0.1(초당 20건 한도의 절반).
        계좌 TR ID 는 모의 V / 실전 T 프리픽스. ws_url 을 비우면 실시간 웹소켓은 모의 ops:31000 / 실전 ops:21000.
        hts_id 는 실시간 주문 통보(H0STCNI9/H0STCNI0) 구독 키 - 비우면 주문 통보 스트림을 쓰지 않는다."""
        self.environment = environment
        self._ws_url = ws_url or (self.LIVE_WS_URL if environment == TradingEnvironment.LIVE else self.PAPER_WS_URL)
        self._hts_id = hts_id
        self._appkey = appkey
        self._appsecret = appsecret
        self._cano = cano
        self._acnt_prdt_cd = acnt_prdt_cd
        self._custtype = custtype
        live = environment == TradingEnvironment.LIVE
        self._http = _Http(base_url or (self.LIVE_URL if live else self.PAPER_URL))
        # 초당 요청 제한 - 쓰로틀 + EGW00201 백오프 재시도
        self._limiter = RateLimiter(throttle_seconds or (0.1 if live else 0.6), max_retries=3,
                                    backoff=lambda attempt: 1.0 * attempt)
        self._tokens = TokenManager("kis", appkey, self._issue_token, 300)
        self._tracked: dict[str, _Tracked] = {}

    # ------------------------------------------------------------------ market

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        quotes = []
        for symbol in symbols:
            out = self._call("GET", "/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
                             query={"FID_COND_MRKT_DIV_CODE": "J",
                                    "FID_INPUT_ISCD": self.capabilities.symbol_code(symbol)})["output"]
            rate = _d_or_none(out.get("prdy_ctrt"))
            quotes.append(Quote(
                symbol=symbol,
                price=_d(out.get("stck_prpr")),
                bid_price=None, ask_price=None,
                volume=int(_d(out.get("acml_vol"))),
                change=_d_or_none(out.get("prdy_vrss")),
                # KIS 는 % 단위(-8.76) -> 비율(-0.0876)
                change_rate=(rate / 100) if rate is not None else None,
                timestamp=datetime.now(timezone.utc),
            ))
        return quotes

    def get_candles(self, symbol: str, interval: CandleInterval, limit: int | None = None) -> list[Candle]:
        if interval != CandleInterval.DAY_1:
            raise ValueError("KIS 어댑터는 일봉(DAY_1)만 지원합니다 - 분봉 API 가 당일 데이터만 제공됩니다")
        count = limit or 30
        today = datetime.now(KST).date()
        start = today - timedelta(days=count * 16 // 10 + 10)  # 휴장일 감안 여유 조회

        rows = self._call("GET", "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
                          query={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": self.capabilities.symbol_code(symbol),
                                 "FID_INPUT_DATE_1": start.strftime("%Y%m%d"),
                                 "FID_INPUT_DATE_2": today.strftime("%Y%m%d"),
                                 "FID_PERIOD_DIV_CODE": "D", "FID_ORG_ADJ_PRC": "0"})["output2"]

        candles = [
            Candle(
                timestamp=datetime.strptime(r["stck_bsop_date"], "%Y%m%d").replace(tzinfo=KST),
                open=_d(r["stck_oprc"]), high=_d(r["stck_hgpr"]),
                low=_d(r["stck_lwpr"]), close=_d(r["stck_clpr"]),
                volume=int(_d(r.get("acml_vol"))),
            )
            for r in rows if r.get("stck_bsop_date")
        ]
        candles.sort(key=lambda c: c.timestamp)  # KIS 최신순 -> 과거→최신
        return candles[-count:]

    def get_calendar(self) -> list[MarketDay]:
        return krx_calendar()

    # ----------------------------------------------------------------- account

    def get_account(self) -> Account:
        summary_rows = self._balance().get("output2") or []
        if not summary_rows:
            raise BrokerApiError(200, None, "KIS 잔고 요약(output2)이 비어 있습니다")
        summary = summary_rows[0]
        return Account(
            account_id=self._cano,
            currency="KRW",
            cash=_d(summary.get("dnca_tot_amt")),
            portfolio_value=_d(summary.get("tot_evlu_amt")),
        )

    def get_holdings(self) -> list[Holding]:
        holdings = []
        for row in self._balance().get("output1") or []:
            qty = _d(row.get("hldg_qty"))
            if qty <= 0:
                continue
            rate = _d_or_none(row.get("evlu_pfls_rt"))
            holdings.append(Holding(
                symbol=row["pdno"],
                quantity=qty,
                avg_entry_price=_d(row.get("pchs_avg_pric")),
                current_price=_d_or_none(row.get("prpr")),
                market_value=_d_or_none(row.get("evlu_amt")),
                unrealized_pnl=_d_or_none(row.get("evlu_pfls_amt")),
                unrealized_pnl_rate=(rate / 100) if rate is not None else None,
            ))
        return holdings

    def get_buying_power(self) -> Decimal:
        out = self._call("GET", "/uapi/domestic-stock/v1/trading/inquire-psbl-order", self._tr("TTC8908R"),
                         query={**self._acct(), "PDNO": "005930", "ORD_UNPR": "", "ORD_DVSN": "01",
                                "CMA_EVLU_AMT_ICLD_YN": "N", "OVRS_ICLD_YN": "N"})["output"]
        return _d(out.get("ord_psbl_cash"))

    # ------------------------------------------------------------------ orders

    def create_order(self, request: CreateOrderRequest) -> Order:
        tr_id = self._tr("TTC0802U" if request.side == OrderSide.BUY else "TTC0801U")
        code = self.capabilities.symbol_code(request.symbol)
        is_limit = request.order_type.value == "LIMIT"
        out = self._call("POST", "/uapi/domestic-stock/v1/trading/order-cash", tr_id,
                         body={**self._acct(), "PDNO": code,
                               "ORD_DVSN": "00" if is_limit else "01",
                               "ORD_QTY": str(request.quantity),
                               "ORD_UNPR": str(krx_tick_round(request.limit_price)) if is_limit else "0"})["output"]

        order = Order(
            order_id=out["ODNO"],
            status=OrderStatus.SUBMITTED,
            symbol=code,  # 보유/추적과 같은 단일 시장 표기(접두 없음)
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            limit_price=request.limit_price,
            filled_quantity=Decimal(0),
            submitted_at=datetime.now(timezone.utc),
        )
        self._tracked[order.order_id] = _Tracked(
            order=order, baseline_qty=self._holding_qty(code), day=datetime.now(KST).date())
        return order

    def get_orders(self) -> list[Order]:
        self._refresh_tracked()
        return [t.order for t in self._tracked.values() if t.order.status.is_open]

    def get_order(self, order_id: str) -> Order:
        self._refresh_tracked()
        tracked = self._tracked.get(order_id)
        if tracked:
            return tracked.order
        # 추적 밖(재시작 등) - 알 수 없어 취소로 간주
        return Order(order_id=order_id, status=OrderStatus.CANCELED)

    def cancel_order(self, order_id: str) -> Order:
        # 실측: 모의 서버는 지점번호 없이 ODNO 만으로 취소된다
        self._call("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl", self._tr("TTC0803U"),
                   body={**self._acct(), "KRX_FWDG_ORD_ORGNO": "", "ORGN_ODNO": order_id,
                         "ORD_DVSN": "00", "RVSE_CNCL_DVSN_CD": "02",
                         "ORD_QTY": "0", "ORD_UNPR": "0", "QTY_ALL_ORD_YN": "Y"})

        canceled_at = datetime.now(timezone.utc)
        tracked = self._tracked.get(order_id)
        if tracked:
            self._tracked[order_id] = replace(
                tracked, order=replace(tracked.order, status=OrderStatus.CANCELED, canceled_at=canceled_at))
        return Order(order_id=order_id, status=OrderStatus.CANCELED, canceled_at=canceled_at)

    def get_fills(self) -> list[Fill]:
        # 모의 서버가 체결 내역을 제공하지 않음 - 추적 주문 중 체결 판정된 것으로 근사
        self._refresh_tracked()
        return [
            Fill(fill_id=t.order.order_id, order_id=t.order.order_id, symbol=t.order.symbol,
                 side=t.order.side, quantity=t.order.quantity, price=t.order.limit_price)
            for t in self._tracked.values() if t.order.status == OrderStatus.FILLED
        ]

    # ---------------------------------------------------------------- internal

    def _refresh_tracked(self) -> None:
        """추적 중인 미체결 주문의 체결 여부를 보유 수량 변화로 판정한다 (모의 서버 제약의 근사)."""
        today = datetime.now(KST).date()
        for order_id in [oid for oid, t in self._tracked.items() if t.day != today]:
            del self._tracked[order_id]  # DAY 주문 - 날짜가 바뀌면 소멸

        open_tracked = [t for t in self._tracked.values() if t.order.status.is_open]
        if not open_tracked:
            return

        holdings = {h.symbol: h.quantity for h in self.get_holdings()}
        for tracked in open_tracked:
            current = holdings.get(tracked.order.symbol, Decimal(0))
            qty = tracked.order.quantity or Decimal(0)
            filled = (current >= tracked.baseline_qty + qty) if tracked.order.side == OrderSide.BUY \
                else (current <= tracked.baseline_qty - qty)
            if filled:
                self._tracked[tracked.order.order_id] = replace(
                    tracked, order=replace(tracked.order, status=OrderStatus.FILLED, filled_quantity=qty))

    def _holding_qty(self, symbol: str) -> Decimal:
        for h in self.get_holdings():
            if h.symbol == symbol:
                return h.quantity
        return Decimal(0)

    def _balance(self) -> dict:
        return self._call("GET", "/uapi/domestic-stock/v1/trading/inquire-balance", self._tr("TTC8434R"),
                          query={**self._acct(), "AFHR_FLPR_YN": "N", "OFL_YN": "", "INQR_DVSN": "02",
                                 "UNPR_DVSN": "01", "FUND_STTL_ICLD_YN": "N", "FNCG_AMT_AUTO_RDPT_YN": "N",
                                 "PRCS_DVSN": "00", "CTX_AREA_FK100": "", "CTX_AREA_NK100": ""})

    def _acct(self) -> dict:
        return {"CANO": self._cano, "ACNT_PRDT_CD": self._acnt_prdt_cd}

    def _call(self, method: str, path: str, tr_id: str, *, query: dict | None = None,
              body: dict | None = None) -> dict:
        return self._limiter.execute(
            lambda: self._tokens.call(lambda token: self._call_once(method, path, tr_id, token, query=query, body=body)),
            f"KIS {tr_id}")

    def _call_once(self, method: str, path: str, tr_id: str, token: str, *, query: dict | None,
                   body: dict | None) -> dict:
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": self._appkey, "appsecret": self._appsecret,
            "tr_id": tr_id, "custtype": self._custtype,
        }
        status, parsed = self._http.request(method, path, headers=headers, query=query, json_body=body)

        if status < 200 or status >= 300 or parsed.get("rt_cd") != "0":
            code = parsed.get("msg_cd")
            msg = f"KIS({tr_id}) {parsed.get('msg1', '')}".strip()
            if code == "EGW00201":
                raise RateLimitError(status, code, msg)
            if "장종료" in msg or "장운영일이 아닙" in msg:
                raise MarketClosedError(status, code, msg)
            if status == 401 or code in _TOKEN_REJECTED_CODES:
                raise AuthError(status, code, msg)
            raise BrokerApiError(status, code, msg)
        return parsed

    # ------------------------------------------------------------------ stream

    def open_stream(self) -> MarketStream:
        return KisMarketStream(self._ws_url, self._custtype, self.approval_key, hts_id=self._hts_id,
                               live=self.environment == TradingEnvironment.LIVE, usage=self._usage)

    def apply_order_event(self, event: OrderEvent) -> None:
        """주문 통보를 메모리 추적에 반영한다 - 모의 서버가 주문 조회를 제공하지 않아 보유 수량 변화로 근사하던 체결 판정을
        통보가 오면 즉시 확정한다. 통보 주문번호는 10자리 0 패딩이라 order_id_matches 로 맞춘다."""
        order_id = next((oid for oid in self._tracked if event.order_id_matches(oid)), None)
        if order_id is None:
            return
        tracked = self._tracked[order_id]
        order = tracked.order
        if event.type == OrderEventType.FILLED:
            filled = (order.filled_quantity or Decimal(0)) + (event.quantity or Decimal(0))
            total = order.quantity if order.quantity is not None else filled
            done = filled >= total
            updated = replace(order, filled_quantity=filled,
                              avg_fill_price=event.price if event.price is not None else order.avg_fill_price,
                              status=OrderStatus.FILLED if done else OrderStatus.PARTIALLY_FILLED)
        elif event.type == OrderEventType.CANCELED:
            updated = replace(order, status=OrderStatus.CANCELED, canceled_at=event.timestamp)
        elif event.type == OrderEventType.REJECTED:
            updated = replace(order, status=OrderStatus.REJECTED)
        else:
            return  # ACCEPTED / MODIFIED - 상태 변화 없음
        self._tracked[order_id] = replace(tracked, order=updated)
        logger.info("KIS order event applied / %s %s -> %s", order_id, event.type.value, updated.status.value)

    def approval_key(self) -> str:
        """웹소켓 접속키 (POST /oauth2/Approval). 토큰과 달리 캐시하지 않는다 - 접속마다 새로 받아도 무방하다.
        필드명이 REST 토큰(appsecret)과 달리 secretkey 인 점에 주의."""
        with self._usage.measure("auth"):
            self._limiter.throttle.wait()
            status, body = self._http.request(
                "POST", "/oauth2/Approval",
                json_body={"grant_type": "client_credentials", "appkey": self._appkey, "secretkey": self._appsecret})
            if status != 200 or not body.get("approval_key"):
                raise AuthError(status, body.get("error_code"),
                                f"KIS 웹소켓 접속키 발급 실패: {body.get('error_description', '')}")
            return body["approval_key"]

    def _get_token(self) -> str:
        return self._tokens.get()

    def _issue_token(self) -> tuple[str, float]:
        with self._usage.measure("auth"):
            self._limiter.throttle.wait()
            status, body = self._http.request(
                "POST", "/oauth2/tokenP",
                json_body={"grant_type": "client_credentials",
                           "appkey": self._appkey, "appsecret": self._appsecret})
            if status != 200 or "access_token" not in body:
                code = body.get("error_code")
                detail = body.get("error_description", "")
                if code == "EGW00133" or status == 429:
                    raise RateLimitError(status, code, f"KIS 토큰 발급 유량 초과({code}): {detail} (발급은 1분당 1회 제한)",
                                         FAILURE_COOLDOWN_SECONDS)
                raise AuthError(status, code, f"KIS 토큰 발급 실패({code}): {detail}")
            return body["access_token"], time.time() + float(body.get("expires_in", 86400))
