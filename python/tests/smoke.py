"""실서버 스모크 테스트 (수동 실행 전용).

    python tests/smoke.py next     # NEXT_CLIENT_ID / NEXT_CLIENT_SECRET
    python tests/smoke.py kis      # KIS_APPKEY / KIS_APPSECRET / KIS_CANO
    python tests/smoke.py kiwoom   # KIWOOM_APPKEY / KIWOOM_SECRETKEY
    python tests/smoke.py toss     # TOSS_CLIENT_ID / TOSS_CLIENT_SECRET (선택 TOSS_ACCOUNT_SEQ)

시세 -> 캔들 -> 캘린더 -> 계좌 -> 보유 -> 주문가능액 -> (장중이면) 주문 생성->조회->취소.
토스는 실전 계좌뿐이라 조회만 하고, TOSS_SMOKE_ORDER=1 일 때만 체결되지 않을 지정가 1주 매수 후 즉시 취소한다.
"""
import logging
import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")

from hermetix import (  # noqa: E402
    CandleInterval, CreateOrderRequest, KisClient, KiwoomClient, NextClient, TossClient,
    MarketClosedError, BrokerApiError, OrderSide, OrderType,
)
from hermetix.engine import MarketCalendar, pnl_report  # noqa: E402


def make_client(name: str):
    if name == "next":
        return NextClient(os.environ["NEXT_CLIENT_ID"], os.environ["NEXT_CLIENT_SECRET"]), "AAPL", Decimal("150")
    if name == "kis":
        return KisClient(os.environ["KIS_APPKEY"], os.environ["KIS_APPSECRET"], os.environ["KIS_CANO"]), "005930", None
    if name == "kiwoom":
        return KiwoomClient(os.environ["KIWOOM_APPKEY"], os.environ["KIWOOM_SECRETKEY"]), "005930", None
    if name == "toss":
        client = TossClient(os.environ["TOSS_CLIENT_ID"], os.environ["TOSS_CLIENT_SECRET"], os.environ.get("TOSS_ACCOUNT_SEQ", ""))
        return client, "KRX:005930", None
    raise SystemExit(f"unknown broker: {name}")


def main(name: str) -> None:
    client, symbol, far_price = make_client(name)
    live_only = name == "toss"  # 모의투자가 없어 실계좌 - 잔고 0 일 수 있고 주문은 명시적으로 켤 때만
    print(f"== {name} ({client.capabilities.market}) ==")

    quotes = client.get_quotes([symbol])
    assert quotes and quotes[0].price > 0
    print(f"현재가 {symbol}: {quotes[0].price} / 등락률 {quotes[0].change_rate}")

    candles = client.get_candles(symbol, CandleInterval.DAY_1, 30)
    assert len(candles) >= 20 and candles[0].timestamp < candles[-1].timestamp
    print(f"일봉 {len(candles)}개 / 최신 종가 {candles[-1].close}")

    print(f"정규장 open = {MarketCalendar(client).is_regular_open()}")

    account = client.get_account()
    assert live_only or account.cash > 0
    print(f"예수금 {account.cash} / 총평가 {account.portfolio_value}")

    holdings = client.get_holdings()
    print(f"보유 {len(holdings)}종목: {[(h.symbol, str(h.quantity)) for h in holdings[:5]]}")

    power = client.get_buying_power()
    assert live_only or power > 0
    print(f"주문가능 {power}")

    client.get_orders()
    client.get_fills()

    # 주문 사이클 - 시장가와 먼 지정가 매수 후 취소
    if live_only and os.environ.get("TOSS_SMOKE_ORDER") != "1":
        print("주문 스킵: 실전 계좌 (TOSS_SMOKE_ORDER=1 일 때만 주문)")
    else:
        if far_price is None:
            far_price = (quotes[0].price * Decimal("0.8")).quantize(Decimal("100"))
        try:
            order = client.create_order(CreateOrderRequest(
                symbol=symbol, side=OrderSide.BUY, order_type=OrderType.LIMIT,
                quantity=Decimal(1), limit_price=far_price))
            print(f"주문 접수: {order.order_id}")
            detail = client.get_order(order.order_id)
            print(f"주문 조회: {detail.status.value}")
            canceled = client.cancel_order(order.order_id)
            print(f"주문 취소: {canceled.status.value}")
        except (MarketClosedError, BrokerApiError) as e:
            print(f"주문 스킵: {e.message}")

    report = pnl_report(client)
    print(f"PnL: portfolio={report['portfolio_value']} unrealized={report['total_unrealized_pnl']}")
    print("SMOKE OK")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "next")
