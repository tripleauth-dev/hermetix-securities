"""네 언어가 공유하는 골든 픽스처(conformance/fixtures)를 가짜 HTTP 로 재생해 세 어댑터를 컨포먼스 시나리오에 통과시킨다.
어댑터의 실제 요청 경로(헤더·인증·쓰로틀)를 그대로 지난다."""
import json
from decimal import Decimal
from pathlib import Path

import pytest

from hermetix import (
    ConformanceScenario, DbClient, KbClient, KisClient, KiwoomClient, LsClient, NextClient, NhClient, RateLimitError,
    RateLimiter, TossClient, verify_broker_conformance,
)

FIXTURES = Path(__file__).resolve().parents[2] / "conformance" / "fixtures"


class FakeHttp:
    """픽스처 routes 를 순서대로 매칭 - method / path(정확히) / header(이름·값) 중 지정된 조건만 검사."""

    def __init__(self, routes: list[dict]):
        self.routes = routes
        self.last_headers: dict[str, str] = {}
        self.calls: list[tuple[str, str]] = []

    def request(self, method, path, *, headers=None, query=None, json_body=None, form_body=None):
        headers = headers or {}
        self.calls.append((method, path))
        for r in self.routes:
            if "method" in r and r["method"] != method:
                continue
            if "path" in r and r["path"] != path:
                continue
            if "header" in r and headers.get(r["header"][0]) != r["header"][1]:
                continue
            return r.get("status", 200), r["body"]
        return 599, {"error": f"no fixture route for {method} {path} headers={headers}"}


def load(broker: str):
    fx = json.loads((FIXTURES / f"{broker}.json").read_text())
    scenario = ConformanceScenario(symbol=fx["scenario"]["symbol"], quantity=Decimal(fx["scenario"]["quantity"]),
                                   limit_price=Decimal(fx["scenario"]["limitPrice"]))
    return FakeHttp(fx["routes"]), scenario


@pytest.mark.parametrize("broker", ["next", "kis", "kiwoom", "nh", "db", "ls", "toss", "kb"])
def test_adapter_passes_conformance(broker):
    http, scenario = load(broker)
    if broker == "next":
        client = NextClient("pk_test_conf", "sk_test_conf")
    elif broker == "kis":
        client = KisClient("k", "s", "50199202", throttle_seconds=0.001)
    elif broker == "kiwoom":
        client = KiwoomClient("k", "s", throttle_seconds=0.001)
    elif broker == "nh":
        client = NhClient("k", "s", throttle_seconds=0.001)  # account_no 비움 → /n2/acctinfo 로 모의(03) 계좌 선택
        client._auth_http = http  # 토큰은 운영 호스트 전용이라 별도 클라이언트 — 테스트에선 같은 가짜로
    elif broker == "db":
        client = DbClient("k", "s", throttle_seconds=0.001)
    elif broker == "ls":
        client = LsClient("k", "s", throttle_seconds=0.001, chart_throttle_seconds=0.001)
    elif broker == "toss":
        client = TossClient("c_conf", "s_conf", throttle_seconds=0.001)  # account_seq 비움 → /api/v1/accounts 로 BROKERAGE 선택
    else:
        client = KbClient("k", "s", throttle_seconds=0.001)
    client._http = http
    report = verify_broker_conformance(client, scenario)
    assert report.passed, str(report)
    assert {"quotes", "candles", "calendar", "account", "holdings", "buying_power",
            "create_order", "get_order", "get_orders", "cancel_order", "fills"} <= set(report.steps)


def test_rate_limiter_retries_with_backoff_and_retry_after():
    sleeps = []
    now = [0.0]

    def sleep(s):
        sleeps.append(round(s, 3))
        now[0] += s

    limiter = RateLimiter(0.6, max_retries=2, backoff=lambda a: 1.0 * a, sleep=sleep, clock=lambda: now[0])
    limiter.execute(lambda: now.__setitem__(0, now[0] + 0.1))  # 첫 호출은 대기 없음
    limiter.execute(lambda: None)                                # 0.1s 뒤 -> 0.5s 대기
    assert sleeps == [0.5]

    sleeps.clear()
    calls = [0]

    def flaky():
        calls[0] += 1
        if calls[0] < 3:
            raise RateLimitError(429, "EGW00201", "초당 거래건수 초과")
        return "ok"

    assert RateLimiter(0, max_retries=2, sleep=sleep).execute(flaky) == "ok"
    assert sleeps == [1.0, 2.0]

    sleeps.clear()
    first = [True]

    def with_retry_after():
        if first[0]:
            first[0] = False
            raise RateLimitError(429, None, "rl", retry_after_seconds=3600)
        return "ok"

    assert RateLimiter(0, max_retries=1, sleep=sleep).execute(with_retry_after) == "ok"
    assert sleeps == [RateLimiter.MAX_RETRY_AFTER]

    def always_limited():
        raise RateLimitError(429, None, "x")

    with pytest.raises(RateLimitError):
        RateLimiter(0, max_retries=1, sleep=sleep).execute(always_limited)


def test_toss_us_holdings_are_valued_in_krw_at_mid_rate():
    """미국 종목 평가금액·손익은 USD 로 오므로 매매기준율(midRate 1400)로 원화 환산해 계좌 총평가에 더한다 (실계좌 응답 형태)"""
    http, _ = load("toss")
    client = TossClient("c_conf", "s_conf", throttle_seconds=0.001)
    client._http = http
    aapl = next(h for h in client.get_holdings() if h.symbol == "US:AAPL")
    assert (aapl.current_price, aapl.avg_entry_price) == (Decimal("250"), Decimal("200"))  # 단가는 종목 통화 그대로
    assert (aapl.market_value, aapl.unrealized_pnl) == (Decimal("700000"), Decimal("140000"))
    assert client.get_account().portfolio_value == Decimal("1000000") + Decimal("210000") + Decimal("700000")
