"""실측 제보 도구(hermetix.verify)를 골든 픽스처의 가짜 HTTP 로 돌려 — 단계 통과·마스킹·결과 형식을 확인한다."""
import json
from decimal import Decimal

from hermetix import KisClient, NhClient
from hermetix.verify import VerifyOptions, far_limit_price, krx_tick, mask, run_verification

from test_conformance import load


def test_runs_full_flow_against_fixture_and_masks_secrets():
    http, scenario = load("nh")
    client = NhClient("APPKEY-ABC", "SECRET-XYZ", throttle_seconds=0.001)
    client._auth_http = http
    client._http = http
    report = run_verification(client, VerifyOptions(symbol=scenario.symbol, limit_price=scenario.limit_price, orders=True),
                              secrets=["APPKEY-ABC", "SECRET-XYZ"])
    assert not report.failed, [(s.name, s.detail) for s in report.failed]
    assert {"quotes", "candles", "calendar", "account", "holdings", "buying_power", "create_order", "get_order",
            "get_orders_contains", "cancel_order"} <= {s.name for s in report.steps}
    assert report.http, "HTTP 기록이 비어 있다"
    dumped = json.dumps(report.to_dict(), ensure_ascii=False, default=str)
    assert "APPKEY-ABC" not in dumped and "SECRET-XYZ" not in dumped
    # 토큰 응답·인증 헤더는 키 이름으로 가려진다
    assert '"access_token": "***"' in dumped or '"token": "***"' in dumped or "access_token" not in dumped
    assert report.to_dict()["format"] == 1


def test_read_only_skips_orders_and_records_reason():
    http, scenario = load("kis")
    client = KisClient("k", "s", "50199202", throttle_seconds=0.001)
    client._http = http
    report = run_verification(client, VerifyOptions(symbol=scenario.symbol, orders=False), secrets=["50199202"])
    steps = {s.name: s for s in report.steps}
    assert steps["create_order"].status == "skip"
    assert "get_order" not in steps
    assert "50199202" not in json.dumps(report.to_dict(), default=str)


def test_mask_hides_sensitive_keys_and_known_values():
    masked = mask({"appkey": "abc", "tr_id": "FHKST01010100", "acct_no": "123", "nested": {"cust_nm": "홍길동", "note": "call SEC1"}},
                  secrets=["SEC1"])
    assert masked == {"appkey": "***", "tr_id": "FHKST01010100", "acct_no": "***", "nested": {"cust_nm": "***", "note": "call ***"}}


def test_far_limit_price_follows_krx_ticks():
    assert krx_tick(Decimal("1500")) == 1
    assert krx_tick(Decimal("71000")) == 100
    assert far_limit_price(Decimal("71300"), "KRW") == Decimal("57000")   # 57040 → 100 단위 내림
    assert far_limit_price(Decimal("9990"), "KRW") == Decimal("7990")     # 7992 → 10 단위 내림
    assert far_limit_price(Decimal("150.55"), "USD") == Decimal("120.44")
