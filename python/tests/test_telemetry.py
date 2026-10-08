"""사용량 텔레메트리 - 계약(docs/telemetry.md)대로 합산·분류·직렬화되고, 전송 실패가 호출자에게 새지 않으며,
어댑터 공개 메서드·토큰 발급·스트림이 자동 계측되는지 (Kotlin UsageTelemetryTest 와 동일 시나리오)."""
import json
import uuid
from datetime import datetime, timezone

import pytest

from hermetix import (
    DbClient, KbClient, KisClient, KiwoomClient, LsClient, NextClient, NhClient, TossClient, verify_broker_conformance,
)
from hermetix.errors import (
    AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, MarketClosedError, OrderNotFoundError, RateLimitError,
)
from hermetix.models import StreamChannel, TradingEnvironment
from hermetix.telemetry import BrokerUsage, classify, telemetry

from tests.test_conformance import load


@pytest.fixture(autouse=True)
def isolated_transport():
    telemetry.drain()
    original = telemetry.transport
    sent: list[str] = []
    telemetry.transport = sent.append
    yield sent
    telemetry.transport = original
    telemetry.drain()


def usage(broker="kis", env=TradingEnvironment.PAPER) -> BrokerUsage:
    return telemetry.for_broker(broker, env)


def ops_of(bucket: dict) -> dict:
    return {o["op"]: o for o in bucket["ops"]}


def test_counts_errors_and_latency_are_aggregated_per_hour_bucket():
    u = usage()
    for _ in range(3):
        with u.measure("quotes"):
            pass
    with pytest.raises(RateLimitError):
        with u.measure("quotes"):
            raise RateLimitError(429, "EGW00201", "too many")
    with pytest.raises(InsufficientFundsError):
        with u.measure("create_order"):
            raise InsufficientFundsError(400, "X", "부족")
    with pytest.raises(OSError):
        with u.measure("candles"):
            raise OSError("connection reset")

    payload = json.loads(telemetry.drain(datetime(2026, 9, 15, 1, 2, 3, tzinfo=timezone.utc)))
    assert payload["schema"] == 1
    assert uuid.UUID(payload["installationId"])
    assert payload["sdk"]["language"] == "python"
    assert payload["sentAt"] == "2026-09-15T01:02:03Z"
    bucket, = payload["buckets"]
    assert bucket["broker"] == "kis" and bucket["environment"] == "PAPER"
    assert bucket["hour"].endswith(":00:00Z")
    ops = ops_of(bucket)
    assert ops["quotes"]["ok"] == 3
    assert ops["quotes"]["errors"] == {"rate_limit": 1}
    assert ops["quotes"]["latencyMs"]["count"] == 4
    assert ops["create_order"]["errors"] == {"insufficient_funds": 1}
    assert ops["candles"]["errors"] == {"network": 1}
    assert bucket["reconnects"] == 0
    assert telemetry.drain() is None  # 비워졌다


def test_measure_passes_return_and_exception_through_and_records_in_finally():
    u = usage()

    def early(flag: bool) -> str:
        with u.measure("account"):
            if flag:
                return "early"
            return "late"

    assert early(True) == "early" and early(False) == "late"
    with pytest.raises(AuthError):
        with u.measure("holdings"):
            raise AuthError(401, "EGW00123", "expired")
    ops = ops_of(json.loads(telemetry.drain())["buckets"][0])
    assert ops["account"]["ok"] == 2
    assert ops["holdings"]["errors"] == {"auth": 1}


def test_stream_counters_are_kept_per_broker_and_environment():
    paper = usage("kiwoom", TradingEnvironment.PAPER)
    live = usage("kiwoom", TradingEnvironment.LIVE)
    paper.stream_subscribed(StreamChannel.TRADES, 2)
    for _ in range(5):
        paper.stream_message(StreamChannel.TRADES)
    paper.stream_subscribed(StreamChannel.ORDER_EVENTS)
    paper.reconnected()
    live.stream_message(StreamChannel.ORDER_BOOK, 3)

    buckets = {b["environment"]: b for b in json.loads(telemetry.drain())["buckets"]}
    streams = {s["channel"]: s for s in buckets["PAPER"]["streams"]}
    assert streams["TRADES"] == {"channel": "TRADES", "subscriptions": 2, "messages": 5}
    assert streams["ORDER_EVENTS"]["subscriptions"] == 1
    assert buckets["PAPER"]["reconnects"] == 1
    assert buckets["LIVE"]["streams"] == [{"channel": "ORDER_BOOK", "subscriptions": 0, "messages": 3}]


def test_p50_p95_use_the_last_256_samples():
    for i in range(1, 301):
        telemetry.record("next", TradingEnvironment.PAPER, "quotes", i / 1000, None)
    lat = json.loads(telemetry.drain())["buckets"][0]["ops"][0]["latencyMs"]
    assert lat["count"] == 300
    assert 165 <= lat["p50"] <= 180   # 최근 256개 = 45..300ms
    assert 280 <= lat["p95"] <= 295


def test_flush_now_skips_when_empty_and_swallows_transport_errors(isolated_transport):
    telemetry.flush_now()
    assert isolated_transport == []

    with usage().measure("quotes"):
        pass

    def boom(_body):
        raise RuntimeError("서버 없음")

    telemetry.transport = boom
    telemetry.flush_now()                    # 예외가 밖으로 나오지 않는다
    assert telemetry.drain() is None         # 실패한 페이로드는 재전송하지 않는다

    with usage().measure("quotes"):
        pass
    telemetry.transport = isolated_transport.append
    telemetry.flush_now()
    assert len(isolated_transport) == 1
    assert json.loads(isolated_transport[0])["buckets"][0]["ops"][0]["op"] == "quotes"


def test_classify_table():
    assert classify(RateLimitError(429, None, None)) == "rate_limit"
    assert classify(MarketClosedError(200, None, None)) == "market_closed"
    assert classify(InvalidOrderError(400, None, None)) == "invalid_order"
    assert classify(OrderNotFoundError(None, None)) == "order_not_found"
    assert classify(BrokerApiError(500, None, "x")) == "server"
    assert classify(BrokerApiError(503, None, "x")) == "server"
    assert classify(BrokerApiError(200, "1", "업무 거절")) == "other"
    assert classify(AuthError(500, "EGW00123", "만료된 토큰")) == "auth"  # 하위 분류가 우선
    wrapped = RuntimeError("wrap")
    wrapped.__cause__ = ConnectionRefusedError("refused")
    assert classify(wrapped) == "network"
    assert classify(ValueError("boom")) == "other"


def test_installation_id_is_a_uuid_and_stable():
    assert uuid.UUID(telemetry.installation_id)
    assert telemetry.installation_id == telemetry.installation_id


@pytest.mark.parametrize("broker", ["next", "kis", "kiwoom", "nh", "db", "ls", "toss", "kb"])
def test_adapters_are_instrumented_through_the_conformance_scenario(broker):
    http, scenario = load(broker)
    if broker == "next":
        client = NextClient("pk_test_conf", "sk_test_conf")
    elif broker == "kis":
        client = KisClient("k", "s", "50199202", throttle_seconds=0.001)
    elif broker == "kiwoom":
        client = KiwoomClient("k", "s", throttle_seconds=0.001)
    elif broker == "nh":
        client = NhClient("k", "s", throttle_seconds=0.001)
        client._auth_http = http
    elif broker == "db":
        client = DbClient("k", "s", throttle_seconds=0.001)
    elif broker == "ls":
        client = LsClient("k", "s", throttle_seconds=0.001, chart_throttle_seconds=0.001)
    elif broker == "toss":
        client = TossClient("c_conf", "s_conf", throttle_seconds=0.001)
    else:
        client = KbClient("k", "s", throttle_seconds=0.001)
    client._http = http
    assert verify_broker_conformance(client, scenario).passed

    buckets = json.loads(telemetry.drain())["buckets"]
    bucket = next(b for b in buckets if b["broker"] == broker)
    assert bucket["environment"] == client.environment.name
    ops = ops_of(bucket)
    for op in ("quotes", "candles", "calendar", "account", "holdings", "buying_power",
               "create_order", "get_orders", "get_order", "cancel_order", "fills"):
        assert ops[op]["ok"] >= 1, (broker, op)
        assert ops[op]["latencyMs"]["count"] >= 1
    assert ops["auth"]["ok"] == 1, (broker, ops.get("auth"))  # 토큰 발급은 첫 호출에서 1회, 이후 캐시


def test_instrumentation_wraps_each_subclass_once_and_ignores_fakes_without_capabilities():
    from hermetix.broker import BrokerClient

    class Fake(BrokerClient):
        def get_quotes(self, symbols): return []
        def get_candles(self, symbol, interval, limit=None): return []
        def get_calendar(self): return []
        def get_account(self): return None
        def get_holdings(self): return []
        def get_buying_power(self): return 0
        def create_order(self, request): return None
        def get_orders(self): return []
        def get_order(self, order_id): return None
        def cancel_order(self, order_id): return None
        def get_fills(self): return []

    assert getattr(Fake.get_quotes, "_hermetix_measured", False)
    assert Fake().get_quotes(["005930"]) == []  # capabilities 없음 -> 기록 없이 통과
    assert telemetry.drain() is None


def test_kis_stream_counts_subscriptions_and_messages():
    pytest.importorskip("websockets")
    from hermetix.brokers.kis_stream import KisMarketStream
    from tests.ws_fake_server import FakeServer, wait_until
    from tests.test_stream import KIS_FIELDS

    server = FakeServer()
    stream = KisMarketStream(server.url("/"), "P", lambda: "APPROVAL-KEY", usage=usage("kis"))
    ticks = []
    try:
        stream.subscribe_trades(["KRX:005930", "000660"], ticks.append)
        stream.connect()
        conn = server.take()
        conn.take(); conn.take()  # 구독 2건
        conn.send("0|H0STCNT0|001|" + KIS_FIELDS)
        wait_until(lambda: len(ticks) == 1)
    finally:
        stream.close()
        server.shutdown()

    bucket = json.loads(telemetry.drain())["buckets"][0]
    streams = {s["channel"]: s for s in bucket["streams"]}
    assert streams["TRADES"] == {"channel": "TRADES", "subscriptions": 2, "messages": 1}
    assert bucket["reconnects"] == 0


# ---------------------------------------------------------------- 요청 서명 (docs/telemetry.md "요청 서명")

def test_sign_matches_contract_vector():
    from hermetix import telemetry as tm
    sig = tm.sign('{"schema":1}', 1700000000)
    assert sig == "c0ce56d2a2b120597403cc70160e8db7ae60d242916857319ecc5845522739d2"
    assert len(sig) == 64 and sig == sig.lower()
    assert tm.sign('{"schema":1}', 1700000001) != sig
    assert tm.SIGNING_KEY_ID == "v1"


def test_default_transport_sends_signature_headers(monkeypatch):
    import urllib.request
    from hermetix import telemetry as tm

    captured = {}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_urlopen(request, timeout=None):
        captured["request"] = request
        captured["timeout"] = timeout
        return _Resp()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    body = '{"schema":1}'
    tm._post(body)

    req = captured["request"]
    assert captured["timeout"] == 3
    assert req.full_url == tm.ENDPOINT
    assert req.get_header("X-hermetix-key-id") == "v1"
    ts = int(req.get_header("X-hermetix-timestamp"))
    assert abs(ts - int(__import__("time").time())) < 5
    assert req.get_header("X-hermetix-signature") == tm.sign(body, ts)
    assert req.get_header("Content-type") == "application/json"
    assert req.get_header("User-agent").startswith("hermetix-python/")

