"""토큰 수명주기(TokenManager) - 여덟 어댑터 공통 시나리오.

- 발급 유량 초과(429·EGW00133·IGW00201)는 RateLimitError, 발급 실패 후 60초는 서버에 다시 묻지 않는다
- 업무 호출이 토큰을 거부(401)하면 토큰을 버리고 1회 재발급 후 재시도한다
- 파일 캐시: 다른 프로세스가 새로 저장한 토큰은 지우지 않고 이어 쓴다, 만료 직전 같은 토큰 재발급은 반복하지 않는다
"""
import json
import time
from pathlib import Path

import pytest

from hermetix import (
    AuthError, DbClient, KbClient, KisClient, KiwoomClient, LsClient, NextClient, NhClient, RateLimitError, TossClient,
)
from hermetix import tokens
from hermetix.tokens import TokenManager

FIXTURES = Path(__file__).resolve().parents[2] / "conformance" / "fixtures"
BROKERS = ["next", "kis", "kiwoom", "nh", "db", "ls", "toss", "kb"]


@pytest.fixture
def clock(monkeypatch):
    now = [1_790_000_000.0]  # 2026-09 - 키움 픽스처 토큰(2026-12-31 만료)이 유효한 시점
    monkeypatch.setattr(time, "time", lambda: now[0])
    return now


class FakeHttp:
    """픽스처 routes 재생 - 토큰 발급 응답과 업무 호출 응답을 앞에서부터 덮어쓸 수 있다"""

    def __init__(self, broker: str, token_overrides=(), api_overrides=()):
        fx = json.loads((FIXTURES / f"{broker}.json").read_text())
        self.routes = fx["routes"]
        self.token_path = self.routes[0]["path"]
        self.token_overrides = list(token_overrides)
        self.api_overrides = list(api_overrides)
        self.token_calls = 0
        self.api_calls = 0
        self.last_headers: dict[str, str] = {}

    def request(self, method, path, *, headers=None, query=None, json_body=None, form_body=None):
        headers = headers or {}
        if path == self.token_path:
            self.token_calls += 1
            if self.token_overrides:
                return self.token_overrides.pop(0)
        else:
            self.api_calls += 1
            if self.api_overrides:
                return self.api_overrides.pop(0)
        for r in self.routes:
            if "method" in r and r["method"] != method:
                continue
            if "path" in r and r["path"] != path:
                continue
            if "header" in r and headers.get(r["header"][0]) != r["header"][1]:
                continue
            return r.get("status", 200), r["body"]
        return 599, {"error": f"no fixture route for {method} {path}"}


def make_client(broker: str, http: FakeHttp, clock):
    client = {
        "next": lambda: NextClient("pk_test_conf", "sk_test_conf"),
        "kis": lambda: KisClient("k", "s", "50199202", throttle_seconds=0.001),
        "kiwoom": lambda: KiwoomClient("k", "s", throttle_seconds=0.001),
        "nh": lambda: NhClient("k", "s", throttle_seconds=0.001),
        "db": lambda: DbClient("k", "s", throttle_seconds=0.001),
        "ls": lambda: LsClient("k", "s", throttle_seconds=0.001, chart_throttle_seconds=0.001),
        "toss": lambda: TossClient("c_conf", "s_conf", throttle_seconds=0.001),
        "kb": lambda: KbClient("k", "s", throttle_seconds=0.001),
    }[broker]()
    client._http = http
    if broker == "nh":
        client._auth_http = http
    client._limiter._max_retries = 0  # 재시도 없이 첫 오류를 본다
    client._limiter._sleep = lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    return client


@pytest.mark.parametrize("broker,rejection", [(b, (429, {})) for b in BROKERS] + [
    ("kis", (403, {"error_code": "EGW00133", "error_description": "접근토큰 발급 잠시 후 다시 시도하세요(1분당 1회)"})),
    ("ls", (403, {"rsp_cd": "IGW00201", "rsp_msg": "초당 거래건수를 초과하였습니다."})),
])
def test_token_rate_limit_is_rate_limit_error_with_cooldown(broker, rejection, clock):
    http = FakeHttp(broker, token_overrides=[rejection])
    client = make_client(broker, http, clock)
    with pytest.raises(RateLimitError) as first:
        client.get_account()
    assert first.value.retry_after_seconds == 60
    clock[0] += 20
    with pytest.raises(RateLimitError) as second:
        client.get_account()
    assert round(second.value.retry_after_seconds) == 40
    assert http.token_calls == 1  # 쿨다운 동안 서버에 다시 묻지 않는다

    clock[0] += 41
    client.get_account()  # 쿨다운 뒤 재발급
    assert http.token_calls == 2


@pytest.mark.parametrize("broker", BROKERS)
def test_token_auth_failure_is_not_retried_during_cooldown(broker, clock):
    http = FakeHttp(broker, token_overrides=[(401, {"error": "invalid_client"})])
    client = make_client(broker, http, clock)
    for _ in range(3):
        with pytest.raises(AuthError):
            client.get_account()
    assert http.token_calls == 1
    clock[0] += 61
    client.get_account()
    assert http.token_calls == 2


@pytest.mark.parametrize("broker", BROKERS)
def test_rejected_token_is_discarded_and_call_retried_once(broker, clock):
    http = FakeHttp(broker, api_overrides=[(401, {})])
    client = make_client(broker, http, clock)
    client.get_account()
    assert http.token_calls == 2  # 거부된 토큰을 버리고 1회 재발급

    http.api_overrides = [(401, {}), (401, {})]
    with pytest.raises(AuthError):
        client.get_account()  # 재발급 토큰도 거부되면 그대로 올린다 (무한 재시도 없음)


def issuer(*results):
    calls = []

    def issue():
        calls.append(1)
        result = results[min(len(calls), len(results)) - 1]
        if isinstance(result, Exception):
            raise result
        return result

    return issue, calls


def test_invalidate_keeps_token_saved_by_another_process(tmp_path, monkeypatch, clock):
    monkeypatch.setattr(tokens, "CACHE_DIR", tmp_path)
    issue_a, calls_a = issuer(("tok-a", clock[0] + 86400))
    issue_b, calls_b = issuer(("tok-b", clock[0] + 86400))
    a = TokenManager("toss", "k", issue_a, 60)
    b = TokenManager("toss", "k", issue_b, 60)
    assert a.get() == "tok-a"
    assert b.get() == "tok-a"  # 파일 캐시 공유
    # 토스처럼 재발급이 이전 토큰을 무효로 만드는 서버: b 가 거부당해 재발급하면 a 는 거부당한 뒤 b 의 새 토큰을 쓴다
    b.invalidate("tok-a")
    assert b.get() == "tok-b"
    a.invalidate("tok-a")
    assert a.get() == "tok-b"
    assert (len(calls_a), len(calls_b)) == (1, 1)
    files = list(tmp_path.iterdir())
    assert len(files) == 1 and files[0].name.startswith("toss-")


def test_same_token_reissued_near_expiry_is_used_until_expiry(clock):
    expires_at = clock[0] + 300  # 여유(600초) 안 - DB 는 만료 전 재발급에 남은 시간 그대로의 같은 토큰을 준다
    issue, calls = issuer(("tok-1", expires_at))
    manager = TokenManager("db", "k", issue, 600)
    for _ in range(5):
        assert manager.get() == "tok-1"
    assert len(calls) == 1
    clock[0] = expires_at
    manager.get()
    assert len(calls) == 2


def test_call_does_not_retry_on_issue_failure(clock):
    issue, calls = issuer(AuthError(401, "invalid_client", "rejected"))
    manager = TokenManager("kis", "k", issue, 300)
    with pytest.raises(AuthError):
        manager.call(lambda token: token)
    assert len(calls) == 1
