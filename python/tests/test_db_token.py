"""DB증권 토큰 수명주기 - 발급 1분 1건 제한(403 + IGW00201)을 레이트리밋으로 분류하고, 발급 실패 후 60초는 서버에 다시 묻지 않으며,
발급 토큰을 파일로 저장해 같은 키를 쓰는 다른 프로세스(클라이언트)가 재사용하는지 (Kotlin DbTokenTest 와 동일 시나리오)."""
import json
import stat
import sys

import pytest

from hermetix import tokens
from hermetix.brokers.db import DbClient
from hermetix.errors import AuthError, RateLimitError

db_mod = sys.modules["hermetix.brokers.db"]

BALANCE = {"rsp_cd": "00000", "rsp_msg": "정상", "Out": {"DpsastAmt": "1000", "Dps2": "1000", "TotEvalAmt": "0"}}


class FakeServer:
    """토큰 응답을 차례로 돌려주고, 업무 호출은 잔고 응답으로 받는 가짜 서버"""

    def __init__(self, *token_responses):
        self.token_responses = list(token_responses)
        self.token_calls = 0
        self.api_tokens: list[str] = []

    def request(self, method, path, *, headers=None, query=None, json_body=None, form_body=None):
        if path == "/oauth2/token":
            self.token_calls += 1
            return self.token_responses.pop(0) if len(self.token_responses) > 1 else self.token_responses[0]
        self.api_tokens.append(headers["authorization"])
        return 200, BALANCE


RATE_LIMITED = (403, {"rsp_cd": "IGW00201", "rsp_msg": "초당 거래건수를 초과하였습니다."})
REJECTED = (401, {"rsp_cd": "IGW00121", "rsp_msg": "유효하지 않은 앱키입니다."})


def issued(token="tok-1"):
    return 200, {"access_token": token, "token_type": "Bearer", "expires_in": 86400}


@pytest.fixture
def clock(monkeypatch):
    """time.time 을 고정하고, 레이트리미터의 sleep 이 시계를 앞으로 돌리게 한다"""
    now = [1_800_000_000.0]
    monkeypatch.setattr(db_mod.time, "time", lambda: now[0])
    return now


def client(server, clock, app_key="k"):
    c = DbClient(app_key, "s", throttle_seconds=0.001)
    c._http = server
    c._limiter._sleep = lambda seconds: clock.__setitem__(0, clock[0] + seconds)
    return c


def test_token_rate_limit_is_rate_limit_error_and_not_retried_during_cooldown(clock):
    server = FakeServer(RATE_LIMITED)
    c = client(server, clock)
    c._limiter._max_retries = 0  # 재시도 없이 첫 오류를 그대로 본다
    with pytest.raises(RateLimitError) as first:
        c.get_account()
    assert first.value.retry_after_seconds == 60
    clock[0] += 20
    with pytest.raises(RateLimitError) as second:
        c.get_account()
    assert server.token_calls == 1  # 쿨다운 동안은 서버에 묻지 않는다
    assert second.value.retry_after_seconds == pytest.approx(40)


def test_rate_limiter_waits_out_cooldown_then_issues_again(clock):
    server = FakeServer(RATE_LIMITED, issued())
    c = client(server, clock)
    c.get_account()
    assert server.token_calls == 2  # 실패 1번 + 쿨다운 뒤 1번
    assert server.api_tokens == ["Bearer tok-1"]


def test_auth_failure_is_not_retried_against_server_during_cooldown(clock):
    server = FakeServer(REJECTED, issued())
    c = client(server, clock)
    for _ in range(3):
        with pytest.raises(AuthError):
            c.get_account()
    assert server.token_calls == 1
    clock[0] += 61
    c.get_account()
    assert server.token_calls == 2


def test_issued_token_is_shared_through_file_cache(clock, tmp_path, monkeypatch):
    monkeypatch.setattr(tokens, "CACHE_DIR", tmp_path)
    first = FakeServer(issued("tok-shared"))
    client(first, clock).get_account()

    files = list(tmp_path.iterdir())
    assert len(files) == 1 and files[0].name.startswith("db-")
    assert stat.S_IMODE(files[0].stat().st_mode) == 0o600
    saved = json.loads(files[0].read_text())
    assert saved == {"access_token": "tok-shared", "expires_at": clock[0] + 86400}  # 키·시크릿은 담지 않는다

    second = FakeServer(issued("tok-other"))
    client(second, clock).get_account()  # 새 프로세스 = 새 클라이언트
    assert second.token_calls == 0
    assert second.api_tokens == ["Bearer tok-shared"]

    other_key = FakeServer(issued("tok-other"))
    client(other_key, clock, app_key="k2").get_account()
    assert other_key.token_calls == 1  # 키가 다르면 캐시를 나눠 쓰지 않는다


def test_cached_token_near_expiry_is_ignored(clock, tmp_path, monkeypatch):
    monkeypatch.setattr(tokens, "CACHE_DIR", tmp_path)
    client(FakeServer(issued("tok-old")), clock).get_account()
    clock[0] += 86400 - 300  # 만료 5분 전 - 갱신 여유(10분) 안
    server = FakeServer(issued("tok-new"))
    client(server, clock).get_account()
    assert server.token_calls == 1
    assert server.api_tokens == ["Bearer tok-new"]
