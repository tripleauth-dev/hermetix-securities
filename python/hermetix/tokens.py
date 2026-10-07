"""증권사 접근 토큰 수명주기 - 여덟 어댑터 공용.

- 메모리 → 파일 캐시 → 발급 순으로 찾는다. 파일 캐시는 같은 키를 쓰는 여러 프로세스가 토큰을 나눠 쓰게 한다
  (KIS·DB 는 발급 1분 1건, 토스는 재발급 시 이전 토큰 무효라 프로세스마다 발급하면 서로 부딪친다).
  위치 ~/.hermetix/tokens/<broker>-<sha256(app_key) 앞 16자>.json, 내용은 access_token·expires_at 뿐 - 키·시크릿은 담지 않는다.
- 발급에 실패하면 FAILURE_COOLDOWN_SECONDS 동안 서버에 다시 묻지 않고 같은 오류(유량 초과면 남은 시간)를 돌려준다.
- 업무 호출이 토큰을 거부(AuthError)하면 그 토큰을 메모리·파일에서 버리고 한 번 다시 시도한다 (call).
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Callable, TypeVar

from .errors import AuthError, BrokerApiError, RateLimitError

T = TypeVar("T")

# 발급 실패 후 서버에 다시 묻지 않는 시간 (KIS·DB 발급 1분 1건 제한에 맞춘다)
FAILURE_COOLDOWN_SECONDS = 60.0


def _default_cache_dir() -> Path | None:
    try:
        return Path.home() / ".hermetix" / "tokens"
    except Exception:  # noqa: BLE001
        return None


# None 이면 파일 캐시를 쓰지 않는다 (테스트)
CACHE_DIR: Path | None = _default_cache_dir()


class TokenManager:
    """issue() 는 (토큰, 만료 epoch 초) 를 돌려주거나 AuthError / RateLimitError 를 던진다 - 텔레메트리 "auth" 측정은 issue 가 한다."""

    def __init__(self, broker: str, app_key: str, issue: Callable[[], tuple[str, float]],
                 refresh_margin_seconds: float, clock: Callable[[], float] | None = None):
        self._broker = broker
        self._app_key = app_key
        self._issue = issue
        self._margin = refresh_margin_seconds
        self._clock = clock or (lambda: time.time())
        self._lock = threading.Lock()
        self._token: str | None = None
        self._refresh_at = 0.0
        # 마지막 발급 실패 (쿨다운 종료 시각, 예외)
        self._failure: tuple[float, BrokerApiError] | None = None

    def get(self) -> str:
        token = self._token
        if token and self._clock() < self._refresh_at:
            return token
        with self._lock:
            now = self._clock()
            if self._token and now < self._refresh_at:
                return self._token
            cached = self._load()
            if cached and cached[0] != self._token and now < cached[1] - self._margin:
                self._token, self._refresh_at = cached[0], cached[1] - self._margin
                return self._token
            if self._failure and now < self._failure[0]:
                raise _cooldown_error(self._failure[1], self._failure[0] - now)
            try:
                token, expires_at = self._issue()
            except BrokerApiError as e:
                self._failure = (self._clock() + FAILURE_COOLDOWN_SECONDS, e)
                raise
            self._failure = None
            # 만료 전 재발급에 남은 시간 그대로의 같은 토큰을 돌려주는 서버(DB·KIS)는 여유 구간에서 매번 재발급하지 않도록 만료까지 쓴다
            refresh_at = expires_at - self._margin
            self._token, self._refresh_at = token, refresh_at if refresh_at > self._clock() else expires_at
            self._save(token, expires_at)
            return token

    def invalidate(self, token: str) -> None:
        """업무 호출이 거부한 토큰을 버린다 - 다른 프로세스가 새로 저장한 토큰은 지우지 않는다"""
        with self._lock:
            if self._token == token:
                self._token, self._refresh_at = None, 0.0
            cached = self._load()
            path = self._path()
            if cached and cached[0] == token and path is not None:
                try:
                    path.unlink()
                except OSError:
                    pass

    def call(self, fn: Callable[[str], T]) -> T:
        """토큰을 받아 fn 을 부르고, 업무 호출이 토큰을 거부하면 버린 뒤 한 번 다시 부른다"""
        token = self.get()
        try:
            return fn(token)
        except AuthError:
            self.invalidate(token)
            return fn(self.get())

    def _path(self) -> Path | None:
        if CACHE_DIR is None:
            return None
        return CACHE_DIR / f"{self._broker}-{hashlib.sha256(self._app_key.encode()).hexdigest()[:16]}.json"

    def _load(self) -> tuple[str, float] | None:
        path = self._path()
        try:
            if path is None or not path.exists():
                return None
            data = json.loads(path.read_text(encoding="utf-8"))
            token, expires_at = data.get("access_token"), float(data.get("expires_at", 0))
            return (token, expires_at) if token else None
        except Exception:  # noqa: BLE001
            return None

    def _save(self, token: str, expires_at: float) -> None:
        path = self._path()
        if path is None:
            return
        try:
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            tmp = path.with_suffix(f".{os.getpid()}.tmp")
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump({"access_token": token, "expires_at": expires_at}, f)
            os.replace(tmp, path)
        except Exception:  # noqa: BLE001
            pass  # 캐시 저장 실패는 이번 호출에 영향을 주지 않는다


def _cooldown_error(failure: BrokerApiError, remaining: float) -> BrokerApiError:
    """쿨다운 중 재요청 - 서버에 묻지 않고 같은 종류의 오류를 남은 시간과 함께 돌려준다"""
    if isinstance(failure, RateLimitError):
        return RateLimitError(failure.http_status, failure.error_code, failure.message, remaining)
    return failure
