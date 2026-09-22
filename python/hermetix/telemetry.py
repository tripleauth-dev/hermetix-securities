"""사용량 텔레메트리 (Kotlin UsageTelemetry 와 동일 의미) - 어느 증권사가 얼마나 쓰이는지 시간 버킷으로 합산해
hermetix-service 로 보낸다. 계약(필드·전송 규칙·보내지 않는 것)은 docs/telemetry.md 가 정본이다.

- 매매 경로와 분리: 카운터는 메모리, 전송은 데몬 스레드. 실패는 조용히 버리고 큐를 쌓지 않는다
- 개인정보·매매 내용 없음: 브로커·환경·호출 종류·건수·에러 분류·응답 시간 분포·스트림 건수·SDK 버전·설치 ID 뿐
- 기본 배포본은 항상 켜져 있다 (설정 없음). 테스트는 transport 와 flush_now() 로 전송을 가로챈다
- 표준 라이브러리만 사용한다
"""
from __future__ import annotations

import atexit
import hashlib
import hmac
import json
import logging
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .errors import (
    AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, MarketClosedError, OrderNotFoundError,
    RateLimitError,
)
from .models import StreamChannel, TradingEnvironment

ENDPOINT = "https://service-api-prod.hermetix.dev/v1/usage"
SCHEMA = 1
SDK_LANGUAGE = "python"
# 요청 서명 키 (docs/telemetry.md "요청 서명") — 공개 SDK 라 비밀이 아니며 스팸·스캐너를 거르는 문턱이다
SIGNING_KEY_ID = "v1"
SIGNING_KEY = "d97f20cb942540462ea83648ee30a9786bd658b3dc813f74ef011845da258503"
FLUSH_INTERVAL_SECONDS = 60.0
LATENCY_SAMPLES = 256

logger = logging.getLogger("hermetix")


def sdk_version() -> str:
    try:
        from . import __version__
        return __version__
    except Exception:  # noqa: BLE001
        return "unknown"


def classify(failure: BaseException) -> str:
    """예외를 계약의 에러 분류로 (docs/telemetry.md)"""
    if isinstance(failure, RateLimitError):
        return "rate_limit"
    if isinstance(failure, AuthError):
        return "auth"
    if isinstance(failure, MarketClosedError):
        return "market_closed"
    if isinstance(failure, InsufficientFundsError):
        return "insufficient_funds"
    if isinstance(failure, InvalidOrderError):
        return "invalid_order"
    if isinstance(failure, OrderNotFoundError):
        return "order_not_found"
    if isinstance(failure, BrokerApiError):
        return "other"
    if isinstance(failure, (OSError, TimeoutError, ConnectionError)):  # urllib.error.URLError, socket.timeout 포함
        return "network"
    cause = failure.__cause__
    if cause is not None and cause is not failure:
        return classify(cause)
    return "other"


class _Latency:
    """op 당 최근 LATENCY_SAMPLES 개 표본만 보관 - p50/p95 는 전송 시점에 계산"""

    def __init__(self):
        self._samples = [0] * LATENCY_SAMPLES
        self.count = 0

    def add(self, ms: int) -> None:
        self._samples[self.count % LATENCY_SAMPLES] = ms
        self.count += 1

    def summary(self) -> dict:
        n = min(self.count, LATENCY_SAMPLES)
        if n == 0:
            return {"count": 0, "p50": 0, "p95": 0}
        s = sorted(self._samples[:n])

        def pct(p: float) -> int:
            return s[min(max(int((n - 1) * p), 0), n - 1)]

        return {"count": self.count, "p50": pct(0.50), "p95": pct(0.95)}


class _OpStats:
    def __init__(self):
        self.ok = 0
        self.errors: dict[str, int] = {}
        self.latency = _Latency()


class _Bucket:
    def __init__(self):
        self.ops: dict[str, _OpStats] = {}
        self.streams: dict[str, dict[str, int]] = {}
        self.reconnects = 0

    def to_json(self, hour: datetime, broker: str, environment: str) -> dict:
        return {
            "hour": _iso(hour),
            "broker": broker,
            "environment": environment,
            "ops": [
                {"op": op, "ok": s.ok, "errors": dict(s.errors), "latencyMs": s.latency.summary()}
                for op, s in sorted(self.ops.items())
            ],
            "streams": [
                {"channel": ch, "subscriptions": s["subscriptions"], "messages": s["messages"]}
                for ch, s in sorted(self.streams.items())
            ],
            "reconnects": self.reconnects,
        }


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sign(body: str, timestamp_seconds: int) -> str:
    """`hex(HMAC-SHA256(key, timestamp + "\\n" + body))` — 계약의 요청 서명 (소문자 hex 64자)."""
    message = f"{timestamp_seconds}\n{body}".encode("utf-8")
    return hmac.new(SIGNING_KEY.encode("utf-8"), message, hashlib.sha256).hexdigest()


def _post(body: str) -> None:
    timestamp = int(time.time())
    request = urllib.request.Request(
        ENDPOINT, data=body.encode("utf-8"), method="POST",
        headers={
            "Content-Type": "application/json",
            "User-Agent": f"hermetix-{SDK_LANGUAGE}/{sdk_version()}",
            "X-Hermetix-Key-Id": SIGNING_KEY_ID,
            "X-Hermetix-Timestamp": str(timestamp),
            "X-Hermetix-Signature": sign(body, timestamp),
        })
    with urllib.request.urlopen(request, timeout=3):
        pass  # 응답 본문은 읽지 않는다


class UsageTelemetry:
    """모듈 단위 싱글턴 - `telemetry` 로 쓴다"""

    def __init__(self):
        self._lock = threading.Lock()
        self._buckets: dict[tuple[datetime, str, str], _Bucket] = {}
        self._installation_id: str | None = None
        # 전송 함수 - 기본은 HTTP POST. 테스트에서 교체한다
        self.transport: Callable[[str], None] = _post
        self._started = False

    # ------------------------------------------------------------------ 수집

    def for_broker(self, broker_id: str, environment: TradingEnvironment) -> "BrokerUsage":
        return BrokerUsage(broker_id, environment)

    def record(self, broker: str, environment: TradingEnvironment, op: str, seconds: float,
               failure: BaseException | None) -> None:
        """어댑터 계측용 - 보통 BrokerUsage.measure 를 통해 호출된다"""
        with self._lock:
            self._ensure_started()
            stats = self._bucket(broker, environment).ops.setdefault(op, _OpStats())
            if failure is None:
                stats.ok += 1
            else:
                cls = classify(failure)
                stats.errors[cls] = stats.errors.get(cls, 0) + 1
            stats.latency.add(int(seconds * 1000))

    def record_stream(self, broker: str, environment: TradingEnvironment, channel: StreamChannel,
                      subscriptions: int = 0, messages: int = 0) -> None:
        with self._lock:
            self._ensure_started()
            s = self._bucket(broker, environment).streams.setdefault(channel.name, {"subscriptions": 0, "messages": 0})
            s["subscriptions"] += subscriptions
            s["messages"] += messages

    def record_reconnect(self, broker: str, environment: TradingEnvironment) -> None:
        with self._lock:
            self._ensure_started()
            self._bucket(broker, environment).reconnects += 1

    # ------------------------------------------------------------------ 전송

    @property
    def installation_id(self) -> str:
        if self._installation_id is None:
            self._installation_id = _load_or_create_installation_id()
        return self._installation_id

    def drain(self, now: datetime | None = None) -> str | None:
        """지금까지 쌓인 버킷을 페이로드 JSON 으로 만들고 비운다. 비어 있으면 None"""
        with self._lock:
            if not self._buckets:
                return None
            buckets, self._buckets = self._buckets, {}
        payload = {
            "schema": SCHEMA,
            "installationId": self.installation_id,
            "sdk": {"language": SDK_LANGUAGE, "version": sdk_version()},
            "sentAt": _iso(now or datetime.now(timezone.utc)),
            "buckets": [b.to_json(hour, broker, env) for (hour, broker, env), b in buckets.items()],
        }
        return json.dumps(payload, ensure_ascii=False)

    def flush_now(self) -> None:
        """즉시 전송 시도 (스케줄러·종료 훅·테스트용). 비어 있으면 아무것도 안 한다. 실패한 페이로드는 재전송하지 않는다"""
        body = self.drain()
        if body is None:
            return
        try:
            self.transport(body)
        except Exception as e:  # noqa: BLE001
            logger.debug("telemetry send skipped: %s", e)

    # ------------------------------------------------------------------ 내부

    def _bucket(self, broker: str, environment: TradingEnvironment) -> _Bucket:
        hour = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        key = (hour, broker, environment.name)
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = self._buckets[key] = _Bucket()
        return bucket

    def _ensure_started(self) -> None:
        """첫 기록 때 데몬 플러시 스레드와 종료 훅을 건다 (import 만으로는 스레드를 만들지 않는다)"""
        if self._started:
            return
        self._started = True
        threading.Thread(target=self._flush_loop, name="hermetix-telemetry", daemon=True).start()
        atexit.register(self.flush_now)

    def _flush_loop(self) -> None:
        while True:
            time.sleep(FLUSH_INTERVAL_SECONDS)
            try:
                self.flush_now()
            except Exception:  # noqa: BLE001
                pass


def _load_or_create_installation_id() -> str:
    try:
        path = Path.home() / ".hermetix" / "installation-id"
        if path.exists():
            text = path.read_text().strip()
            uuid.UUID(text)
            return text
    except Exception:  # noqa: BLE001
        path = None
    new_id = str(uuid.uuid4())
    try:
        if path is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(new_id)
    except Exception:  # noqa: BLE001
        pass
    return new_id


telemetry = UsageTelemetry()


class BrokerUsage:
    """브로커 어댑터 하나가 쥐는 계측 핸들. measure(op) 는 컨텍스트 매니저 - finally 에서 기록하므로 어떤 경로로 나가도 세어진다"""

    def __init__(self, broker_id: str, environment: TradingEnvironment):
        self.broker_id = broker_id
        self.environment = environment

    def measure(self, op: str) -> "_Measure":
        return _Measure(self, op)

    def stream_subscribed(self, channel: StreamChannel, count: int = 1) -> None:
        telemetry.record_stream(self.broker_id, self.environment, channel, subscriptions=count)

    def stream_message(self, channel: StreamChannel, count: int = 1) -> None:
        telemetry.record_stream(self.broker_id, self.environment, channel, messages=count)

    def reconnected(self) -> None:
        telemetry.record_reconnect(self.broker_id, self.environment)


class _Measure:
    def __init__(self, usage: BrokerUsage, op: str):
        self._usage = usage
        self._op = op
        self._start = 0.0

    def __enter__(self):
        self._start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        telemetry.record(self._usage.broker_id, self._usage.environment, self._op,
                         time.perf_counter() - self._start, exc)
        return False  # 예외는 그대로 전파
