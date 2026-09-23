"""실측 제보 도구 — 계좌가 있는 사람이 명령 하나로 어댑터를 실서버에 돌리고, 결과 파일 하나를 이슈에 첨부한다.

    HERMETIX_API_KEY=... HERMETIX_API_SECRET=... [HERMETIX_ACCOUNT=...] python -m hermetix.verify nh
    python -m hermetix.verify toss --live --read-only          # 실전 전용 증권사는 조회만 (주문 없음)
    python -m hermetix.verify kb --live --live-orders          # 실전 주문까지 (원거리 지정가 1주 → 즉시 취소)

흐름: capabilities → 시세 → 캔들 → 캘린더 → 계좌 → 보유 → 주문가능액 → 주문 목록 → 체결 → (허용 시) 주문 생성→조회→취소
→ (스트림 지원 시) 체결·호가·주문통보 프레임을 N초간 채집.

결과 `hermetix-verify-<broker>.json` 에는 어댑터가 실제로 주고받은 HTTP 요청·응답과 원시 웹소켓 프레임이 들어간다.
키·토큰·계좌번호·고객 정보는 파일에 쓰기 전에 가린다(`mask`). 메인테이너는 이 파일로 골든 픽스처
(`conformance/fixtures/<broker>.json`)를 실측값으로 교체하고 README 검증 상태를 올린다 — conformance/README.md "실측 제보 절차".
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import Any, Callable

from . import __version__, factory
from .broker import BrokerClient, StreamingBrokerClient
from .errors import BrokerApiError, MarketClosedError
from .models import CandleInterval, CreateOrderRequest, OrderSide, OrderStatus, OrderType, TradingEnvironment

ISSUE_URL = "https://github.com/tripleauth-dev/hermetix-securities/issues/new?template=broker-verification.md"

# 값 전체를 가리는 키 이름 (대소문자 무시, 부분 일치). 종목명·TR ID 같은 건 남긴다
SENSITIVE_KEY = re.compile(
    r"(app_?key|api_?key|secret|token|authorization|cano|acnt|acct|account|hts_?id|cust|holder|owner|phone|tel|email"
    r"|birth|ssn|jumin|mac_?addr|passw|\bpw\b|pin)",
    re.IGNORECASE,
)
MASK = "***"
MAX_BODY_CHARS = 64_000
MAX_HTTP_ENTRIES = 200
MAX_FRAMES = 200


def mask(value: Any, secrets: list[str]) -> Any:
    """키 이름이 민감하면 값 전체를, 그 외 문자열에서는 알려진 비밀값(키·시크릿·계좌번호)을 가린다."""
    if isinstance(value, dict):
        return {k: (MASK if SENSITIVE_KEY.search(str(k)) else mask(v, secrets)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [mask(v, secrets) for v in value]
    if isinstance(value, str):
        out = value
        for s in secrets:
            if s and s in out:
                out = out.replace(s, MASK)
        return out
    return value


class _RecordingHttp:
    """어댑터의 `_http` 앞에 끼워 요청·응답을 기록한다. 그 외 속성(last_headers 등)은 원본에 위임."""

    def __init__(self, inner: Any, secrets: list[str], entries: list[dict]):
        self._inner = inner
        self._secrets = secrets
        self._entries = entries

    def request(self, method: str, path: str, *, headers=None, query=None, json_body=None, form_body=None):
        started = time.monotonic()
        status, body = self._inner.request(method, path, headers=headers, query=query, json_body=json_body, form_body=form_body)
        if len(self._entries) < MAX_HTTP_ENTRIES:
            response = json.dumps(mask(body, self._secrets), ensure_ascii=False)
            self._entries.append({
                "seq": len(self._entries) + 1,
                "method": method,
                "path": mask(path, self._secrets),
                "query": mask(query or {}, self._secrets),
                "request_headers": mask(headers or {}, self._secrets),
                "request_body": mask(json_body if json_body is not None else (form_body or {}), self._secrets),
                "status": status,
                "response": json.loads(response) if len(response) <= MAX_BODY_CHARS else {"_truncated": True, "preview": response[:MAX_BODY_CHARS]},
                "elapsed_ms": int((time.monotonic() - started) * 1000),
            })
        return status, body

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


@dataclass
class StepResult:
    name: str
    status: str  # ok | fail | skip
    detail: str = ""


@dataclass
class VerifyOptions:
    symbol: str
    limit_price: Decimal | None = None
    orders: bool = True
    stream_seconds: int = 0
    candles: int = 30


@dataclass
class VerifyReport:
    broker: str
    environment: str
    steps: list[StepResult] = field(default_factory=list)
    http: list[dict] = field(default_factory=list)
    stream: dict | None = None
    capabilities: dict = field(default_factory=dict)

    @property
    def failed(self) -> list[StepResult]:
        return [s for s in self.steps if s.status == "fail"]

    def to_dict(self) -> dict:
        return {
            "format": 1,
            "tool": "hermetix.verify",
            "sdk": {"language": "python", "version": __version__, "runtime": platform.python_version(), "os": platform.system()},
            "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "broker": self.broker,
            "environment": self.environment,
            "capabilities": self.capabilities,
            "steps": [{"name": s.name, "status": s.status, "detail": s.detail} for s in self.steps],
            "http": self.http,
            "stream": self.stream,
        }


def krx_tick(price: Decimal) -> Decimal:
    """KRX 호가 단위 (2023-01 이후 기준)"""
    for limit, tick in ((2000, 1), (5000, 5), (20000, 10), (50000, 50), (200000, 100), (500000, 500)):
        if price < limit:
            return Decimal(tick)
    return Decimal(1000)


def far_limit_price(price: Decimal, currency: str) -> Decimal:
    """체결되지 않도록 현재가의 80% 에 둔 매수 지정가 — 호가 단위에 맞춘다"""
    target = price * Decimal("0.8")
    if currency == "KRW":
        tick = krx_tick(target)
        return (target / tick).to_integral_value(rounding=ROUND_DOWN) * tick
    return target.quantize(Decimal("0.01"), rounding=ROUND_DOWN)


def run_verification(client: BrokerClient, options: VerifyOptions, secrets: list[str],
                     sleep: Callable[[float], None] = time.sleep) -> VerifyReport:
    """실서버(또는 테스트의 가짜 HTTP)에 대해 검증 흐름을 돌리고 보고서를 만든다. 예외는 단계 실패로 기록하고 계속 간다."""
    caps = client.capabilities
    environment = getattr(client, "environment", TradingEnvironment.PAPER)
    report = VerifyReport(broker=caps.broker_id, environment=environment.value)
    report.capabilities = {
        "market": caps.market,
        "markets": sorted(caps.markets or {caps.market}),
        "currency": caps.currency,
        "candle_intervals": sorted(i.value for i in caps.candle_intervals),
        "environments": sorted(e.value for e in caps.environments),
        "streams": sorted(s.value for s in caps.streams),
        "client_order_id": caps.client_order_id,
        "server_open_orders": caps.server_open_orders,
    }
    # 어댑터의 HTTP 클라이언트를 기록기로 감싼다 (NH 는 인증 호스트가 따로 있다)
    for attr in ("_http", "_auth_http"):
        inner = getattr(client, attr, None)
        if inner is not None and not isinstance(inner, _RecordingHttp):
            setattr(client, attr, _RecordingHttp(inner, secrets, report.http))

    def step(name: str, fn: Callable[[], str]) -> bool:
        try:
            report.steps.append(StepResult(name, "ok", fn()))
            return True
        except MarketClosedError as e:
            report.steps.append(StepResult(name, "skip", f"장 마감: {e}"))
        except Exception as e:  # noqa: BLE001 — 어떤 예외든 단계 실패로 남기고 다음 단계로
            report.steps.append(StepResult(name, "fail", f"{type(e).__name__}: {e}"))
        return False

    step("capabilities", lambda: f"{caps.broker_id} {caps.market} {caps.currency} env={environment.value}")

    quote_price: list[Decimal] = []

    def quotes() -> str:
        qs = client.get_quotes([options.symbol])
        if len(qs) != 1 or qs[0].price <= 0:
            raise AssertionError(f"quotes: {len(qs)}건, price={qs[0].price if qs else None}")
        quote_price.append(qs[0].price)
        return f"price={qs[0].price} change_rate={qs[0].change_rate} volume={qs[0].volume}"

    def candles() -> str:
        interval = CandleInterval.DAY_1 if CandleInterval.DAY_1 in caps.candle_intervals else next(iter(caps.candle_intervals))
        cs = client.get_candles(options.symbol, interval, options.candles)
        if not cs:
            raise AssertionError("candles: 0건")
        if any(a.timestamp >= b.timestamp for a, b in zip(cs, cs[1:])):
            raise AssertionError("candles: 시각이 오름차순이 아니다")
        return f"{interval.value} {len(cs)}건 last_close={cs[-1].close}"

    def calendar() -> str:
        days = client.get_calendar()
        if not days:
            raise AssertionError("calendar: 0일")
        opens = sum(1 for d in days if d.open)
        return f"{len(days)}일 (개장 {opens}) tz={days[0].timezone}"

    def account() -> str:
        a = client.get_account()
        if a.cash < 0 or a.portfolio_value < 0:
            raise AssertionError(f"account: cash={a.cash} portfolio={a.portfolio_value}")
        return f"currency={a.currency} cash>0={a.cash > 0} portfolio>0={a.portfolio_value > 0}"

    def holdings() -> str:
        hs = client.get_holdings()
        bad = [h.symbol for h in hs if h.quantity <= 0]
        if bad:
            raise AssertionError(f"holdings: quantity<=0 {bad}")
        return f"{len(hs)}종목"

    def buying_power() -> str:
        p = client.get_buying_power()
        if p < 0:
            raise AssertionError(f"buying_power: {p}")
        return f">0={p > 0}"

    def orders() -> str:
        return f"{len(client.get_orders())}건"

    def fills() -> str:
        return f"{len(client.get_fills())}건"

    step("quotes", quotes)
    step("candles", candles)
    step("calendar", calendar)
    step("account", account)
    step("holdings", holdings)
    step("buying_power", buying_power)
    step("get_orders", orders)
    step("get_fills", fills)

    if not options.orders:
        report.steps.append(StepResult("create_order", "skip", "--read-only"))
    elif not quote_price:
        report.steps.append(StepResult("create_order", "skip", "시세를 못 받아 지정가를 정할 수 없다"))
    else:
        limit = options.limit_price or far_limit_price(quote_price[0], caps.currency)
        order_id: list[str] = []

        def create_order() -> str:
            o = client.create_order(CreateOrderRequest(
                symbol=options.symbol, side=OrderSide.BUY, order_type=OrderType.LIMIT, quantity=Decimal(1), limit_price=limit))
            if not o.order_id:
                raise AssertionError("create_order: order_id 비어 있음")
            if not o.status.is_open:
                raise AssertionError(f"create_order: 접수 직후 상태가 미체결이 아니다 ({o.status.value})")
            order_id.append(o.order_id)
            return f"BUY LIMIT 1 @ {limit} → {o.order_id} {o.status.value}"

        def get_order() -> str:
            o = client.get_order(order_id[0])
            if o.order_id != order_id[0] or o.status == OrderStatus.UNKNOWN:
                raise AssertionError(f"get_order: {o.order_id} {o.status.value}")
            return o.status.value

        def get_orders_contains() -> str:
            if not any(o.order_id == order_id[0] for o in client.get_orders()):
                raise AssertionError("get_orders: 방금 낸 주문이 목록에 없다")
            return "목록에 있음"

        def cancel_order() -> str:
            c = client.cancel_order(order_id[0])
            if c.status not in (OrderStatus.PENDING_CANCEL, OrderStatus.CANCELED):
                raise AssertionError(f"cancel_order: {c.status.value}")
            return c.status.value

        if step("create_order", create_order):
            step("get_order", get_order)
            step("get_orders_contains", get_orders_contains)
            step("cancel_order", cancel_order)
        elif order_id:
            step("cancel_order", cancel_order)

    if options.stream_seconds > 0 and caps.streams and isinstance(client, StreamingBrokerClient):
        report.stream = _capture_stream(client, options, secrets, sleep)
    elif options.stream_seconds > 0:
        report.steps.append(StepResult("stream", "skip", "이 어댑터는 실시간 채널을 선언하지 않는다"))
    return report


def _capture_stream(client: StreamingBrokerClient, options: VerifyOptions, secrets: list[str],
                    sleep: Callable[[float], None]) -> dict:
    frames: list[str] = []
    counts = {"trades": 0, "order_books": 0, "order_events": 0}
    result: dict[str, Any] = {"seconds": options.stream_seconds, "subscribed": [], "frames": frames, "parsed": counts}
    try:
        stream = client.open_stream()
    except ImportError as e:
        result["error"] = f"{e} — pip install 'hermetix[stream]'"
        return result
    hook_target = stream if hasattr(stream, "raw_frame_hook") else None
    if hook_target is not None:
        hook_target.raw_frame_hook = lambda text: frames.append(mask(text, secrets)) if len(frames) < MAX_FRAMES else None

    def count(key: str):
        def listener(_event):
            counts[key] += 1
        return listener

    try:
        stream.subscribe_trades([options.symbol], count("trades"))
        result["subscribed"].append("TRADES")
    except NotImplementedError:
        pass
    try:
        stream.subscribe_order_book([options.symbol], count("order_books"))
        result["subscribed"].append("ORDER_BOOK")
    except NotImplementedError:
        pass
    try:
        stream.subscribe_order_events(count("order_events"))
        result["subscribed"].append("ORDER_EVENTS")
    except NotImplementedError:
        pass
    try:
        stream.connect()
        sleep(options.stream_seconds)
        result["connected"] = bool(stream.is_connected())
    except Exception as e:  # noqa: BLE001
        result["error"] = f"{type(e).__name__}: {e}"
    finally:
        try:
            stream.close()
        except Exception:  # noqa: BLE001
            pass
    return result


def _parse_extra(pairs: list[str]) -> dict[str, str]:
    extra: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"--extra 는 key=value 형식: {pair!r}")
        key, value = pair.split("=", 1)
        extra[key.strip()] = value.strip()
    return extra


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m hermetix.verify", description="증권사 어댑터 실측 검증 — 결과 파일을 이슈에 첨부하세요")
    parser.add_argument("broker", choices=factory.brokers)
    parser.add_argument("--symbol", default=None, help="검증 종목 (기본: KRX 005930, 미국 AAPL)")
    parser.add_argument("--limit-price", type=Decimal, default=None, help="주문 단계 지정가 (기본: 현재가의 80%%, 호가 단위 맞춤)")
    parser.add_argument("--read-only", action="store_true", help="주문 단계를 건너뛴다 (조회 검증만)")
    parser.add_argument("--live", action="store_true", help="실전 환경으로 돌린다 — 실전 전용 증권사(toss·kb)는 필수")
    parser.add_argument("--live-orders", action="store_true", help="실전에서도 주문 단계를 돌린다 (원거리 지정가 1주 → 즉시 취소)")
    parser.add_argument("--stream", type=int, default=60, metavar="SECONDS", help="실시간 프레임 채집 시간 (0 = 생략, 기본 60)")
    parser.add_argument("--extra", action="append", default=[], metavar="KEY=VALUE", help="브로커별 추가 설정 (docs/broker-factory.md 의 extra 키)")
    parser.add_argument("--out", default=None, help="결과 파일 (기본 hermetix-verify-<broker>.json)")
    args = parser.parse_args(argv)

    api_key = os.environ.get("HERMETIX_API_KEY", "")
    api_secret = os.environ.get("HERMETIX_API_SECRET", "")
    account = os.environ.get("HERMETIX_ACCOUNT", "")
    if not api_key or not api_secret:
        print("HERMETIX_API_KEY / HERMETIX_API_SECRET 환경변수가 필요합니다 (계좌가 필요한 증권사는 HERMETIX_ACCOUNT 도)", file=sys.stderr)
        return 2

    environment = TradingEnvironment.LIVE if args.live else None
    try:
        client = factory.client(args.broker, api_key, api_secret, account, environment=environment, **_parse_extra(args.extra))
    except (ValueError, TypeError) as e:
        print(f"클라이언트 생성 실패: {e}", file=sys.stderr)
        return 2
    resolved = getattr(client, "environment", TradingEnvironment.PAPER)
    if resolved == TradingEnvironment.LIVE and not args.live:
        print(f"{args.broker} 는 실전 환경으로만 동작합니다. 실계좌로 돌리려면 --live 를 붙이세요 (--read-only 권장)", file=sys.stderr)
        return 2
    orders = not args.read_only and (resolved != TradingEnvironment.LIVE or args.live_orders)
    if resolved == TradingEnvironment.LIVE and not args.read_only and not args.live_orders:
        print("실전 환경: 주문 단계는 --live-orders 를 붙여야 돌립니다. 이번 실행은 조회만 검증합니다.")

    symbol = args.symbol or ("AAPL" if client.capabilities.currency == "USD" else "005930")
    options = VerifyOptions(symbol=symbol, limit_price=args.limit_price, orders=orders, stream_seconds=max(0, args.stream))
    secrets = [s for s in (api_key, api_secret, account) if s]

    print(f"== {args.broker} {resolved.value} symbol={symbol} orders={'on' if orders else 'off'} stream={options.stream_seconds}s ==")
    report = run_verification(client, options, secrets)
    for s in report.steps:
        print(f"  [{s.status:4}] {s.name}: {s.detail}")
    if report.stream is not None:
        print(f"  [stream] subscribed={report.stream.get('subscribed')} frames={len(report.stream['frames'])} parsed={report.stream['parsed']}"
              + (f" error={report.stream['error']}" if report.stream.get("error") else ""))

    out = args.out or f"hermetix-verify-{args.broker}.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(report.to_dict(), f, ensure_ascii=False, indent=2, default=str)
    failed = report.failed
    print(f"\n{'FAILED' if failed else 'OK'} — {len(report.steps) - len(failed)}/{len(report.steps)} 단계 통과, HTTP {len(report.http)}건 기록 → {out}")
    print(f"이 파일을 이슈에 첨부해 주세요: {ISSUE_URL}")
    print("첨부 전에 파일을 한 번 열어 남기고 싶지 않은 값이 없는지 확인하세요 (키·토큰·계좌번호는 자동으로 가려집니다).")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
