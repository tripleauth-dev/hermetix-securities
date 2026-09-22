/**
 * 사용량 텔레메트리 — 어느 증권사가 얼마나 쓰이는지 시간 버킷으로 합산해 `hermetix-service` 로 보낸다.
 * 계약(필드·전송 규칙·보내지 않는 것)은 `docs/telemetry.md` 가 정본이다 (Kotlin UsageTelemetry 와 동일 의미).
 *
 * - 매매 경로와 분리: 카운터는 메모리, 전송은 unref 타이머. 실패는 조용히 버리고 큐를 쌓지 않는다
 * - 개인정보·매매 내용 없음: 브로커·환경·호출 종류·건수·에러 분류·응답 시간 분포·스트림 건수·SDK 버전·설치 ID 뿐
 * - 기본 배포본은 항상 켜져 있다 (설정 없음). 테스트는 `transport` 와 `flushNow()` 로 전송을 가로챈다
 */
import { createHmac, randomUUID } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { createRequire } from "node:module";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import type { BrokerClient } from "./broker.js";
import {
  AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, MarketClosedError, OrderNotFoundError, RateLimitError,
} from "./errors.js";
import type { StreamChannel, TradingEnvironment } from "./models.js";

export const TELEMETRY_ENDPOINT = "https://service-api-prod.hermetix.dev/v1/usage";
export const TELEMETRY_SCHEMA = 1;
export const SDK_LANGUAGE = "js";
/** 요청 서명 키 (docs/telemetry.md "요청 서명") — 공개 SDK 라 비밀이 아니며 스팸·스캐너를 거르는 문턱이다 */
export const SIGNING_KEY_ID = "v1";
export const SIGNING_KEY = "d97f20cb942540462ea83648ee30a9786bd658b3dc813f74ef011845da258503";
/** 전송 대상 — 테스트에서만 `__setEndpointForTests` 로 바꾼다 (사용자 설정 없음) */
let endpoint: string = TELEMETRY_ENDPOINT;
export function __setEndpointForTests(url: string | null): void { endpoint = url ?? TELEMETRY_ENDPOINT; }

/** `hex(HMAC-SHA256(key, timestamp + "\n" + body))` — 계약의 요청 서명 */
export function signTelemetry(body: string, timestampSeconds: number): string {
  return createHmac("sha256", SIGNING_KEY).update(`${timestampSeconds}\n${body}`, "utf8").digest("hex");
}
const FLUSH_INTERVAL_MS = 60_000;
const LATENCY_SAMPLES = 256;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export type ErrorClass =
  | "rate_limit" | "auth" | "market_closed" | "insufficient_funds" | "invalid_order" | "order_not_found" | "network" | "other";

function readSdkVersion(): string {
  try {
    const require = createRequire(import.meta.url);
    // dist/src/telemetry.js → ../../package.json, src/telemetry.ts → ../package.json
    for (const candidate of ["../../package.json", "../package.json"]) {
      try {
        const pkg = require(candidate) as { name?: string; version?: string };
        if (pkg.name === "hermetix" && pkg.version) return pkg.version;
      } catch { /* 다음 후보 */ }
    }
  } catch { /* 무시 */ }
  return "unknown";
}

function loadOrCreateInstallationId(): string {
  try {
    const path = join(homedir(), ".hermetix", "installation-id");
    if (existsSync(path)) {
      const id = readFileSync(path, "utf-8").trim();
      if (UUID_RE.test(id)) return id;
    }
    const id = randomUUID();
    try { mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, id); } catch { /* 못 쓰면 임시 ID */ }
    return id;
  } catch {
    return randomUUID();
  }
}

/** op 당 최근 256개 표본만 보관 — p50/p95 는 전송 시점에 계산 */
class LatencyReservoir {
  private readonly samples = new Array<number>(LATENCY_SAMPLES);
  private count = 0;
  add(ms: number): void { this.samples[this.count % LATENCY_SAMPLES] = ms; this.count++; }
  summary(): { count: number; p50: number; p95: number } {
    const n = Math.min(this.count, LATENCY_SAMPLES);
    if (n === 0) return { count: 0, p50: 0, p95: 0 };
    const sorted = this.samples.slice(0, n).sort((a, b) => a - b);
    const pct = (p: number) => sorted[Math.min(n - 1, Math.max(0, Math.floor((n - 1) * p)))];
    return { count: this.count, p50: pct(0.5), p95: pct(0.95) };
  }
}

class OpStats { ok = 0; readonly errors = new Map<ErrorClass, number>(); readonly latency = new LatencyReservoir(); }
class StreamStats { subscriptions = 0; messages = 0; }
class BucketStats {
  readonly ops = new Map<string, OpStats>();
  readonly streams = new Map<string, StreamStats>();
  reconnects = 0;
  toJson(hour: string, broker: string, environment: string) {
    return {
      hour, broker, environment,
      ops: [...this.ops.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([op, s]) => ({
        op, ok: s.ok, errors: Object.fromEntries(s.errors), latencyMs: s.latency.summary(),
      })),
      streams: [...this.streams.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([channel, s]) => ({
        channel, subscriptions: s.subscriptions, messages: s.messages,
      })),
      reconnects: this.reconnects,
    };
  }
}

/** 예외를 계약의 에러 분류로 (docs/telemetry.md) */
export function classifyError(err: unknown): ErrorClass {
  if (err instanceof RateLimitError) return "rate_limit";
  if (err instanceof AuthError) return "auth";
  if (err instanceof MarketClosedError) return "market_closed";
  if (err instanceof InsufficientFundsError) return "insufficient_funds";
  if (err instanceof InvalidOrderError) return "invalid_order";
  if (err instanceof OrderNotFoundError) return "order_not_found";
  if (err instanceof BrokerApiError) return "other";
  if (err && typeof err === "object") {
    const e = err as { name?: string; code?: string; cause?: unknown; message?: string };
    if (e.name === "AbortError" || e.name === "TimeoutError") return "network";
    if (typeof e.code === "string" && /^(ECONN|ENOTFOUND|ETIMEDOUT|EAI_AGAIN|EPIPE|UND_ERR)/.test(e.code)) return "network";
    if (err instanceof TypeError && /fetch failed|network|socket|ECONN/i.test(e.message ?? "")) return "network";
    if (e.cause !== undefined && e.cause !== err) return classifyError(e.cause);
  }
  return "other";
}

const buckets = new Map<string, { hour: string; broker: string; environment: string; stats: BucketStats }>();
let sdkVersionCache: string | null = null;
let installationIdCache: string | null = null;
let timer: NodeJS.Timeout | null = null;

function bucketFor(broker: string, environment: TradingEnvironment): BucketStats {
  const hour = new Date(Math.floor(Date.now() / 3_600_000) * 3_600_000).toISOString().replace(".000Z", "Z");
  const key = `${hour}|${broker}|${environment}`;
  let b = buckets.get(key);
  if (!b) { b = { hour, broker, environment, stats: new BucketStats() }; buckets.set(key, b); }
  return b.stats;
}

async function postDefault(body: string): Promise<void> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), 3_000);
  t.unref?.();
  try {
    const timestamp = Math.floor(Date.now() / 1000);
    await fetch(endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "User-Agent": `hermetix-${SDK_LANGUAGE}/${UsageTelemetry.sdkVersion}`,
        "X-Hermetix-Key-Id": SIGNING_KEY_ID,
        "X-Hermetix-Timestamp": String(timestamp),
        "X-Hermetix-Signature": signTelemetry(body, timestamp),
      },
      body,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(t);
  }
}

export const UsageTelemetry = {
  get sdkVersion(): string { return (sdkVersionCache ??= readSdkVersion()); },
  get installationId(): string { return (installationIdCache ??= loadOrCreateInstallationId()); },

  /** 전송 함수 — 기본은 fetch POST(3초 타임아웃, 응답 버림). 테스트에서 교체한다 */
  transport: postDefault as (body: string) => void | Promise<void>,

  /** 어댑터가 자기 브로커·환경으로 만들어 두고 쓰는 핸들 */
  forBroker(brokerId: string, environment: TradingEnvironment): BrokerUsage { return new BrokerUsage(brokerId, environment); },

  record(broker: string, environment: TradingEnvironment, op: string, ms: number, failure: unknown | null): void {
    const stats = bucketFor(broker, environment);
    let s = stats.ops.get(op);
    if (!s) { s = new OpStats(); stats.ops.set(op, s); }
    if (failure === null || failure === undefined) s.ok++;
    else { const c = classifyError(failure); s.errors.set(c, (s.errors.get(c) ?? 0) + 1); }
    s.latency.add(Math.max(0, Math.round(ms)));
  },

  recordStream(broker: string, environment: TradingEnvironment, channel: StreamChannel, subscriptions: number, messages: number): void {
    const stats = bucketFor(broker, environment);
    let s = stats.streams.get(channel);
    if (!s) { s = new StreamStats(); stats.streams.set(channel, s); }
    s.subscriptions += subscriptions;
    s.messages += messages;
  },

  recordReconnect(broker: string, environment: TradingEnvironment): void { bucketFor(broker, environment).reconnects++; },

  /** 지금까지 쌓인 버킷을 페이로드 JSON 으로 만들고 비운다. 비어 있으면 null */
  drain(now: Date = new Date()): string | null {
    if (buckets.size === 0) return null;
    const list = [...buckets.values()];
    buckets.clear();
    return JSON.stringify({
      schema: TELEMETRY_SCHEMA,
      installationId: this.installationId,
      sdk: { language: SDK_LANGUAGE, version: this.sdkVersion },
      sentAt: now.toISOString().replace(/\.\d{3}Z$/, "Z"),
      buckets: list.map((b) => b.stats.toJson(b.hour, b.broker, b.environment)),
    });
  },

  /** 즉시 전송 시도 (타이머·종료·테스트용). 비어 있으면 아무것도 안 하고, 전송 실패는 삼킨다 — 절대 reject 하지 않는다 */
  async flushNow(): Promise<void> {
    const body = this.drain();
    if (body === null) return;
    try { await this.transport(body); } catch { /* 실패한 페이로드는 다시 보내지 않는다 */ }
  },
};

function startScheduler(): void {
  if (timer) return;
  timer = setInterval(() => { void UsageTelemetry.flushNow(); }, FLUSH_INTERVAL_MS);
  timer.unref?.();
  process.once("beforeExit", () => { void UsageTelemetry.flushNow(); });
}

/**
 * 브로커 어댑터 하나가 쥐는 계측 핸들. 공개 메서드를 [measure] 로 감싸면 건수·에러 분류·응답 시간이 쌓인다.
 * 동기·비동기 함수 모두 지원하며 기록은 finally 에서 하므로 어떤 경로로 나가도 세어진다.
 */
export class BrokerUsage {
  constructor(readonly brokerId: string, readonly environment: TradingEnvironment) { startScheduler(); }

  measure<T>(op: string, fn: () => T): T {
    const start = performance.now();
    let result: T;
    try {
      result = fn();
    } catch (e) {
      UsageTelemetry.record(this.brokerId, this.environment, op, performance.now() - start, e);
      throw e;
    }
    if (result instanceof Promise) {
      return result.then(
        (v) => { UsageTelemetry.record(this.brokerId, this.environment, op, performance.now() - start, null); return v; },
        (e) => { UsageTelemetry.record(this.brokerId, this.environment, op, performance.now() - start, e); throw e; },
      ) as unknown as T;
    }
    UsageTelemetry.record(this.brokerId, this.environment, op, performance.now() - start, null);
    return result;
  }

  streamSubscribed(channel: StreamChannel, count = 1): void { if (count !== 0) UsageTelemetry.recordStream(this.brokerId, this.environment, channel, count, 0); }
  streamMessage(channel: StreamChannel, count = 1): void { UsageTelemetry.recordStream(this.brokerId, this.environment, channel, 0, count); }
  reconnected(): void { UsageTelemetry.recordReconnect(this.brokerId, this.environment); }
}

const PUBLIC_OPS: [keyof BrokerClient, string][] = [
  ["getQuotes", "quotes"], ["getCandles", "candles"], ["getCalendar", "calendar"], ["getAccount", "account"],
  ["getHoldings", "holdings"], ["getBuyingPower", "buying_power"], ["createOrder", "create_order"],
  ["getOrders", "get_orders"], ["getOrder", "get_order"], ["cancelOrder", "cancel_order"], ["getFills", "fills"],
];
const INSTRUMENTED = new WeakSet<object>();

/**
 * 어댑터 인스턴스의 공개 메서드 11개를 계측으로 감싼다 (정확히 한 번). 어댑터 생성자 끝에서 호출해
 * 사용자가 클라이언트를 직접 만들어도 항상 계측된다. 돌려주는 핸들은 토큰 발급(`auth`)·스트림 카운트에 쓴다.
 */
export function instrumentBroker(client: BrokerClient): BrokerUsage {
  const usage = UsageTelemetry.forBroker(client.capabilities.brokerId, client.environment ?? "PAPER");
  if (INSTRUMENTED.has(client)) return usage;
  INSTRUMENTED.add(client);
  const target = client as unknown as Record<string, (...args: unknown[]) => unknown>;
  for (const [method, op] of PUBLIC_OPS) {
    const original = target[method];
    if (typeof original !== "function") continue;
    target[method] = function (this: unknown, ...args: unknown[]) { return usage.measure(op, () => original.apply(this, args)); };
  }
  return usage;
}
