/** 사용량 텔레메트리 — 계약(docs/telemetry.md)대로 합산·분류·직렬화되고, 전송 실패가 호출자에게 새지 않는지 (Kotlin UsageTelemetryTest 대응). */
import assert from "node:assert/strict";
import { createHmac } from "node:crypto";
import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { test } from "node:test";
import { KisMarketStream } from "../src/brokers/kisStream.js";
import {
  AuthError, BrokerApiError, DbClient, Decimal, InsufficientFundsError, InvalidOrderError, KbClient, KisClient, KiwoomClient, LsClient,
  MarketClosedError, NextClient, NhClient, OrderNotFoundError, RateLimitError, TossClient, verifyBrokerConformance,
} from "../src/index.js";
import type { BrokerClient, TradeTick } from "../src/index.js";
import { BrokerUsage, SIGNING_KEY, SIGNING_KEY_ID, UsageTelemetry, __postForTests, __setEndpointForTests, classifyError, instrumentBroker, signTelemetry } from "../src/telemetry.js";
import { Queue, withServer } from "./wsHarness.js";

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const FRAME_FIELDS = "005930^093012^71500^5^300^-0.42^71480^71800^71900^71300^71500^71400^15^1234567^88000000000^1200^1300^100^105.2^600000";

type Payload = {
  schema: number; installationId: string; sdk: { language: string; version: string }; sentAt: string;
  buckets: { hour: string; broker: string; environment: string; reconnects: number;
    ops: { op: string; ok: number; errors: Record<string, number>; latencyMs: { count: number; p50: number; p95: number } }[];
    streams: { channel: string; subscriptions: number; messages: number }[] }[];
};
const drain = (): Payload | null => { const s = UsageTelemetry.drain(); return s === null ? null : (JSON.parse(s) as Payload); };
const ops = (p: Payload, i = 0) => Object.fromEntries(p.buckets[i].ops.map((o) => [o.op, o]));

test("성공·에러 건수와 분류, 응답 시간 분포가 시간 버킷으로 합산된다", async () => {
  UsageTelemetry.drain();
  const u = new BrokerUsage("kis", "PAPER");
  for (let i = 0; i < 3; i++) u.measure("quotes", () => "ok");
  assert.throws(() => u.measure("quotes", () => { throw new RateLimitError(429, "EGW00201", "too many"); }), RateLimitError);
  await assert.rejects(u.measure("create_order", async () => { throw new InsufficientFundsError(400, "X", "부족"); }), InsufficientFundsError);
  await assert.rejects(u.measure("candles", async () => { throw Object.assign(new TypeError("fetch failed"), { code: "ECONNRESET" }); }));

  const p = drain()!;
  assert.equal(p.schema, 1);
  assert.match(p.installationId, UUID_RE);
  assert.equal(p.sdk.language, "js");
  assert.match(p.sentAt, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/);
  assert.equal(p.buckets.length, 1);
  const b = p.buckets[0];
  assert.equal(b.broker, "kis");
  assert.equal(b.environment, "PAPER");
  assert.match(b.hour, /:00:00Z$/);
  const o = ops(p);
  assert.equal(o.quotes.ok, 3);
  assert.equal(o.quotes.errors.rate_limit, 1);
  assert.equal(o.quotes.latencyMs.count, 4);
  assert.equal(o.create_order.errors.insufficient_funds, 1);
  assert.equal(o.candles.errors.network, 1);
  assert.equal(b.reconnects, 0);
  assert.equal(UsageTelemetry.drain(), null); // 비워졌다
});

test("measure 는 동기·비동기 반환값과 예외를 그대로 통과시키고 finally 에서 기록한다", async () => {
  UsageTelemetry.drain();
  const u = new BrokerUsage("next", "PAPER");
  assert.equal(u.measure("account", () => "sync"), "sync");
  assert.equal(await u.measure("account", async () => "async"), "async");
  const early = (flag: boolean) => u.measure("holdings", () => { if (flag) return "early"; return "late"; });
  assert.equal(early(true), "early");
  await assert.rejects(u.measure("holdings", async () => { throw new AuthError(401, "EGW00123", "expired"); }), AuthError);
  const o = ops(drain()!);
  assert.equal(o.account.ok, 2);
  assert.equal(o.holdings.ok, 1);
  assert.equal(o.holdings.errors.auth, 1);
});

test("스트림 구독·메시지·재접속이 브로커·환경별로 따로 쌓인다", () => {
  UsageTelemetry.drain();
  const paper = new BrokerUsage("kiwoom", "PAPER");
  const live = new BrokerUsage("kiwoom", "LIVE");
  paper.streamSubscribed("TRADES", 2);
  for (let i = 0; i < 5; i++) paper.streamMessage("TRADES");
  paper.streamSubscribed("ORDER_EVENTS");
  paper.reconnected();
  live.streamMessage("ORDER_BOOK", 3);

  const p = drain()!;
  const byEnv = Object.fromEntries(p.buckets.map((b) => [b.environment, b]));
  const streams = Object.fromEntries(byEnv.PAPER.streams.map((s) => [s.channel, s]));
  assert.equal(streams.TRADES.subscriptions, 2);
  assert.equal(streams.TRADES.messages, 5);
  assert.equal(streams.ORDER_EVENTS.subscriptions, 1);
  assert.equal(byEnv.PAPER.reconnects, 1);
  assert.equal(byEnv.LIVE.streams.length, 1);
  assert.equal(byEnv.LIVE.streams[0].messages, 3);
});

test("p50·p95 는 최근 256개 표본으로 계산한다", () => {
  UsageTelemetry.drain();
  for (let i = 1; i <= 300; i++) UsageTelemetry.record("next", "PAPER", "quotes", i, null);
  const lat = ops(drain()!).quotes.latencyMs;
  assert.equal(lat.count, 300);
  assert.ok(lat.p50 >= 165 && lat.p50 <= 180, `p50=${lat.p50}`); // 최근 256개 = 45..300
  assert.ok(lat.p95 >= 280 && lat.p95 <= 295, `p95=${lat.p95}`);
});

test("flushNow 는 비어 있으면 보내지 않고, 전송 예외는 삼키며, 실패한 페이로드는 재전송하지 않는다", async () => {
  UsageTelemetry.drain();
  const sent: string[] = [];
  const original = UsageTelemetry.transport;
  try {
    UsageTelemetry.transport = (b) => { sent.push(b); };
    await UsageTelemetry.flushNow();
    assert.equal(sent.length, 0);

    new BrokerUsage("kis", "PAPER").measure("quotes", () => 1);
    UsageTelemetry.transport = async () => { throw new Error("서버 없음"); };
    await UsageTelemetry.flushNow(); // reject 하지 않는다
    assert.equal(UsageTelemetry.drain(), null);

    new BrokerUsage("kis", "PAPER").measure("quotes", () => 1);
    UsageTelemetry.transport = (b) => { sent.push(b); };
    await UsageTelemetry.flushNow();
    assert.equal(sent.length, 1);
    assert.equal((JSON.parse(sent[0]) as Payload).buckets[0].ops[0].op, "quotes");
  } finally {
    UsageTelemetry.transport = original;
  }
});

test("예외 분류 표", () => {
  assert.equal(classifyError(new RateLimitError(429, null, "x")), "rate_limit");
  assert.equal(classifyError(new MarketClosedError(200, null, "x")), "market_closed");
  assert.equal(classifyError(new InvalidOrderError(400, null, "x")), "invalid_order");
  assert.equal(classifyError(new OrderNotFoundError(null, "x")), "order_not_found");
  assert.equal(classifyError(new BrokerApiError(500, null, "x")), "other");
  assert.equal(classifyError(Object.assign(new Error("abort"), { name: "AbortError" })), "network");
  assert.equal(classifyError(new Error("wrap", { cause: Object.assign(new Error("refused"), { code: "ECONNREFUSED" }) })), "network");
  assert.equal(classifyError(new Error("boom")), "other");
  assert.equal(classifyError("string"), "other");
});

test("설치 ID 는 UUID 이고 프로세스 안에서 고정이며, SDK 버전은 package.json 에서 온다", () => {
  assert.match(UsageTelemetry.installationId, UUID_RE);
  assert.equal(UsageTelemetry.installationId, UsageTelemetry.installationId);
  const pkg = JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf-8")) as { version: string };
  assert.equal(UsageTelemetry.sdkVersion, pkg.version);
});

// ---------------------------------------------------------------- 자동 계측: 8개 어댑터가 컨포먼스를 도는 동안 op 가 쌓인다

interface Route { method?: string; path?: string; header?: [string, string]; status?: number; body: unknown; }
const fixturesDir = new URL("../../../conformance/fixtures/", import.meta.url);
function fakeFetch(routes: Route[]) {
  return async (url: string, init: { method: string; headers?: Record<string, string> }) => {
    const path = new URL(url).pathname;
    const headers = init.headers ?? {};
    const route = routes.find((r) =>
      (r.method === undefined || r.method === init.method) && (r.path === undefined || r.path === path) &&
      (r.header === undefined || headers[r.header[0]] === r.header[1]));
    const body = route ? route.body : { error: `no fixture route for ${init.method} ${path}` };
    return { status: route ? (route.status ?? 200) : 599, text: async () => JSON.stringify(body), headers: { forEach: () => {} } };
  };
}
const REST_OPS = ["quotes", "candles", "calendar", "account", "holdings", "buying_power", "create_order", "get_orders", "get_order", "cancel_order", "fills"];
const clients: Record<string, () => BrokerClient> = {
  next: () => new NextClient("pk_test_conf", "sk_test_conf", "acc_main", "http://next.test"),
  kis: () => new KisClient("k", "s", "50199202", "01", "http://kis.test", 1),
  kiwoom: () => new KiwoomClient("k", "s", "http://kiwoom.test", 1),
  nh: () => new NhClient("k", "s", "", "http://nh.test", "http://nh.test", "KRX", "KRX", 1),
  db: () => new DbClient("k", "s", "http://db.test", "", "J", 1),
  ls: () => new LsClient("k", "s", "http://ls.test", "", "", 1, 1),
  toss: () => new TossClient("c_conf", "s_conf", "", "http://toss.test", 1),
  kb: () => new KbClient("k", "s", "http://kb.test", "1", "K", "0", 1),
};

test("자동 계측: 8개 어댑터의 공개 메서드·토큰 발급이 브로커별 버킷에 쌓인다", async () => {
  UsageTelemetry.drain();
  const restore = (globalThis as any).fetch;
  try {
    for (const [broker, make] of Object.entries(clients)) {
      const fx = JSON.parse(readFileSync(new URL(`${broker}.json`, fixturesDir), "utf-8")) as {
        scenario: { symbol: string; quantity: string; limitPrice: string }; routes: Route[];
      };
      (globalThis as any).fetch = fakeFetch(fx.routes);
      const report = await verifyBrokerConformance(make(), {
        symbol: fx.scenario.symbol, quantity: new Decimal(fx.scenario.quantity), limitPrice: new Decimal(fx.scenario.limitPrice),
      });
      assert.deepEqual(report.violations, [], `${broker}: ${report.violations.join("\n")}`);
    }
  } finally {
    (globalThis as any).fetch = restore;
  }
  const p = drain()!;
  const byBroker = Object.fromEntries(p.buckets.map((b) => [b.broker, b]));
  for (const broker of Object.keys(clients)) {
    const b = byBroker[broker];
    assert.ok(b, `${broker} 버킷 없음`);
    assert.equal(b.environment, broker === "toss" || broker === "kb" ? "LIVE" : "PAPER");
    const o = Object.fromEntries(b.ops.map((x) => [x.op, x]));
    for (const op of REST_OPS) assert.ok(o[op] && o[op].ok >= 1, `${broker}: op ${op} 미기록`);
    assert.ok(o.auth && o.auth.ok >= 1, `${broker}: auth 미기록`);
    for (const x of b.ops) assert.equal(x.latencyMs.count, x.ok + Object.values(x.errors).reduce((a, c) => a + c, 0));
  }
});

test("자동 계측은 인스턴스당 한 번만 감싼다", async () => {
  UsageTelemetry.drain();
  const client = new KisClient("k", "s", "50199202");
  instrumentBroker(client); // 생성자가 이미 호출했으므로 두 번째는 무시
  await client.getCalendar();
  const o = ops(drain()!);
  assert.equal(o.calendar.ok, 1); // 두 번 감쌌다면 2
});

test("스트림 구독·메시지가 어댑터의 usage 핸들로 쌓인다 (KIS)", () =>
  withServer(async (url, connections) => {
    UsageTelemetry.drain();
    const ticks = new Queue<TradeTick>();
    const usage = new BrokerUsage("kis", "PAPER");
    const stream = new KisMarketStream({ wsUrl: url, custtype: "P", approvalKey: async () => "K", usage });
    try {
      stream.subscribeTrades(["KRX:005930", "000660"], (t) => ticks.put(t));
      stream.subscribeOrderBook(["005930"], () => {});
      stream.connect();
      const conn = await connections.take();
      await conn.received.take(); await conn.received.take(); await conn.received.take();
      conn.send(`0|H0STCNT0|002|${FRAME_FIELDS}^${FRAME_FIELDS}`);
      await ticks.take(); await ticks.take();
    } finally { stream.close(); }
    const streams = Object.fromEntries(drain()!.buckets[0].streams.map((s) => [s.channel, s]));
    assert.equal(streams.TRADES.subscriptions, 2);
    assert.equal(streams.TRADES.messages, 2);
    assert.equal(streams.ORDER_BOOK.subscriptions, 1);
    assert.equal(streams.ORDER_BOOK.messages, 0);
  }));

test("요청 서명은 계약대로 HMAC-SHA256(key, timestamp + 개행 + body) 의 소문자 hex 다", () => {
  const body = '{"schema":1}';
  const sig = signTelemetry(body, 1_700_000_000);
  assert.equal(sig, "c0ce56d2a2b120597403cc70160e8db7ae60d242916857319ecc5845522739d2"); // 계약 벡터 (Kotlin 과 동일)
  assert.match(sig, /^[0-9a-f]{64}$/);
  assert.equal(sig, createHmac("sha256", SIGNING_KEY).update(`1700000000\n${body}`).digest("hex"));
  assert.notEqual(signTelemetry(body, 1_700_000_001), sig);
  assert.equal(SIGNING_KEY_ID, "v1");
});

test("기본 전송은 서명 헤더 3개를 싣고 본문을 그대로 보낸다", async () => {
  const received: { headers: Record<string, string | string[] | undefined>; body: string }[] = [];
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (c) => { body += c; });
    req.on("end", () => { received.push({ headers: req.headers, body }); res.statusCode = 202; res.end(); });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const port = (server.address() as AddressInfo).port;
  const originalTransport = UsageTelemetry.transport;
  try {
    __setEndpointForTests(`http://127.0.0.1:${port}/v1/usage`);
    UsageTelemetry.transport = __postForTests;
    UsageTelemetry.drain();
    new BrokerUsage("kis", "PAPER").measure("quotes", () => 1);
    await UsageTelemetry.flushNow();
    assert.equal(received.length, 1);
    const { headers, body } = received[0];
    assert.equal(headers["x-hermetix-key-id"], "v1");
    const ts = Number(headers["x-hermetix-timestamp"]);
    assert.ok(Math.abs(ts - Date.now() / 1000) < 60);
    assert.equal(headers["x-hermetix-signature"], signTelemetry(body, ts));
    assert.equal(headers["content-type"], "application/json");
    assert.match(String(headers["user-agent"]), /^hermetix-js\//);
    assert.equal((JSON.parse(body) as Payload).buckets[0].ops[0].op, "quotes");
  } finally {
    UsageTelemetry.transport = originalTransport;
    __setEndpointForTests(null);
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
});
