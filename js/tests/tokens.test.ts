/**
 * 토큰 수명주기(TokenManager) — 여덟 어댑터 공통 시나리오 (Python test_tokens.py 와 동일).
 *
 * - 발급 유량 초과(429·EGW00133·IGW00201)는 RateLimitError, 발급 실패 후 60초는 서버에 다시 묻지 않는다
 * - 업무 호출이 토큰을 거부(401)하면 토큰을 버리고 1회 재발급 후 재시도한다
 * - 파일 캐시: 다른 프로세스가 새로 저장한 토큰은 지우지 않고 이어 쓴다, 만료 직전 같은 토큰 재발급은 반복하지 않는다
 */
import assert from "node:assert/strict";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, test } from "node:test";
import {
  AuthError, DbClient, KbClient, KisClient, KiwoomClient, LsClient, NextClient, NhClient, RateLimitError, RateLimiter, TossClient,
} from "../src/index.js";
import type { BrokerClient } from "../src/index.js";
import { TokenManager, __setTokenCacheDirForTests } from "../src/tokens.js";

interface Route { method?: string; path?: string; header?: [string, string]; status?: number; body: unknown; }
type Reply = [number, Record<string, unknown>];

const fixturesDir = new URL("../../../conformance/fixtures/", import.meta.url);
const BROKERS = ["next", "kis", "kiwoom", "nh", "db", "ls", "toss", "kb"];

// 2026-09 — 키움 픽스처 토큰(2026-12-31 만료)이 유효한 시점
const START = 1_790_000_000_000;
let now = START;
const realNow = Date.now;
const realFetch = globalThis.fetch;

beforeEach(() => {
  now = START;
  Date.now = () => now;
});
afterEach(() => {
  Date.now = realNow;
  globalThis.fetch = realFetch;
  __setTokenCacheDirForTests(null);
});

/** 픽스처 routes 재생 — 토큰 발급 응답과 업무 호출 응답을 앞에서부터 덮어쓸 수 있다 */
class FakeServer {
  readonly routes: Route[];
  readonly tokenPath: string;
  tokenCalls = 0;
  apiCalls = 0;
  constructor(broker: string, public tokenOverrides: Reply[] = [], public apiOverrides: Reply[] = []) {
    this.routes = (JSON.parse(readFileSync(new URL(`${broker}.json`, fixturesDir), "utf-8")) as { routes: Route[] }).routes;
    this.tokenPath = this.routes[0].path!;
    (globalThis as { fetch: unknown }).fetch = (url: string, init: { method: string; headers?: Record<string, string> }) => this.fetch(url, init);
  }

  private async fetch(url: string, init: { method: string; headers?: Record<string, string> }) {
    const path = new URL(url).pathname;
    const headers = init.headers ?? {};
    let reply: Reply | undefined;
    if (path === this.tokenPath) {
      this.tokenCalls++;
      reply = this.tokenOverrides.shift();
    } else {
      this.apiCalls++;
      reply = this.apiOverrides.shift();
    }
    if (!reply) {
      const route = this.routes.find((r) =>
        (r.method === undefined || r.method === init.method) &&
        (r.path === undefined || r.path === path) &&
        (r.header === undefined || headers[r.header[0]] === r.header[1]));
      reply = route ? [route.status ?? 200, route.body as Record<string, unknown>] : [599, { error: `no fixture route for ${init.method} ${path}` }];
    }
    const [status, body] = reply;
    return { status, text: async () => JSON.stringify(body), headers: { forEach: () => {} } };
  }
}

/** 재시도 없이 첫 오류를 보고, 레이트리미터의 대기는 시계를 앞으로 돌린다 */
function makeClient(broker: string): BrokerClient {
  const client = ({
    next: () => new NextClient("pk_test_conf", "sk_test_conf", "acc_main", "http://next.test"),
    kis: () => new KisClient("k", "s", "50199202", "01", "http://kis.test", 1),
    kiwoom: () => new KiwoomClient("k", "s", "http://kiwoom.test", 1),
    nh: () => new NhClient("k", "s", "", "http://nh.test", "http://nh.test", "KRX", "KRX", 1),
    db: () => new DbClient("k", "s", "http://db.test", "", "J", 1),
    ls: () => new LsClient("k", "s", "http://ls.test", "", "", 1, 1),
    toss: () => new TossClient("c_conf", "s_conf", "", "http://toss.test", 1),
    kb: () => new KbClient("k", "s", "http://kb.test", "1", "K", "0", 1),
  } as Record<string, () => BrokerClient>)[broker]();
  (client as unknown as { limiter: RateLimiter }).limiter =
    new RateLimiter(0, 0, (a) => 1000 * a, async (ms) => { now += ms; }, () => now);
  return client;
}

const rateLimitCases: [string, Reply][] = [
  ...BROKERS.map((b): [string, Reply] => [b, [429, {}]]),
  ["kis", [403, { error_code: "EGW00133", error_description: "접근토큰 발급 잠시 후 다시 시도하세요(1분당 1회)" }]],
  ["ls", [403, { rsp_cd: "IGW00201", rsp_msg: "초당 거래건수를 초과하였습니다." }]],
];

for (const [broker, rejection] of rateLimitCases) {
  test(`${broker}: 토큰 발급 유량 초과(${rejection[0]})는 RateLimitError 이고 쿨다운 동안 서버에 다시 묻지 않는다`, async () => {
    const server = new FakeServer(broker, [rejection]);
    const client = makeClient(broker);
    const first = await client.getAccount().catch((e) => e);
    assert.ok(first instanceof RateLimitError, String(first));
    assert.equal(first.retryAfterSeconds, 60);
    now += 20_000;
    const second = await client.getAccount().catch((e) => e);
    assert.ok(second instanceof RateLimitError, String(second));
    assert.equal(Math.round(second.retryAfterSeconds ?? 0), 40);
    assert.equal(server.tokenCalls, 1); // 쿨다운 동안 서버에 다시 묻지 않는다

    now += 41_000;
    await client.getAccount(); // 쿨다운 뒤 재발급
    assert.equal(server.tokenCalls, 2);
  });
}

for (const broker of BROKERS) {
  test(`${broker}: 토큰 발급 인증 실패도 쿨다운 동안은 서버에 다시 묻지 않는다`, async () => {
    const server = new FakeServer(broker, [[401, { error: "invalid_client" }]]);
    const client = makeClient(broker);
    for (let i = 0; i < 3; i++) await assert.rejects(client.getAccount(), AuthError);
    assert.equal(server.tokenCalls, 1);
    now += 61_000;
    await client.getAccount();
    assert.equal(server.tokenCalls, 2);
  });

  test(`${broker}: 업무 호출이 거부한 토큰은 버리고 1회 재발급 후 재시도한다`, async () => {
    const server = new FakeServer(broker, [], [[401, {}]]);
    const client = makeClient(broker);
    await client.getAccount();
    assert.equal(server.tokenCalls, 2); // 거부된 토큰을 버리고 1회 재발급

    server.apiOverrides = [[401, {}], [401, {}]];
    await assert.rejects(client.getAccount(), AuthError); // 재발급 토큰도 거부되면 그대로 올린다 (무한 재시도 없음)
  });
}

function issuer(...results: (IssuedLike | Error)[]) {
  const calls: number[] = [];
  const issue = async () => {
    calls.push(1);
    const result = results[Math.min(calls.length, results.length) - 1];
    if (result instanceof Error) throw result;
    return result;
  };
  return { issue, calls };
}
type IssuedLike = { token: string; expiresAt: number };

test("다른 프로세스가 새로 저장한 토큰은 invalidate 가 지우지 않는다", async () => {
  const dir = mkdtempSync(join(tmpdir(), "hermetix-tokens-"));
  try {
    __setTokenCacheDirForTests(dir);
    const a = issuer({ token: "tok-a", expiresAt: now + 86_400_000 });
    const b = issuer({ token: "tok-b", expiresAt: now + 86_400_000 });
    const ma = new TokenManager("toss", "k", a.issue, 60);
    const mb = new TokenManager("toss", "k", b.issue, 60);
    assert.equal(await ma.get(), "tok-a");
    assert.equal(await mb.get(), "tok-a"); // 파일 캐시 공유
    // 토스처럼 재발급이 이전 토큰을 무효로 만드는 서버: b 가 거부당해 재발급하면 a 는 거부당한 뒤 b 의 새 토큰을 쓴다
    mb.invalidate("tok-a");
    assert.equal(await mb.get(), "tok-b");
    ma.invalidate("tok-a");
    assert.equal(await ma.get(), "tok-b");
    assert.deepEqual([a.calls.length, b.calls.length], [1, 1]);
    const files = readdirSync(dir);
    assert.equal(files.length, 1);
    assert.ok(files[0].startsWith("toss-"));
    assert.deepEqual(JSON.parse(readFileSync(join(dir, files[0]), "utf8")), { access_token: "tok-b", expires_at: now / 1000 + 86400 });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("만료 직전 같은 토큰을 다시 주는 서버는 만료까지 재발급하지 않는다", async () => {
  const expiresAt = now + 300_000; // 여유(600초) 안 — DB 는 만료 전 재발급에 남은 시간 그대로의 같은 토큰을 준다
  const { issue, calls } = issuer({ token: "tok-1", expiresAt });
  const manager = new TokenManager("db", "k", issue, 600);
  for (let i = 0; i < 5; i++) assert.equal(await manager.get(), "tok-1");
  assert.equal(calls.length, 1);
  now = expiresAt;
  await manager.get();
  assert.equal(calls.length, 2);
});

test("call 은 발급 실패를 재시도하지 않는다", async () => {
  const { issue, calls } = issuer(new AuthError(401, "invalid_client", "rejected"));
  const manager = new TokenManager("kis", "k", issue, 300);
  await assert.rejects(manager.call(async (token) => token), AuthError);
  assert.equal(calls.length, 1);
});

test("동시 호출은 진행 중인 발급 하나를 기다린다", async () => {
  const { issue, calls } = issuer({ token: "tok-1", expiresAt: now + 86_400_000 });
  const manager = new TokenManager("kis", "k", issue, 300);
  const tokens = await Promise.all([manager.get(), manager.get(), manager.call(async (token) => token)]);
  assert.deepEqual(tokens, ["tok-1", "tok-1", "tok-1"]);
  assert.equal(calls.length, 1);
});
