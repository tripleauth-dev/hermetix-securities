/**
 * DB증권 토큰 수명주기 — 발급 1분 1건 제한(403 + IGW00201)을 레이트리밋으로 분류하고, 발급 실패 후 60초는 서버에 다시 묻지 않으며,
 * 발급 토큰을 파일로 저장해 같은 키를 쓰는 다른 프로세스(클라이언트)가 재사용하는지 (Python test_db_token.py 와 동일 시나리오).
 */
import assert from "node:assert/strict";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, test } from "node:test";
import { DbClient, __setDbTokenCacheDirForTests } from "../src/brokers/db.js";
import { RateLimiter } from "../src/broker.js";
import { AuthError, RateLimitError } from "../src/errors.js";

const BALANCE = { rsp_cd: "00000", rsp_msg: "정상", Out: { DpsastAmt: "1000", Dps2: "1000", TotEvalAmt: "0" } };
type Reply = [number, Record<string, unknown>];
const RATE_LIMITED: Reply = [403, { rsp_cd: "IGW00201", rsp_msg: "초당 거래건수를 초과하였습니다." }];
const REJECTED: Reply = [401, { rsp_cd: "IGW00121", rsp_msg: "유효하지 않은 앱키입니다." }];
const issued = (token = "tok-1"): Reply => [200, { access_token: token, token_type: "Bearer", expires_in: 86400 }];

/** 토큰 응답을 차례로 돌려주고(마지막 응답 반복), 업무 호출은 잔고 응답으로 받는 가짜 서버 */
class FakeServer {
  tokenCalls = 0;
  apiTokens: string[] = [];
  constructor(private readonly tokenReplies: Reply[]) {}
  fetch = async (url: string, init: { headers?: Record<string, string> }) => {
    let reply: Reply;
    if (url.endsWith("/oauth2/token")) {
      this.tokenCalls++;
      reply = this.tokenReplies.length > 1 ? this.tokenReplies.shift()! : this.tokenReplies[0];
    } else {
      this.apiTokens.push(init.headers?.authorization ?? "");
      reply = [200, BALANCE];
    }
    return { status: reply[0], text: async () => JSON.stringify(reply[1]), headers: new Map<string, string>() };
  };
}

let now = 1_800_000_000_000;
const realNow = Date.now;
const realFetch = globalThis.fetch;
let server: FakeServer;

beforeEach(() => {
  now = 1_800_000_000_000;
  Date.now = () => now;
});
afterEach(() => {
  Date.now = realNow;
  globalThis.fetch = realFetch;
  __setDbTokenCacheDirForTests(null);
});

function serve(s: FakeServer): FakeServer {
  server = s;
  (globalThis as { fetch: unknown }).fetch = (url: string, init: { headers?: Record<string, string> }) => server.fetch(url, init);
  return s;
}

/** 레이트리미터의 대기가 시계를 앞으로 돌리게 한다 */
function client(appKey = "k", maxRetries = 4): DbClient {
  const c = new DbClient(appKey, "s", "http://db.test", "", "J", 1);
  (c as unknown as { limiter: RateLimiter }).limiter =
    new RateLimiter(1, maxRetries, (a) => 1000 * 2 ** (a - 1), async (ms) => { now += ms; }, () => now);
  return c;
}

test("토큰 발급 유량 초과는 RateLimitError 이고 쿨다운 동안 서버에 다시 묻지 않는다", async () => {
  const s = serve(new FakeServer([RATE_LIMITED]));
  const c = client("k", 0);
  const first = await c.getAccount().catch((e) => e);
  assert.ok(first instanceof RateLimitError);
  assert.equal(first.retryAfterSeconds, 60);
  now += 20_000;
  const second = await c.getAccount().catch((e) => e);
  assert.ok(second instanceof RateLimitError);
  assert.equal(s.tokenCalls, 1);
  assert.equal(Math.round(second.retryAfterSeconds ?? 0), 40);
});

test("레이트리미터가 쿨다운을 기다린 뒤 다시 발급한다", async () => {
  const s = serve(new FakeServer([RATE_LIMITED, issued()]));
  await client().getAccount();
  assert.equal(s.tokenCalls, 2);
  assert.deepEqual(s.apiTokens, ["Bearer tok-1"]);
});

test("인증 실패도 쿨다운 동안은 서버에 다시 묻지 않는다", async () => {
  const s = serve(new FakeServer([REJECTED, issued()]));
  const c = client();
  for (let i = 0; i < 3; i++) await assert.rejects(c.getAccount(), AuthError);
  assert.equal(s.tokenCalls, 1);
  now += 61_000;
  await c.getAccount();
  assert.equal(s.tokenCalls, 2);
});

test("발급 토큰은 파일 캐시로 같은 키의 다른 클라이언트와 나눠 쓴다", async () => {
  const dir = mkdtempSync(join(tmpdir(), "hermetix-db-token-"));
  try {
    __setDbTokenCacheDirForTests(dir);
    serve(new FakeServer([issued("tok-shared")]));
    await client().getAccount();

    const files = readdirSync(dir);
    assert.equal(files.length, 1);
    assert.ok(files[0].startsWith("db-"));
    assert.equal(statSync(join(dir, files[0])).mode & 0o777, 0o600);
    assert.deepEqual(JSON.parse(readFileSync(join(dir, files[0]), "utf8")), { access_token: "tok-shared", expires_at: now / 1000 + 86400 });

    const second = serve(new FakeServer([issued("tok-other")]));
    await client().getAccount();
    assert.equal(second.tokenCalls, 0);
    assert.deepEqual(second.apiTokens, ["Bearer tok-shared"]);

    const otherKey = serve(new FakeServer([issued("tok-other")]));
    await client("k2").getAccount();
    assert.equal(otherKey.tokenCalls, 1);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("만료가 가까운 캐시 토큰은 쓰지 않는다", async () => {
  const dir = mkdtempSync(join(tmpdir(), "hermetix-db-token-"));
  try {
    __setDbTokenCacheDirForTests(dir);
    serve(new FakeServer([issued("tok-old")]));
    await client().getAccount();
    now += (86400 - 300) * 1000;
    const s = serve(new FakeServer([issued("tok-new")]));
    await client().getAccount();
    assert.equal(s.tokenCalls, 1);
    assert.deepEqual(s.apiTokens, ["Bearer tok-new"]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
