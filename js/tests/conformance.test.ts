/** 네 언어가 공유하는 골든 픽스처(conformance/fixtures)를 가짜 fetch 로 재생해 세 어댑터를 컨포먼스 시나리오에 통과시킨다. */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import {
  DbClient, Decimal, KbClient, KisClient, KiwoomClient, LsClient, NextClient, NhClient, RateLimitError, RateLimiter, TossClient,
  verifyBrokerConformance,
} from "../src/index.js";
import { pnlReport } from "../src/engine.js";
import type { BrokerClient } from "../src/index.js";

interface Route { method?: string; path?: string; header?: [string, string]; status?: number; body: unknown; }

const fixturesDir = new URL("../../../conformance/fixtures/", import.meta.url);

function load(broker: string) {
  const fx = JSON.parse(readFileSync(new URL(`${broker}.json`, fixturesDir), "utf-8")) as {
    scenario: { symbol: string; quantity: string; limitPrice: string }; routes: Route[];
  };
  return {
    routes: fx.routes,
    scenario: { symbol: fx.scenario.symbol, quantity: new Decimal(fx.scenario.quantity), limitPrice: new Decimal(fx.scenario.limitPrice) },
  };
}

/** 픽스처 routes 를 순서대로 매칭 — method / path(정확히) / header(이름·값) 중 지정된 조건만 검사 */
function fakeFetch(routes: Route[]) {
  return async (url: string, init: { method: string; headers?: Record<string, string> }) => {
    const path = new URL(url).pathname;
    const headers = init.headers ?? {};
    const route = routes.find((r) =>
      (r.method === undefined || r.method === init.method) &&
      (r.path === undefined || r.path === path) &&
      (r.header === undefined || headers[r.header[0]] === r.header[1]));
    const status = route ? (route.status ?? 200) : 599;
    const body = route ? route.body : { error: `no fixture route for ${init.method} ${path}` };
    return { status, text: async () => JSON.stringify(body), headers: { forEach: () => {} } };
  };
}

async function run(broker: string, client: () => BrokerClient) {
  const { routes, scenario } = load(broker);
  const restore = (globalThis as any).fetch;
  (globalThis as any).fetch = fakeFetch(routes);
  try {
    const report = await verifyBrokerConformance(client(), scenario);
    assert.deepEqual(report.violations, [], report.violations.join("\n"));
    for (const s of ["quotes", "candles", "calendar", "account", "holdings", "buyingPower", "createOrder", "getOrder", "getOrders", "cancelOrder", "fills"]) {
      assert.ok(report.steps.includes(s), `step ${s} missing`);
    }
  } finally {
    (globalThis as any).fetch = restore;
  }
}

test("next 어댑터는 컨포먼스 시나리오를 통과한다", () => run("next", () => new NextClient("pk_test_conf", "sk_test_conf", "acc_main", "http://next.test")));
test("kis 어댑터는 컨포먼스 시나리오를 통과한다", () => run("kis", () => new KisClient("k", "s", "50199202", "01", "http://kis.test", 1)));
test("kiwoom 어댑터는 컨포먼스 시나리오를 통과한다", () => run("kiwoom", () => new KiwoomClient("k", "s", "http://kiwoom.test", 1)));
// accountNo 를 비워 /n2/acctinfo 로 모의(acct_type=03) 계좌를 고르는 경로까지 검증. 토큰은 운영 호스트 전용 — 테스트에선 같은 가짜 서버
test("nh 어댑터는 컨포먼스 시나리오를 통과한다 (문서 기반 픽스처)", () => run("nh", () => new NhClient("k", "s", "", "http://nh.test", "http://nh.test", "KRX", "KRX", 1)));
test("db 어댑터는 컨포먼스 시나리오를 통과한다 (문서 기반 픽스처)", () => run("db", () => new DbClient("k", "s", "http://db.test", "", "J", 1)));
test("ls 어댑터는 컨포먼스 시나리오를 통과한다 (문서 기반 픽스처)", () => run("ls", () => new LsClient("k", "s", "http://ls.test", "", "", 1, 1)));
test("toss 어댑터는 컨포먼스 시나리오를 통과한다 (실전 전용)", () => run("toss", () => new TossClient("c_conf", "s_conf", "", "http://toss.test", 1)));
test("kb 어댑터는 컨포먼스 시나리오를 통과한다 (실전 전용)", () => run("kb", () => new KbClient("k", "s", "http://kb.test", "1", "K", "0", 1)));

test("RateLimiter: 쓰로틀·백오프·Retry-After·재시도 소진", async () => {
  const sleeps: number[] = [];
  let now = 0;
  const sleeper = async (ms: number) => { sleeps.push(ms); now += ms; };
  const limiter = new RateLimiter(600, 2, (a) => 1000 * a, sleeper, () => now);
  await limiter.execute(async () => { now += 100; });
  await limiter.execute(async () => {});
  assert.deepEqual(sleeps, [500]);

  sleeps.length = 0;
  let calls = 0;
  const result = await new RateLimiter(0, 2, (a) => 1000 * a, sleeper).execute(async () => {
    calls++;
    if (calls < 3) throw new RateLimitError(429, "EGW00201", "초당 거래건수 초과");
    return "ok";
  });
  assert.equal(result, "ok");
  assert.deepEqual(sleeps, [1000, 2000]);

  sleeps.length = 0;
  let first = true;
  await new RateLimiter(0, 1, (a) => 1000 * a, sleeper).execute(async () => {
    if (first) { first = false; throw new RateLimitError(429, null, "rl", 3600); }
    return "ok";
  });
  assert.deepEqual(sleeps, [RateLimiter.MAX_RETRY_AFTER_MS]);

  await assert.rejects(new RateLimiter(0, 1, () => 0, sleeper).execute(async () => { throw new RateLimitError(429, null, "x"); }), RateLimitError);
});

test("toss 미국 보유 평가금액은 매매기준율로 원화 환산해 계좌 총평가에 더하고, 손익은 USD 그대로 둔다", async () => {
  const restore = (globalThis as any).fetch;
  (globalThis as any).fetch = fakeFetch(load("toss").routes);
  try {
    const client = new TossClient("c_conf", "s_conf", "", "http://toss.test", 1);
    const aapl = (await client.getHoldings()).find((h) => h.symbol === "US:AAPL")!;
    assert.equal(aapl.currentPrice?.toString(), "250"); // 단가는 종목 통화 그대로
    assert.equal(aapl.marketValue?.toString(), "700000");
    assert.equal(aapl.unrealizedPnl?.toString(), "100"); // 손익은 USD 그대로 — 현재 환율로 환산하면 환차손익이 빠진다
    assert.equal(aapl.currency, "USD");
    const report = await pnlReport(client); // 손익 합계는 원화 종목만 (USD 손익 100 을 섞지 않는다)
    assert.equal(report.totalMarketValue.toString(), "910000");
    assert.equal(report.totalUnrealizedPnl.toString(), "6000");
    assert.equal((await client.getAccount()).portfolioValue.toString(), "1910000");
  } finally {
    (globalThis as any).fetch = restore;
  }
});
