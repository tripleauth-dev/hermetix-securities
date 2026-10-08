/** 여덟 증권사 골든 픽스처를 MCP tool 호출로 재생한다 — 조회 tool 전부가 에러 없이 정규화 결과를 돌려주고, 키·계좌번호가 출력에 없어야 한다 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { connect, fixtureRoutes, installFakeFetch, writeConfig } from "./helpers.js";

const KEY = "AKEYSHOULDNOTLEAK0001";
const SECRET = "ASECRETSHOULDNOTLEAK0002";

const ACCOUNTS: Record<string, { entry: Record<string, unknown>; symbol: string; account?: string }> = {
  next: { entry: { apiKey: `pk_test_${KEY}`, apiSecret: `sk_test_${SECRET}`, account: "acc_main_7788", base_url: "http://next.test" }, symbol: "AAPL", account: "acc_main_7788" },
  kis: { entry: { apiKey: KEY, apiSecret: SECRET, account: "50199202", base_url: "http://kis.test", throttle_seconds: 0.001 }, symbol: "005930", account: "50199202" },
  kiwoom: { entry: { apiKey: KEY, apiSecret: SECRET, base_url: "http://kiwoom.test", throttle_seconds: 0.001 }, symbol: "005930" },
  nh: { entry: { apiKey: KEY, apiSecret: SECRET, base_url: "http://nh.test", auth_url: "http://nh.test", throttle_seconds: 0.001 }, symbol: "005930" },
  ls: { entry: { apiKey: KEY, apiSecret: SECRET, base_url: "http://ls.test", throttle_seconds: 0.001, chart_throttle_seconds: 0.001 }, symbol: "005930" },
  db: { entry: { apiKey: KEY, apiSecret: SECRET, base_url: "http://db.test", market_div_code: "J", throttle_seconds: 0.001 }, symbol: "005930" },
  toss: { entry: { apiKey: KEY, apiSecret: SECRET, base_url: "http://toss.test", throttle_seconds: 0.001 }, symbol: "005930" },
  kb: { entry: { apiKey: KEY, apiSecret: SECRET, base_url: "http://kb.test", excg_clsf: "1", sor_order_ccd: "K", chart_market_clsf: "0", throttle_seconds: 0.001 }, symbol: "005930" },
};

for (const [broker, { entry, symbol, account }] of Object.entries(ACCOUNTS)) {
  test(`${broker}: 조회 tool 이 픽스처를 정규화해 돌려주고 키·계좌번호를 싣지 않는다`, async () => {
    const restore = installFakeFetch(fixtureRoutes(broker));
    const mcp = await connect(writeConfig({ accounts: { main: { broker, ...entry } } }));
    try {
      const outputs: string[] = [];
      const expectOk = async (name: string, args: Record<string, unknown> = {}) => {
        const r = await mcp.call(name, args);
        assert.equal(r.isError, false, `${name}: ${r.text}`);
        outputs.push(r.text);
        return JSON.parse(r.text);
      };

      const accounts = await expectOk("list_accounts");
      assert.equal(accounts.default, "main");
      assert.equal(accounts.accounts[0].broker, broker);
      const intervals: string[] = accounts.accounts[0].capabilities.candleIntervals;

      const quotes = await expectOk("get_quotes", { symbols: [symbol] });
      assert.equal(quotes.length, 1);
      assert.match(quotes[0].price, /^\d+(\.\d+)?$/);
      const candles = await expectOk("get_candles", { symbol, interval: intervals.includes("1d") ? "1d" : intervals[0], limit: 3 });
      assert.ok(candles.length > 0);
      await expectOk("get_calendar");
      const acct = await expectOk("get_account");
      assert.match(acct.cash, /^-?\d+(\.\d+)?$/);
      const holdings = await expectOk("get_holdings");
      assert.equal(typeof holdings.truncated, "boolean");
      const bp = await expectOk("get_buying_power");
      assert.match(bp.buyingPower, /^-?\d+(\.\d+)?$/);
      await expectOk("list_orders");
      await expectOk("get_fills");

      const all = outputs.join("\n");
      for (const secret of [KEY, SECRET, ...(account ? [account] : [])]) {
        assert.ok(!all.includes(secret), `출력에 '${secret}' 가 있다`);
      }
    } finally {
      await mcp.close();
      restore();
    }
  });
}

test("주문 생성·취소 tool 은 없다", async () => {
  const mcp = await connect(writeConfig({ accounts: { main: { broker: "kis", ...ACCOUNTS.kis.entry } } }));
  try {
    const { tools } = await mcp.client.listTools();
    const names = tools.map((t) => t.name).sort();
    assert.deepEqual(names, [
      "get_account", "get_buying_power", "get_calendar", "get_candles", "get_fills", "get_holdings", "get_order", "get_quotes", "list_accounts", "list_orders",
    ]);
    for (const t of tools) assert.equal(t.annotations?.readOnlyHint, true, t.name);
  } finally {
    await mcp.close();
  }
});

test("지원하지 않는 캔들 간격은 증권사 호출 전에 거부한다", async () => {
  const restore = installFakeFetch([]);
  const mcp = await connect(writeConfig({ accounts: { main: { broker: "kb", ...ACCOUNTS.kb.entry } } }));
  try {
    const r = await mcp.call("get_candles", { symbol: "005930", interval: "1m" }); // kb 는 1d 만
    assert.equal(r.isError, true);
    assert.match(r.text, /kb 는 1m 캔들을 지원하지 않습니다 \(지원: 1d\)/);
  } finally {
    await mcp.close();
    restore();
  }
});

test("get_order 는 주문 한 건을 돌려준다", async () => {
  const restore = installFakeFetch(fixtureRoutes("toss"));
  const mcp = await connect(writeConfig({ accounts: { main: { broker: "toss", ...ACCOUNTS.toss.entry } } }));
  try {
    const r = await mcp.call("get_order", { orderId: "0d5QIHconformance1" });
    assert.equal(r.isError, false, r.text);
    const order = JSON.parse(r.text);
    assert.equal(order.orderId, "0d5QIHconformance1");
    assert.equal(order.limitPrice, "70000");
  } finally {
    await mcp.close();
    restore();
  }
});

test("증권사 인증 실패는 원인을 한 줄로 알려 준다", async () => {
  const restore = installFakeFetch([{ body: { error: "invalid_client", error_description: "rejected" }, status: 401 }]);
  const mcp = await connect(writeConfig({ accounts: { main: { broker: "toss", ...ACCOUNTS.toss.entry } } }));
  try {
    const r = await mcp.call("get_account");
    assert.equal(r.isError, true);
    assert.match(r.text, /^인증 실패 — main \(toss LIVE\)/);
    assert.ok(!r.text.includes(SECRET));
  } finally {
    await mcp.close();
    restore();
  }
});
