/** 설정 파일 로딩 — 값은 에러에 싣지 않고, 틀린 별칭만 빼고 나머지는 쓴다 */
import assert from "node:assert/strict";
import { test } from "node:test";
import { loadConfig } from "../src/config.js";
import { connect, writeConfig } from "./helpers.js";

const kis = { broker: "kis", apiKey: "k-value-1234", apiSecret: "s-value-5678", account: "50199202", base_url: "http://kis.test" };

test("설정 파일이 없으면 서버는 뜨고 tool 이 예시와 함께 이유를 알려 준다", async () => {
  const mcp = await connect("/nonexistent/hermetix/mcp.json");
  try {
    for (const name of ["list_accounts", "get_account"]) {
      const r = await mcp.call(name);
      assert.equal(r.isError, true);
      assert.match(r.text, /설정 파일이 없습니다: \/nonexistent\/hermetix\/mcp\.json/);
      assert.match(r.text, /"accounts"/);
    }
  } finally {
    await mcp.close();
  }
});

test("JSON 오류·accounts 누락", () => {
  assert.match(loadConfig(writeConfig("{ nope")).error ?? "", /JSON 오류/);
  assert.match(loadConfig(writeConfig({ default: "x" })).error ?? "", /"accounts" 객체가 없습니다/);
});

test("env: 참조는 환경변수에서 읽고, 비어 있으면 그 별칭만 쓸 수 없다", () => {
  process.env.HERMETIX_MCP_TEST_KEY = "from-env-key";
  const cfg = loadConfig(writeConfig({
    accounts: {
      ok: { ...kis, apiKey: "env:HERMETIX_MCP_TEST_KEY" },
      missing: { ...kis, apiSecret: "env:HERMETIX_MCP_TEST_NOPE" },
    },
  }));
  assert.deepEqual([...cfg.accounts.keys()], ["ok"]);
  assert.match(cfg.invalid.get("missing") ?? "", /apiSecret 가 가리키는 환경변수 HERMETIX_MCP_TEST_NOPE 가 비어 있습니다/);
  assert.equal(cfg.defaultAlias, null); // 별칭이 둘(정상+오류)이고 default 가 없다
});

test("팩토리 검증 에러는 별칭과 함께, 값 없이 전달한다", () => {
  const cfg = loadConfig(writeConfig({
    accounts: {
      typo: { ...kis, hts: "x" },
      nobroker: { apiKey: "k", apiSecret: "s" },
      unknown: { ...kis, broker: "mirae" },
    },
  }));
  assert.equal(cfg.accounts.size, 0);
  assert.match(cfg.invalid.get("typo") ?? "", /^'typo': hermetix\.kis: 모르는 항목 'hts'/);
  assert.match(cfg.invalid.get("nobroker") ?? "", /"broker" 가 필요합니다/);
  assert.match(cfg.invalid.get("unknown") ?? "", /모르는 브로커 'mirae'/);
  for (const reason of cfg.invalid.values()) {
    assert.ok(!reason.includes("k-value-1234") && !reason.includes("s-value-5678") && !reason.includes("50199202"));
  }
});

test("default 별칭·권한 경고", () => {
  const cfg = loadConfig(writeConfig({ accounts: { a: kis, b: kis }, default: "b" }, 0o644));
  assert.equal(cfg.defaultAlias, "b");
  assert.equal(cfg.accounts.get("a")?.environment, "PAPER");
  if (process.platform !== "win32") assert.match(cfg.warnings.join("\n"), /chmod 600/);

  const noDefault = loadConfig(writeConfig({ accounts: { a: kis, b: kis } }));
  assert.equal(noDefault.defaultAlias, null);
  assert.ok(loadConfig(writeConfig({ accounts: { a: kis }, default: "zzz" })).warnings.includes(`"default" 별칭 'zzz' 이 accounts 에 없습니다`));
});

test("별칭이 여럿이고 default 가 없으면 account 를 요구한다", async () => {
  const mcp = await connect(writeConfig({ accounts: { a: kis, b: kis } }));
  try {
    const r = await mcp.call("get_account");
    assert.equal(r.isError, true);
    assert.match(r.text, /account 를 지정하세요. 쓸 수 있는 별칭: a, b/);
    const unknown = await mcp.call("get_account", { account: "c" });
    assert.match(unknown.text, /모르는 별칭 'c'/);
  } finally {
    await mcp.close();
  }
});

test("기본 별칭이 설정 오류로 빠지면 account 를 생략한 호출이 그 이유를 받는다", async () => {
  const mcp = await connect(writeConfig({ accounts: { toss: { ...kis, apiSecret: "env:HERMETIX_MCP_TEST_EMPTY" } }, default: "toss" }));
  try {
    const r = await mcp.call("get_account");
    assert.equal(r.isError, true);
    assert.match(r.text, /^설정 오류 — 'toss': apiSecret 가 가리키는 환경변수 HERMETIX_MCP_TEST_EMPTY 가 비어 있습니다/);
  } finally {
    await mcp.close();
  }
});

test("keychain: 참조는 키체인 리더로 읽고, 값은 어디에도 싣지 않는다", () => {
  const seen: string[] = [];
  const keychain = (service: string, account: string) => {
    seen.push(`${service}/${account}`);
    return account === "KIS_KEY" ? "kc-key-value-9999" : account === "KIS_SECRET" ? "kc-secret-value-8888" : null;
  };
  const cfg = loadConfig(writeConfig({
    accounts: {
      ok: { ...kis, apiKey: "keychain:KIS_KEY", apiSecret: "keychain:KIS_SECRET" },
      missing: { ...kis, apiKey: "keychain:KIS_KEY", apiSecret: "keychain:NOPE" },
      blank: { ...kis, apiKey: "keychain:", apiSecret: "keychain:KIS_SECRET" },
    },
  }), { keychain, platform: "darwin" });
  assert.deepEqual([...cfg.accounts.keys()], ["ok"]);
  assert.deepEqual(seen, ["hermetix/KIS_KEY", "hermetix/KIS_SECRET", "hermetix/KIS_KEY", "hermetix/NOPE"]);
  assert.match(cfg.invalid.get("missing") ?? "", /apiSecret 가 가리키는 키체인 항목 NOPE 이 없습니다 — security add-generic-password -s hermetix -a NOPE -w/);
  assert.match(cfg.invalid.get("blank") ?? "", /apiKey 의 keychain: 뒤에 항목 이름이 없습니다/);
  const everything = [...cfg.invalid.values(), ...cfg.warnings].join("\n");
  assert.ok(!everything.includes("kc-key-value-9999") && !everything.includes("kc-secret-value-8888"));
  // 모든 비밀이 참조라 평문 경고가 없다
  assert.ok(!cfg.warnings.some((w) => w.includes("평문")));
});

test("keychain: 참조는 macOS 외에서는 그 별칭만 거부하고 env: 를 안내한다", () => {
  const cfg = loadConfig(writeConfig({ accounts: { a: { ...kis, apiKey: "keychain:KIS_KEY" } } }), {
    keychain: () => { throw new Error("호출되면 안 된다"); },
    platform: "linux",
  });
  assert.equal(cfg.accounts.size, 0);
  assert.match(cfg.invalid.get("a") ?? "", /keychain: 참조는 macOS 에서만 됩니다 — env: 를 쓰세요/);
});

test("macOS 에서 apiKey/apiSecret 이 평문이면 경고하고, env:·keychain: 참조만이면 경고하지 않는다", () => {
  process.env.HERMETIX_MCP_TEST_KEY = "from-env-key";
  const plain = loadConfig(writeConfig({ accounts: { a: kis } }), { platform: "darwin" });
  assert.ok(plain.warnings.some((w) => /평문/.test(w) && /keychain:/.test(w)));
  assert.ok(!plain.warnings.join("\n").includes("k-value-1234"));

  const refs = loadConfig(writeConfig({ accounts: { a: { ...kis, apiKey: "env:HERMETIX_MCP_TEST_KEY", apiSecret: "keychain:S" } } }), {
    keychain: () => "kc", platform: "darwin",
  });
  assert.equal(refs.accounts.size, 1);
  assert.ok(!refs.warnings.some((w) => /평문/.test(w)));

  const linux = loadConfig(writeConfig({ accounts: { a: kis } }), { platform: "linux" });
  assert.ok(!linux.warnings.some((w) => /평문/.test(w)));
});
