import assert from "node:assert/strict";
import { test } from "node:test";
import { connect, writeConfig } from "./helpers.js";

test("문서 resource 4개를 읽을 수 있다", async () => {
  const mcp = await connect(writeConfig({ accounts: {} }));
  try {
    const { resources } = await mcp.client.listResources();
    assert.deepEqual(resources.map((r) => r.uri).sort(), [
      "hermetix://docs/broker-factory", "hermetix://docs/brokers", "hermetix://docs/js", "hermetix://docs/strategy-guide",
    ]);
    for (const r of resources) {
      const { contents } = await mcp.client.readResource({ uri: r.uri });
      assert.ok(String((contents[0] as { text: string }).text).length > 500, r.uri);
    }
  } finally {
    await mcp.close();
  }
});
