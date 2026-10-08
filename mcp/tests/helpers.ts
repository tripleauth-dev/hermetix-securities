import { chmodSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { loadConfig } from "../src/config.js";
import { buildServer } from "../src/tools.js";

interface Route { method?: string; path?: string; header?: [string, string]; status?: number; body: unknown; }

const fixturesDir = new URL("../../../conformance/fixtures/", import.meta.url);

export const fixtureRoutes = (broker: string): Route[] =>
  (JSON.parse(readFileSync(new URL(`${broker}.json`, fixturesDir), "utf-8")) as { routes: Route[] }).routes;

/** 픽스처 routes 재생 fetch (js/tests/conformance.test.ts 와 같은 매칭) */
export function installFakeFetch(routes: Route[]): () => void {
  const restore = globalThis.fetch;
  (globalThis as any).fetch = async (url: string, init: { method: string; headers?: Record<string, string> }) => {
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
  return () => { (globalThis as any).fetch = restore; };
}

export function writeConfig(content: unknown, mode = 0o600): string {
  const path = join(mkdtempSync(join(tmpdir(), "hermetix-mcp-cfg-")), "mcp.json");
  writeFileSync(path, typeof content === "string" ? content : JSON.stringify(content));
  chmodSync(path, mode);
  return path;
}

/** 설정 파일 → 서버 → in-memory 클라이언트 */
export async function connect(configPath: string) {
  const server = buildServer(loadConfig(configPath));
  const [clientSide, serverSide] = InMemoryTransport.createLinkedPair();
  await server.connect(serverSide);
  const client = new Client({ name: "test", version: "0" });
  await client.connect(clientSide);
  const call = async (name: string, args: Record<string, unknown> = {}) => {
    const r = await client.callTool({ name, arguments: args }) as { content: { type: string; text: string }[]; isError?: boolean };
    return { text: r.content.map((c) => c.text).join("\n"), isError: r.isError === true };
  };
  return { client, call, close: () => client.close() };
}
