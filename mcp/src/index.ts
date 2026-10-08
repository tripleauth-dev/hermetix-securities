#!/usr/bin/env node
/**
 * hermetix-mcp — 로컬 stdio MCP 서버. 설계: mcp/DESIGN.md
 * stdout 은 MCP 프로토콜 전용이라, SDK 의 console.log/info 를 stderr 로 돌린 뒤 기동한다.
 */
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { loadConfig } from "./config.js";
import { buildServer } from "./tools.js";
import { VERSION } from "./version.js";

console.log = console.error;
console.info = console.error;

const config = loadConfig();
for (const warning of config.warnings) console.error(`WARN hermetix-mcp ${warning}`);
if (config.error) console.error(`WARN hermetix-mcp ${config.error}`);
for (const [alias, reason] of config.invalid) console.error(`WARN hermetix-mcp 계좌 ${alias} 사용 불가: ${reason}`);

const server = buildServer(config);
await server.connect(new StdioServerTransport());
console.error(`hermetix-mcp ${VERSION} 기동 (${config.path}, 계좌 ${config.accounts.size}개)`);
