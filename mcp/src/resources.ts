/**
 * 문서 resource — AI 가 hermetix 로 코드를 짤 때 참고한다.
 * 빌드 시 저장소 docs/·js/README.md 를 패키지의 docs/ 로 복사한다 (scripts/copy-docs.mjs).
 */
import { readFileSync } from "node:fs";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

export const DOCS = [
  { name: "brokers", file: "brokers.md", title: "증권사별 지원 기능·제약" },
  { name: "broker-factory", file: "broker-factory.md", title: "브로커 팩토리·자격 증명 규약" },
  { name: "strategy-guide", file: "strategy-guide.md", title: "전략 작성 가이드" },
  { name: "js", file: "js-README.md", title: "JS/TypeScript SDK 사용법" },
] as const;

const docsDir = new URL("../../docs/", import.meta.url);

export function registerResources(server: McpServer): void {
  for (const doc of DOCS) {
    server.registerResource(
      doc.name,
      `hermetix://docs/${doc.name}`,
      { title: doc.title, mimeType: "text/markdown" },
      async (uri) => ({ contents: [{ uri: uri.href, mimeType: "text/markdown", text: readFileSync(new URL(doc.file, docsDir), "utf-8") }] }),
    );
  }
}
