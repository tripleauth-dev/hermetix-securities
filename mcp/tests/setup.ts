/**
 * 테스트 실행은 운영 텔레메트리로 보내지 않고, 토큰 캐시가 실제 ~/.hermetix 에 남지 않게 한다.
 * HOME 을 임시 디렉터리로 바꾼 뒤 SDK 를 불러야 SDK 의 토큰 캐시 경로가 그쪽을 가리킨다.
 */
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

process.env.HOME = mkdtempSync(join(tmpdir(), "hermetix-mcp-test-"));
const { UsageTelemetry } = await import("hermetix");
UsageTelemetry.transport = () => {};
