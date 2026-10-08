/** 패키지 버전 — package.json 한 곳에서 읽는다 (dist/src → 패키지 루트) */
import { readFileSync } from "node:fs";

export const VERSION: string = (JSON.parse(readFileSync(new URL("../../package.json", import.meta.url), "utf-8")) as { version: string }).version;
