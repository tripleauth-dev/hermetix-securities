/**
 * 테스트 실행은 운영 텔레메트리로 보내지 않는다 — 픽스처 재생 호출이 사용량 랭킹·API 현황에 섞이지 않도록
 * 전송 함수를 버린다. `node --test --import` 로 모든 테스트 파일보다 먼저 로드된다.
 * 전송 자체를 검증하는 테스트(telemetry.test.ts)는 자기 transport 로 다시 바꿔 쓴다.
 */
import { __setDbTokenCacheDirForTests } from "../src/brokers/db.js";
import { UsageTelemetry } from "../src/telemetry.js";

UsageTelemetry.transport = () => {};

// DB 토큰 파일 캐시도 끈다 — 가짜 키로 받은 토큰이 ~/.hermetix/tokens 에 남거나 테스트끼리 공유되지 않도록
__setDbTokenCacheDirForTests(null);
