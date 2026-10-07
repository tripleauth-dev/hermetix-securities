/**
 * 증권사 접근 토큰 수명주기 — 여덟 어댑터 공용.
 *
 * - 메모리 → 파일 캐시 → 발급 순으로 찾는다. 파일 캐시는 같은 키를 쓰는 여러 프로세스가 토큰을 나눠 쓰게 한다
 *   (KIS·DB 는 발급 1분 1건, 토스는 재발급 시 이전 토큰 무효라 프로세스마다 발급하면 서로 부딪친다).
 *   위치 ~/.hermetix/tokens/<broker>-<sha256(appKey) 앞 16자>.json, 내용은 access_token·expires_at(epoch 초) 뿐 — 키·시크릿은 담지 않는다.
 * - 발급에 실패하면 FAILURE_COOLDOWN_SECONDS 동안 서버에 다시 묻지 않고 같은 오류(유량 초과면 남은 시간)를 돌려준다.
 * - 업무 호출이 토큰을 거부(AuthError)하면 그 토큰을 메모리·파일에서 버리고 한 번 다시 시도한다 (call).
 * - 동시 호출은 진행 중인 발급 하나를 기다린다 (Python 의 락과 같은 역할).
 */
import { createHash } from "node:crypto";
import { chmodSync, mkdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { AuthError, BrokerApiError, RateLimitError } from "./errors.js";

/** 발급 실패 후 서버에 다시 묻지 않는 시간 (KIS·DB 발급 1분 1건 제한에 맞춘다) */
export const FAILURE_COOLDOWN_SECONDS = 60;

/** null 이면 파일 캐시를 쓰지 않는다 (테스트) */
let cacheDir: string | null = (() => { try { return join(homedir(), ".hermetix", "tokens"); } catch { return null; } })();
export function __setTokenCacheDirForTests(dir: string | null): void { cacheDir = dir; }

/** 발급 결과 — expiresAt 은 epoch ms */
export interface IssuedToken { token: string; expiresAt: number; }

/** issue() 는 토큰을 돌려주거나 AuthError / RateLimitError 를 던진다 — 텔레메트리 "auth" 측정은 issue 가 한다 */
export class TokenManager {
  private token: string | null = null;
  private refreshAt = 0;
  /** 진행 중인 갱신 — 동시 호출이 발급을 한 번만 하게 한다 */
  private pending: Promise<string> | null = null;
  /** 마지막 발급 실패 (쿨다운 종료 시각 ms, 오류) */
  private failure: { until: number; error: BrokerApiError } | null = null;
  private readonly marginMs: number;

  constructor(
    private readonly broker: string,
    private readonly appKey: string,
    private readonly issue: () => Promise<IssuedToken>,
    refreshMarginSeconds: number,
    private readonly clock: () => number = () => Date.now(),
  ) {
    this.marginMs = refreshMarginSeconds * 1000;
  }

  get(): Promise<string> {
    if (this.token && this.clock() < this.refreshAt) return Promise.resolve(this.token);
    this.pending ??= this.refresh().finally(() => { this.pending = null; });
    return this.pending;
  }

  /** 업무 호출이 거부한 토큰을 버린다 — 다른 프로세스가 새로 저장한 토큰은 지우지 않는다 */
  invalidate(token: string): void {
    if (this.token === token) {
      this.token = null;
      this.refreshAt = 0;
    }
    const path = this.path();
    if (path !== null && this.load()?.token === token) {
      try { unlinkSync(path); } catch { /* 이미 지워졌으면 그만 */ }
    }
  }

  /** 토큰을 받아 fn 을 부르고, 업무 호출이 토큰을 거부하면 버린 뒤 한 번 다시 부른다 */
  async call<T>(fn: (token: string) => Promise<T>): Promise<T> {
    const token = await this.get();
    try {
      return await fn(token);
    } catch (e) {
      if (!(e instanceof AuthError)) throw e;
      this.invalidate(token);
      return fn(await this.get());
    }
  }

  private async refresh(): Promise<string> {
    const now = this.clock();
    if (this.token && now < this.refreshAt) return this.token;
    const cached = this.load();
    if (cached && cached.token !== this.token && now < cached.expiresAt - this.marginMs) {
      this.token = cached.token;
      this.refreshAt = cached.expiresAt - this.marginMs;
      return cached.token;
    }
    if (this.failure && now < this.failure.until) throw cooldownError(this.failure.error, (this.failure.until - now) / 1000);
    let issued: IssuedToken;
    try {
      issued = await this.issue();
    } catch (e) {
      if (e instanceof BrokerApiError) this.failure = { until: this.clock() + FAILURE_COOLDOWN_SECONDS * 1000, error: e };
      throw e;
    }
    this.failure = null;
    // 만료 전 재발급에 남은 시간 그대로의 같은 토큰을 돌려주는 서버(DB·KIS)는 여유 구간에서 매번 재발급하지 않도록 만료까지 쓴다
    const refreshAt = issued.expiresAt - this.marginMs;
    this.token = issued.token;
    this.refreshAt = refreshAt > this.clock() ? refreshAt : issued.expiresAt;
    this.save(issued.token, issued.expiresAt);
    return issued.token;
  }

  private path(): string | null {
    return cacheDir === null ? null
      : join(cacheDir, `${this.broker}-${createHash("sha256").update(this.appKey).digest("hex").slice(0, 16)}.json`);
  }

  private load(): IssuedToken | null {
    const path = this.path();
    if (path === null) return null;
    try {
      const data = JSON.parse(readFileSync(path, "utf8")) as { access_token?: unknown; expires_at?: unknown };
      const expiresAt = Number(data.expires_at ?? 0) * 1000;
      return typeof data.access_token === "string" && data.access_token && Number.isFinite(expiresAt)
        ? { token: data.access_token, expiresAt } : null;
    } catch { return null; }
  }

  private save(token: string, expiresAt: number): void {
    const path = this.path();
    if (path === null) return;
    try {
      mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
      const tmp = `${path.slice(0, -".json".length)}.${process.pid}.tmp`;
      writeFileSync(tmp, JSON.stringify({ access_token: token, expires_at: expiresAt / 1000 }), { mode: 0o600 });
      chmodSync(tmp, 0o600);
      renameSync(tmp, path);
    } catch { /* 캐시 저장 실패는 이번 호출에 영향을 주지 않는다 */ }
  }
}

/** 쿨다운 중 재요청 — 서버에 묻지 않고 같은 종류의 오류를 남은 시간과 함께 돌려준다 */
function cooldownError(failure: BrokerApiError, remainingSeconds: number): BrokerApiError {
  return failure instanceof RateLimitError
    ? new RateLimitError(failure.httpStatus, failure.errorCode, failure.message, remainingSeconds)
    : failure;
}
