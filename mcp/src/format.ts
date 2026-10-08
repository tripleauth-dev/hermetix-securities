/** SDK 모델 → tool 출력 JSON. Decimal 은 문자열, 시각은 ISO-8601, Set 은 배열. 계좌 식별자는 마스킹 */
import { Decimal } from "hermetix";

export const LIST_LIMIT = 50;

export function toPlain(value: unknown): unknown {
  if (value === null || value === undefined) return value ?? null;
  if (Decimal.isDecimal(value)) return (value as Decimal).toString();
  if (value instanceof Date) return Number.isNaN(value.getTime()) ? null : value.toISOString();
  if (value instanceof Set) return [...value].map(toPlain);
  if (Array.isArray(value)) return value.map(toPlain);
  if (typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) out[k] = toPlain(v);
    return out;
  }
  return value;
}

/**
 * 앞 4자만 남긴다 — 어느 계좌인지 구분은 되고 전체 번호는 대화에 남지 않게.
 * 4자 이하는 계좌번호가 아니라 순번(토스 accountSeq 등)이라 그대로 둔다
 */
export function maskAccount(id: string | null | undefined): string | null {
  if (!id) return null;
  if (id.length <= 4) return id;
  return id.slice(0, 4) + "*".repeat(id.length - 4);
}

/** 목록을 limit 건으로 자르고 잘렸는지 표시 */
export function limited<T>(items: T[], limit = LIST_LIMIT): { items: unknown[]; total: number; truncated: boolean } {
  return { items: items.slice(0, limit).map(toPlain), total: items.length, truncated: items.length > limit };
}
