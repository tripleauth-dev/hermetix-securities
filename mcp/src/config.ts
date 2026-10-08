/**
 * 설정 파일(~/.hermetix/mcp.json) 로딩 — 계좌 별칭 → SDK 클라이언트.
 *
 * - 항목 모양은 SDK 팩토리 자격 증명 그대로 + `broker`. 검증은 팩토리(`client`)에 맡긴다
 * - 문자열 값 `"env:NAME"` 은 환경변수 NAME 에서 읽는다
 * - 에러 메시지에 값(키·시크릿·계좌)을 싣지 않는다 — 별칭과 항목 이름만
 * - 설정이 없거나 틀려도 서버는 뜬다. tool 이 이유와 고치는 법을 돌려준다 (MCP 클라이언트는 기동 실패 이유를 잘 보여 주지 않는다)
 */
import { readFileSync, statSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { client, type BrokerClient, type Credentials, type TradingEnvironment } from "hermetix";

export interface AccountEntry {
  alias: string;
  broker: string;
  environment: TradingEnvironment;
  /** 표시용 계좌 식별자 (마스킹 전). 설정에 없으면 null */
  account: string | null;
  client: BrokerClient;
}

export interface LoadedConfig {
  path: string;
  accounts: Map<string, AccountEntry>;
  defaultAlias: string | null;
  /** 설정을 쓸 수 없는 이유 (파일 없음·JSON 오류 등). 있으면 accounts 는 비어 있다 */
  error: string | null;
  /** 쓸 수 없는 별칭 → 이유. 다른 별칭은 정상 동작한다 */
  invalid: Map<string, string>;
  warnings: string[];
}

export const defaultConfigPath = (): string =>
  process.env.HERMETIX_MCP_CONFIG?.trim() || join(homedir(), ".hermetix", "mcp.json");

export const CONFIG_EXAMPLE = `{
  "accounts": {
    "kis-paper": { "broker": "kis", "apiKey": "env:KIS_APPKEY", "apiSecret": "env:KIS_APPSECRET", "account": "env:KIS_CANO" }
  },
  "default": "kis-paper"
}`;

type Factory = (id: string, creds: Credentials) => BrokerClient;

export function loadConfig(path = defaultConfigPath(), factory: Factory = client): LoadedConfig {
  const result: LoadedConfig = { path, accounts: new Map(), defaultAlias: null, error: null, invalid: new Map(), warnings: [] };
  let text: string;
  try {
    text = readFileSync(path, "utf-8");
  } catch {
    result.error = `설정 파일이 없습니다: ${path}`;
    return result;
  }
  try {
    const mode = statSync(path).mode;
    if (process.platform !== "win32" && (mode & 0o077) !== 0) {
      result.warnings.push(`설정 파일을 다른 사용자가 읽을 수 있습니다 — chmod 600 ${path}`);
    }
  } catch { /* 권한 확인 실패는 무시 */ }

  let raw: unknown;
  try {
    raw = JSON.parse(text);
  } catch (e) {
    result.error = `설정 파일 JSON 오류: ${path} (${(e as Error).message})`;
    return result;
  }
  const accounts = (raw as { accounts?: unknown })?.accounts;
  if (!accounts || typeof accounts !== "object" || Array.isArray(accounts)) {
    result.error = `설정 파일에 "accounts" 객체가 없습니다: ${path}`;
    return result;
  }

  for (const [alias, entry] of Object.entries(accounts as Record<string, unknown>)) {
    try {
      result.accounts.set(alias, buildEntry(alias, entry, factory));
    } catch (e) {
      result.invalid.set(alias, (e as Error).message);
    }
  }

  const declared = (raw as { default?: unknown }).default;
  if (typeof declared === "string" && declared) {
    if (result.accounts.has(declared)) result.defaultAlias = declared;
    else result.warnings.push(`"default" 별칭 '${declared}' 을 쓸 수 없습니다`);
  } else if (result.accounts.size === 1) {
    result.defaultAlias = [...result.accounts.keys()][0];
  }
  return result;
}

function buildEntry(alias: string, entry: unknown, factory: Factory): AccountEntry {
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) throw new Error(`'${alias}': 객체여야 합니다`);
  const { broker, ...rest } = entry as Record<string, unknown>;
  if (typeof broker !== "string" || !broker) throw new Error(`'${alias}': "broker" 가 필요합니다`);
  const creds: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(rest)) creds[key] = resolveValue(alias, key, value);
  let built: BrokerClient;
  try {
    built = factory(broker, creds as unknown as Credentials);
  } catch (e) {
    // 팩토리 메시지는 항목 이름만 담는다 (값을 싣지 않음)
    throw new Error(`'${alias}': ${(e as Error).message}`);
  }
  return {
    alias,
    broker: built.capabilities.brokerId,
    environment: built.environment ?? "PAPER",
    account: typeof creds.account === "string" && creds.account ? creds.account : null,
    client: built,
  };
}

function resolveValue(alias: string, key: string, value: unknown): unknown {
  if (typeof value !== "string" || !value.startsWith("env:")) return value;
  const name = value.slice(4);
  const resolved = process.env[name];
  if (resolved === undefined || resolved === "") throw new Error(`'${alias}': ${key} 가 가리키는 환경변수 ${name} 가 비어 있습니다`);
  return resolved;
}
