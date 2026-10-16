/**
 * 설정 파일(~/.hermetix/mcp.json) 로딩 — 계좌 별칭 → SDK 클라이언트.
 *
 * - 항목 모양은 SDK 팩토리 자격 증명 그대로 + `broker`. 검증은 팩토리(`client`)에 맡긴다
 * - 문자열 값 `"env:NAME"` 은 환경변수 NAME 에서, `"keychain:NAME"` 은 macOS 키체인(서비스 `hermetix`, 계정 NAME)에서 읽는다
 *   → 평문 키가 디스크에 남지 않는다. AI 클라이언트의 파일 도구가 설정 파일을 읽어도 키는 없다
 * - 에러 메시지에 값(키·시크릿·계좌)을 싣지 않는다 — 별칭과 항목 이름만
 * - 설정이 없거나 틀려도 서버는 뜬다. tool 이 이유와 고치는 법을 돌려준다 (MCP 클라이언트는 기동 실패 이유를 잘 보여 주지 않는다)
 */
import { execFileSync } from "node:child_process";
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

/** `keychain:NAME` 이 찾는 키체인 서비스 이름 — `security add-generic-password -s hermetix -a NAME -w` */
export const KEYCHAIN_SERVICE = "hermetix";

export const CONFIG_EXAMPLE = `{
  "accounts": {
    "kis-paper": { "broker": "kis", "apiKey": "keychain:KIS_APPKEY", "apiSecret": "keychain:KIS_APPSECRET", "account": "50123456" }
  },
  "default": "kis-paper"
}`;

type Factory = (id: string, creds: Credentials) => BrokerClient;

/**
 * 키체인 항목을 읽는다. 없으면 null. macOS 외 플랫폼은 호출하지 않는다 (resolveValue 가 먼저 거른다).
 * 값은 반환만 하고 어디에도 기록하지 않는다. `security` 의 stderr 는 버린다 — 실패 메시지에 계정 이름이 섞여 나오기 때문
 */
export type KeychainReader = (service: string, account: string) => string | null;

export const readMacKeychain: KeychainReader = (service, account) => {
  try {
    return execFileSync("/usr/bin/security", ["find-generic-password", "-s", service, "-a", account, "-w"], {
      encoding: "utf-8",
      stdio: ["ignore", "pipe", "ignore"],
    }).replace(/\r?\n$/, "");
  } catch {
    return null;
  }
};

export interface LoadOptions {
  factory?: Factory;
  keychain?: KeychainReader;
  platform?: NodeJS.Platform;
}

export function loadConfig(path = defaultConfigPath(), options: LoadOptions = {}): LoadedConfig {
  const factory = options.factory ?? client;
  const resolver: Resolver = { keychain: options.keychain ?? readMacKeychain, platform: options.platform ?? process.platform };
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
      result.accounts.set(alias, buildEntry(alias, entry, factory, resolver));
    } catch (e) {
      result.invalid.set(alias, (e as Error).message);
    }
  }

  // 설정 오류로 빠진 별칭도 기본값으로 남긴다 — account 를 생략한 호출이 "별칭 없음" 대신 그 오류 이유를 받게
  const declared = (raw as { default?: unknown }).default;
  const known = (alias: string) => result.accounts.has(alias) || result.invalid.has(alias);
  if (typeof declared === "string" && declared) {
    if (known(declared)) result.defaultAlias = declared;
    else result.warnings.push(`"default" 별칭 '${declared}' 이 accounts 에 없습니다`);
  } else if (result.accounts.size + result.invalid.size === 1) {
    result.defaultAlias = [...result.accounts.keys(), ...result.invalid.keys()][0];
  }

  if (resolver.platform === "darwin" && hasPlainSecret(accounts as Record<string, unknown>)) {
    result.warnings.push(`설정 파일에 키가 평문으로 있습니다 — "keychain:이름" 으로 바꾸면 파일에 키가 남지 않습니다 (README 의 "키 보관" 절)`);
  }
  return result;
}

interface Resolver { keychain: KeychainReader; platform: NodeJS.Platform }

const SECRET_KEYS = new Set(["apiKey", "apiSecret"]);

/** apiKey/apiSecret 중 하나라도 `env:`·`keychain:` 참조가 아닌 문자열이면 평문으로 본다 */
function hasPlainSecret(accounts: Record<string, unknown>): boolean {
  return Object.values(accounts).some((entry) =>
    !!entry && typeof entry === "object" && !Array.isArray(entry) &&
    Object.entries(entry as Record<string, unknown>).some(([key, value]) =>
      SECRET_KEYS.has(key) && typeof value === "string" && value !== "" && !value.startsWith("env:") && !value.startsWith("keychain:")));
}

function buildEntry(alias: string, entry: unknown, factory: Factory, resolver: Resolver): AccountEntry {
  if (!entry || typeof entry !== "object" || Array.isArray(entry)) throw new Error(`'${alias}': 객체여야 합니다`);
  const { broker, ...rest } = entry as Record<string, unknown>;
  if (typeof broker !== "string" || !broker) throw new Error(`'${alias}': "broker" 가 필요합니다`);
  const creds: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(rest)) creds[key] = resolveValue(alias, key, value, resolver);
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

function resolveValue(alias: string, key: string, value: unknown, resolver: Resolver): unknown {
  if (typeof value !== "string") return value;
  if (value.startsWith("env:")) {
    const name = value.slice(4);
    const resolved = process.env[name];
    if (resolved === undefined || resolved === "") throw new Error(`'${alias}': ${key} 가 가리키는 환경변수 ${name} 가 비어 있습니다`);
    return resolved;
  }
  if (value.startsWith("keychain:")) {
    const name = value.slice(9);
    if (!name) throw new Error(`'${alias}': ${key} 의 keychain: 뒤에 항목 이름이 없습니다`);
    if (resolver.platform !== "darwin") throw new Error(`'${alias}': ${key} 의 keychain: 참조는 macOS 에서만 됩니다 — env: 를 쓰세요`);
    const resolved = resolver.keychain(KEYCHAIN_SERVICE, name);
    if (resolved === null || resolved === "") {
      throw new Error(`'${alias}': ${key} 가 가리키는 키체인 항목 ${name} 이 없습니다 — security add-generic-password -s ${KEYCHAIN_SERVICE} -a ${name} -w`);
    }
    return resolved;
  }
  return value;
}
