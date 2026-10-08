/**
 * tool 등록 — 전부 조회 전용. 주문 생성·취소(createOrder·cancelOrder)를 부르는 tool 은 의도적으로 없다.
 * 모든 tool 은 account(설정 별칭)를 받는다. 증권사별 tool 을 따로 두지 않는다.
 */
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import {
  AuthError, BrokerApiError, MarketClosedError, OrderNotFoundError, RateLimitError,
  type BrokerClient, type CandleInterval,
} from "hermetix";
import { CONFIG_EXAMPLE, type AccountEntry, type LoadedConfig } from "./config.js";
import { limited, maskAccount, toPlain } from "./format.js";
import { registerResources } from "./resources.js";
import { VERSION } from "./version.js";

const MAX_SYMBOLS = 20;
const MAX_CANDLES = 200;

type ToolResult = { content: { type: "text"; text: string }[]; isError?: boolean };

const ok = (value: unknown): ToolResult => ({ content: [{ type: "text", text: JSON.stringify(value, null, 2) }] });
const fail = (message: string): ToolResult => ({ content: [{ type: "text", text: message }], isError: true });

class ToolError extends Error {}

export function buildServer(config: LoadedConfig): McpServer {
  const server = new McpServer({ name: "hermetix-mcp", version: VERSION });

  const resolve = (alias: string | undefined): AccountEntry => {
    if (config.error) throw new ToolError(`${config.error}\n\n예시 (${config.path}, 권한 600):\n${CONFIG_EXAMPLE}`);
    const name = alias ?? config.defaultAlias;
    if (!name) throw new ToolError(`account 를 지정하세요. 쓸 수 있는 별칭: ${[...config.accounts.keys()].join(", ") || "(없음)"}`);
    const entry = config.accounts.get(name);
    if (entry) return entry;
    const reason = config.invalid.get(name);
    if (reason) throw new ToolError(`설정 오류 — ${reason}`);
    throw new ToolError(`모르는 별칭 '${name}'. 쓸 수 있는 별칭: ${[...config.accounts.keys()].join(", ") || "(없음)"}`);
  };

  /** 별칭을 클라이언트로 바꿔 fn 을 부르고, 예외를 사람이 읽을 한 줄로 바꾼다 */
  const run = async (alias: string | undefined, fn: (client: BrokerClient, entry: AccountEntry) => Promise<unknown>): Promise<ToolResult> => {
    let entry: AccountEntry;
    try {
      entry = resolve(alias);
    } catch (e) {
      return fail((e as Error).message);
    }
    try {
      return ok(await fn(entry.client, entry));
    } catch (e) {
      return fail(describeError(entry, e));
    }
  };

  const account = z.string().optional().describe("설정 파일의 계좌 별칭. 생략하면 default 별칭 (list_accounts 로 확인)");
  const readOnly = { readOnlyHint: true, openWorldHint: true };

  server.registerTool(
    "list_accounts",
    {
      title: "계좌 별칭 목록",
      description: "설정된 계좌 별칭과 증권사·환경(PAPER 모의 / LIVE 실전)·지원 기능. 다른 tool 의 account 인자에 별칭을 넣는다",
      inputSchema: {},
      annotations: { readOnlyHint: true, openWorldHint: false },
    },
    async () => {
      if (config.error) return fail(`${config.error}\n\n예시 (${config.path}, 권한 600):\n${CONFIG_EXAMPLE}`);
      return ok({
        default: config.defaultAlias,
        accounts: [...config.accounts.values()].map((a) => ({
          alias: a.alias,
          broker: a.broker,
          environment: a.environment,
          account: maskAccount(a.account),
          capabilities: toPlain(a.client.capabilities),
        })),
        invalid: Object.fromEntries(config.invalid),
        warnings: config.warnings,
      });
    },
  );

  server.registerTool(
    "get_quotes",
    {
      title: "현재가",
      description: `종목 현재가·호가·등락. symbols 는 증권사 종목 코드 (예: 005930, AAPL). 한 계좌가 여러 시장을 다루면 KRX:005930·US:AAPL 처럼 시장 접두. 최대 ${MAX_SYMBOLS}개. 금액은 소수 문자열`,
      inputSchema: { symbols: z.array(z.string().min(1)).min(1).max(MAX_SYMBOLS), account },
      annotations: readOnly,
    },
    ({ symbols, account }) => run(account, async (c) => toPlain(await c.getQuotes(symbols))),
  );

  server.registerTool(
    "get_candles",
    {
      title: "캔들",
      description: `OHLCV 캔들. interval 은 1m·5m·1h·1d 중 그 증권사가 지원하는 것 (list_accounts 의 candleIntervals). limit 기본 30, 최대 ${MAX_CANDLES}`,
      inputSchema: {
        symbol: z.string().min(1),
        interval: z.enum(["1m", "5m", "1h", "1d"]),
        limit: z.number().int().min(1).max(MAX_CANDLES).optional(),
        account,
      },
      annotations: readOnly,
    },
    ({ symbol, interval, limit, account }) => run(account, async (c) => {
      if (!c.capabilities.candleIntervals.has(interval as CandleInterval)) {
        throw new ToolError(`${c.capabilities.brokerId} 는 ${interval} 캔들을 지원하지 않습니다 (지원: ${[...c.capabilities.candleIntervals].join(", ")})`);
      }
      return toPlain(await c.getCandles(symbol, interval as CandleInterval, limit ?? 30));
    }),
  );

  server.registerTool(
    "get_calendar",
    { title: "장 운영일", description: "앞으로의 개장일·정규장 시간 (거래소 현지 시각)", inputSchema: { account }, annotations: readOnly },
    ({ account }) => run(account, async (c) => toPlain(await c.getCalendar())),
  );

  server.registerTool(
    "get_account",
    { title: "계좌 요약", description: "예수금·평가금액·통화. 계좌번호는 앞 4자리만 표시", inputSchema: { account }, annotations: readOnly },
    ({ account }) => run(account, async (c) => {
      const a = await c.getAccount();
      return { ...(toPlain(a) as object), accountId: maskAccount(a.accountId) };
    }),
  );

  server.registerTool(
    "get_holdings",
    { title: "보유 종목", description: "보유 수량·평균단가·평가금액·평가손익", inputSchema: { account }, annotations: readOnly },
    ({ account }) => run(account, async (c) => limited(await c.getHoldings())),
  );

  server.registerTool(
    "get_buying_power",
    { title: "매수 가능 금액", description: "주문에 쓸 수 있는 현금 (계좌 통화)", inputSchema: { account }, annotations: readOnly },
    ({ account }) => run(account, async (c) => ({ buyingPower: (await c.getBuyingPower()).toString(), currency: c.capabilities.currency })),
  );

  server.registerTool(
    "list_orders",
    { title: "주문 목록", description: "최근 주문과 상태. 조회 전용 — 이 서버로는 주문을 넣거나 취소할 수 없다", inputSchema: { account }, annotations: readOnly },
    ({ account }) => run(account, async (c) => limited(await c.getOrders())),
  );

  server.registerTool(
    "get_order",
    { title: "주문 상세", description: "orderId 로 주문 한 건의 상태·체결 수량", inputSchema: { orderId: z.string().min(1), account }, annotations: readOnly },
    ({ orderId, account }) => run(account, async (c) => toPlain(await c.getOrder(orderId))),
  );

  server.registerTool(
    "get_fills",
    { title: "체결 내역", description: "최근 체결", inputSchema: { account }, annotations: readOnly },
    ({ account }) => run(account, async (c) => limited(await c.getFills())),
  );

  registerResources(server);
  return server;
}

function describeError(entry: AccountEntry, e: unknown): string {
  const where = `${entry.alias} (${entry.broker} ${entry.environment})`;
  if (e instanceof ToolError) return e.message;
  if (e instanceof AuthError) return `인증 실패 — ${where}: 키와 환경(PAPER/LIVE)이 맞는지 확인하세요. ${detail(e)}`;
  if (e instanceof RateLimitError) {
    const wait = e.retryAfterSeconds !== null ? `${Math.ceil(e.retryAfterSeconds)}초 뒤` : "잠시 뒤";
    return `호출 한도 초과 — ${where}: ${wait} 다시 시도하세요. ${detail(e)}`;
  }
  if (e instanceof MarketClosedError) return `장 운영 시간이 아닙니다 — ${where}. ${detail(e)}`;
  if (e instanceof OrderNotFoundError) return `주문이 없습니다 — ${where}. ${detail(e)}`;
  if (e instanceof BrokerApiError) return `증권사 오류 — ${where}: HTTP ${e.httpStatus} ${detail(e)}`;
  // fetch 의 연결 실패(DNS·거부·타임아웃)는 TypeError("fetch failed") 로 온다
  if (e instanceof TypeError && e.message === "fetch failed") return `증권사 서버에 연결하지 못했습니다 — ${where}: 네트워크를 확인하세요`;
  return `호출 실패 — ${where}: ${(e as Error)?.message ?? String(e)}`;
}

const detail = (e: BrokerApiError): string => `[${e.errorCode ?? "-"}] ${e.message}`.trim();
