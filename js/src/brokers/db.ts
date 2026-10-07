/**
 * DB증권 REST OpenAPI 어댑터.
 *
 * ⚠️ 문서 기반 구현 (실측 전) — 공식 SDK(DBsecurities/dbsec-open-api) 소스·예제 docstring 과 포털 문서에서
 * 엔드포인트·필드명·에러 코드를 역추적했다. 모의서버 실측 전까지 상태는 "미검증".
 * - 모든 API 는 POST, 본문 {"In": {...}}, 응답 rsp_cd/rsp_msg + Out(객체 또는 배열) / Out1. TR 코드는 문서용, 경로로 식별
 * - 헤더 authorization: Bearer + cont_yn/cont_key (+ 법인만 mac_address). appkey 헤더 없음
 * - 토큰 POST /oauth2/token 은 form-urlencoded(appsecretkey), 24h, 발급 1분 1건(초과 시 403 + IGW00201 → RateLimitError).
 *   발급 토큰은 ~/.hermetix/tokens/db-<키 해시>.json 에 저장해 프로세스 간 재사용하고, 발급 실패 후 60초는 서버에 다시 묻지 않는다
 * - 운영/모의 같은 호스트 — 모의 키로만 분기. HTTP 200 + rsp_cd != 00000 이 업무 오류(이때 Out 없음)
 * - 앱 20 TPS 이지만 잔고·체결 2 TPS, 예수금 1 TPS → 500ms 쓰로틀 + IGW00201 지수 백오프
 * 미확인(실측 필요): 응답 숫자의 JSON 타입, IsuNo 의 A 접두 여부, 일봉 정렬(최신일 우선 추정), PrdyVrss 부호 여부
 */
import { instrumentBroker, type BrokerUsage } from "../telemetry.js";
import { Decimal } from "decimal.js";
import { MarketStream, RateLimiter, StreamingBrokerClient, httpJson, krxCalendar, krxTickRound } from "../broker.js";
import {
  AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, MarketClosedError, OrderNotFoundError, RateLimitError,
} from "../errors.js";
import type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, Quote, TradingEnvironment,
} from "../models.js";
import { symbolCodeFor } from "../models.js";
import { FAILURE_COOLDOWN_SECONDS, TokenManager } from "../tokens.js";
import { DbMarketStream } from "./dbStream.js";

const AUTH_CODES = new Set(["IGW00121", "IGW00122", "IGW00123", "IGW40342"]);
const MARKET_CLOSED_CODES = new Set(["2611", "3589", "3590", "3563"]);
const INSUFFICIENT_CODES = new Set(["1584", "2714", "2752", "M100"]);
const INVALID_ORDER_CODES = new Set(["2706", "3180", "3181"]);
const ORDER_NOT_FOUND_CODES = new Set(["3056", "3416"]);

/** 토큰 발급은 1분 1건 (초과 시 HTTP 403 + IGW00201) — 쿨다운·파일 캐시는 TokenManager */
const TOKEN_REFRESH_MARGIN_SECONDS = 600;

const num = (v: unknown): Decimal | null => {
  if (v === null || v === undefined) return null;
  const text = String(v).trim().replace(/,/g, "");
  if (!text) return null;
  try { return new Decimal(text); } catch { return null; }
};
const pct = (v: unknown): Decimal | null => { const r = num(v); return r ? r.div(100) : null; };
const positive = (v: unknown): Decimal | null => { const d = num(v); return d && d.gt(0) ? d : null; };

/** 계좌·주문계 IsuNo 는 A005930 형태일 수 있다 → 6자리 코드 */
export const dbNormalizeCode = (raw: unknown): string => {
  const text = String(raw ?? "").trim();
  return text.length === 7 && text[0] === "A" ? text.slice(1) : text;
};

const kstDate = (offsetDays: number): string => {
  const d = new Date(Date.now() + offsetDays * 86_400_000);
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Seoul" }).format(d).replace(/-/g, "");
};
const sameNo = (a: unknown, b: unknown) => String(a ?? "").trim().replace(/^0+/, "") === String(b ?? "").trim().replace(/^0+/, "");
const sideOf = (row: Record<string, unknown>): OrderSide => String(row.BnsTpCode ?? "").trim() === "2" ? "BUY" : "SELL";
const remainingOf = (row: Record<string, unknown>): Decimal =>
  num(row.MrcAbleQty) ?? (num(row.OrdQty) ?? new Decimal(0)).minus(num(row.AllExecQty) ?? 0).minus(num(row.MrcQty) ?? 0);

export class DbClient implements StreamingBrokerClient {
  /** 실시간 — 운영 7070 / 모의 17070. 접속 후 10초 내 첫 전송, 계좌당 세션 2개·종목 50개, 연결 6회/분 (문서 기반, 실측 전) */
  static readonly PAPER_WS_URL = "wss://openapi.dbsec.co.kr:17070/websocket";
  static readonly LIVE_WS_URL = "wss://openapi.dbsec.co.kr:7070/websocket";

  /** 사용량 텔레메트리 핸들 — 생성자 끝에서 공개 메서드를 계측하며 만든다 (docs/telemetry.md) */
  private readonly usage: BrokerUsage;
  readonly capabilities: BrokerCapabilities = {
    brokerId: "db", market: "KRX", currency: "KRW",
    candleIntervals: new Set<CandleInterval>(["1d"]),
    clientOrderId: false, nativeBracket: false, fractionalShares: false, serverOpenOrders: true,
    environments: new Set<TradingEnvironment>(["PAPER", "LIVE"]),
    streams: new Set(["TRADES", "ORDER_BOOK", "ORDER_EVENTS"]), // 문서 기반, 실측 전
  };

  readonly wsUrl: string;
  private readonly tokens: TokenManager;
  private readonly limiter: RateLimiter;

  /** 운영/모의는 같은 호스트 — 모의투자용 키로만 분기된다. environment 는 엔진의 실전 게이트용 선언이다 */
  constructor(
    private readonly appKey: string,
    private readonly appSecret: string,
    readonly baseUrl: string = "https://openapi.dbsec.co.kr:8443",
    private readonly macAddress = "",
    private readonly marketDivCode = "J",
    throttleMs = 500,
    readonly environment: TradingEnvironment = "PAPER",
    wsUrl = "",
  ) {
    this.limiter = new RateLimiter(throttleMs, 4, (attempt) => 1000 * 2 ** (attempt - 1));
    this.wsUrl = wsUrl || (environment === "LIVE" ? DbClient.LIVE_WS_URL : DbClient.PAPER_WS_URL);
    this.tokens = new TokenManager("db", appKey, () => this.issueToken(), TOKEN_REFRESH_MARGIN_SECONDS);
    this.usage = instrumentBroker(this);
  }

  /** 웹소켓은 REST 접근토큰을 매 메시지 헤더에 싣는다 (문서 기반, 실측 전) */
  openStream(): MarketStream {
    return new DbMarketStream({ usage: this.usage, wsUrl: this.wsUrl, token: () => this.getToken() });
  }

  // ---------------------------------------------------------------- market

  async getQuotes(symbols: string[]): Promise<Quote[]> {
    const quotes: Quote[] = [];
    for (const symbol of symbols) {
      const out = ((await this.call("/api/v1/quote/kr-stock/inquiry/price", {
        InputCondMrktDivCode: this.marketDivCode, InputIscd1: symbolCodeFor(this.capabilities, symbol),
      })).Out ?? {}) as Record<string, unknown>;
      quotes.push({
        symbol, price: num(out.Prpr) ?? new Decimal(0), bidPrice: positive(out.Bidp1), askPrice: positive(out.Askp1),
        volume: Number(num(out.AcmlVol) ?? 0), change: num(out.PrdyVrss), changeRate: pct(out.PrdyCtrt), timestamp: new Date(),
      });
    }
    return quotes;
  }

  async getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]> {
    if (interval !== "1d") throw new Error("DB 어댑터는 일봉(1d)만 지원합니다.");
    const count = limit ?? 30;
    const rows = ((await this.call("/api/v1/quote/kr-chart/day", {
      InputOrgAdjPrc: "1", InputCondMrktDivCode: this.marketDivCode, InputIscd1: symbolCodeFor(this.capabilities, symbol),
      InputDate1: kstDate(-(Math.floor(count * 16 / 10) + 10)), InputDate2: kstDate(0),
    })).Out ?? []) as Record<string, unknown>[];
    const candles: Candle[] = [];
    for (const r of rows) {
      const date = String(r.Date ?? "").trim();
      if (date.length !== 8) continue;
      candles.push({
        timestamp: new Date(`${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}T00:00:00+09:00`),
        open: num(r.Oprc) ?? new Decimal(0), high: num(r.Hprc) ?? new Decimal(0), low: num(r.Lprc) ?? new Decimal(0),
        close: num(r.Prpr) ?? new Decimal(0), volume: Number(num(r.CntgVol) ?? num(r.AcmlVol) ?? 0), // 2024 샘플은 AcmlVol
      });
    }
    candles.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
    return candles.slice(-count);
  }

  async getCalendar(): Promise<MarketDay[]> { return krxCalendar(); }

  // --------------------------------------------------------------- account

  async getAccount(): Promise<Account> {
    const out = ((await this.balance()).Out ?? {}) as Record<string, unknown>;
    const portfolio = num(out.DpsastAmt) ?? new Decimal(0); // 예탁자산 = 현금 + 평가
    const cash = num(out.Dps2) ?? portfolio.minus(num(out.TotEvalAmt) ?? 0);
    return { accountId: `db-${this.environment.toLowerCase()}`, currency: "KRW", cash, portfolioValue: portfolio, status: "ACTIVE", name: null };
  }

  async getHoldings(): Promise<Holding[]> {
    const rows = ((await this.balance()).Out1 ?? []) as Record<string, unknown>[];
    const holdings: Holding[] = [];
    for (const row of rows) {
      const quantity = num(row.BalQty0) ?? num(row.BalQty);
      if (!quantity || quantity.lte(0)) continue;
      holdings.push({
        symbol: dbNormalizeCode(row.IsuNo), quantity, avgEntryPrice: num(row.ExecPrc) ?? new Decimal(0),
        currentPrice: num(row.NowPrc), marketValue: num(row.EvalAmt), unrealizedPnl: num(row.EvalPnlAmt), unrealizedPnlRate: pct(row.Ernrat),
      });
    }
    return holdings;
  }

  async getBuyingPower(): Promise<Decimal> {
    const out = ((await this.call("/api/v1/trading/kr-stock/inquiry/acnt-deposit", {})).Out1 ?? {}) as Record<string, unknown>;
    return num(out.DpsBalAmt) ?? num(out.WthdwAbleAmt) ?? new Decimal(0);
  }

  // ---------------------------------------------------------------- orders

  async createOrder(request: CreateOrderRequest): Promise<Order> {
    const code = symbolCodeFor(this.capabilities, request.symbol);
    const isLimit = request.orderType === "LIMIT";
    const out = ((await this.call("/api/v1/trading/kr-stock/order", {
      IsuNo: code, TrchNo: 1, OrdQty: request.quantity.toNumber(),
      OrdPrc: isLimit ? krxTickRound(request.limitPrice!).toNumber() : 0, // KRX 호가단위 보정(2706)
      BnsTpCode: request.side === "BUY" ? "2" : "1", OrdprcPtnCode: isLimit ? "00" : "03",
      MgntrnCode: "000", LoanDt: "00000000", OrdCndiTpCode: "0",
    })).Out ?? {}) as Record<string, unknown>;
    const orderId = String(out.OrdNo ?? "").trim();
    if (!orderId) throw new BrokerApiError(200, null, "DB 주문 응답에 OrdNo 가 없습니다");
    return {
      orderId, status: "SUBMITTED", symbol: code, side: request.side, orderType: request.orderType,
      quantity: request.quantity, limitPrice: request.limitPrice ?? null, filledQuantity: new Decimal(0),
      clientOrderId: request.clientOrderId ?? null, submittedAt: new Date(),
    };
  }

  async getOrders(): Promise<Order[]> {
    return (await this.historyRows()).map((r) => this.toOrder(r)).filter((o) => o.status === "SUBMITTED" || o.status === "PARTIALLY_FILLED" || o.status === "PENDING_CANCEL");
  }

  async getOrder(orderId: string): Promise<Order> {
    const row = (await this.historyRows()).find((r) => sameNo(r.OrdNo, orderId));
    return row ? this.toOrder(row) : { orderId, status: "CANCELED" };
  }

  async cancelOrder(orderId: string): Promise<Order> {
    const row = (await this.historyRows()).find((r) => sameNo(r.OrdNo, orderId));
    if (!row) throw new OrderNotFoundError("order-not-found", `DB 당일 주문에서 찾을 수 없습니다: ${orderId}`);
    await this.call("/api/v1/trading/kr-stock/order-cancel", {
      OrgOrdNo: Number(orderId.replace(/^0+/, "") || "0"), IsuNo: dbNormalizeCode(row.IsuNo), OrdQty: remainingOf(row).toNumber(),
    });
    return { orderId, status: "PENDING_CANCEL", canceledAt: new Date() };
  }

  async getFills(): Promise<Fill[]> {
    return (await this.historyRows())
      .filter((r) => (num(r.AllExecQty) ?? new Decimal(0)).gt(0))
      .map((r) => ({
        fillId: null, orderId: String(r.OrdNo ?? "").trim(), symbol: dbNormalizeCode(r.IsuNo), side: sideOf(r),
        quantity: num(r.AllExecQty), price: num(r.AvrExecPrc),
      }));
  }

  // -------------------------------------------------------------- internal

  private balance(): Promise<Record<string, unknown>> {
    return this.call("/api/v1/trading/kr-stock/inquiry/balance", { QryTpCode0: "0" });
  }

  private async historyRows(): Promise<Record<string, unknown>[]> {
    return ((await this.call("/api/v1/trading/kr-stock/inquiry/transaction-history", {
      SorTpYn: "2", ExecYn: "0", TrdMktCode: "0", BnsTpCode: "0", IsuTpCode: "0", QryTp: "0",
    })).Out1 ?? []) as Record<string, unknown>[];
  }

  private toOrder(row: Record<string, unknown>): Order {
    const ordQty = num(row.OrdQty) ?? new Decimal(0);
    const filled = num(row.AllExecQty) ?? new Decimal(0);
    const remaining = remainingOf(row);
    const trx = String(row.OrdTrxPtnCode ?? "").trim();
    let status: OrderStatus;
    if (trx === "9") status = "PENDING_CANCEL";
    else if (trx === "8") status = "CANCELED";
    else if (remaining.gt(0) && filled.gt(0)) status = "PARTIALLY_FILLED";
    else if (remaining.gt(0)) status = "SUBMITTED";
    else if (filled.gt(0) && filled.gte(ordQty)) status = "FILLED";
    else if (filled.gt(0)) status = "PARTIALLY_FILLED";
    else status = "CANCELED";
    return {
      orderId: String(row.OrdNo ?? "").trim(), status, symbol: dbNormalizeCode(row.IsuNo), side: sideOf(row),
      orderType: String(row.OrdprcPtnCode ?? "") === "03" ? "MARKET" : "LIMIT",
      quantity: ordQty, limitPrice: positive(row.OrdPrc), filledQuantity: filled, avgFillPrice: positive(row.AvrExecPrc),
    };
  }

  private call(path: string, body: Record<string, unknown>): Promise<Record<string, unknown>> {
    return this.limiter.execute(() => this.tokens.call((token) => this.callOnce(path, body, token)), `DB ${path}`);
  }

  private async callOnce(path: string, body: Record<string, unknown>, token: string): Promise<Record<string, unknown>> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json; charset=utf-8", authorization: `Bearer ${token}`, cont_yn: "N", cont_key: "",
    };
    if (this.macAddress) headers.mac_address = this.macAddress;
    const [status, parsed, resHeaders] = await httpJson(this.baseUrl + path, { method: "POST", headers, body: JSON.stringify({ In: body }) });
    const code = String(parsed.rsp_cd ?? "").trim();
    const rspMsg = String(parsed.rsp_msg ?? "");
    const msg = `DB(${path}) [${code}] ${rspMsg}`.trim();
    if (status < 200 || status >= 300 || code !== "00000") {
      const retryAfter = Number(resHeaders?.["retry-after"]);
      if (code === "IGW00201" || status === 429) throw new RateLimitError(status, code, msg, Number.isFinite(retryAfter) ? retryAfter : null);
      if (AUTH_CODES.has(code) || status === 401) throw new AuthError(status, code, msg);
      if (MARKET_CLOSED_CODES.has(code)) throw new MarketClosedError(status, code, msg);
      if (INSUFFICIENT_CODES.has(code) || rspMsg.includes("부족")) throw new InsufficientFundsError(status, code, msg);
      if (INVALID_ORDER_CODES.has(code)) throw new InvalidOrderError(status, code, msg);
      if (ORDER_NOT_FOUND_CODES.has(code)) throw new OrderNotFoundError(code, msg);
      throw new BrokerApiError(status, code, msg);
    }
    return parsed;
  }

  private getToken(): Promise<string> {
    return this.tokens.get();
  }

  private issueToken(): Promise<{ token: string; expiresAt: number }> {
    return this.usage.measure("auth", () => this.requestToken());
  }

  private async requestToken(): Promise<{ token: string; expiresAt: number }> {
    await this.limiter.throttle.wait();
    const form = new URLSearchParams({ grant_type: "client_credentials", appkey: this.appKey, appsecretkey: this.appSecret, scope: "oob" }); // JSON/appsecret 은 IGW00133
    const [status, body] = await httpJson(`${this.baseUrl}/oauth2/token`, {
      method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body: form.toString(),
    });
    if (status !== 200 || typeof body.access_token !== "string") {
      const code = String(body.rsp_cd ?? body.error ?? "");
      const detail = String(body.rsp_msg ?? body.error_description ?? "");
      if (code === "IGW00201" || status === 429) {
        throw new RateLimitError(status, code || null, `DB 토큰 발급 유량 초과(${code}): ${detail} (발급은 1분당 1회 제한)`, FAILURE_COOLDOWN_SECONDS);
      }
      throw new AuthError(status, code || null, `DB 토큰 발급 실패(${code}): ${detail}`);
    }
    return { token: body.access_token, expiresAt: Date.now() + Number(body.expires_in ?? 86400) * 1000 };
  }
}
