/**
 * NH투자증권 NH PLUG(나무 PLUG) REST OpenAPI 어댑터.
 *
 * ⚠️ 문서 기반 구현 (실측 전) — 공식 Python SDK(PLUG-OpenAPI/nhplug-sdk)와 포털 OpenAPI 문서(2026-09-08)에서
 * 엔드포인트·필드명·에러 코드를 역추적했다. 모의서버 실측 전까지 상태는 "미검증".
 * - 모든 API 는 POST, 본문 {"Input_0": {...}}, 응답 rsp_cd/rsp_msg + Output_0(+Output_1). TR 헤더 없이 경로로 식별
 * - 인증 헤더 authorization: Bearer + x-client-id / x-client-secret. 토큰은 운영 호스트 /oauth2/token 에서 쿼리스트링으로 발급(24h)
 * - 모의/운영은 호스트로만 구분(moapi / api). 계좌 acct_type 은 환경과 맞아야 한다(모의 03, 운영 01)
 * - HTTP 200 이어도 업무 오류 가능 — rsp_cd ∈ {00000,00166,00221,13578} 또는 rsp_msg 에 "완료" 면 성공(SDK 판정식)
 * - 초당 5회 한도 → 250ms 쓰로틀 + 429(IGW4290x, Retry-After) 재시도
 * 미확인(실측 필요): mkt_orr_no 와 itg_orr_no 의 동일 여부, ost_cns_dit 코드 의미, 등락률·수익률 단위(% 추정), 응답 숫자 타입
 */
import { instrumentBroker, type BrokerUsage } from "../telemetry.js";
import { Decimal } from "decimal.js";
import { MarketStream, RateLimiter, StreamingBrokerClient, httpJson, krxCalendar, krxTickRound, kstYyyymmdd } from "../broker.js";
import {
  AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, OrderNotFoundError, RateLimitError,
} from "../errors.js";
import type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, Quote, TradingEnvironment,
} from "../models.js";
import { symbolCodeFor } from "../models.js";
import { FAILURE_COOLDOWN_SECONDS, TokenManager } from "../tokens.js";
import { NhMarketStream } from "./nhStream.js";

const SUCCESS_CODES = new Set(["00000", "00166", "00221", "13578", "00165", "00218"]);
const FALLING_SIGNS = new Set(["4", "5", "8", "9"]);

/** 숫자/문자열 어느 쪽으로 와도 파싱 — 문서상 와이어 타입이 API 마다 다르다 */
const num = (v: unknown): Decimal | null => {
  if (v === null || v === undefined) return null;
  const text = String(v).trim().replace(/,/g, "");
  if (!text) return null;
  try { return new Decimal(text); } catch { return null; }
};
const pct = (v: unknown): Decimal | null => { const r = num(v); return r ? r.div(100) : null; };
const positive = (v: unknown): Decimal | null => { const d = num(v); return d && d.gt(0) ? d : null; };

/** 계좌·주문 API 의 iem_cd 는 길이 12(선행 0)일 수 있고 A 접두가 붙을 수 있다 → 6자리 코드 */
export const nhNormalizeCode = (raw: unknown): string => {
  const text = String(raw ?? "").trim().replace(/^A/, "");
  return text.length > 6 && /^\d+$/.test(text) ? text.slice(-6) : text;
};

const parseDate = (raw: unknown): Date | null => {
  const t = String(raw ?? "").trim();
  if (/^\d{8}$/.test(t)) return new Date(`${t.slice(0, 4)}-${t.slice(4, 6)}-${t.slice(6, 8)}T00:00:00+09:00`);
  if (/^\d{4}-\d{2}-\d{2}$/.test(t)) return new Date(`${t}T00:00:00+09:00`);
  if (/^\d{2}\/\d{2}\/\d{2}$/.test(t)) return new Date(`20${t.slice(0, 2)}-${t.slice(3, 5)}-${t.slice(6, 8)}T00:00:00+09:00`);
  return null;
};

const sameNo = (a: unknown, b: unknown) => String(a ?? "").trim().replace(/^0+/, "") === String(b ?? "").trim().replace(/^0+/, "");
const sideOf = (row: Record<string, unknown>): OrderSide => String(row.sby_dit_cd_nm ?? "").includes("매수") ? "BUY" : "SELL";

export class NhClient implements StreamingBrokerClient {
  static readonly PAPER_URL = "https://moapi.nhplug.com:8443";
  static readonly LIVE_URL = "https://api.nhplug.com:8443";
  static readonly AUTH_URL = "https://api.nhplug.com:8443";
  /** 실시간 — 모의는 포털 가이드에 시세 채널 "미제공" 표기라 통보만 올 수 있다 (실측 전). 운영 7070 은 국내 시세·통보 공용, 세션당 등록 10건(SDK 실측)/30건(공식), 앱키당 세션 2개 */
  static readonly PAPER_WS_URL = "wss://moapi.nhplug.com:17070/websocket";
  static readonly LIVE_WS_URL = "wss://api.nhplug.com:7070/websocket";

  /** 사용량 텔레메트리 핸들 — 생성자 끝에서 공개 메서드를 계측하며 만든다 (docs/telemetry.md) */
  private readonly usage: BrokerUsage;
  readonly capabilities: BrokerCapabilities = {
    brokerId: "nh", market: "KRX", currency: "KRW",
    candleIntervals: new Set<CandleInterval>(["1d"]),
    clientOrderId: false, nativeBracket: false, fractionalShares: false, serverOpenOrders: true,
    environments: new Set<TradingEnvironment>(["PAPER", "LIVE"]),
    streams: new Set(["TRADES", "ORDER_BOOK", "ORDER_EVENTS"]), // 문서 기반, 실측 전
  };

  readonly baseUrl: string;
  readonly wsUrl: string;
  private readonly tokens: TokenManager;
  private accountNo: string;
  private readonly limiter: RateLimiter;

  /** accountNo 를 비우면 /n2/acctinfo 에서 환경에 맞는 acct_type(모의 03 / 운영 01)의 첫 계좌를 고른다 */
  constructor(
    private readonly appKey: string,
    private readonly appSecret: string,
    accountNo = "",
    baseUrl = "",
    readonly authUrl: string = NhClient.AUTH_URL,
    private readonly marketCd = "KRX",
    private readonly orderMarketCd = "KRX",
    throttleMs = 250,
    readonly environment: TradingEnvironment = "PAPER",
    wsUrl = "",
  ) {
    this.accountNo = accountNo;
    this.baseUrl = baseUrl || (environment === "LIVE" ? NhClient.LIVE_URL : NhClient.PAPER_URL);
    this.wsUrl = wsUrl || (environment === "LIVE" ? NhClient.LIVE_WS_URL : NhClient.PAPER_WS_URL);
    this.limiter = new RateLimiter(throttleMs, 3, (attempt) => 1000 * attempt);
    this.tokens = new TokenManager("nh", appKey, () => this.issueToken(), 300);
    this.usage = instrumentBroker(this);
  }

  // ---------------------------------------------------------------- market

  async getQuotes(symbols: string[]): Promise<Quote[]> {
    const quotes: Quote[] = [];
    for (const symbol of symbols) {
      const out = ((await this.call("/krstock/quote/v1/currentPrice", { market_cd: this.marketCd, iem_cd: symbolCodeFor(this.capabilities, symbol) })).Output_0 ?? {}) as Record<string, unknown>;
      const sign = String(out.prdy_vrss_sign ?? "");
      let change = num(out.prdy_vrss);
      let rate = num(out.prdy_ctrt);
      if (FALLING_SIGNS.has(sign)) {
        if (change && change.gt(0)) change = change.neg();
        if (rate && rate.gt(0)) rate = rate.neg();
      }
      quotes.push({
        symbol, price: num(out.stck_prpr) ?? new Decimal(0),
        bidPrice: positive(out.bidp), askPrice: positive(out.askp),
        volume: Number(num(out.acml_vol) ?? 0), change, changeRate: rate ? rate.div(100) : null,
        timestamp: new Date(),
      });
    }
    return quotes;
  }

  async getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]> {
    if (interval !== "1d") throw new Error("NH 어댑터는 일봉(1d)만 지원합니다.");
    const count = limit ?? 30;
    const rows = ((await this.call("/krstock/quote/v1/currentDaily", {
      market_cd: this.marketCd, iem_cd: symbolCodeFor(this.capabilities, symbol), array_cnt: String(count),
    })).Output_0 ?? []) as Record<string, unknown>[];
    const candles: Candle[] = [];
    for (const r of rows) {
      const ts = parseDate(r.bsop_date);
      if (!ts) continue;
      candles.push({
        timestamp: ts, open: num(r.stck_oprc) ?? new Decimal(0), high: num(r.stck_hgpr) ?? new Decimal(0),
        low: num(r.stck_lwpr) ?? new Decimal(0), close: num(r.stck_clpr) ?? new Decimal(0), volume: Number(num(r.acml_vol) ?? 0),
      });
    }
    candles.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime()); // 문서상 최신일 우선 → 과거→최신
    return candles.slice(-count);
  }

  async getCalendar(): Promise<MarketDay[]> { return krxCalendar(); }

  // --------------------------------------------------------------- account

  async getAccount(): Promise<Account> {
    const summary = ((await this.balance()).Output_0 ?? {}) as Record<string, unknown>;
    const cash = num(summary.dca) ?? new Decimal(0);
    const portfolio = positive(summary.tot_aet_amt) ?? cash.plus(num(summary.tot_eal_amt) ?? 0);
    return { accountId: await this.account(), currency: "KRW", cash, portfolioValue: portfolio, status: "ACTIVE", name: null };
  }

  async getHoldings(): Promise<Holding[]> {
    const rows = ((await this.balance()).Output_1 ?? []) as Record<string, unknown>[];
    const holdings: Holding[] = [];
    for (const row of rows) {
      const quantity = num(row.itg_bnc_qty);
      if (!quantity || quantity.lte(0)) continue;
      holdings.push({
        symbol: nhNormalizeCode(row.iem_cd), quantity, avgEntryPrice: num(row.phs_pr) ?? new Decimal(0),
        currentPrice: num(row.now_pr), marketValue: num(row.eal_amt), unrealizedPnl: num(row.eal_pls_amt), unrealizedPnlRate: pct(row.pft_rt),
      });
    }
    return holdings;
  }

  async getBuyingPower(): Promise<Decimal> {
    const summary = ((await this.balance()).Output_0 ?? {}) as Record<string, unknown>;
    return num(summary.orr_pbl_amt) ?? num(summary.dca) ?? new Decimal(0);
  }

  // ---------------------------------------------------------------- orders

  async createOrder(request: CreateOrderRequest): Promise<Order> {
    const code = symbolCodeFor(this.capabilities, request.symbol);
    const isLimit = request.orderType === "LIMIT";
    const body: Record<string, unknown> = {
      act_no: await this.account(), iem_cd: code, orr_qty: request.quantity.toNumber(),
      nmn_pr_tp_cd: isLimit ? "01" : "05", orr_cnd_dit_cd: "00", ssl_nmn_pr_dit_cd: "00",
      rmt_mkt_cd: this.orderMarketCd, sor_mkt_sli_yn: "N",
    };
    if (isLimit) body.orr_pr = krxTickRound(request.limitPrice!).toNumber(); // KRX 호가단위 보정
    const out = ((await this.call(request.side === "BUY" ? "/krstock/order/v1/cashBuy" : "/krstock/order/v1/cashSell", body)).Output_0 ?? {}) as Record<string, unknown>;
    const orderId = String(out.mkt_orr_no ?? "").trim();
    if (!orderId) throw new BrokerApiError(200, null, "NH 주문 응답에 mkt_orr_no 가 없습니다");
    return {
      orderId, status: "SUBMITTED", symbol: code, side: request.side, orderType: request.orderType,
      quantity: request.quantity, limitPrice: request.limitPrice ?? null, filledQuantity: new Decimal(0),
      clientOrderId: request.clientOrderId ?? null, submittedAt: new Date(),
    };
  }

  async getOrders(): Promise<Order[]> {
    return (await this.executionRows()).map((r) => this.toOrder(r)).filter((o) => o.status === "SUBMITTED" || o.status === "PARTIALLY_FILLED" || o.status === "PENDING_CANCEL");
  }

  async getOrder(orderId: string): Promise<Order> {
    const row = (await this.executionRows()).find((r) => sameNo(r.itg_orr_no, orderId));
    return row ? this.toOrder(row) : { orderId, status: "CANCELED" };
  }

  async cancelOrder(orderId: string): Promise<Order> {
    const row = (await this.executionRows()).find((r) => sameNo(r.itg_orr_no, orderId));
    if (!row) throw new OrderNotFoundError("order-not-found", `NH 당일 주문에서 찾을 수 없습니다: ${orderId}`);
    await this.call("/krstock/order/v1/cancel", {
      act_no: await this.account(), org_mkt_orr_no: Number(orderId.replace(/^0+/, "") || "0"), all_pat_dit_cd: "1", iem_cd: nhNormalizeCode(row.iem_cd),
    });
    return { orderId, status: "CANCELED", canceledAt: new Date() };
  }

  async getFills(): Promise<Fill[]> {
    return (await this.executionRows())
      .filter((r) => (num(r.tot_cns_qty) ?? new Decimal(0)).gt(0))
      .map((r) => ({
        fillId: null, orderId: String(r.itg_orr_no ?? "").trim(), symbol: nhNormalizeCode(r.iem_cd), side: sideOf(r),
        quantity: num(r.tot_cns_qty), price: num(r.cns_avg_uit_pr),
      }));
  }

  // ---------------------------------------------------------------- stream

  /** 웹소켓은 REST 접근토큰을 매 메시지 헤더에 싣는다. 채널은 marketCd(KRX oc/ob, NXT nc/nb, UNT mc/mb) 로 고른다 */
  openStream(): MarketStream {
    return new NhMarketStream({ usage: this.usage, wsUrl: this.wsUrl, token: () => this.getToken(), marketCd: this.marketCd, accountNo: this.accountNo });
  }

  // -------------------------------------------------------------- internal

  private async balance(): Promise<Record<string, unknown>> {
    return this.call("/krstock/inquiry/v1/balance", { act_no: await this.account(), bnc_bse_cd: "5", ltg_aot_dit_cd: "9", aet_bse: "2", qut_dit_cd: this.marketCd });
  }

  /** 당일 주문·체결 전체(ost_cns_dit=0) — 문서마다 체결구분 코드 의미가 달라 전체를 받아 ny_cns_qty 로 가른다 */
  private async executionRows(): Promise<Record<string, unknown>[]> {
    return ((await this.call("/krstock/inquiry/v1/dailyOrderExecution", {
      orr_dt: kstYyyymmdd(), act_no: await this.account(), ost_cns_dit: "0", orr_mkt_cd: "00",
    })).Output_1 ?? []) as Record<string, unknown>[];
  }

  private toOrder(row: Record<string, unknown>): Order {
    const ordQty = num(row.orr_qty) ?? new Decimal(0);
    const filled = num(row.tot_cns_qty) ?? new Decimal(0);
    const remaining = num(row.ny_cns_qty) ?? ordQty.minus(filled);
    const canceled = num(row.can_qty) ?? new Decimal(0);
    const reject = String(row.orr_rjt_rsn_cd_nm ?? "").trim();
    let status: OrderStatus;
    if (remaining.gt(0) && filled.gt(0)) status = "PARTIALLY_FILLED";
    else if (remaining.gt(0)) status = "SUBMITTED";
    else if (filled.gt(0) && filled.gte(ordQty)) status = "FILLED";
    else if (filled.gt(0)) status = "PARTIALLY_FILLED";
    else if (canceled.gt(0)) status = "CANCELED";
    else if (reject) status = "REJECTED";
    else status = "CANCELED";
    return {
      orderId: String(row.itg_orr_no ?? "").trim(), status, symbol: nhNormalizeCode(row.iem_cd), side: sideOf(row),
      orderType: String(row.nmn_pr_tp_cd_nm ?? "").includes("시장가") ? "MARKET" : "LIMIT",
      quantity: ordQty, limitPrice: positive(row.orr_pr), filledQuantity: filled, avgFillPrice: positive(row.cns_avg_uit_pr),
    };
  }

  private async account(): Promise<string> {
    if (this.accountNo) return this.accountNo;
    const expected = this.environment === "LIVE" ? "01" : "03";
    const accounts = ((await this.call("/n2/acctinfo", {})).Output_0 ?? []) as Record<string, unknown>[];
    const picked = accounts.find((a) => String(a.acct_type) === expected);
    if (!picked) throw new BrokerApiError(200, null, `NH 계좌 목록에 ${this.environment} 용 계좌(acct_type=${expected})가 없습니다`);
    this.accountNo = String(picked.acct_no);
    return this.accountNo;
  }

  private call(path: string, body: Record<string, unknown>): Promise<Record<string, unknown>> {
    return this.limiter.execute(() => this.tokens.call((token) => this.callOnce(path, body, token)), `NH ${path}`);
  }

  private async callOnce(path: string, body: Record<string, unknown>, token: string): Promise<Record<string, unknown>> {
    const [status, parsed, resHeaders] = await httpJson(this.baseUrl + path, {
      method: "POST",
      headers: {
        "Content-Type": "application/json; charset=UTF-8",
        authorization: `Bearer ${token}`, "x-client-id": this.appKey, "x-client-secret": this.appSecret,
      },
      body: JSON.stringify({ Input_0: body }),
    });
    const rspCd = String(parsed.rsp_cd ?? "").trim();
    const rspMsg = String(parsed.rsp_msg ?? "");
    const gw = String(parsed.code ?? (parsed.error as Record<string, unknown> | undefined)?.code ?? rspCd);
    const msg = `NH(${path}) [${gw || rspCd}] ${rspMsg || parsed.message || ""}`.trim();
    if (status < 200 || status >= 300) {
      const retryAfter = Number(resHeaders?.["retry-after"]);
      if (status === 429 || gw.startsWith("IGW429")) throw new RateLimitError(status, gw, msg, Number.isFinite(retryAfter) ? retryAfter : null);
      if (status === 401 || gw.startsWith("IGW4004") || gw.startsWith("IGW4003") || gw === "IGW40051") throw new AuthError(status, gw, msg);
      if (status === 400) throw new InvalidOrderError(status, gw, msg);
      throw new BrokerApiError(status, gw, msg);
    }
    if (!SUCCESS_CODES.has(rspCd) && !rspMsg.includes("완료")) {
      if (rspMsg.includes("부족")) throw new InsufficientFundsError(status, rspCd, msg);
      throw new BrokerApiError(status, rspCd, msg);
    }
    return parsed;
  }

  private getToken(): Promise<string> {
    return this.tokens.get();
  }

  private issueToken(): Promise<{ token: string; expiresAt: number }> {
    return this.usage.measure("auth", async () => {
      await this.limiter.throttle.wait();
      // SDK 규약: 파라미터는 쿼리스트링, 본문 없음, content-type 은 form-urlencoded
      const query = new URLSearchParams({ appkey: this.appKey, appsecretkey: this.appSecret, grant_type: "client_credentials", scope: "oob" });
      const [status, body] = await httpJson(`${this.authUrl}/oauth2/token?${query}`, {
        method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" },
      });
      if (status !== 200 || typeof body.access_token !== "string") {
        const code = String(body.code ?? body.rsp_cd ?? "");
        const msg = `NH 토큰 발급 실패(${code}): ${body.message ?? body.rsp_msg ?? ""}`;
        if (status === 429 || code.startsWith("IGW429")) throw new RateLimitError(status, code || null, msg, FAILURE_COOLDOWN_SECONDS);
        throw new AuthError(status, code || null, msg);
      }
      return { token: body.access_token, expiresAt: Date.now() + Number(body.expires_in ?? 86400) * 1000 };
    });
  }
}
