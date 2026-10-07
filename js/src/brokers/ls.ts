/**
 * LS증권(구 이베스트투자증권) OPEN API 어댑터.
 * ⚠️ 문서 기반 구현 (실측 전) — 포털 TR 문서와 커뮤니티 카탈로그에서 역추적. 모의서버 실측 전까지 "미검증".
 * - 모든 API 는 POST, 경로는 기능군(/stock/market-data, /stock/chart, /stock/accno, /stock/order)이고 TR 은 tr_cd 헤더로 고른다
 * - 본문 {"<TR>InBlock": {...}} / {"<TR>InBlock1": {...}}, 응답 rsp_cd("00000" 성공)/rsp_msg + OutBlock. 계좌번호는 토큰 바인딩
 * - 실전/모의 같은 호스트(모의 appkey 로 라우팅). 모의 주문은 IsuNo 에 A 접두 필수 → 항상 A+코드. HTTP 200 + rsp_cd != 00000 이 업무 오류
 * - TR 별 TPS(시세 3, 차트 1, 계좌 2, 예수금 1, 주문 10) → 전역 500ms + 차트 전용 1100ms 쓰로틀
 * 미확인: 잔고 expcode 의 A 접두, sign 코드(4·5 하락 가정), 응답 숫자 타입, 장 마감 코드(메시지 판단), medosu 표기
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
import { LsMarketStream } from "./lsStream.js";

const AUTH_CODES = new Set(["IGW00121", "IGW00123"]);
const FALLING_SIGNS = new Set(["4", "5"]);

const num = (v: unknown): Decimal | null => {
  if (v === null || v === undefined) return null;
  const text = String(v).trim().replace(/,/g, "");
  if (!text) return null;
  try { return new Decimal(text); } catch { return null; }
};
const pct = (v: unknown): Decimal | null => { const r = num(v); return r ? r.div(100) : null; };
const positive = (v: unknown): Decimal | null => { const d = num(v); return d && d.gt(0) ? d : null; };

/** 계좌 TR 의 종목코드는 A005930 형태(추정) → 6자리 코드 */
export const lsNormalizeCode = (raw: unknown): string => {
  const text = String(raw ?? "").trim();
  return text.length === 7 && text[0] === "A" ? text.slice(1) : text;
};

const kstDate = (offsetDays: number): string => {
  const d = new Date(Date.now() + offsetDays * 86_400_000);
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Seoul" }).format(d).replace(/-/g, "");
};
const sameNo = (a: unknown, b: unknown) => String(a ?? "").trim().replace(/^0+/, "") === String(b ?? "").trim().replace(/^0+/, "");
const sideOf = (row: Record<string, unknown>): OrderSide => {
  const t = String(row.medosu ?? "").trim();
  return t.includes("매수") || t === "2" ? "BUY" : "SELL";
};

export class LsClient implements StreamingBrokerClient {
  /** 실시간 — 모의 29443 / 실전 9443. 토큰 익일 07:00 만료, 한도 미문서 (문서 기반, 실측 전) */
  static readonly PAPER_WS_URL = "wss://openapi.ls-sec.co.kr:29443/websocket";
  static readonly LIVE_WS_URL = "wss://openapi.ls-sec.co.kr:9443/websocket";

  /** 사용량 텔레메트리 핸들 — 생성자 끝에서 공개 메서드를 계측하며 만든다 (docs/telemetry.md) */
  private readonly usage: BrokerUsage;
  readonly capabilities: BrokerCapabilities = {
    brokerId: "ls", market: "KRX", currency: "KRW",
    candleIntervals: new Set<CandleInterval>(["1d"]),
    clientOrderId: false, nativeBracket: false, fractionalShares: false, serverOpenOrders: true,
    environments: new Set<TradingEnvironment>(["PAPER", "LIVE"]),
    streams: new Set(["TRADES", "ORDER_BOOK", "ORDER_EVENTS"]), // 문서 기반, 실측 전
  };

  readonly wsUrl: string;
  private readonly tokens: TokenManager;
  private readonly limiter: RateLimiter;
  private readonly chartLimiter: RateLimiter;

  constructor(
    private readonly appKey: string,
    private readonly appSecret: string,
    readonly baseUrl: string = "https://openapi.ls-sec.co.kr:8080",
    private readonly macAddress = "",
    private readonly exchGubun = "",
    throttleMs = 500,
    chartThrottleMs = 1100,
    readonly environment: TradingEnvironment = "PAPER",
    wsUrl = "",
  ) {
    this.limiter = new RateLimiter(throttleMs, 3, (attempt) => 1000 * attempt);
    this.chartLimiter = new RateLimiter(chartThrottleMs, 0); // 차트 TR 초당 1건
    this.wsUrl = wsUrl || (environment === "LIVE" ? LsClient.LIVE_WS_URL : LsClient.PAPER_WS_URL);
    this.tokens = new TokenManager("ls", appKey, () => this.issueToken(), 600);
    this.usage = instrumentBroker(this);
  }

  /** 웹소켓은 REST 접근토큰을 매 메시지 헤더에 싣는다. 종목마다 KOSPI·KOSDAQ TR 을 둘 다 등록한다 (문서 기반, 실측 전) */
  openStream(): MarketStream {
    return new LsMarketStream({ usage: this.usage, wsUrl: this.wsUrl, token: () => this.getToken() });
  }

  async getQuotes(symbols: string[]): Promise<Quote[]> {
    const quotes: Quote[] = [];
    for (const symbol of symbols) {
      const out = ((await this.call("/stock/market-data", "t1102", "t1102InBlock", { shcode: symbolCodeFor(this.capabilities, symbol), exchgubun: this.exchGubun })).t1102OutBlock ?? {}) as Record<string, unknown>;
      const falling = FALLING_SIGNS.has(String(out.sign ?? ""));
      let change = num(out.change);
      let rate = num(out.diff);
      if (falling) {
        if (change && change.gt(0)) change = change.neg();
        if (rate && rate.gt(0)) rate = rate.neg();
      }
      quotes.push({ symbol, price: num(out.price) ?? new Decimal(0), bidPrice: null, askPrice: null, volume: Number(num(out.volume) ?? 0), change, changeRate: rate ? rate.div(100) : null, timestamp: new Date() });
    }
    return quotes;
  }

  async getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]> {
    if (interval !== "1d") throw new Error("LS 어댑터는 일봉(1d)만 지원합니다.");
    const count = limit ?? 30;
    const body = await this.chartLimiter.execute(() => this.call("/stock/chart", "t8410", "t8410InBlock", {
      shcode: symbolCodeFor(this.capabilities, symbol), gubun: "2", qrycnt: count,
      sdate: kstDate(-(Math.floor(count * 16 / 10) + 10)), edate: kstDate(0), cts_date: "", comp_yn: "N", sujung: "Y",
    }), "LS t8410");
    const candles: Candle[] = [];
    for (const r of (body.t8410OutBlock1 ?? []) as Record<string, unknown>[]) {
      const date = String(r.date ?? "").trim();
      if (date.length !== 8) continue;
      candles.push({
        timestamp: new Date(`${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}T00:00:00+09:00`),
        open: num(r.open) ?? new Decimal(0), high: num(r.high) ?? new Decimal(0), low: num(r.low) ?? new Decimal(0), close: num(r.close) ?? new Decimal(0),
        volume: Number(num(r.jdiff_vol) ?? 0),
      });
    }
    candles.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
    return candles.slice(-count);
  }

  async getCalendar(): Promise<MarketDay[]> { return krxCalendar(); }

  async getAccount(): Promise<Account> {
    const s = ((await this.balance()).t0424OutBlock ?? {}) as Record<string, unknown>;
    const cash = num(s.sunamt1) ?? new Decimal(0);
    const portfolio = positive(s.sunamt) ?? cash.plus(num(s.tappamt) ?? 0);
    return { accountId: `ls-${this.environment.toLowerCase()}`, currency: "KRW", cash, portfolioValue: portfolio, status: "ACTIVE", name: null };
  }

  async getHoldings(): Promise<Holding[]> {
    const holdings: Holding[] = [];
    for (const row of ((await this.balance()).t0424OutBlock1 ?? []) as Record<string, unknown>[]) {
      const quantity = num(row.janqty);
      if (!quantity || quantity.lte(0)) continue;
      holdings.push({ symbol: lsNormalizeCode(row.expcode), quantity, avgEntryPrice: num(row.pamt) ?? new Decimal(0), currentPrice: num(row.price), marketValue: num(row.appamt), unrealizedPnl: num(row.dtsunik), unrealizedPnlRate: pct(row.sunikrt) });
    }
    return holdings;
  }

  async getBuyingPower(): Promise<Decimal> {
    const out = ((await this.call("/stock/accno", "CSPAQ12200", "CSPAQ12200InBlock1", { BalCreTp: "0" })).CSPAQ12200OutBlock2 ?? {}) as Record<string, unknown>;
    return num(out.MnyOrdAbleAmt) ?? num(out.Dps) ?? new Decimal(0);
  }

  async createOrder(request: CreateOrderRequest): Promise<Order> {
    const code = symbolCodeFor(this.capabilities, request.symbol);
    const isLimit = request.orderType === "LIMIT";
    const out = ((await this.call("/stock/order", "CSPAT00601", "CSPAT00601InBlock1", {
      IsuNo: `A${code}`, OrdQty: request.quantity.toNumber(), OrdPrc: isLimit ? krxTickRound(request.limitPrice!).toNumber() : 0,
      BnsTpCode: request.side === "BUY" ? "2" : "1", OrdprcPtnCode: isLimit ? "00" : "03", MgntrnCode: "000", LoanDt: "", OrdCndiTpCode: "0",
    })).CSPAT00601OutBlock2 ?? {}) as Record<string, unknown>;
    const orderId = String(out.OrdNo ?? "").trim();
    if (!orderId || orderId === "0") throw new BrokerApiError(200, null, "LS 주문 응답에 OrdNo 가 없습니다");
    return { orderId, status: "SUBMITTED", symbol: code, side: request.side, orderType: request.orderType, quantity: request.quantity, limitPrice: request.limitPrice ?? null, filledQuantity: new Decimal(0), clientOrderId: request.clientOrderId ?? null, submittedAt: new Date() };
  }

  async getOrders(): Promise<Order[]> {
    return (await this.orderRows()).map((r) => this.toOrder(r)).filter((o) => o.status === "SUBMITTED" || o.status === "PARTIALLY_FILLED" || o.status === "PENDING_CANCEL");
  }

  async getOrder(orderId: string): Promise<Order> {
    const row = (await this.orderRows()).find((r) => sameNo(r.ordno, orderId));
    return row ? this.toOrder(row) : { orderId, status: "CANCELED" };
  }

  async cancelOrder(orderId: string): Promise<Order> {
    const row = (await this.orderRows()).find((r) => sameNo(r.ordno, orderId));
    if (!row) throw new OrderNotFoundError("order-not-found", `LS 당일 주문에서 찾을 수 없습니다: ${orderId}`);
    const remaining = num(row.ordrem) ?? (num(row.qty) ?? new Decimal(0)).minus(num(row.cheqty) ?? 0);
    await this.call("/stock/order", "CSPAT00801", "CSPAT00801InBlock1", { OrgOrdNo: Number(orderId.replace(/^0+/, "") || "0"), IsuNo: `A${lsNormalizeCode(row.expcode)}`, OrdQty: remaining.toNumber() });
    return { orderId, status: "PENDING_CANCEL", canceledAt: new Date() };
  }

  async getFills(): Promise<Fill[]> {
    return (await this.orderRows()).filter((r) => (num(r.cheqty) ?? new Decimal(0)).gt(0))
      .map((r) => ({ fillId: null, orderId: String(r.ordno ?? "").trim(), symbol: lsNormalizeCode(r.expcode), side: sideOf(r), quantity: num(r.cheqty), price: num(r.cheprice) }));
  }

  private balance(): Promise<Record<string, unknown>> {
    return this.call("/stock/accno", "t0424", "t0424InBlock", { prcgb: "1", chegb: "2", dangb: "0", charge: "1", cts_expcode: "" });
  }

  private async orderRows(): Promise<Record<string, unknown>[]> {
    return ((await this.call("/stock/accno", "t0425", "t0425InBlock", { expcode: "", chegb: "0", medosu: "0", sortgb: "1", cts_ordno: "" })).t0425OutBlock1 ?? []) as Record<string, unknown>[];
  }

  private toOrder(row: Record<string, unknown>): Order {
    const qty = num(row.qty) ?? new Decimal(0);
    const filled = num(row.cheqty) ?? new Decimal(0);
    const remaining = num(row.ordrem) ?? qty.minus(filled);
    const text = String(row.status ?? "");
    let status: OrderStatus;
    if (text.includes("취소") && remaining.gt(0)) status = "PENDING_CANCEL";
    else if (remaining.gt(0) && filled.gt(0)) status = "PARTIALLY_FILLED";
    else if (remaining.gt(0)) status = "SUBMITTED";
    else if (filled.gt(0) && filled.gte(qty)) status = "FILLED";
    else if (filled.gt(0)) status = "PARTIALLY_FILLED";
    else if (text.includes("거부")) status = "REJECTED";
    else status = "CANCELED";
    return { orderId: String(row.ordno ?? "").trim(), status, symbol: lsNormalizeCode(row.expcode), side: sideOf(row), orderType: String(row.hogagb ?? "") === "03" ? "MARKET" : "LIMIT", quantity: qty, limitPrice: positive(row.price), filledQuantity: filled, avgFillPrice: positive(row.cheprice) };
  }

  private call(path: string, trCd: string, inBlock: string, input: Record<string, unknown>): Promise<Record<string, unknown>> {
    return this.limiter.execute(() => this.tokens.call((token) => this.callOnce(path, trCd, inBlock, input, token)), `LS ${trCd}`);
  }

  private async callOnce(path: string, trCd: string, inBlock: string, input: Record<string, unknown>, token: string): Promise<Record<string, unknown>> {
    const headers: Record<string, string> = { "Content-Type": "application/json; charset=utf-8", authorization: `Bearer ${token}`, tr_cd: trCd, tr_cont: "N", tr_cont_key: "" };
    if (this.macAddress) headers.mac_address = this.macAddress;
    const [status, parsed, resHeaders] = await httpJson(this.baseUrl + path, { method: "POST", headers, body: JSON.stringify({ [inBlock]: input }) });
    const code = String(parsed.rsp_cd ?? parsed.error_code ?? "").trim();
    const rspMsg = String(parsed.rsp_msg ?? parsed.error_description ?? "");
    const msg = `LS(${trCd}) [${code}] ${rspMsg}`.trim();
    if (status < 200 || status >= 300 || ("rsp_cd" in parsed && code !== "00000")) {
      const retryAfter = Number(resHeaders?.["retry-after"]);
      if (code === "IGW00201" || status === 429) throw new RateLimitError(status, code, msg, Number.isFinite(retryAfter) ? retryAfter : null);
      if (AUTH_CODES.has(code) || status === 401) throw new AuthError(status, code, msg);
      if (["장종료", "장운영", "장 마감", "장마감"].some((k) => rspMsg.includes(k))) throw new MarketClosedError(status, code, msg);
      if (rspMsg.includes("부족")) throw new InsufficientFundsError(status, code, msg);
      if (rspMsg.includes("호가") || rspMsg.includes("단위")) throw new InvalidOrderError(status, code, msg);
      if (rspMsg.includes("주문번호") || rspMsg.includes("원주문")) throw new OrderNotFoundError(code, msg);
      throw new BrokerApiError(status, code, msg);
    }
    return parsed;
  }

  private getToken(): Promise<string> {
    return this.tokens.get();
  }

  private issueToken(): Promise<{ token: string; expiresAt: number }> {
    return this.usage.measure("auth", async () => {
      await this.limiter.throttle.wait();
      const form = new URLSearchParams({ grant_type: "client_credentials", appkey: this.appKey, appsecretkey: this.appSecret, scope: "oob" });
      const [status, body] = await httpJson(`${this.baseUrl}/oauth2/token`, { method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body: form.toString() });
      if (status !== 200 || typeof body.access_token !== "string") {
        const code = String(body.error_code ?? body.rsp_cd ?? "");
        const detail = String(body.error_description ?? body.rsp_msg ?? "");
        if (code === "IGW00201" || status === 429) throw new RateLimitError(status, code || null, `LS 토큰 발급 유량 초과(${code}): ${detail}`, FAILURE_COOLDOWN_SECONDS);
        throw new AuthError(status, code || null, `LS 토큰 발급 실패(${code}): ${detail}`);
      }
      return { token: body.access_token, expiresAt: Date.now() + Number(body.expires_in ?? 86400) * 1000 };
    });
  }
}
