/**
 * 토스증권 Open API 어댑터 (REST v1.2.15).
 * ⚠️ 실전 전용 · 실측 전 — 샌드박스가 없다. 공식 OpenAPI 문서로 구현, 실계좌 소액 검증 전까지 "미검증". liveTradingEnabled 와 금액 상한 필수.
 * - /api/v1/… + Authorization: Bearer. 계좌·자산·주문 API 는 X-Tossinvest-Account: {accountSeq} 헤더 필수
 * - 성공 {"result": …}, 에러 {"error": {requestId, code, message}}. client 당 유효 토큰 1개(재발급 시 이전 토큰 무효)
 * - 한 계좌로 KRX·미국을 다룬다 → 보유·주문 심볼은 KRX:005930 / US:AAPL 로 접두를 붙여 돌려준다
 * - 시세에 등락·거래량 없음, 예수금 없음(KRW 매수가능금액 대체), 체결 엔드포인트 없음(종료 주문 execution 집계), 취소는 새 orderId 발급, 캘린더 KRX 합성
 * - 보유 평가금액·손익은 원화(미국 종목은 USD 금액을 매매기준율로 환산) → 계좌 총평가도 원화 한 숫자
 */
import { instrumentBroker, type BrokerUsage } from "../telemetry.js";
import { randomUUID } from "node:crypto";
import { Decimal } from "decimal.js";
import { MarketStream, RateLimiter, StreamingBrokerClient, httpJson, krxCalendar, krxTickRound } from "../broker.js";
import {
  AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, MarketClosedError, OrderNotFoundError, RateLimitError,
} from "../errors.js";
import type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote, TradingEnvironment,
} from "../models.js";
import { parseSymbol, symbolCodeFor } from "../models.js";
import { FAILURE_COOLDOWN_SECONDS, TokenManager } from "../tokens.js";
import { TossMarketStream } from "./tossStream.js";

const STATUS: Record<string, OrderStatus> = {
  PENDING: "SUBMITTED", PENDING_REPLACE: "SUBMITTED", PENDING_CANCEL: "PENDING_CANCEL", PARTIAL_FILLED: "PARTIALLY_FILLED",
  FILLED: "FILLED", CANCELED: "CANCELED", REPLACED: "CANCELED", REJECTED: "REJECTED", CANCEL_REJECTED: "REJECTED", REPLACE_REJECTED: "REJECTED",
};
const num = (v: unknown): Decimal | null => { if (v === null || v === undefined || v === "") return null; try { return new Decimal(String(v)); } catch { return null; } };
const ts = (v: unknown): Date | null => { if (!v) return null; const d = new Date(String(v)); return Number.isNaN(d.getTime()) ? null : d; };

export class TossClient implements StreamingBrokerClient {
  /** 실시간 — AsyncAPI 1.2.2. 계정당 연결 2개, 구독 100개, 선언 5회/초, 180초 무송신 시 끊김(60초 PING), 토큰은 핸드셰이크에서만 검사 (실측 전) */
  static readonly WS_URL = "wss://openapi-ws.tossinvest.com/ws/v1";

  /** 사용량 텔레메트리 핸들 — 생성자 끝에서 공개 메서드를 계측하며 만든다 (docs/telemetry.md) */
  private readonly usage: BrokerUsage;
  readonly capabilities: BrokerCapabilities = {
    brokerId: "toss", market: "KRX", currency: "KRW",
    candleIntervals: new Set<CandleInterval>(["1m", "1d"]),
    clientOrderId: true, nativeBracket: false, fractionalShares: false, serverOpenOrders: true,
    environments: new Set<TradingEnvironment>(["LIVE"]), markets: new Set(["KRX", "US"]),
    streams: new Set(["TRADES", "ORDER_BOOK", "ORDER_EVENTS"]), // AsyncAPI 1.2.2 기반, 실측 전
  };

  private readonly tokens: TokenManager;
  private accountSeq: string;
  private readonly limiter: RateLimiter;

  /** accountSeq 를 비우면 GET /api/v1/accounts 의 첫 BROKERAGE 계좌를 쓴다 */
  constructor(
    private readonly clientId: string,
    private readonly clientSecret: string,
    accountSeq = "",
    readonly baseUrl: string = "https://openapi.tossinvest.com",
    throttleMs = 200,
    readonly environment: TradingEnvironment = "LIVE",
    readonly wsUrl: string = TossClient.WS_URL,
  ) {
    this.accountSeq = accountSeq;
    this.limiter = new RateLimiter(throttleMs, 3, (attempt) => 1000 * 2 ** (attempt - 1));
    this.tokens = new TokenManager("toss", clientId, () => this.issueToken(), 60);
    this.usage = instrumentBroker(this);
  }

  /** 웹소켓은 REST 와 같은 토큰을 핸드셰이크 헤더에 싣고, 주문 이벤트 구독은 accountSeq 로 계좌를 고른다 */
  openStream(): MarketStream {
    return new TossMarketStream({ usage: this.usage, wsUrl: this.wsUrl, token: () => this.getToken(), accountSeq: () => this.account() });
  }

  async getQuotes(symbols: string[]): Promise<Quote[]> {
    const requested = new Map(symbols.map((s) => [symbolCodeFor(this.capabilities, s), s]));
    const result = (await this.call("GET", `/api/v1/prices?symbols=${[...requested.keys()].join(",")}`)) as Record<string, unknown>[];
    const quotes: Quote[] = [];
    for (const q of result ?? []) {
      const price = num(q.lastPrice);
      if (!price) continue;
      quotes.push({ symbol: requested.get(String(q.symbol)) ?? String(q.symbol), price, bidPrice: null, askPrice: null, volume: 0, change: null, changeRate: null, timestamp: ts(q.timestamp) ?? new Date() });
    }
    return quotes;
  }

  async getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]> {
    if (!this.capabilities.candleIntervals.has(interval)) throw new Error(`토스 어댑터는 1m/1d 캔들만 지원합니다 (${interval})`);
    const count = Math.max(1, Math.min(limit ?? 100, 200));
    const result = (await this.call("GET", `/api/v1/candles?symbol=${symbolCodeFor(this.capabilities, symbol)}&interval=${interval}&count=${count}`)) as Record<string, unknown>;
    const candles: Candle[] = [];
    for (const c of ((result?.candles ?? []) as Record<string, unknown>[])) {
      const t = ts(c.timestamp);
      if (!t) continue;
      candles.push({ timestamp: t, open: num(c.openPrice) ?? new Decimal(0), high: num(c.highPrice) ?? new Decimal(0), low: num(c.lowPrice) ?? new Decimal(0), close: num(c.closePrice) ?? new Decimal(0), volume: Number(num(c.volume) ?? 0) });
    }
    candles.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
    return candles;
  }

  async getCalendar(): Promise<MarketDay[]> { return krxCalendar(); }

  async getAccount(): Promise<Account> {
    const cash = await this.buyingPower("KRW");
    const holdings = await this.getHoldings();
    const marketValue = holdings.reduce((acc, h) => acc.plus(h.marketValue ?? 0), new Decimal(0));
    return { accountId: await this.account(), currency: "KRW", cash, portfolioValue: cash.plus(marketValue), status: "ACTIVE", name: null };
  }

  /** 평가금액·평가손익은 원화 — 미국 종목은 종목 통화(USD) 금액을 매매기준율(midRate)로 환산한다. 단가는 종목 통화 그대로. */
  async getHoldings(): Promise<Holding[]> {
    const result = (await this.call("GET", "/api/v1/holdings", undefined, true)) as Record<string, unknown>;
    const rates = new Map<string, Decimal>([["KRW", new Decimal(1)]]);
    const holdings: Holding[] = [];
    for (const h of ((result?.items ?? []) as Record<string, unknown>[])) {
      const quantity = num(h.quantity);
      if (!quantity || quantity.lte(0)) continue;
      const market = h.marketCountry === "US" ? "US" : "KRX";
      const currency = String(h.currency || (market === "US" ? "USD" : "KRW"));
      if (!rates.has(currency)) rates.set(currency, await this.krwRate(currency));
      const rate = rates.get(currency)!;
      const mv = num(((h.marketValue ?? {}) as Record<string, unknown>).amount);
      const pl = (h.profitLoss ?? {}) as Record<string, unknown>;
      const pnl = num(pl.amount);
      holdings.push({
        symbol: `${market}:${h.symbol}`, quantity, avgEntryPrice: num(h.averagePurchasePrice) ?? new Decimal(0),
        currentPrice: num(h.lastPrice), marketValue: mv ? mv.times(rate) : null, unrealizedPnl: pnl ? pnl.times(rate) : null, unrealizedPnlRate: num(pl.rate),
      });
    }
    return holdings;
  }

  async getBuyingPower(): Promise<Decimal> { return this.buyingPower("KRW"); }

  async createOrder(request: CreateOrderRequest): Promise<Order> {
    const { market } = parseSymbol(request.symbol);
    const code = symbolCodeFor(this.capabilities, request.symbol);
    const isKrx = (market ?? "KRX") === "KRX";
    const isLimit = request.orderType === "LIMIT";
    const body: Record<string, unknown> = { clientOrderId: request.clientOrderId || randomUUID(), symbol: code, side: request.side, orderType: request.orderType, timeInForce: "DAY", quantity: request.quantity.toString(), confirmHighValueOrder: false };
    if (isLimit) body.price = isKrx ? krxTickRound(request.limitPrice!).toString() : request.limitPrice!.toString();
    const result = (await this.call("POST", "/api/v1/orders", body, true)) as Record<string, unknown>;
    const orderId = String(result?.orderId ?? "").trim();
    if (!orderId) throw new BrokerApiError(200, null, "토스 주문 응답에 orderId 가 없습니다");
    return { orderId, status: "SUBMITTED", symbol: `${isKrx ? "KRX" : "US"}:${code}`, side: request.side, orderType: request.orderType, quantity: request.quantity, limitPrice: request.limitPrice ?? null, filledQuantity: new Decimal(0), clientOrderId: (result.clientOrderId as string) ?? request.clientOrderId ?? null, submittedAt: new Date() };
  }

  async getOrders(): Promise<Order[]> {
    const result = (await this.call("GET", "/api/v1/orders?status=OPEN", undefined, true)) as Record<string, unknown>;
    return ((result?.orders ?? []) as Record<string, unknown>[]).map((o) => this.toOrder(o));
  }

  async getOrder(orderId: string): Promise<Order> {
    return this.toOrder((await this.call("GET", `/api/v1/orders/${orderId}`, undefined, true)) as Record<string, unknown>);
  }

  async cancelOrder(orderId: string): Promise<Order> {
    await this.call("POST", `/api/v1/orders/${orderId}/cancel`, {}, true); // 새 orderId 발급 — 원주문 ID 유지
    return { orderId, status: "PENDING_CANCEL", canceledAt: new Date() };
  }

  async getFills(): Promise<Fill[]> {
    const result = (await this.call("GET", "/api/v1/orders?status=CLOSED", undefined, true)) as Record<string, unknown>;
    const fills: Fill[] = [];
    for (const o of ((result?.orders ?? []) as Record<string, unknown>[])) {
      const ex = (o.execution ?? {}) as Record<string, unknown>;
      const qty = num(ex.filledQuantity);
      if (!qty || qty.lte(0)) continue;
      fills.push({ fillId: null, orderId: String(o.orderId), symbol: `${o.currency === "USD" ? "US" : "KRX"}:${o.symbol}`, side: (o.side as OrderSide) ?? null, quantity: qty, price: num(ex.averageFilledPrice) });
    }
    return fills;
  }

  private toOrder(o: Record<string, unknown>): Order {
    const ex = (o.execution ?? {}) as Record<string, unknown>;
    return {
      orderId: String(o.orderId ?? ""), status: STATUS[String(o.status)] ?? "UNKNOWN", symbol: `${o.currency === "USD" ? "US" : "KRX"}:${o.symbol}`,
      side: (o.side as OrderSide) ?? null, orderType: (o.orderType as OrderType) ?? null, quantity: num(o.quantity), limitPrice: num(o.price),
      filledQuantity: num(ex.filledQuantity) ?? new Decimal(0), avgFillPrice: num(ex.averageFilledPrice), clientOrderId: (o.clientOrderId as string) ?? null,
      submittedAt: ts(o.orderedAt), canceledAt: ts(o.canceledAt),
    };
  }

  /** 1 {currency} 의 원화 매매기준율 (GET /api/v1/exchange-rate, 약 5분마다 갱신) */
  private async krwRate(currency: string): Promise<Decimal> {
    const result = (await this.call("GET", `/api/v1/exchange-rate?baseCurrency=${currency}&quoteCurrency=KRW`)) as Record<string, unknown>;
    const rate = num(result?.midRate) ?? num(result?.rate);
    if (!rate || rate.lte(0)) throw new BrokerApiError(200, null, `토스 ${currency}/KRW 환율을 받지 못했습니다`);
    return rate;
  }

  private async buyingPower(currency: string): Promise<Decimal> {
    return num(((await this.call("GET", `/api/v1/buying-power?currency=${currency}`, undefined, true)) as Record<string, unknown>)?.cashBuyingPower) ?? new Decimal(0);
  }

  private async account(): Promise<string> {
    if (this.accountSeq) return this.accountSeq;
    const accounts = (await this.call("GET", "/api/v1/accounts")) as Record<string, unknown>[];
    const picked = accounts.find((a) => a.accountType === "BROKERAGE") ?? accounts[0];
    if (!picked) throw new BrokerApiError(200, null, "토스 계좌 목록이 비어 있습니다");
    this.accountSeq = String(picked.accountSeq);
    return this.accountSeq;
  }

  private call(method: string, path: string, body?: Record<string, unknown>, account = false): Promise<unknown> {
    return this.limiter.execute(() => this.tokens.call((token) => this.callOnce(method, path, token, body, account)), `toss ${path}`);
  }

  private async callOnce(method: string, path: string, token: string, body?: Record<string, unknown>, account = false): Promise<unknown> {
    const headers: Record<string, string> = { Authorization: `Bearer ${token}` };
    if (account) headers["X-Tossinvest-Account"] = await this.account();
    if (body) headers["Content-Type"] = "application/json";
    const [status, parsed, resHeaders] = await httpJson(this.baseUrl + path, { method, headers, body: body ? JSON.stringify(body) : undefined });
    if (status < 200 || status >= 300) {
      const error = (parsed.error ?? {}) as Record<string, unknown>;
      const code = String(error.code ?? "");
      const msg = `Toss(${path}) [${code}] ${error.message ?? ""} requestId=${error.requestId ?? ""}`.trim();
      const retryAfter = Number(resHeaders?.["retry-after"]);
      if (status === 429) throw new RateLimitError(status, code, msg, Number.isFinite(retryAfter) ? retryAfter : null);
      if (status === 401) throw new AuthError(status, code, msg);
      if (code === "order-not-found") throw new OrderNotFoundError(code, msg);
      if (code === "insufficient-buying-power") throw new InsufficientFundsError(status, code, msg);
      if (code === "order-hours-closed") throw new MarketClosedError(status, code, msg);
      if (status === 400 || status === 409 || status === 422) throw new InvalidOrderError(status, code, msg);
      throw new BrokerApiError(status, code, msg);
    }
    return parsed.result;
  }

  private getToken(): Promise<string> {
    return this.tokens.get();
  }

  private issueToken(): Promise<{ token: string; expiresAt: number }> {
    return this.usage.measure("auth", async () => {
      await this.limiter.throttle.wait();
      const form = new URLSearchParams({ grant_type: "client_credentials", client_id: this.clientId, client_secret: this.clientSecret });
      const [status, body, resHeaders] = await httpJson(`${this.baseUrl}/oauth2/token`, { method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body: form.toString() });
      if (status !== 200 || typeof body.access_token !== "string") {
        // OAuth 표준 {"error": "invalid_client", ...} 또는 플랫폼 엔벨로프 {"error": {"code", "message"}}
        const error = body.error;
        const envelope = typeof error === "object" && error !== null ? error as Record<string, unknown> : null;
        const code = envelope ? (envelope.code as string | undefined) ?? null : (error as string | undefined) ?? null;
        const msg = `토스 토큰 발급 실패(${code}): ${body.error_description || (envelope ? envelope.message : "") || ""}`;
        if (status === 429) {
          const retryAfter = Number(resHeaders?.["retry-after"]);
          throw new RateLimitError(status, code, msg, Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter : FAILURE_COOLDOWN_SECONDS);
        }
        throw new AuthError(status, code, msg);
      }
      return { token: body.access_token, expiresAt: Date.now() + Number(body.expires_in ?? 86400) * 1000 };
    });
  }
}
