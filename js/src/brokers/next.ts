/**
 * 넥스트증권 모의투자 어댑터 (미국주식) — 공개 스펙 v1.3 기준.
 *
 * v1.3 응답(quotes outcome, 캔들 time, 계좌 cashAmount, 보유 averageBuyPrice, 캘린더 status+sessions[] …)을
 * 브로커 중립 공통 모델(Quote/Candle/Holding/Order …)로 정규화한다. 공통 모델은 전략이 보는 타입이므로
 * 서버 스펙 변경은 이 어댑터 안에서만 흡수한다 (KIS/키움 어댑터와 같은 방식).
 *
 * - OAuth client_credentials, 토큰 12h(expires_in=43200), 401 시 1회 재발급-재시도
 * - 공통 헤더(v1.3): X-Request-Id 는 토큰 발급 외 전 API 필수, 계좌·자산·주문 API 는 X-Next-Account-Id 필수
 *   (구 X-Nextsecurities-Account 에서 개명)
 * - 시각은 ISO 8601 · KST (오프셋 생략 시 KST). 등락률·손익률은 % 단위 → 공통 모델 규약(비율)로 /100
 * - 토큰 발급 400/401 만 OAuth 표준 {error, error_description} 형식
 */
import { instrumentBroker, type BrokerUsage } from "../telemetry.js";
import { randomUUID } from "node:crypto";
import { Decimal } from "decimal.js";
import { BrokerClient, D, DorNull, RateLimiter, httpJson } from "../broker.js";
import {
  AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError,
  MarketClosedError, OrderNotFoundError, RateLimitError,
} from "../errors.js";
import type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, OrderType, Quote, TradingEnvironment,
} from "../models.js";
import { symbolCodeFor } from "../models.js";
import { FAILURE_COOLDOWN_SECONDS, TokenManager } from "../tokens.js";

interface NextError { type?: string; code?: string; message?: string; requestId?: string; }

export const NEXT_ACCOUNT_HEADER = "X-Next-Account-Id";
export const NEXT_REQUEST_ID_HEADER = "X-Request-Id";

/** v1.3 규칙: 영숫자·점·밑줄·하이픈만, 최대 64자. hmx- 프리픽스 + UUID = 40자 */
export const newRequestId = (): string => `hmx-${randomUUID()}`;

const MARKET = "US";
const NEW_YORK = "America/New_York";
const ORDER_STATUSES = ["SUBMITTED", "PARTIALLY_FILLED", "PENDING_CANCEL", "FILLED", "CANCELED", "REJECTED", "EXPIRED"];

/** v1.3 시각: ISO 8601, 오프셋 생략 시 KST */
export function parseKst(value: unknown): Date | null {
  if (typeof value !== "string" || value.trim() === "") return null;
  const hasOffset = /(Z|[+-]\d{2}:?\d{2})$/i.test(value);
  const date = new Date(hasOffset ? value : `${value}+09:00`);
  return Number.isNaN(date.getTime()) ? null : date;
}

/** KST ISO 시각 → 뉴욕 현지 HH:mm (공통 캘린더 모델은 현지 타임존 + HH:mm) */
function nyClock(value: unknown): string | null {
  const date = parseKst(value);
  if (!date) return null;
  return new Intl.DateTimeFormat("en-GB", {
    timeZone: NEW_YORK, hour: "2-digit", minute: "2-digit", hourCycle: "h23",
  }).format(date);
}

/** % 단위 → 비율 (3.3333 → 0.033333) */
const pct = (value: unknown): Decimal | null => {
  const rate = DorNull(value);
  return rate ? rate.div(100) : rate;
};

export class NextClient implements BrokerClient {
  /** 사용량 텔레메트리 핸들 — 생성자 끝에서 공개 메서드를 계측하며 만든다 (docs/telemetry.md) */
  private readonly usage: BrokerUsage;
  readonly capabilities: BrokerCapabilities = {
    brokerId: "next",
    market: "US",
    currency: "USD",
    candleIntervals: new Set<CandleInterval>(["1m", "1d"]), // v1.3: 1m · 1d
    clientOrderId: true,
    nativeBracket: false, // 서버 /v2/orders/advanced(BRACKET) 연동 전까지 소프트웨어 브라켓
    fractionalShares: false, // v1.3 주문 수량은 정수만
    serverOpenOrders: true,
    environments: new Set<TradingEnvironment>(["PAPER", "LIVE"]), // 키 프리픽스로 결정 (pk_test_ / pk_live_)
  };

  private readonly tokens: TokenManager;
  /** 429 는 Retry-After 만큼 기다렸다가 최대 2회 재시도. 쓰로틀은 없다 (초당 한도가 넉넉함) */
  private readonly limiter = new RateLimiter(0, 2, (attempt) => 1000 * attempt);

  constructor(
    private readonly clientId: string,
    private readonly clientSecret: string,
    private readonly accountId: string = "acc_main",
    private readonly baseUrl: string = "https://openapi.nextsecurities.dev",
    readonly environment: TradingEnvironment = "PAPER",
  ) {
    // 환경은 키 프리픽스가 결정한다 (pk_test_=모의, pk_live_=실전) — 설정과 어긋나면 기동 실패
    const expected = environment === "LIVE" ? "pk_live_" : "pk_test_";
    if (clientId.startsWith("pk_") && !clientId.startsWith(expected)) {
      throw new Error(`environment=${environment} 인데 clientId 가 '${expected}' 로 시작하지 않습니다 (모의=pk_test_, 실전=pk_live_). 키와 환경 설정을 맞추세요.`);
    }
    this.tokens = new TokenManager("next", clientId, () => this.issueToken(), 60);
    this.usage = instrumentBroker(this);
  }

  // ---------------------------------------------------------------- market

  async getQuotes(symbols: string[]): Promise<Quote[]> {
    const requested = new Map(symbols.map((s) => [symbolCodeFor(this.capabilities, s), s]));
    const body = await this.request("GET", `/v1/market/quotes?symbols=${[...requested.keys()].join(",")}`);
    const quotes: Quote[] = [];
    for (const q of body.quotes as Record<string, unknown>[]) {
      // NOT_FOUND / NO_DATA 는 개별 종목의 정상 결과 — 가격이 없으므로 제외한다 (ctx.quote() 가 undefined)
      if (q.outcome !== "OK" || q.price == null) continue;
      quotes.push({
        symbol: requested.get(String(q.symbol)) ?? String(q.symbol), // 요청받은 표기(시장 접두 포함)로
        price: D(q.price),
        bidPrice: DorNull(q.bidPrice),
        askPrice: DorNull(q.askPrice),
        volume: Number(q.volume ?? 0),
        change: DorNull(q.change),
        changeRate: pct(q.changeRate),
        timestamp: parseKst(q.lastTradeAt) ?? parseKst(q.requestedAt) ?? new Date(),
      });
    }
    return quotes;
  }

  async getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]> {
    let path = `/v1/market/candles?symbol=${symbolCodeFor(this.capabilities, symbol)}&interval=${interval}`;
    if (limit !== undefined) path += `&limit=${limit}`;
    const body = await this.request("GET", path);
    return (body.candles as Record<string, unknown>[]).map((c) => ({
      timestamp: parseKst(c.time) ?? new Date(0),
      open: D(c.open), high: D(c.high), low: D(c.low), close: D(c.close),
      volume: Number(c.volume ?? 0),
    }));
  }

  /** v1.3: date 는 거래소 현지 일자, 세션 시각은 KST → 뉴욕 현지 HH:mm 으로 바꿔 담는다 */
  async getCalendar(): Promise<MarketDay[]> {
    const body = await this.request("GET", "/v1/market/calendar");
    return (body.calendar as Record<string, unknown>[]).map((d) => {
      const sessions = (d.sessions as Record<string, unknown>[] | null) ?? [];
      const regular = sessions.find((s) => s.type === "REGULAR");
      const start = regular ? nyClock(regular.open) : null;
      const end = regular ? nyClock(regular.close) : null;
      const open = (d.status === "OPEN" || d.status === "HALF_DAY") && start !== null && end !== null;
      return {
        date: String(d.date),
        open,
        regular: open ? { start: start!, end: end! } : null,
        timezone: NEW_YORK,
        holiday: (d.holidayName as string | null) ?? null,
      };
    });
  }

  // --------------------------------------------------------------- account

  /** v1.3 계좌 응답은 예수금(cashAmount)만 준다. 총평가는 예수금 + 보유 평가금액 합 (보유 조회 1회 추가) */
  async getAccount(): Promise<Account> {
    const body = await this.request("GET", "/v1/account", { account: true });
    const cash = D(body.cashAmount);
    const holdings = await this.getHoldings();
    const marketValue = holdings.reduce((acc, h) => acc.plus(h.marketValue ?? 0), new Decimal(0));
    return {
      accountId: String(body.accountId),
      currency: String(body.currency ?? "USD"),
      cash,
      portfolioValue: cash.plus(marketValue),
      status: "ACTIVE",
      name: null,
    };
  }

  async getHoldings(): Promise<Holding[]> {
    const body = await this.request("GET", "/v1/account/holdings", { account: true });
    return (body.holdings as Record<string, unknown>[]).map((h) => ({
      symbol: String(h.symbol),
      quantity: D(h.quantity),
      avgEntryPrice: D(h.averageBuyPrice),
      currentPrice: DorNull(h.currentPrice),
      marketValue: DorNull(h.evaluationAmount),
      unrealizedPnl: DorNull(h.evaluationPnl),
      unrealizedPnlRate: pct(h.evaluationPnlRate),
    }));
  }

  async getBuyingPower(): Promise<Decimal> {
    const body = await this.request("GET", "/v1/account/buying-power", { account: true });
    return D(body.buyingPower);
  }

  // ---------------------------------------------------------------- orders

  async createOrder(request: CreateOrderRequest): Promise<Order> {
    const payload: Record<string, unknown> = {
      // v1.3: clientOrderId(멱등키)·market 필수 — 호출자가 안 주면 어댑터가 UUID 를 만든다
      clientOrderId: request.clientOrderId || randomUUID(),
      market: MARKET,
      symbol: symbolCodeFor(this.capabilities, request.symbol),
      side: request.side,
      orderType: request.orderType,
      quantity: request.quantity.toString(),
      timeInForce: request.timeInForce ?? "DAY",
    };
    if (request.limitPrice) payload.limitPrice = request.limitPrice.toString();
    return this.toOrder(await this.request("POST", "/v1/orders", { account: true, json: payload }));
  }

  async getOrders(): Promise<Order[]> {
    const body = await this.request("GET", "/v1/orders", { account: true });
    return (body.orders as Record<string, unknown>[]).map((o) => this.toOrder(o));
  }

  async getOrder(orderId: string): Promise<Order> {
    return this.toOrder(await this.request("GET", `/v1/orders/${orderId}`, { account: true }));
  }

  async cancelOrder(orderId: string): Promise<Order> {
    return this.toOrder(await this.request("DELETE", `/v1/orders/${orderId}`, { account: true }));
  }

  async getFills(): Promise<Fill[]> {
    const body = await this.request("GET", "/v1/orders/fills", { account: true });
    return (body.fills as Record<string, unknown>[]).map((f) => ({
      fillId: null, // v1.3: 원장이 체결 ID 를 발급하지 않는다
      orderId: (f.orderId as string) ?? null,
      symbol: (f.symbol as string) ?? null,
      side: (f.side as OrderSide) ?? null,
      quantity: DorNull(f.quantity),
      price: DorNull(f.price),
    }));
  }

  // -------------------------------------------------------------- internal

  /** 생성/상세/취소 응답 공통 — 생성·취소는 orderId/status/requestedAt 만 온다 */
  private toOrder(body: Record<string, unknown>): Order {
    const status = ORDER_STATUSES.includes(String(body.status)) ? (body.status as OrderStatus) : "UNKNOWN";
    const orderType = String(body.orderType ?? "").toUpperCase();
    return {
      orderId: String(body.orderId),
      status,
      symbol: (body.symbol as string) ?? null,
      side: (body.side as OrderSide) ?? null,
      orderType: orderType === "MARKET" || orderType === "LIMIT" ? (orderType as OrderType) : null,
      quantity: DorNull(body.quantity),
      limitPrice: DorNull(body.limitPrice),
      filledQuantity: DorNull(body.filledQuantity),
      avgFillPrice: DorNull(body.avgFillPrice),
      clientOrderId: (body.requestId as string) ?? null, // v1.3 상세 조회의 clientOrderId 필드명
      submittedAt: parseKst(body.requestedAt),
      canceledAt: null,
    };
  }

  private async request(
    method: string, path: string,
    opts: { account?: boolean; json?: Record<string, unknown> } = {},
  ): Promise<Record<string, unknown>> {
    const call = async (token: string): Promise<Record<string, unknown>> => {
      const headers: Record<string, string> = {
        Authorization: `Bearer ${token}`,
        [NEXT_REQUEST_ID_HEADER]: newRequestId(),
      };
      if (opts.account) headers[NEXT_ACCOUNT_HEADER] = this.accountId;
      if (opts.json) headers["Content-Type"] = "application/json";
      const [status, body, resHeaders] = await httpJson(this.baseUrl + path, {
        method, headers, body: opts.json ? JSON.stringify(opts.json) : undefined,
      });
      if (status < 200 || status >= 300) {
        const retryAfter = Number(resHeaders?.["retry-after"]);
        throw this.mapError(status, (body.error ?? {}) as NextError, Number.isFinite(retryAfter) ? retryAfter : null);
      }
      return body;
    };
    // 토큰 거부(만료) 시 1회 재발급 후 재시도는 TokenManager.call
    return this.limiter.execute(() => this.tokens.call(call), "next");
  }

  /**
   * v1.3 에러 type ↔ HTTP: validation(400) authentication(401) permission(403) not_found(404)
   * conflict(409) business_rule(422) locked(423, 킬스위치 trading-halted) rate_limit(429) server(5xx)
   */
  private mapError(status: number, error: NextError, retryAfterSeconds: number | null = null): BrokerApiError {
    const code = error.code ?? null;
    const message = `Next(${code}) ${error.message ?? ""} requestId=${error.requestId}`;
    if (status === 401 || error.type === "authentication") return new AuthError(status, code, message);
    if (status === 429) return new RateLimitError(status, code, message, retryAfterSeconds);
    // 조회전용 키(insufficient-scope)·계좌 불일치 — 자금 부족으로 오인하지 않는다
    if (error.type === "permission") return new BrokerApiError(status, code, message);
    if (code === "order-not-found") return new OrderNotFoundError(code, message);
    if (code?.includes("insufficient")) return new InsufficientFundsError(status, code, message);
    if (code === "trading-halted" || code?.includes("market-closed")) return new MarketClosedError(status, code, message);
    if (error.type === "validation" || error.type === "business_rule") return new InvalidOrderError(status, code, message);
    return new BrokerApiError(status, code, message);
  }

  private issueToken(): Promise<{ token: string; expiresAt: number }> {
    return this.usage.measure("auth", async () => {
      const form = new URLSearchParams({
        grant_type: "client_credentials",
        client_id: this.clientId,
        client_secret: this.clientSecret,
      });
      const [status, body, resHeaders] = await httpJson(`${this.baseUrl}/v1/oauth/token`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          [NEXT_REQUEST_ID_HEADER]: newRequestId(),
        },
        body: form.toString(),
      });
      if (status !== 200 || typeof body.access_token !== "string") {
        const retryAfter = Number(resHeaders?.["retry-after"]);
        throw this.tokenError(status, body, Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter : null);
      }
      return { token: body.access_token, expiresAt: Date.now() + Number(body.expires_in ?? 43200) * 1000 };
    });
  }

  private tokenError(status: number, body: Record<string, unknown>, retryAfterSeconds: number | null = null): BrokerApiError {
    // 토큰 발급 400/401 은 OAuth 표준 형식: {"error":"invalid_client","error_description":"..."}
    if (typeof body.error === "string") {
      return new AuthError(status, body.error, `Next 토큰 발급 실패(${body.error}): ${body.error_description ?? ""}`);
    }
    // 429/5xx 는 플랫폼 엔벨로프: {"error":{"code":..,"message":..}}
    const error = (body.error ?? {}) as NextError;
    const message = `Next 토큰 발급 실패(${error.code}): ${error.message ?? ""}`;
    if (status === 429) return new RateLimitError(status, error.code ?? null, message, retryAfterSeconds ?? FAILURE_COOLDOWN_SECONDS);
    return new AuthError(status, error.code ?? null, message);
  }
}
