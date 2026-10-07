/**
 * 키움증권 REST 모의투자 어댑터 (KRX). 실측 기반 (2026-08).
 *
 * - POST + api-id 헤더 라우팅 / return_code(0=성공)
 * - 가격에 등락 부호 접두 (cur_prc "-239500") -> 절대값 / 금액은 zero-padded
 * - TR당 초당 1회 유량 제한 -> 1.1s 쓰로틀 + 백오프
 */
import { instrumentBroker, type BrokerUsage } from "../telemetry.js";
import { Decimal } from "decimal.js";
import {
  D, DorNull, RateLimiter, httpJson, krxCalendar, krxTickRound, kstYyyymmdd,
} from "../broker.js";
import type { MarketStream, StreamingBrokerClient } from "../broker.js";
import { KiwoomMarketStream } from "./kiwoomStream.js";
import { AuthError, BrokerApiError, MarketClosedError, OrderNotFoundError, RateLimitError } from "../errors.js";
import { FAILURE_COOLDOWN_SECONDS, TokenManager } from "../tokens.js";
import type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, Quote,
  TradingEnvironment,
} from "../models.js";
import { symbolCodeFor } from "../models.js";

/** 등락 부호 접두 필드 파싱 - 부호 유지. */
const signed = (v: unknown, fallback = "0"): Decimal => {
  const text = v === null || v === undefined || v === "" ? fallback : String(v).trim().replace(/^\+/, "");
  return new Decimal(text || fallback);
};
const signedOrNull = (v: unknown): Decimal | null => {
  if (v === null || v === undefined || v === "") return null;
  try { return new Decimal(String(v).trim().replace(/^\+/, "")); } catch { return null; }
};

export class KiwoomClient implements StreamingBrokerClient {
  /** 사용량 텔레메트리 핸들 — 생성자 끝에서 공개 메서드를 계측하며 만든다 (docs/telemetry.md) */
  private readonly usage: BrokerUsage;
  readonly capabilities: BrokerCapabilities = {
    brokerId: "kiwoom",
    market: "KRX",
    currency: "KRW",
    candleIntervals: new Set<CandleInterval>(["1d"]),
    clientOrderId: false,
    nativeBracket: false,
    fractionalShares: false,
    serverOpenOrders: true, // ka10075 미체결 조회 제공
    environments: new Set<TradingEnvironment>(["PAPER", "LIVE"]),
    // 0B 주식체결·0D 호가 — 2026-09 모의 실측. 00 주문체결 — 문서 기반 (통보 프레임 실측 전)
    streams: new Set(["TRADES", "ORDER_BOOK", "ORDER_EVENTS"]),
  };

  private readonly tokens: TokenManager;
  /** TR 당 초당 1회 유량 제한 — 쓰로틀 + 백오프 재시도 */
  private readonly limiter: RateLimiter;

  static readonly PAPER_URL = "https://mockapi.kiwoom.com";
  static readonly LIVE_URL = "https://api.kiwoom.com";
  static readonly PAPER_WS_URL = "wss://mockapi.kiwoom.com:10000/api/dostk/websocket";
  static readonly LIVE_WS_URL = "wss://api.kiwoom.com:10000/api/dostk/websocket";
  readonly baseUrl: string;
  readonly wsUrl: string;

  /** baseUrl 을 비우면 환경에 따라 결정(모의 mockapi / 실전 api.kiwoom.com). TR ID 는 공통. wsUrl 도 비우면 환경에 따라 결정 */
  constructor(
    private readonly appkey: string,
    private readonly secretkey: string,
    baseUrl: string = "",
    throttleMs = 1100,
    readonly environment: TradingEnvironment = "PAPER",
    wsUrl: string = "",
  ) {
    this.baseUrl = baseUrl || (environment === "LIVE" ? KiwoomClient.LIVE_URL : KiwoomClient.PAPER_URL);
    this.wsUrl = wsUrl || (environment === "LIVE" ? KiwoomClient.LIVE_WS_URL : KiwoomClient.PAPER_WS_URL);
    this.limiter = new RateLimiter(throttleMs, 3, (attempt) => 1100 * attempt);
    this.tokens = new TokenManager("kiwoom", appkey, () => this.issueToken(), 300);
    this.usage = instrumentBroker(this);
  }

  // ---------------------------------------------------------------- market

  async getQuotes(symbols: string[]): Promise<Quote[]> {
    const quotes: Quote[] = [];
    for (const symbol of symbols) {
      const node = await this.call("/api/dostk/stkinfo", "ka10001", { stk_cd: symbolCodeFor(this.capabilities, symbol) });
      const rate = signedOrNull(node.flu_rt);
      quotes.push({
        symbol,
        price: signed(node.cur_prc).abs(),
        bidPrice: null, askPrice: null,
        volume: Number(signed(node.trde_qty).abs()),
        change: signedOrNull(node.pred_pre),
        changeRate: rate ? rate.div(100) : null,
        timestamp: new Date(),
      });
    }
    return quotes;
  }

  async getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]> {
    if (interval !== "1d") throw new Error("키움 어댑터는 일봉(1d)만 지원합니다");
    const body = await this.call("/api/dostk/chart", "ka10081",
      { stk_cd: symbolCodeFor(this.capabilities, symbol), base_dt: kstYyyymmdd(), upd_stkpc_tp: "1" });
    const candles = ((body.stk_dt_pole_chart_qry ?? []) as Record<string, unknown>[])
      .filter((r) => r.dt)
      .map((r) => ({
        timestamp: new Date(`${String(r.dt).slice(0, 4)}-${String(r.dt).slice(4, 6)}-${String(r.dt).slice(6, 8)}T00:00:00+09:00`),
        open: signed(r.open_pric).abs(), high: signed(r.high_pric).abs(),
        low: signed(r.low_pric).abs(), close: signed(r.cur_prc).abs(),
        volume: Number(signed(r.trde_qty).abs()),
      }))
      .sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
    return limit ? candles.slice(-limit) : candles;
  }

  async getCalendar(): Promise<MarketDay[]> {
    return krxCalendar();
  }

  // --------------------------------------------------------------- account

  async getAccount(): Promise<Account> {
    const deposit = await this.call("/api/dostk/acnt", "kt00001", { qry_tp: "3" });
    const balance = await this.balance();
    const cash = D(deposit.entr);
    let portfolio = D(balance.prsm_dpst_aset_amt);
    if (portfolio.lte(0)) portfolio = cash.plus(D(balance.tot_evlt_amt));
    return { accountId: "kiwoom-mock", currency: "KRW", cash, portfolioValue: portfolio, status: "ACTIVE" };
  }

  async getHoldings(): Promise<Holding[]> {
    return (((await this.balance()).acnt_evlt_remn_indv_tot ?? []) as Record<string, unknown>[])
      .filter((row) => D(row.rmnd_qty).gt(0))
      .map((row) => {
        const rate = signedOrNull(row.prft_rt);
        return {
          symbol: String(row.stk_cd ?? "").replace(/^A/, ""),
          quantity: D(row.rmnd_qty),
          avgEntryPrice: D(row.pur_pric),
          currentPrice: row.cur_prc ? signed(row.cur_prc).abs() : null,
          marketValue: row.evlt_amt ? D(row.evlt_amt) : null,
          unrealizedPnl: signedOrNull(row.evltv_prft),
          unrealizedPnlRate: rate ? rate.div(100) : null,
        };
      });
  }

  async getBuyingPower(): Promise<Decimal> {
    const deposit = await this.call("/api/dostk/acnt", "kt00001", { qry_tp: "3" });
    const power = D(deposit.ord_alow_amt);
    return power.gt(0) ? power : D(deposit.entr);
  }

  // ---------------------------------------------------------------- orders

  async createOrder(request: CreateOrderRequest): Promise<Order> {
    const apiId = request.side === "BUY" ? "kt10000" : "kt10001";
    const isLimit = request.orderType === "LIMIT";
    const node = await this.call("/api/dostk/ordr", apiId, {
      dmst_stex_tp: "KRX",
      stk_cd: symbolCodeFor(this.capabilities, request.symbol),
      ord_qty: request.quantity.toString(),
      ord_uv: isLimit ? krxTickRound(request.limitPrice!).toString() : "",
      trde_tp: isLimit ? "0" : "3",
      cond_uv: "",
    });
    return {
      orderId: String(node.ord_no ?? ""),
      status: "SUBMITTED",
      symbol: symbolCodeFor(this.capabilities, request.symbol), side: request.side, orderType: request.orderType,
      quantity: request.quantity, limitPrice: request.limitPrice ?? null,
      filledQuantity: new Decimal(0), submittedAt: new Date(),
    };
  }

  async getOrders(): Promise<Order[]> {
    return (await this.openRows()).map((row) => this.toOpenOrder(row));
  }

  async getOrder(orderId: string): Promise<Order> {
    const key = orderId.replace(/^0+/, "");
    for (const row of await this.openRows()) {
      if (String(row.ord_no ?? "").replace(/^0+/, "") === key) return this.toOpenOrder(row);
    }
    for (const row of await this.fillRows()) {
      if (String(row.ord_no ?? "").replace(/^0+/, "") === key) {
        return {
          orderId, status: "FILLED",
          symbol: String(row.stk_cd ?? "").replace(/^A/, ""),
          filledQuantity: D(row.cntr_qty),
          avgFillPrice: row.cntr_pric ? signed(row.cntr_pric).abs() : null,
        };
      }
    }
    return { orderId, status: "CANCELED" };
  }

  async cancelOrder(orderId: string): Promise<Order> {
    const key = orderId.replace(/^0+/, "");
    const row = (await this.openRows()).find((r) => String(r.ord_no ?? "").replace(/^0+/, "") === key);
    if (!row) throw new OrderNotFoundError("order-not-found", `키움 미체결 주문을 찾을 수 없습니다: ${orderId}`);
    await this.call("/api/dostk/ordr", "kt10003", {
      dmst_stex_tp: "KRX", orig_ord_no: orderId,
      stk_cd: String(row.stk_cd ?? "").replace(/^A/, ""), cncl_qty: "0",
    });
    return { orderId, status: "CANCELED", canceledAt: new Date() };
  }

  async getFills(): Promise<Fill[]> {
    return (await this.fillRows()).map((r) => ({
      fillId: String(r.ord_no ?? ""), orderId: String(r.ord_no ?? ""),
      symbol: String(r.stk_cd ?? "").replace(/^A/, ""),
      side: String(r.io_tp_nm ?? "").includes("매수") ? "BUY" : "SELL",
      quantity: D(r.cntr_qty),
      price: r.cntr_pric ? signed(r.cntr_pric).abs() : null,
    }));
  }

  // ---------------------------------------------------------------- stream

  /** 웹소켓 로그인은 REST 접근토큰을 그대로 쓴다 — 만료 시 재접속 때 getToken 이 갱신한다 */
  openStream(): MarketStream {
    return new KiwoomMarketStream({ usage: this.usage, wsUrl: this.wsUrl, token: () => this.getToken() });
  }

  // -------------------------------------------------------------- internal

  private toOpenOrder(row: Record<string, unknown>): Order {
    const ordQty = D(row.ord_qty);
    const remaining = row.oso_qty ? D(row.oso_qty) : ordQty;
    const filled = ordQty.minus(remaining);
    return {
      orderId: String(row.ord_no ?? ""),
      status: filled.gt(0) ? "PARTIALLY_FILLED" : "SUBMITTED",
      symbol: String(row.stk_cd ?? "").replace(/^A/, ""),
      side: String(row.io_tp_nm ?? "").includes("매수") ? "BUY" : "SELL",
      orderType: "LIMIT",
      quantity: ordQty,
      limitPrice: row.ord_pric ? signed(row.ord_pric).abs() : null,
      filledQuantity: filled,
    };
  }

  private async openRows(): Promise<Record<string, unknown>[]> {
    const body = await this.call("/api/dostk/acnt", "ka10075",
      { all_stk_tp: "0", trde_tp: "0", stk_cd: "", stex_tp: "0" });
    return (body.oso ?? []) as Record<string, unknown>[];
  }

  private async fillRows(): Promise<Record<string, unknown>[]> {
    const body = await this.call("/api/dostk/acnt", "ka10076",
      { stk_cd: "", qry_tp: "0", sell_tp: "0", ord_no: "", stex_tp: "0" });
    return (body.cntr ?? []) as Record<string, unknown>[];
  }

  private balance(): Promise<Record<string, unknown>> {
    return this.call("/api/dostk/acnt", "kt00018", { qry_tp: "1", dmst_stex_tp: "KRX" });
  }

  private async call(path: string, apiId: string, json: Record<string, string>): Promise<Record<string, unknown>> {
    return this.limiter.execute(() => this.tokens.call((token) => this.callOnce(path, apiId, json, token)), `키움 ${apiId}`);
  }

  private async callOnce(path: string, apiId: string, json: Record<string, string>, token: string): Promise<Record<string, unknown>> {
    const [status, body] = await httpJson(this.baseUrl + path, {
      method: "POST",
      headers: {
        "Content-Type": "application/json;charset=UTF-8",
        authorization: `Bearer ${token}`,
        "api-id": apiId,
      },
      body: JSON.stringify(json),
    });
    if (status < 200 || status >= 300 || body.return_code !== 0) {
      const code = String(body.return_code ?? "");
      const msg = `키움(${apiId}) ${body.return_msg ?? ""}`.trim();
      if (msg.includes("요청 개수를 초과")) throw new RateLimitError(status, code, msg);
      if (msg.includes("장종료") || msg.includes("RC4058")) throw new MarketClosedError(status, code, msg);
      if (status === 401 || msg.includes("8005")) throw new AuthError(status, code, msg); // 8005: Token이 유효하지 않습니다
      throw new BrokerApiError(status, code, msg);
    }
    return body;
  }

  private getToken(): Promise<string> {
    return this.tokens.get();
  }

  private issueToken(): Promise<{ token: string; expiresAt: number }> {
    return this.usage.measure("auth", async () => {
      await this.limiter.throttle.wait();
      const [status, body] = await httpJson(`${this.baseUrl}/oauth2/token`, {
        method: "POST",
        headers: { "Content-Type": "application/json;charset=UTF-8" },
        body: JSON.stringify({ grant_type: "client_credentials", appkey: this.appkey, secretkey: this.secretkey }),
      });
      if (status !== 200 || body.return_code !== 0 || typeof body.token !== "string") {
        const code = String(body.return_code ?? "");
        const msg = `키움 토큰 발급 실패: ${body.return_msg ?? ""}`;
        if (status === 429 || msg.includes("요청 개수를 초과")) throw new RateLimitError(status, code, msg, FAILURE_COOLDOWN_SECONDS);
        throw new AuthError(status, code, msg);
      }
      // expires_dt: yyyyMMddHHmmss (KST)
      const dt = String(body.expires_dt ?? "");
      const expiresAt = dt.length === 14
        ? new Date(`${dt.slice(0, 4)}-${dt.slice(4, 6)}-${dt.slice(6, 8)}T${dt.slice(8, 10)}:${dt.slice(10, 12)}:${dt.slice(12, 14)}+09:00`).getTime()
        : NaN;
      return { token: body.token, expiresAt: Number.isFinite(expiresAt) ? expiresAt : Date.now() + 86400_000 };
    });
  }
}
