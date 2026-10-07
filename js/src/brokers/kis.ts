/**
 * 한국투자증권(KIS) 모의투자 어댑터 (KRX). 실측 기반 (2026-08).
 *
 * - 초당 요청 제한 -> 600ms 쓰로틀 + EGW00201 백오프 재시도
 * - 토큰 발급 1분당 1회 제한(초과 시 EGW00133 → RateLimitError). 토큰은 24h, 파일 캐시로 프로세스 간 재사용 (TokenManager)
 * - 모의 서버는 미체결/체결 조회 미제공 -> 메모리 주문 추적, 체결은 보유수량 변화 근사
 * - 취소는 지점번호 없이 ODNO 만으로 동작 / 캔들은 일봉만 / 지정가는 호가단위 보정
 */
import { instrumentBroker, type BrokerUsage } from "../telemetry.js";
import { Decimal } from "decimal.js";
import {
  D, DorNull, RateLimiter, httpJson, krxCalendar, krxTickRound, kstToday, kstYyyymmdd,
} from "../broker.js";
import type { MarketStream, StreamingBrokerClient } from "../broker.js";
import { KisMarketStream } from "./kisStream.js";
import { AuthError, BrokerApiError, MarketClosedError, RateLimitError } from "../errors.js";
import { FAILURE_COOLDOWN_SECONDS, TokenManager } from "../tokens.js";
import type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, OrderEvent, Quote,
  TradingEnvironment,
} from "../models.js";
import { orderIdMatches, symbolCodeFor } from "../models.js";
import { isOpenStatus } from "../models.js";

/** 업무 호출이 토큰을 거부 — EGW00121 유효하지 않은 token, EGW00123 기간이 만료된 token (HTTP 500 으로 온다) */
const TOKEN_REJECTED_CODES = new Set(["EGW00121", "EGW00123"]);

interface Tracked { order: Order; baselineQty: Decimal; day: string; }

export class KisClient implements StreamingBrokerClient {
  /** 사용량 텔레메트리 핸들 — 생성자 끝에서 공개 메서드를 계측하며 만든다 (docs/telemetry.md) */
  private readonly usage: BrokerUsage;
  readonly capabilities: BrokerCapabilities = {
    brokerId: "kis",
    market: "KRX",
    currency: "KRW",
    candleIntervals: new Set<CandleInterval>(["1d"]),
    clientOrderId: false,
    nativeBracket: false,
    fractionalShares: false,
    serverOpenOrders: false, // 모의 서버가 주문 조회 미제공 - 어댑터 내부 추적
    environments: new Set<TradingEnvironment>(["PAPER", "LIVE"]),
    // H0STCNT0 체결가·H0STASP0 호가 — 2026-09 모의 실측. H0STCNI9 주문 통보 — 문서 기반 (HTS ID 필요)
    streams: new Set(["TRADES", "ORDER_BOOK", "ORDER_EVENTS"]),
  };

  private readonly tokens: TokenManager;
  /** 초당 요청 제한 — 쓰로틀 + EGW00201 백오프 재시도 */
  private readonly limiter: RateLimiter;
  private readonly tracked = new Map<string, Tracked>();

  static readonly PAPER_URL = "https://openapivts.koreainvestment.com:29443";
  static readonly LIVE_URL = "https://openapi.koreainvestment.com:9443";
  static readonly PAPER_WS_URL = "ws://ops.koreainvestment.com:31000";
  static readonly LIVE_WS_URL = "ws://ops.koreainvestment.com:21000";
  readonly baseUrl: string;
  readonly wsUrl: string;

  /**
   * baseUrl 을 비우면 환경에 따라 결정(모의 openapivts:29443 / 실전 openapi:9443).
   * throttleMs 0 이면 자동 — 모의 600(초당 2건), 실전 100(초당 20건 한도의 절반). 계좌 TR ID 는 모의 V / 실전 T 프리픽스.
   * wsUrl 을 비우면 환경에 따라 결정(모의 ws://ops…:31000 / 실전 :21000).
   * htsId 는 실시간 주문 통보(H0STCNI9/H0STCNI0) 구독 키 — 비우면 주문 통보 스트림을 쓰지 않는다.
   */
  constructor(
    private readonly appkey: string,
    private readonly appsecret: string,
    private readonly cano: string,
    private readonly acntPrdtCd: string = "01",
    baseUrl: string = "",
    throttleMs = 0,
    readonly environment: TradingEnvironment = "PAPER",
    wsUrl: string = "",
    readonly htsId: string = "",
  ) {
    const live = environment === "LIVE";
    this.baseUrl = baseUrl || (live ? KisClient.LIVE_URL : KisClient.PAPER_URL);
    this.wsUrl = wsUrl || (live ? KisClient.LIVE_WS_URL : KisClient.PAPER_WS_URL);
    this.limiter = new RateLimiter(throttleMs || (live ? 100 : 600), 3, (attempt) => 1000 * attempt);
    this.tokens = new TokenManager("kis", appkey, () => this.issueToken(), 300);
    this.usage = instrumentBroker(this);
  }

  /** 계좌 TR ID — 모의 V, 실전 T 프리픽스 (예: tr("TTC0802U") → VTTC0802U / TTTC0802U) */
  tr(suffix: string): string { return (this.environment === "LIVE" ? "T" : "V") + suffix; }

  // ---------------------------------------------------------------- market

  async getQuotes(symbols: string[]): Promise<Quote[]> {
    const quotes: Quote[] = [];
    for (const symbol of symbols) {
      const body = await this.call("GET", "/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
        { query: { FID_COND_MRKT_DIV_CODE: "J", FID_INPUT_ISCD: symbolCodeFor(this.capabilities, symbol) } });
      const out = body.output as Record<string, unknown>;
      const rate = DorNull(out.prdy_ctrt);
      quotes.push({
        symbol,
        price: D(out.stck_prpr),
        bidPrice: null, askPrice: null,
        volume: Number(D(out.acml_vol)),
        change: DorNull(out.prdy_vrss),
        changeRate: rate ? rate.div(100) : null, // % -> 비율
        timestamp: new Date(),
      });
    }
    return quotes;
  }

  async getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]> {
    if (interval !== "1d") throw new Error("KIS 어댑터는 일봉(1d)만 지원합니다");
    const count = limit ?? 30;
    const body = await this.call("GET", "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
      { query: {
        FID_COND_MRKT_DIV_CODE: "J", FID_INPUT_ISCD: symbolCodeFor(this.capabilities, symbol),
        FID_INPUT_DATE_1: kstYyyymmdd(-(count * 1.6 + 10)),
        FID_INPUT_DATE_2: kstYyyymmdd(),
        FID_PERIOD_DIV_CODE: "D", FID_ORG_ADJ_PRC: "0",
      } });
    const candles = (body.output2 as Record<string, unknown>[])
      .filter((r) => r.stck_bsop_date)
      .map((r) => ({
        timestamp: kstDate(String(r.stck_bsop_date)),
        open: D(r.stck_oprc), high: D(r.stck_hgpr), low: D(r.stck_lwpr), close: D(r.stck_clpr),
        volume: Number(D(r.acml_vol)),
      }))
      .sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime()); // 최신순 -> 과거→최신
    return candles.slice(-count);
  }

  async getCalendar(): Promise<MarketDay[]> {
    return krxCalendar();
  }

  // --------------------------------------------------------------- account

  async getAccount(): Promise<Account> {
    const summary = ((await this.balance()).output2 as Record<string, unknown>[])[0];
    if (!summary) throw new BrokerApiError(200, null, "KIS 잔고 요약(output2)이 비어 있습니다");
    return {
      accountId: this.cano, currency: "KRW",
      cash: D(summary.dnca_tot_amt), portfolioValue: D(summary.tot_evlu_amt), status: "ACTIVE",
    };
  }

  async getHoldings(): Promise<Holding[]> {
    return ((await this.balance()).output1 as Record<string, unknown>[])
      .filter((row) => D(row.hldg_qty).gt(0))
      .map((row) => {
        const rate = DorNull(row.evlu_pfls_rt);
        return {
          symbol: String(row.pdno),
          quantity: D(row.hldg_qty),
          avgEntryPrice: D(row.pchs_avg_pric),
          currentPrice: DorNull(row.prpr),
          marketValue: DorNull(row.evlu_amt),
          unrealizedPnl: DorNull(row.evlu_pfls_amt),
          unrealizedPnlRate: rate ? rate.div(100) : null,
        };
      });
  }

  async getBuyingPower(): Promise<Decimal> {
    const body = await this.call("GET", "/uapi/domestic-stock/v1/trading/inquire-psbl-order", this.tr("TTC8908R"),
      { query: { ...this.acct(), PDNO: "005930", ORD_UNPR: "", ORD_DVSN: "01",
                 CMA_EVLU_AMT_ICLD_YN: "N", OVRS_ICLD_YN: "N" } });
    return D((body.output as Record<string, unknown>).ord_psbl_cash);
  }

  // ---------------------------------------------------------------- orders

  async createOrder(request: CreateOrderRequest): Promise<Order> {
    const trId = this.tr(request.side === "BUY" ? "TTC0802U" : "TTC0801U");
    const code = symbolCodeFor(this.capabilities, request.symbol);
    const isLimit = request.orderType === "LIMIT";
    const body = await this.call("POST", "/uapi/domestic-stock/v1/trading/order-cash", trId,
      { json: { ...this.acct(), PDNO: code,
                ORD_DVSN: isLimit ? "00" : "01",
                ORD_QTY: request.quantity.toString(),
                // KRX 호가단위 보정 - 맞지 않는 지정가는 거래소가 거부한다
                ORD_UNPR: isLimit ? krxTickRound(request.limitPrice!).toString() : "0" } });
    const order: Order = {
      orderId: String((body.output as Record<string, unknown>).ODNO),
      status: "SUBMITTED",
      symbol: code, side: request.side, orderType: request.orderType, // 보유/추적과 같은 단일 시장 표기
      quantity: request.quantity, limitPrice: request.limitPrice ?? null,
      filledQuantity: new Decimal(0), submittedAt: new Date(),
    };
    this.tracked.set(order.orderId, {
      order, baselineQty: await this.holdingQty(code), day: kstToday(),
    });
    return order;
  }

  async getOrders(): Promise<Order[]> {
    await this.refreshTracked();
    return [...this.tracked.values()].filter((t) => isOpenStatus(t.order.status)).map((t) => t.order);
  }

  async getOrder(orderId: string): Promise<Order> {
    await this.refreshTracked();
    // 추적 밖(재시작 등)은 알 수 없어 취소로 간주
    return this.tracked.get(orderId)?.order ?? { orderId, status: "CANCELED" };
  }

  async cancelOrder(orderId: string): Promise<Order> {
    // 실측: 모의 서버는 지점번호 없이 ODNO 만으로 취소된다
    await this.call("POST", "/uapi/domestic-stock/v1/trading/order-rvsecncl", this.tr("TTC0803U"),
      { json: { ...this.acct(), KRX_FWDG_ORD_ORGNO: "", ORGN_ODNO: orderId, ORD_DVSN: "00",
                RVSE_CNCL_DVSN_CD: "02", ORD_QTY: "0", ORD_UNPR: "0", QTY_ALL_ORD_YN: "Y" } });
    const canceled: Order = { orderId, status: "CANCELED", canceledAt: new Date() };
    const tracked = this.tracked.get(orderId);
    if (tracked) this.tracked.set(orderId, { ...tracked, order: { ...tracked.order, ...canceled } });
    return canceled;
  }

  async getFills(): Promise<Fill[]> {
    await this.refreshTracked();
    return [...this.tracked.values()]
      .filter((t) => t.order.status === "FILLED")
      .map((t) => ({
        fillId: t.order.orderId, orderId: t.order.orderId,
        symbol: t.order.symbol ?? null, side: t.order.side ?? null,
        quantity: t.order.quantity ?? null, price: t.order.limitPrice ?? null,
      }));
  }

  // ---------------------------------------------------------------- stream

  openStream(): MarketStream {
    return new KisMarketStream({ usage: this.usage,
      wsUrl: this.wsUrl, custtype: "P", approvalKey: () => this.approvalKey(),
      htsId: this.htsId, live: this.environment === "LIVE",
    });
  }

  /**
   * 주문 통보를 메모리 추적에 반영한다 — 모의 서버가 주문 조회를 제공하지 않아 보유 수량 변화로 근사하던 체결 판정을
   * 통보가 오면 즉시 확정한다. 통보 주문번호는 10자리 0 패딩이라 orderIdMatches 로 맞춘다.
   */
  applyOrderEvent(event: OrderEvent): void {
    const entry = [...this.tracked.entries()].find(([id]) => orderIdMatches(event.orderId, id));
    if (!entry) return;
    const [id, tracked] = entry;
    const order = tracked.order;
    let updated: Order;
    switch (event.type) {
      case "FILLED": {
        const filled = (order.filledQuantity ?? new Decimal(0)).plus(event.quantity ?? 0);
        const total = order.quantity ?? filled;
        const done = filled.gte(total);
        updated = {
          ...order,
          filledQuantity: filled,
          avgFillPrice: event.price ?? order.avgFillPrice ?? null,
          status: done ? "FILLED" : "PARTIALLY_FILLED",
        };
        break;
      }
      case "CANCELED":
        updated = { ...order, status: "CANCELED", canceledAt: event.timestamp };
        break;
      case "REJECTED":
        updated = { ...order, status: "REJECTED" };
        break;
      default:
        return; // ACCEPTED / MODIFIED — 상태 변화 없음
    }
    this.tracked.set(id, { ...tracked, order: updated });
    console.log(`INFO hermetix KIS order event applied / ${id} ${event.type} -> ${updated.status}`);
  }

  /**
   * 웹소켓 접속키 (POST /oauth2/Approval). 토큰과 달리 캐시하지 않는다 — 접속마다 새로 받아도 무방.
   * 필드명이 REST 토큰(appsecret)과 달리 secretkey 인 점에 주의.
   */
  async approvalKey(): Promise<string> {
    return this.usage.measure("auth", async () => {
    await this.limiter.throttle.wait();
    const [status, body] = await httpJson(`${this.baseUrl}/oauth2/Approval`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ grant_type: "client_credentials", appkey: this.appkey, secretkey: this.appsecret }),
    });
    if (status < 200 || status >= 300 || typeof body.approval_key !== "string") {
      throw new AuthError(status, (body.error_code as string) ?? null, `KIS 웹소켓 접속키 발급 실패: ${body.error_description ?? ""}`);
    }
    return body.approval_key;
    });
  }

  // -------------------------------------------------------------- internal

  /** 추적 중인 미체결의 체결 여부를 보유 수량 변화로 판정 (모의 서버 제약의 근사). */
  private async refreshTracked(): Promise<void> {
    const today = kstToday();
    for (const [id, t] of this.tracked) if (t.day !== today) this.tracked.delete(id); // DAY 주문 소멸

    const open = [...this.tracked.values()].filter((t) => isOpenStatus(t.order.status));
    if (open.length === 0) return;

    const holdings = new Map((await this.getHoldings()).map((h) => [h.symbol, h.quantity]));
    for (const t of open) {
      const current = holdings.get(t.order.symbol!) ?? new Decimal(0);
      const qty = t.order.quantity ?? new Decimal(0);
      const filled = t.order.side === "BUY"
        ? current.gte(t.baselineQty.plus(qty))
        : current.lte(t.baselineQty.minus(qty));
      if (filled) {
        this.tracked.set(t.order.orderId,
          { ...t, order: { ...t.order, status: "FILLED", filledQuantity: qty } });
      }
    }
  }

  private async holdingQty(symbol: string): Promise<Decimal> {
    return (await this.getHoldings()).find((h) => h.symbol === symbol)?.quantity ?? new Decimal(0);
  }

  private balance(): Promise<Record<string, unknown>> {
    return this.call("GET", "/uapi/domestic-stock/v1/trading/inquire-balance", this.tr("TTC8434R"),
      { query: { ...this.acct(), AFHR_FLPR_YN: "N", OFL_YN: "", INQR_DVSN: "02", UNPR_DVSN: "01",
                 FUND_STTL_ICLD_YN: "N", FNCG_AMT_AUTO_RDPT_YN: "N", PRCS_DVSN: "00",
                 CTX_AREA_FK100: "", CTX_AREA_NK100: "" } });
  }

  private acct(): Record<string, string> {
    return { CANO: this.cano, ACNT_PRDT_CD: this.acntPrdtCd };
  }

  private async call(
    method: string, path: string, trId: string,
    opts: { query?: Record<string, string>; json?: Record<string, string> } = {},
  ): Promise<Record<string, unknown>> {
    return this.limiter.execute(() => this.tokens.call((token) => this.callOnce(method, path, trId, token, opts)), `KIS ${trId}`);
  }

  private async callOnce(
    method: string, path: string, trId: string, token: string,
    opts: { query?: Record<string, string>; json?: Record<string, string> },
  ): Promise<Record<string, unknown>> {
    let url = this.baseUrl + path;
    if (opts.query) url += "?" + new URLSearchParams(opts.query).toString();
    const [status, body] = await httpJson(url, {
      method,
      headers: {
        "Content-Type": "application/json; charset=utf-8",
        authorization: `Bearer ${token}`,
        appkey: this.appkey, appsecret: this.appsecret, tr_id: trId, custtype: "P",
      },
      body: opts.json ? JSON.stringify(opts.json) : undefined,
    });
    if (status < 200 || status >= 300 || body.rt_cd !== "0") {
      const code = (body.msg_cd as string) ?? null;
      const msg = `KIS(${trId}) ${body.msg1 ?? ""}`.trim();
      if (code === "EGW00201") throw new RateLimitError(status, code, msg);
      if (msg.includes("장종료") || msg.includes("장운영일이 아닙")) throw new MarketClosedError(status, code, msg);
      if (status === 401 || (code !== null && TOKEN_REJECTED_CODES.has(code))) throw new AuthError(status, code, msg);
      throw new BrokerApiError(status, code, msg);
    }
    return body;
  }

  private issueToken(): Promise<{ token: string; expiresAt: number }> {
    return this.usage.measure("auth", async () => {
      await this.limiter.throttle.wait();
      const [status, body] = await httpJson(`${this.baseUrl}/oauth2/tokenP`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ grant_type: "client_credentials", appkey: this.appkey, appsecret: this.appsecret }),
      });
      if (status !== 200 || typeof body.access_token !== "string") {
        const code = (body.error_code as string) ?? null;
        const detail = body.error_description ?? "";
        if (code === "EGW00133" || status === 429) {
          throw new RateLimitError(status, code, `KIS 토큰 발급 유량 초과(${code}): ${detail} (발급은 1분당 1회 제한)`, FAILURE_COOLDOWN_SECONDS);
        }
        throw new AuthError(status, code, `KIS 토큰 발급 실패(${code}): ${detail}`);
      }
      return { token: body.access_token, expiresAt: Date.now() + Number(body.expires_in ?? 86400) * 1000 };
    });
  }
}

function kstDate(yyyymmdd: string): Date {
  return new Date(`${yyyymmdd.slice(0, 4)}-${yyyymmdd.slice(4, 6)}-${yyyymmdd.slice(6, 8)}T00:00:00+09:00`);
}
