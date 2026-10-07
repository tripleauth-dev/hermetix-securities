/**
 * KB증권 Open API(개인 오픈베타) 어댑터.
 * ⚠️ 실전 전용 · 실측 전 — 모의투자 없는 운영 단일 환경. 포털 공개 명세와 공식 GitHub 예제로 구현, 실계좌 검증 전까지 "미검증". 오픈베타라 스펙 변동 가능.
 * - 모든 API 는 POST /api/v1/{tr}, 본문·응답 {"dataHeader","dataBody"} 봉투. 헤더 Authorization: bearer + appKey
 * - 성공 dataHeader.processFlag == "A"(HTTP 200 이어도 "B" 면 업무 오류). 숫자 zero-padded, 문자열 공백 패딩 → trim
 * 미확인: bdy_cmpr_ccd 부호 코드(4·5 하락 가정), 체결 조회 레코드 이름(Record1 가정), 표준코드(KR7005930003)→6자리 환산, 차트 시장구분(KOSPI 기본)
 */
import { instrumentBroker, type BrokerUsage } from "../telemetry.js";
import { Decimal } from "decimal.js";
import { BrokerClient, RateLimiter, httpJson, krxCalendar, krxTickRound, kstYyyymmdd } from "../broker.js";
import {
  AuthError, BrokerApiError, InsufficientFundsError, InvalidOrderError, MarketClosedError, OrderNotFoundError, RateLimitError,
} from "../errors.js";
import type {
  Account, BrokerCapabilities, Candle, CandleInterval, CreateOrderRequest,
  Fill, Holding, MarketDay, Order, OrderSide, OrderStatus, Quote, TradingEnvironment,
} from "../models.js";
import { symbolCodeFor } from "../models.js";
import { FAILURE_COOLDOWN_SECONDS, TokenManager } from "../tokens.js";

const FALLING_SIGNS = new Set(["4", "5"]);
const t = (v: unknown): string => String(v ?? "").trim();
const num = (v: unknown): Decimal | null => { const s = t(v).replace(/,/g, ""); if (!s) return null; try { return new Decimal(s); } catch { return null; } };
const pct = (v: unknown): Decimal | null => { const r = num(v); return r ? r.div(100) : null; };
const positive = (v: unknown): Decimal | null => { const d = num(v); return d && d.gt(0) ? d : null; };

/** 잔고 A005930 → 005930, 체결 조회 표준코드 KR7005930003(ISIN) → 4~9번째 자리 */
export const kbNormalizeCode = (raw: unknown): string => {
  const s = t(raw);
  if (s.length === 7 && s[0] === "A") return s.slice(1);
  if (s.length === 12 && s.startsWith("KR")) return s.slice(3, 9);
  return s;
};
const sameNo = (a: unknown, b: unknown) => t(a).replace(/^0+/, "") === t(b).replace(/^0+/, "");
const sideOf = (row: Record<string, unknown>): OrderSide => t(row.trd_dl_ccd_nm).includes("매수") ? "BUY" : "SELL";

export class KbClient implements BrokerClient {
  /** 사용량 텔레메트리 핸들 — 생성자 끝에서 공개 메서드를 계측하며 만든다 (docs/telemetry.md) */
  private readonly usage: BrokerUsage;
  readonly capabilities: BrokerCapabilities = {
    brokerId: "kb", market: "KRX", currency: "KRW",
    candleIntervals: new Set<CandleInterval>(["1d"]),
    clientOrderId: false, nativeBracket: false, fractionalShares: false, serverOpenOrders: true,
    environments: new Set<TradingEnvironment>(["LIVE"]),
  };

  private readonly tokens: TokenManager;
  private readonly limiter: RateLimiter;

  /** chartMarketClsf: 통합차트 시장구분 0 KOSPI / 1 KOSDAQ — 차트 TR 이 종목의 시장을 요구한다 */
  constructor(
    private readonly appKey: string,
    private readonly appSecret: string,
    readonly baseUrl: string = "https://developer.kbsec.com:32484",
    private readonly excgClsf = "1",
    private readonly sorOrderCcd = "K",
    private readonly chartMarketClsf = "0",
    throttleMs = 100,
    readonly environment: TradingEnvironment = "LIVE",
  ) {
    this.limiter = new RateLimiter(throttleMs, 3, (attempt) => 1000 * attempt);
    this.tokens = new TokenManager("kb", appKey, () => this.issueToken(), 300);
    this.usage = instrumentBroker(this);
  }

  async getQuotes(symbols: string[]): Promise<Quote[]> {
    const quotes: Quote[] = [];
    for (const symbol of symbols) {
      const out = await this.call("/api/v1/ivu10140", { excg_clsf: this.excgClsf, shrt_cd: symbolCodeFor(this.capabilities, symbol) });
      const falling = FALLING_SIGNS.has(t(out.bdy_cmpr_ccd));
      let change = num(out.bdy_cmpr);
      let rate = num(out.up_dwn_r_p2);
      if (falling) { if (change && change.gt(0)) change = change.neg(); if (rate && rate.gt(0)) rate = rate.neg(); }
      quotes.push({ symbol, price: num(out.now_prc) ?? new Decimal(0), bidPrice: positive(out.b_sq1_askprc), askPrice: positive(out.s_sq1_askprc), volume: Number(num(out.acml_vlm) ?? 0), change, changeRate: rate ? rate.div(100) : null, timestamp: new Date() });
    }
    return quotes;
  }

  async getCandles(symbol: string, interval: CandleInterval, limit?: number): Promise<Candle[]> {
    if (interval !== "1d") throw new Error("KB 어댑터는 일봉(1d)만 지원합니다.");
    const count = limit ?? 30;
    const out = await this.call("/api/v1/ivs11560", { chrt_clsf: "D", inq_clsf: "2", strt_dy: kstYyyymmdd(), is_cd: symbolCodeFor(this.capabilities, symbol), minute_tck_indx: "일", info_ccd: "1", mkt_clsf: this.chartMarketClsf, inq_cnt: String(count) });
    const candles: Candle[] = [];
    for (const r of ((out.out2 ?? []) as Record<string, unknown>[])) {
      const date = t(r.dt);
      if (date.length !== 8) continue;
      candles.push({ timestamp: new Date(`${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}T00:00:00+09:00`), open: num(r.opn_prc_p2) ?? new Decimal(0), high: num(r.hgh_prc_p2) ?? new Decimal(0), low: num(r.lw_prc_p2) ?? new Decimal(0), close: num(r.cls_prc_p2) ?? new Decimal(0), volume: Number(num(r.vlm) ?? 0) });
    }
    candles.sort((a, b) => a.timestamp.getTime() - b.timestamp.getTime());
    return candles.slice(-count);
  }

  async getCalendar(): Promise<MarketDay[]> { return krxCalendar(); }

  async getAccount(): Promise<Account> {
    const out = await this.balance();
    const cash = num(out.dy_tfnd) ?? new Decimal(0);
    const portfolio = positive(out.nt_asts_val_amt) ?? cash.plus(num(out.val_amt_sum) ?? 0);
    return { accountId: "kb-live", currency: "KRW", cash, portfolioValue: portfolio, status: "ACTIVE", name: null };
  }

  async getHoldings(): Promise<Holding[]> {
    const holdings: Holding[] = [];
    for (const row of (((await this.balance()).Record1 ?? []) as Record<string, unknown>[])) {
      const candidates = [num(row.ec_q), num(row.hld_q)].filter((q): q is Decimal => q !== null);
      const quantity = candidates.length ? Decimal.max(...candidates) : null;
      if (!quantity || quantity.lte(0)) continue;
      holdings.push({ symbol: kbNormalizeCode(row.is_cd), quantity, avgEntryPrice: num(row.byng_avr_prc) ?? new Decimal(0), currentPrice: num(row.now_prc), marketValue: num(row.val_amt), unrealizedPnl: num(row.val_pl), unrealizedPnlRate: pct(row.val_yld) });
    }
    return holdings;
  }

  async getBuyingPower(): Promise<Decimal> {
    const out = await this.call("/api/v1/ssqm1802", { bnd_mktio_ccd: "1", is_no: "" });
    return num(out.ordr_psbl_csh) ?? num(out.ordr_psbl_tl_amt) ?? new Decimal(0);
  }

  async createOrder(request: CreateOrderRequest): Promise<Order> {
    const code = symbolCodeFor(this.capabilities, request.symbol);
    const isLimit = request.orderType === "LIMIT";
    const out = await this.call(request.side === "BUY" ? "/api/v1/ssam1802" : "/api/v1/ssam1801",
      this.orderBody(request.side === "BUY" ? "2" : "1", code, request.quantity.toString(), isLimit ? krxTickRound(request.limitPrice!).toString() : "0", isLimit ? "00" : "03"));
    const orderId = t(out.ordr_no);
    if (!orderId.replace(/^0+/, "")) throw new BrokerApiError(200, null, `KB 주문 응답에 ordr_no 가 없습니다: ${t(out.o_msg)}`);
    return { orderId, status: "SUBMITTED", symbol: code, side: request.side, orderType: request.orderType, quantity: request.quantity, limitPrice: request.limitPrice ?? null, filledQuantity: new Decimal(0), clientOrderId: request.clientOrderId ?? null, submittedAt: new Date() };
  }

  async getOrders(): Promise<Order[]> {
    return (await this.orderRows()).map((r) => this.toOrder(r)).filter((o) => o.status === "SUBMITTED" || o.status === "PARTIALLY_FILLED" || o.status === "PENDING_CANCEL");
  }

  async getOrder(orderId: string): Promise<Order> {
    const row = (await this.orderRows()).find((r) => sameNo(r.ordr_no, orderId));
    return row ? this.toOrder(row) : { orderId, status: "CANCELED" };
  }

  async cancelOrder(orderId: string): Promise<Order> {
    const row = (await this.orderRows()).find((r) => sameNo(r.ordr_no, orderId));
    if (!row) throw new OrderNotFoundError("order-not-found", `KB 당일 주문에서 찾을 수 없습니다: ${orderId}`);
    const remaining = num(row.nccls_q) ?? (num(row.ordr_q) ?? new Decimal(0)).minus(num(row.tl_ccls_q) ?? 0);
    await this.call("/api/v1/ssam1806", { ...this.orderBody("4", kbNormalizeCode(t(row.stnd_is_no) || row.stnd_is_cd), remaining.toString(), "0", "00"), crct_clsf: "2", orgn_ordr_no: orderId.padStart(10, "0") });
    return { orderId, status: "PENDING_CANCEL", canceledAt: new Date() };
  }

  async getFills(): Promise<Fill[]> {
    return (await this.orderRows()).filter((r) => (num(r.tl_ccls_q) ?? new Decimal(0)).gt(0))
      .map((r) => ({ fillId: null, orderId: t(r.ordr_no), symbol: kbNormalizeCode(t(r.stnd_is_no) || r.stnd_is_cd), side: sideOf(r), quantity: num(r.tl_ccls_q), price: positive(r.ccls_uprc) }));
  }

  private orderBody(jbClsf: string, code: string, qty: string, price: string, ordrCcd: string): Record<string, string> {
    return { mkt_tm_clsf: "1", ordr_jb_clsf: jbClsf, s_clsf: "", is_cd: code, ordr_q: qty, ordr_uprc: price, ordr_ccd: ordrCcd, crdt_typ_cd: "00", ln_dt: "", crct_clsf: "", orgn_ordr_no: "", gtc_ccd: "", ordr_mng_no: "", spclz_ordr_ccd: "", acct_cd: "", sor_ordr_ccd: this.sorOrderCcd, stpd_prc: "" };
  }

  private balance(): Promise<Record<string, unknown>> { return this.call("/api/v1/ssqm2952", { excg_mktpr_ccd: "" }); }

  private async orderRows(): Promise<Record<string, unknown>[]> {
    return ((await this.call("/api/v1/ssqm2341", { inq_clsf: "9", ccls_clsf: "0", ordr_dt: kstYyyymmdd(), is_cd: "", ordr_no: "", mthr_ordr_no: "", orgn_ordr_no: "", s_ccls_amt: "", b_ccls_amt: "", s_ccls_q: "", b_ccls_q: "", ac_nm: "", is_nm: "", cn_clsf: "", nxt_key: "" })).Record1 ?? []) as Record<string, unknown>[];
  }

  private toOrder(row: Record<string, unknown>): Order {
    const qty = num(row.ordr_q) ?? new Decimal(0);
    const filled = num(row.tl_ccls_q) ?? new Decimal(0);
    const remaining = num(row.nccls_q) ?? qty.minus(filled);
    const cancelText = t(row.crct_cncl_ccd);
    const reject = t(row.rfsl_rsn_nm);
    let status: OrderStatus;
    if (cancelText.includes("취소") && remaining.gt(0)) status = "PENDING_CANCEL";
    else if (remaining.gt(0) && filled.gt(0)) status = "PARTIALLY_FILLED";
    else if (remaining.gt(0)) status = "SUBMITTED";
    else if (filled.gt(0) && filled.gte(qty)) status = "FILLED";
    else if (filled.gt(0)) status = "PARTIALLY_FILLED";
    else if (reject) status = "REJECTED";
    else status = "CANCELED";
    return { orderId: t(row.ordr_no), status, symbol: kbNormalizeCode(t(row.stnd_is_no) || row.stnd_is_cd), side: sideOf(row), orderType: t(row.ordr_ccd).replace(/^0+/, "") === "3" ? "MARKET" : "LIMIT", quantity: qty, limitPrice: positive(row.ordr_uprc), filledQuantity: filled, avgFillPrice: positive(row.ccls_uprc) };
  }

  private call(path: string, body: Record<string, string>): Promise<Record<string, unknown>> {
    return this.limiter.execute(() => this.tokens.call((token) => this.callOnce(path, body, token)), `KB ${path}`);
  }

  private async callOnce(path: string, body: Record<string, string>, token: string): Promise<Record<string, unknown>> {
    const [status, parsed, resHeaders] = await httpJson(this.baseUrl + path, {
      method: "POST", headers: { "Content-Type": "application/json", Authorization: `bearer ${token}`, appKey: this.appKey },
      body: JSON.stringify({ dataHeader: { ipAddr: "", macAddr: "" }, dataBody: body }),
    });
    const header = (parsed.dataHeader ?? {}) as Record<string, unknown>;
    const data = (parsed.dataBody ?? {}) as Record<string, unknown>;
    const flag = t(header.processFlag);
    const code = t(header.processCode) || t(header.resultCode);
    const message = [t(header.processMessage), t(header.resultMessage), t(data.o_msg)].find((m) => m) ?? "";
    const msg = `KB(${path}) [${code}] ${message}`.trim();
    if (status < 200 || status >= 300 || (flag && flag !== "A")) {
      const retryAfter = Number(resHeaders?.["retry-after"]);
      if (status === 429 || message.includes("한도") || message.includes("초과")) throw new RateLimitError(status, code, msg, Number.isFinite(retryAfter) ? retryAfter : null);
      if (status === 401 || status === 403 || message.includes("토큰") || message.includes("인증")) throw new AuthError(status, code, msg);
      if (["장종료", "장운영", "장마감", "휴장"].some((k) => message.includes(k))) throw new MarketClosedError(status, code, msg);
      if (message.includes("부족")) throw new InsufficientFundsError(status, code, msg);
      if (["호가", "수량", "단위"].some((k) => message.includes(k))) throw new InvalidOrderError(status, code, msg);
      if (message.includes("주문번호") || message.includes("원주문")) throw new OrderNotFoundError(code, msg);
      throw new BrokerApiError(status, code, msg);
    }
    return data;
  }

  private issueToken(): Promise<{ token: string; expiresAt: number }> {
    return this.usage.measure("auth", async () => {
      await this.limiter.throttle.wait();
      const [status, body] = await httpJson(`${this.baseUrl}/oauth2/token`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ dataHeader: { ipAddr: "", macAddr: "" }, dataBody: { appKey: this.appKey, appSecret: this.appSecret, grantType: "client_credentials" } }) });
      const data = (body.dataBody ?? {}) as Record<string, unknown>;
      if (status !== 200 || typeof data.access_token !== "string") {
        const h = (body.dataHeader ?? {}) as Record<string, unknown>;
        const code = t(h.processCode) || null;
        const msg = `KB 토큰 발급 실패(${t(h.resultCode)}): ${t(h.processMessage) || t(h.resultMessage)}`;
        if (status === 429) throw new RateLimitError(status, code, msg, FAILURE_COOLDOWN_SECONDS);
        throw new AuthError(status, code, msg);
      }
      return { token: data.access_token, expiresAt: Date.now() + Number(data.expires_in ?? 86400) * 1000 };
    });
  }
}
