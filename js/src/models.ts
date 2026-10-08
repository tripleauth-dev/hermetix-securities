/**
 * 공통 도메인 모델 (Kotlin 레퍼런스의 dto 와 동일 의미).
 *
 * 금액/수량은 전부 Decimal(decimal.js) - JS number 를 절대 섞지 말 것 (0.1+0.2 문제).
 */
import { Decimal } from "decimal.js";

export { Decimal };

export type CandleInterval = "1m" | "5m" | "1h" | "1d";

/** 거래 환경. PAPER=모의투자(기본), LIVE=실전 — 엔진은 liveTradingEnabled 없이는 LIVE 를 기동하지 않는다. 키는 항상 사용자 기기에서만 쓰인다 */
export type TradingEnvironment = "PAPER" | "LIVE";

const MARKET_PREFIX = /^([A-Z]{2,6}):(.+)$/;

/** `MARKET:CODE` 표기를 { market, code } 로 나눈다. 접두가 없으면 market=null */
export function parseSymbol(symbol: string): { market: string | null; code: string } {
  const m = MARKET_PREFIX.exec(symbol);
  return m ? { market: m[1], code: m[2] } : { market: null, code: symbol };
}

/** 접두를 뗀 브로커 심볼 코드 */
export const symbolCode = (symbol: string): string => parseSymbol(symbol).code;

/** 코드가 같고, 둘 다 시장을 명시했다면 시장도 같아야 한다 */
export function symbolsMatch(a: string, b: string): boolean {
  const x = parseSymbol(a);
  const y = parseSymbol(b);
  return x.code === y.code && (x.market === null || y.market === null || x.market === y.market);
}

/** 심볼의 시장 접두가 지원 시장인지 확인하고 브로커 코드를 돌려준다. 미지원이면 throw */
export function symbolCodeFor(caps: BrokerCapabilities, symbol: string): string {
  const { market, code } = parseSymbol(symbol);
  const markets = caps.markets ?? new Set([caps.market]);
  if (market !== null && !markets.has(market)) {
    throw new Error(`브로커 '${caps.brokerId}' 는 시장 '${market}' 을 지원하지 않습니다 (지원: ${[...markets].join(",")}): ${symbol}`);
  }
  return code;
}
export type OrderSide = "BUY" | "SELL";
export type OrderType = "MARKET" | "LIMIT";
export type TimeInForce = "DAY" | "GTC";
/**
 * 주문 상태 (넥스트증권 공개 스펙 v1.3 부록 D 7종 + UNKNOWN 폴백).
 * PENDING_CANCEL 은 취소 접수 후 미확정 — 원주문이 체결될 수 있으므로 OPEN 으로 분류한다.
 */
export type OrderStatus =
  | "SUBMITTED" | "PARTIALLY_FILLED" | "PENDING_CANCEL" | "FILLED"
  | "CANCELED" | "REJECTED" | "EXPIRED" | "UNKNOWN";

export const isOpenStatus = (s: OrderStatus): boolean =>
  s === "SUBMITTED" || s === "PARTIALLY_FILLED" || s === "PENDING_CANCEL";

export interface Quote {
  symbol: string;
  price: Decimal;
  bidPrice: Decimal | null;
  askPrice: Decimal | null;
  volume: number;
  change: Decimal | null;
  changeRate: Decimal | null;
  timestamp: Date;
}

export interface Candle {
  timestamp: Date;
  open: Decimal;
  high: Decimal;
  low: Decimal;
  close: Decimal;
  volume: number;
}

export interface SessionHours { start: string; end: string; }

export interface MarketDay {
  date: string;             // "2026-08-06"
  open: boolean;
  regular: SessionHours | null;
  timezone: string;
  holiday?: string | null;
}

export interface Account {
  accountId: string;
  currency: string;
  cash: Decimal;
  portfolioValue: Decimal;
  status: string;
  name?: string | null;
}

export interface Holding {
  symbol: string;
  quantity: Decimal;
  avgEntryPrice: Decimal;
  currentPrice?: Decimal | null;
  marketValue?: Decimal | null;
  unrealizedPnl?: Decimal | null;
  unrealizedPnlRate?: Decimal | null;
  /** 종목 거래 통화 — avgEntryPrice·currentPrice·unrealizedPnl 의 단위. 생략이면 계좌 통화. marketValue 는 항상 계좌 통화 (계좌 총평가에 더하는 값) */
  currency?: string | null;
}

export interface CreateOrderRequest {
  symbol: string;
  side: OrderSide;
  orderType: OrderType;
  quantity: Decimal;
  limitPrice?: Decimal | null;
  timeInForce?: TimeInForce;
  clientOrderId?: string | null;
}

export interface Order {
  orderId: string;
  status: OrderStatus;
  symbol?: string | null;
  side?: OrderSide | null;
  orderType?: OrderType | null;
  quantity?: Decimal | null;
  limitPrice?: Decimal | null;
  filledQuantity?: Decimal | null;
  avgFillPrice?: Decimal | null;
  clientOrderId?: string | null;
  submittedAt?: Date | null;
  canceledAt?: Date | null;
}

export interface Fill {
  fillId: string | null;
  orderId: string | null;
  symbol: string | null;
  side: OrderSide | null;
  quantity: Decimal | null;
  price: Decimal | null;
}

/** 브로커가 지원하는 기능의 코드 선언. 실측으로 확인한 것만 true 로 선언한다. */
export interface BrokerCapabilities {
  brokerId: string;
  market: "US" | "KRX";
  currency: string;
  candleIntervals: ReadonlySet<CandleInterval>;
  clientOrderId: boolean;
  nativeBracket: boolean;
  fractionalShares: boolean;
  /** false 면 어댑터가 메모리 추적 (재시작 시 추적 소실) */
  serverOpenOrders: boolean;
  /** 지원 거래 환경. 생략 시 PAPER 만. 실전(LIVE)은 실측으로 확인한 어댑터만 선언 */
  environments?: ReadonlySet<TradingEnvironment>;
  /** 한 계좌로 다룰 수 있는 시장 목록 (MARKET:CODE 접두 허용 값). 생략 시 {market} */
  markets?: ReadonlySet<string>;
  /** 실시간 스트림 채널. 생략/빈 집합이면 폴링만. 선언한 어댑터는 StreamingBrokerClient 를 구현해야 한다 */
  streams?: ReadonlySet<StreamChannel>;
}

/** 브로커가 제공하는 실시간 스트림 채널 (BrokerCapabilities.streams 로 선언) — 체결가 / 호가(10단계) / 내 주문 통보 */
export type StreamChannel = "TRADES" | "ORDER_BOOK" | "ORDER_EVENTS";

/**
 * 체결 1건 — 브로커 프레임을 공통 모델로 정규화한 것.
 * symbol 은 구독 요청 표기 그대로 (`KRX:005930` 으로 구독하면 `KRX:005930`). quantity 는 이 체결의 수량, cumulativeVolume 은 당일 누적
 */
export interface TradeTick {
  symbol: string;
  price: Decimal;
  quantity: Decimal;
  timestamp: Date;
  bidPrice?: Decimal | null;
  askPrice?: Decimal | null;
  cumulativeVolume?: number | null;
  change?: Decimal | null;
  changeRate?: Decimal | null;
}

/** 호가 한 단계 */
export interface OrderBookLevel { price: Decimal; quantity: Decimal; }

/** 호가창 스냅샷. asks/bids 는 최우선(1호가)부터 순서대로, 브로커가 주는 만큼(보통 10단계). 심볼은 구독 요청 표기 그대로 */
export interface OrderBookTick {
  symbol: string;
  timestamp: Date;
  asks: OrderBookLevel[];
  bids: OrderBookLevel[];
  totalAskQuantity?: Decimal | null;
  totalBidQuantity?: Decimal | null;
}

export const bestAsk = (tick: OrderBookTick): OrderBookLevel | undefined => tick.asks[0];
export const bestBid = (tick: OrderBookTick): OrderBookLevel | undefined => tick.bids[0];

/** ACCEPTED 접수 / FILLED 체결(부분 포함 — quantity 가 이번 체결량) / CANCELED 취소 확인 / MODIFIED 정정 확인 / REJECTED 거부 */
export type OrderEventType = "ACCEPTED" | "FILLED" | "CANCELED" | "MODIFIED" | "REJECTED";

/**
 * 내 주문 통보 1건.
 * - orderId 는 브로커 주문번호. REST 응답과 자릿수(0 패딩)가 다를 수 있어 비교는 orderIdMatches 로
 * - quantity/price 는 FILLED 면 체결량·체결가, 그 외는 주문량·주문가
 * - remainingQuantity 는 브로커가 주는 경우만 (키움 902). KIS 통보에는 없다
 */
export interface OrderEvent {
  orderId: string;
  type: OrderEventType;
  timestamp: Date;
  symbol?: string | null;
  side?: OrderSide | null;
  quantity?: Decimal | null;
  price?: Decimal | null;
  remainingQuantity?: Decimal | null;
  originalOrderId?: string | null;
  reason?: string | null;
}

/** 앞자리 0 패딩을 무시한 주문번호 정규화 (KIS 통보 10자리 vs REST ODNO 7자리 등) */
export function normalizeOrderId(id: string): string {
  const t = id.trim().replace(/^0+/, "");
  return t === "" ? "0" : t;
}

export const orderIdMatches = (a: string, b: string): boolean => normalizeOrderId(a) === normalizeOrderId(b);

/** 스트림 틱을 REST 현재가와 같은 모양으로 — 엔진이 quotes 호출을 아낄 때 쓴다 */
export function tradeTickToQuote(tick: TradeTick): Quote {
  return {
    symbol: tick.symbol,
    price: tick.price,
    bidPrice: tick.bidPrice ?? null,
    askPrice: tick.askPrice ?? null,
    volume: tick.cumulativeVolume ?? 0,
    change: tick.change ?? null,
    changeRate: tick.changeRate ?? null,
    timestamp: tick.timestamp,
  };
}
