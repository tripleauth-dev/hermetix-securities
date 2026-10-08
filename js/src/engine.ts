/** 전략 실행 엔진 - 매 틱: 장시간 확인 -> 스냅샷 -> 브라켓 점검 -> 전략 호출 -> 시그널 실행. */
import { Decimal } from "decimal.js";
import type { BrokerClient, MarketStream } from "./broker.js";
import { isStreamingBrokerClient, sleep } from "./broker.js";
import { InsufficientFundsError, MarketClosedError, RateLimitError } from "./errors.js";
import type { Holding, MarketDay, Order, OrderBookTick, OrderEvent, Quote, StreamChannel, TradeTick, TradingEnvironment } from "./models.js";
import { isOpenStatus, orderIdMatches, tradeTickToQuote } from "./models.js";
import type { Buy, Sell, Signal, Strategy } from "./strategy.js";
import { StrategyContext } from "./strategy.js";

const log = {
  info: (msg: string) => console.log(`INFO hermetix ${msg}`),
  warn: (msg: string) => console.warn(`WARN hermetix ${msg}`),
  error: (msg: string) => console.error(`ERROR hermetix ${msg}`),
  debug: (msg: string) => { if (process.env.HERMETIX_DEBUG) console.log(`DEBUG hermetix ${msg}`); },
};

/** 개장 판단 - 브로커 캘린더를 6시간 캐시. */
export class MarketCalendar {
  private cache: MarketDay[] | null = null;
  private cachedAt = 0;

  constructor(private readonly broker: BrokerClient) {}

  async isRegularOpen(now = new Date()): Promise<boolean> {
    const days = await this.calendar();
    if (days.length === 0) return false;
    const zone = days[0].timezone;
    const date = new Intl.DateTimeFormat("en-CA", { timeZone: zone }).format(now);
    const today = days.find((d) => d.date === date);
    if (!today?.open || !today.regular) return false;
    const hhmm = new Intl.DateTimeFormat("en-GB", {
      timeZone: zone, hour: "2-digit", minute: "2-digit", hour12: false,
    }).format(now);
    return today.regular.start <= hhmm && hhmm < today.regular.end;
  }

  private async calendar(): Promise<MarketDay[]> {
    if (!this.cache || Date.now() - this.cachedAt > 6 * 3600_000) {
      this.cache = await this.broker.getCalendar();
      this.cachedAt = Date.now();
      log.info(`market calendar refreshed / days=${this.cache.length}`);
    }
    return this.cache;
  }
}

/** 비상정지 - 연속 실패 임계치 도달 시 미체결 전량 취소 + 신규 주문 차단. */
export class TradingGuard {
  private failures = 0;
  private halted = false;

  constructor(private readonly broker: BrokerClient, private readonly maxFailures = 5) {}

  get isHalted(): boolean { return this.halted; }

  recordSuccess(): void { this.failures = 0; }

  async recordFailure(cause: unknown): Promise<void> {
    this.failures++;
    log.warn(`engine failure ${this.failures}/${this.maxFailures} - ${cause}`);
    if (this.failures >= this.maxFailures && !this.halted) await this.halt(`연속 실패 ${this.failures}회`);
  }

  async halt(reason: string): Promise<void> {
    if (this.halted) return;
    this.halted = true;
    log.error(`TRADING HALTED / ${reason} - 미체결 전량 취소`);
    try {
      for (const order of await this.broker.getOrders()) {
        if (!isOpenStatus(order.status)) continue;
        try {
          await this.broker.cancelOrder(order.orderId);
          log.info(`halt-cancel ok / ${order.orderId}`);
        } catch (e) {
          log.error(`halt-cancel failed / ${order.orderId}: ${e}`);
        }
      }
    } catch (e) {
      log.error(`halt: open order lookup failed: ${e}`);
    }
  }

  resume(): void {
    this.failures = 0;
    this.halted = false;
    log.info("trading resumed");
  }
}

/**
 * 주문 금액 상한 — 실전투자에서 봇 폭주 손실 규모를 제한한다 (모의에서도 설정하면 적용).
 * maxOrderValue: 주문 1건 추정 금액(수량 × 지정가 또는 현재가) 상한. maxDailyOrderValue: 하루(UTC) 누적 상한(매수·매도 합산).
 * 추정 금액을 알 수 없으면 상한이 설정된 경우 거부한다. 누적치는 메모리에만 있다.
 */
export class RiskGuard {
  private day: string;
  private dailyTotal = new Decimal(0);

  constructor(
    private readonly maxOrderValue: Decimal | null = null,
    private readonly maxDailyOrderValue: Decimal | null = null,
    private readonly now: () => Date = () => new Date(),
  ) {
    this.day = this.today();
  }

  get isActive(): boolean { return this.maxOrderValue !== null || this.maxDailyOrderValue !== null; }

  /** 허용하면 일일 누적에 반영하고 null, 거부하면 사유. 누적은 제출 전에 잡는다 (보수적) */
  tryReserve(symbol: string, quantity: Decimal, price: Decimal | null | undefined): string | null {
    if (!this.isActive) return null;
    if (!price || price.lte(0)) return `가격을 알 수 없어 주문 금액 상한을 검증할 수 없습니다 (symbol=${symbol})`;
    const value = quantity.mul(price);
    if (this.maxOrderValue && value.gt(this.maxOrderValue)) {
      return `주문 금액 ${value} 이(가) 1건 상한 ${this.maxOrderValue} 을(를) 초과합니다 (symbol=${symbol})`;
    }
    this.rollDay();
    if (this.maxDailyOrderValue && this.dailyTotal.plus(value).gt(this.maxDailyOrderValue)) {
      return `일일 누적 주문 금액 ${this.dailyTotal.plus(value)} 이(가) 상한 ${this.maxDailyOrderValue} 을(를) 초과합니다 (오늘 누적=${this.dailyTotal})`;
    }
    this.dailyTotal = this.dailyTotal.plus(value);
    return null;
  }

  getDailyTotal(): Decimal { this.rollDay(); return this.dailyTotal; }

  private today(): string { return this.now().toISOString().slice(0, 10); }
  private rollDay(): void {
    const today = this.today();
    if (today !== this.day) { this.day = today; this.dailyTotal = new Decimal(0); }
  }
}

interface Bracket {
  entryOrderId: string;
  symbol: string;
  quantity: Decimal;
  takeProfit: Decimal | null;
  stopLoss: Decimal | null;
  active: boolean;
  /** 주문 통보로 누적된 체결 수량 */
  filledQuantity: Decimal;
}

/** 소프트웨어 익절/손절. 상태는 메모리에만 (재시작 시 소실). */
export class BracketMonitor {
  private readonly brackets = new Map<string, Bracket>();

  constructor(private readonly broker: BrokerClient) {}

  register(entryOrderId: string, symbol: string, quantity: Decimal,
           takeProfit: Decimal | null, stopLoss: Decimal | null): void {
    if (!takeProfit && !stopLoss) return;
    this.brackets.set(entryOrderId, { entryOrderId, symbol, quantity, takeProfit, stopLoss, active: false, filledQuantity: new Decimal(0) });
    log.info(`bracket registered / ${entryOrderId} ${symbol} qty=${quantity} tp=${takeProfit} sl=${stopLoss}`);
  }

  async check(ctx: StrategyContext): Promise<Sell[]> {
    const signals: Sell[] = [];
    for (let bracket of [...this.brackets.values()]) {
      if (!bracket.active) {
        const resolved = await this.resolveEntry(bracket);
        if (!resolved?.active) continue;
        bracket = resolved;
      }
      const quote = ctx.quote(bracket.symbol);
      if (!quote) continue;
      const tpHit = bracket.takeProfit !== null && quote.price.gte(bracket.takeProfit);
      const slHit = bracket.stopLoss !== null && quote.price.lte(bracket.stopLoss);
      if (!tpHit && !slHit) continue;

      this.brackets.delete(bracket.entryOrderId);
      const held = ctx.holding(bracket.symbol)?.quantity ?? new Decimal(0);
      const qty = Decimal.min(bracket.quantity, held);
      if (qty.lte(0)) {
        log.warn(`bracket hit but no holdings / ${bracket.symbol}`);
        continue;
      }
      log.info(`bracket ${tpHit ? "TAKE-PROFIT" : "STOP-LOSS"} / ${bracket.symbol} price=${quote.price}`);
      signals.push({ kind: "sell", symbol: bracket.symbol, quantity: qty });
    }
    return signals;
  }

  private async resolveEntry(bracket: Bracket): Promise<Bracket | null> {
    let order: Order;
    try {
      order = await this.broker.getOrder(bracket.entryOrderId);
    } catch (e) {
      log.warn(`bracket entry lookup failed / ${bracket.entryOrderId}: ${e}`);
      return bracket;
    }
    if (order.status === "FILLED") {
      const updated = { ...bracket, active: true };
      this.brackets.set(bracket.entryOrderId, updated);
      log.info(`bracket activated / entry filled ${bracket.entryOrderId}`);
      return updated;
    }
    if (!isOpenStatus(order.status)) {
      this.brackets.delete(bracket.entryOrderId);
      log.info(`bracket dropped / entry ${order.status} ${bracket.entryOrderId}`);
      return null;
    }
    return bracket;
  }

  /**
   * 주문 통보로 진입 주문 상태를 바로 반영한다 (서버 조회 없이). 체결은 누적해 주문 수량을 채우면 활성화,
   * 취소·거부는 폐기. 통보가 없는 브로커에서는 기존처럼 틱마다 resolveEntry 가 조회한다.
   */
  onOrderEvent(event: OrderEvent): void {
    const bracket = [...this.brackets.values()].find((b) => orderIdMatches(event.orderId, b.entryOrderId));
    if (!bracket) return;
    switch (event.type) {
      case "FILLED": {
        const filled = bracket.filledQuantity.plus(event.quantity ?? 0);
        const active = bracket.active || filled.gte(bracket.quantity);
        this.brackets.set(bracket.entryOrderId, { ...bracket, filledQuantity: filled, active });
        if (active && !bracket.active) log.info(`bracket activated / entry filled by event ${bracket.entryOrderId}`);
        break;
      }
      case "CANCELED":
      case "REJECTED":
        this.brackets.delete(bracket.entryOrderId);
        log.info(`bracket dropped / entry ${event.type} by event ${bracket.entryOrderId}`);
        break;
      default:
        break; // ACCEPTED / MODIFIED
    }
  }

  get activeCount(): number { return this.brackets.size; }
}

/** Signal -> 주문 실행. 매도 클램프(공매도 방지), 멱등키(지원 브로커만). */
export class OrderExecutor {
  constructor(
    private readonly broker: BrokerClient,
    private readonly brackets: BracketMonitor,
    private readonly guard: TradingGuard,
    private readonly risk: RiskGuard = new RiskGuard(),
  ) {}

  /** 추정 금액 = 수량 × (지정가 ?? 현재가). 상한을 넘으면 경고 후 false */
  private withinRisk(strategyName: string, symbol: string, quantity: Decimal, limitPrice: Decimal | null | undefined, ctx: StrategyContext): boolean {
    const price = limitPrice ?? ctx.quote(symbol)?.price ?? null;
    const rejection = this.risk.tryReserve(symbol, quantity, price);
    if (rejection === null) return true;
    log.warn(`[${strategyName}] 주문 금액 상한으로 시그널 스킵: ${rejection}`);
    return false;
  }

  async execute(strategyName: string, signals: Signal[], ctx: StrategyContext): Promise<void> {
    for (const signal of signals) {
      if (this.guard.isHalted) {
        log.warn(`[${strategyName}] halted - signal skipped`);
        continue;
      }
      try {
        if (signal.kind === "buy") await this.buy(strategyName, signal, ctx);
        else if (signal.kind === "sell") await this.sell(strategyName, signal, ctx);
        else {
          const order = await this.broker.cancelOrder(signal.orderId);
          log.info(`[${strategyName}] CANCEL / ${signal.orderId} -> ${order.status}`);
        }
      } catch (e) {
        if (e instanceof InsufficientFundsError) {
          log.warn(`[${strategyName}] 주문가능금액 부족으로 시그널 스킵`);
        } else {
          log.error(`[${strategyName}] signal 실행 실패: ${e}`);
        }
      }
    }
  }

  private async buy(strategyName: string, signal: Buy, ctx: StrategyContext): Promise<void> {
    if (!this.withinRisk(strategyName, signal.symbol, signal.quantity, signal.limitPrice, ctx)) return;
    const order = await this.broker.createOrder({
      symbol: signal.symbol, side: "BUY",
      orderType: signal.orderType ?? "MARKET",
      quantity: signal.quantity,
      limitPrice: signal.limitPrice ?? null,
      timeInForce: signal.timeInForce ?? "DAY",
      clientOrderId: this.clientOrderId(strategyName),
    });
    log.info(`[${strategyName}] BUY 접수 / ${signal.symbol} qty=${signal.quantity} orderId=${order.orderId}`);
    this.brackets.register(order.orderId, signal.symbol, signal.quantity,
      signal.takeProfitPrice ?? null, signal.stopLossPrice ?? null);
  }

  private async sell(strategyName: string, signal: Sell, ctx: StrategyContext): Promise<void> {
    const held = ctx.holding(signal.symbol)?.quantity ?? new Decimal(0);
    const qty = Decimal.min(signal.quantity, held);
    if (qty.lte(0)) {
      log.warn(`[${strategyName}] SELL 스킵 / ${signal.symbol} 보유 수량 없음`);
      return;
    }
    if (!this.withinRisk(strategyName, signal.symbol, qty, signal.limitPrice, ctx)) return;
    const order = await this.broker.createOrder({
      symbol: signal.symbol, side: "SELL",
      orderType: signal.orderType ?? "MARKET",
      quantity: qty,
      limitPrice: signal.limitPrice ?? null,
      timeInForce: signal.timeInForce ?? "DAY",
      clientOrderId: this.clientOrderId(strategyName),
    });
    log.info(`[${strategyName}] SELL 접수 / ${signal.symbol} qty=${qty} orderId=${order.orderId}`);
  }

  private clientOrderId(strategyName: string): string | null {
    if (!this.broker.capabilities.clientOrderId) return null;
    return `${strategyName}-${Math.random().toString(36).slice(2, 10)}`;
  }
}

export interface EngineOptions {
  /** 브로커가 LIVE 환경이면 true 여야 스케줄한다 (실전 명시 동의) */
  liveTradingEnabled?: boolean;
  /** 주문 1건 추정 금액 상한 (브로커 통화) */
  maxOrderValue?: Decimal;
  /** 하루(UTC) 누적 주문 금액 상한 */
  maxDailyOrderValue?: Decimal;
}

/**
 * 등록된 전략들을 각자의 pollInterval 로 순차 호출한다.
 *
 * 실시간 트리거(trigger: "ON_TRADE"): 브로커가 StreamingBrokerClient 이고 TRADES 채널을 선언하면 전략 심볼의 체결가 스트림을 구독하고,
 * 틱마다 tick 을 단일 실행 큐에 넣는다. 대기 중인 틱이 있으면 합치고, 직전 tick 종료 후 minTickIntervalMs 가 지나야 다음을 돌린다.
 * 스트림 틱이 모든 심볼을 덮으면 quotes REST 호출 대신 마지막 틱을 현재가로 쓴다. 폴링은 안전망으로 계속 돈다.
 * 호가(spec.orderBook): 심볼 호가창을 구독해 ctx.orderBook(symbol) 로 공급한다 — 틱을 촉발하지는 않는다.
 * 주문 통보: 브로커가 ORDER_EVENTS 를 선언하면 자동 구독해 어댑터(applyOrderEvent)와 브라켓(onOrderEvent)에 반영한다.
 */
export class StrategyEngine {
  readonly guard: TradingGuard;
  readonly brackets: BracketMonitor;
  readonly executor: OrderExecutor;
  readonly calendar: MarketCalendar;
  readonly risk: RiskGuard;
  readonly strategies: Strategy[];
  private stopped = false;
  /** 체결가 스트림 (ON_TRADE 전략이 있고 브로커가 지원할 때만 연다) */
  private stream: MarketStream | null = null;
  private streamsAttached = false;
  /** 심볼(요청 표기) → 마지막 체결 틱 */
  private readonly latestTrades = new Map<string, TradeTick>();
  /** 심볼(요청 표기) → 마지막 호가창 (spec.orderBook 전략만) */
  private readonly latestOrderBooks = new Map<string, OrderBookTick>();
  private orderEventsAttached = false;
  /** 전략 이름 → 스케줄 대기 중인 스트림 틱이 있는지 (합치기용) */
  private readonly pendingTicks = new Set<string>();
  /** 전략 이름 → 직전 tick 종료 시각 (epoch ms) */
  private readonly lastTickEndedAt = new Map<string, number>();
  private readonly tickTimers = new Set<NodeJS.Timeout>();
  /** tick 은 절대 동시에 돌지 않는다 — 폴링·스트림 모두 이 체인을 통과한다 */
  private tickChain: Promise<void> = Promise.resolve();

  /** 스트림이 열려 있고 로그인까지 끝났는지 — 상태 확인용 */
  get streamConnected(): boolean { return this.stream?.isConnected === true; }

  constructor(readonly broker: BrokerClient, strategies: Strategy[], maxConsecutiveFailures = 5, options: EngineOptions = {}) {
    this.guard = new TradingGuard(broker, maxConsecutiveFailures);
    this.brackets = new BracketMonitor(broker);
    this.risk = new RiskGuard(options.maxOrderValue ?? null, options.maxDailyOrderValue ?? null);
    this.executor = new OrderExecutor(broker, this.brackets, this.guard, this.risk);
    this.calendar = new MarketCalendar(broker);

    const caps = broker.capabilities;
    const environment: TradingEnvironment = broker.environment ?? "PAPER";
    const supported = caps.environments ?? new Set<TradingEnvironment>(["PAPER"]);
    if (!supported.has(environment)) {
      log.error(`브로커 '${caps.brokerId}' 는 ${environment} 환경을 지원하지 않습니다 (지원: ${[...supported].join(",")}) - 엔진을 시작하지 않습니다`);
      this.strategies = [];
      return;
    }
    if (environment === "LIVE" && !options.liveTradingEnabled) {
      log.error(`브로커 '${caps.brokerId}' 가 실전투자(LIVE)로 설정돼 있지만 liveTradingEnabled 가 없습니다 - 엔진을 시작하지 않습니다. 실제 돈으로 거래하려면 명시하세요.`);
      this.strategies = [];
      return;
    }
    if (environment === "LIVE") {
      log.warn("***** 실전투자(LIVE) 모드 - 주문이 실제 계좌에서 체결됩니다. 주문 금액 상한(maxOrderValue 등) 설정을 권장합니다 *****");
    }
    this.strategies = strategies.filter((s) => {
      const interval = s.spec.candleInterval ?? "1d";
      if (!caps.candleIntervals.has(interval)) {
        // capability 검증 - 미지원 조합은 스케줄하지 않는다 (fail-fast)
        log.error(`[${s.spec.name}] 스케줄 제외: 브로커 '${caps.brokerId}' 는 ${interval} 캔들을 지원하지 않습니다 ` +
          `(지원: ${[...caps.candleIntervals].join(",")})`);
        return false;
      }
      return true;
    });
    log.info(`broker=${caps.brokerId} environment=${environment} market=${caps.market} / strategies=${this.strategies.map((s) => s.spec.name).join(",")}`);
  }

  /** 블로킹 실행 루프. stop() 으로 종료. */
  async run(): Promise<void> {
    this.attachStreams();
    const nextRun = new Map(this.strategies.map((s) => [s.spec.name, 0]));
    while (!this.stopped) {
      for (const strategy of this.strategies) {
        if (Date.now() >= (nextRun.get(strategy.spec.name) ?? 0)) {
          await this.runExclusive(() => this.tick(strategy));
          nextRun.set(strategy.spec.name, Date.now() + (strategy.spec.pollIntervalSeconds ?? 60) * 1000);
        }
      }
      await sleep(1000);
    }
  }

  stop(): void {
    this.stopped = true;
    for (const t of this.tickTimers) clearTimeout(t);
    this.tickTimers.clear();
    try { this.stream?.close(); } catch { /* 무시 */ }
  }

  /** 브로커가 채널을 제공하면 공유 스트림을 (필요 시 열어) 돌려주고, 아니면 null */
  private streamFor(channel: StreamChannel): MarketStream | null {
    const broker = this.broker;
    if (!isStreamingBrokerClient(broker) || !broker.capabilities.streams?.has(channel)) return null;
    if (!this.stream) {
      this.stream = broker.openStream();
      this.stream.connect();
    }
    return this.stream;
  }

  /** ON_TRADE·orderBook 전략을 스트림에 붙이고 주문 통보를 구독한다 (한 번만). 브로커가 지원하지 않으면 경고만 남기고 폴링으로 둔다 */
  attachStreams(): void {
    if (this.streamsAttached) return;
    this.streamsAttached = true;
    for (const strategy of this.strategies) {
      const spec = strategy.spec;
      if (spec.trigger === "ON_TRADE") {
        const s = this.streamFor("TRADES");
        if (!s) {
          log.warn(`[${spec.name}] trigger=ON_TRADE 이지만 브로커 '${this.broker.capabilities.brokerId}' 는 체결가 스트림을 제공하지 않습니다 - ` +
            `pollIntervalSeconds=${spec.pollIntervalSeconds ?? 60} 폴링으로 동작합니다`);
        } else {
          s.subscribeTrades(spec.symbols, (tick) => {
            this.latestTrades.set(tick.symbol, tick);
            this.requestTick(strategy);
          });
          log.info(`[${spec.name}] trade stream attached / symbols=${spec.symbols.join(",")} minTickIntervalMs=${spec.minTickIntervalMs ?? 1000}`);
        }
      }
      if (spec.orderBook) {
        const s = this.streamFor("ORDER_BOOK");
        if (!s) {
          log.warn(`[${spec.name}] orderBook=true 이지만 브로커 '${this.broker.capabilities.brokerId}' 는 호가 스트림을 제공하지 않습니다 - 컨텍스트의 orderBook 은 비어 있습니다`);
        } else {
          s.subscribeOrderBook(spec.symbols, (tick) => { this.latestOrderBooks.set(tick.symbol, tick); });
          log.info(`[${spec.name}] order book stream attached / symbols=${spec.symbols.join(",")}`);
        }
      }
    }
    this.attachOrderEvents();
  }

  /**
   * 주문 통보를 구독해 어댑터 추적(applyOrderEvent)과 브라켓(onOrderEvent)에 반영한다.
   * 브로커가 제공하면 항상 붙인다 — 구독 실패(KIS HTS ID 미설정 등)는 경고만 남기고 폴링 판정으로 둔다
   */
  private attachOrderEvents(): void {
    if (this.orderEventsAttached) return;
    const broker = this.broker;
    if (!isStreamingBrokerClient(broker)) return;
    const s = this.streamFor("ORDER_EVENTS");
    if (!s) return;
    try {
      s.subscribeOrderEvents((event) => this.onOrderEvent(event));
      this.orderEventsAttached = true;
      log.info("order event stream attached");
    } catch (e) {
      log.warn(`주문 통보 스트림을 구독하지 못했습니다 - 체결 판정은 폴링으로 계속합니다: ${e instanceof Error ? e.message : e}`);
    }
  }

  private onOrderEvent(event: OrderEvent): void {
    log.info(`order event / ${event.type} order=${event.orderId} ${event.symbol ?? ""} ${event.side ?? ""} qty=${event.quantity} price=${event.price}`);
    const broker = this.broker;
    if (isStreamingBrokerClient(broker)) {
      try { broker.applyOrderEvent?.(event); } catch (e) { log.warn(`applyOrderEvent 실패 / ${event.orderId}: ${e}`); }
    }
    try { this.brackets.onOrderEvent(event); } catch (e) { log.warn(`bracket onOrderEvent 실패 / ${event.orderId}: ${e}`); }
  }

  /**
   * 스트림 틱으로 tick 을 요청한다. 이미 대기 중이면 합친다. 최소 간격은 실행 직전에 다시 확인한다 —
   * 틱이 tick 실행 도중 도착하면 스케줄 시점의 "직전 종료 시각" 이 아직 갱신 전이기 때문
   */
  requestTick(strategy: Strategy): void {
    if (this.stopped) return;
    const name = strategy.spec.name;
    if (this.pendingTicks.has(name)) return;
    this.pendingTicks.add(name);
    this.scheduleStreamTick(strategy);
  }

  private scheduleStreamTick(strategy: Strategy): void {
    const timer = setTimeout(() => {
      this.tickTimers.delete(timer);
      void this.runExclusive(async () => {
        if (this.stopped) return;
        if (this.remainingInterval(strategy) > 0) { this.scheduleStreamTick(strategy); return; }
        this.pendingTicks.delete(strategy.spec.name);
        await this.tick(strategy);
      });
    }, this.remainingInterval(strategy));
    this.tickTimers.add(timer);
  }

  /** 직전 tick 종료 후 minTickIntervalMs 까지 남은 ms (0 이면 바로 실행 가능) */
  private remainingInterval(strategy: Strategy): number {
    const min = strategy.spec.minTickIntervalMs ?? 1000;
    return Math.max(0, (this.lastTickEndedAt.get(strategy.spec.name) ?? 0) + min - Date.now());
  }

  private runExclusive(fn: () => Promise<void>): Promise<void> {
    const next = this.tickChain.then(fn, fn);
    this.tickChain = next.catch(() => {});
    return next;
  }

  async tick(strategy: Strategy): Promise<void> {
    const spec = strategy.spec;
    try {
      if (this.guard.isHalted) return;
      if ((spec.regularHoursOnly ?? true) && !(await this.calendar.isRegularOpen())) {
        log.debug(`[${spec.name}] market closed - tick skipped`);
        return;
      }
      const ctx = await this.buildContext(strategy);

      const bracketSignals = await this.brackets.check(ctx); // 익절/손절이 전략보다 우선
      if (bracketSignals.length > 0) await this.executor.execute(spec.name, bracketSignals, ctx);

      const signals = await strategy.decide(ctx);
      if (signals.length > 0) await this.executor.execute(spec.name, signals, ctx);

      this.guard.recordSuccess();
    } catch (e) {
      if (e instanceof MarketClosedError) {
        log.debug(`[${spec.name}] market closed - ${e.message}`); // 휴장 - 실패 아님
      } else if (e instanceof RateLimitError) {
        log.warn(`[${spec.name}] rate limited - ${e.message}`);
      } else {
        log.error(`[${spec.name}] tick failed: ${e}`);
        await this.guard.recordFailure(e);
      }
    } finally {
      this.lastTickEndedAt.set(spec.name, Date.now());
    }
  }

  /** ON_TRADE 전략의 모든 심볼에 스트림 틱이 있으면 그것을 현재가로 쓴다 (REST quotes 1회 절약). 하나라도 없으면 null → REST */
  private streamQuotes(spec: Strategy["spec"]): Map<string, Quote> | null {
    if (spec.trigger !== "ON_TRADE" || !this.stream) return null;
    const quotes = new Map<string, Quote>();
    for (const symbol of spec.symbols) {
      const tick = this.latestTrades.get(symbol);
      if (!tick) return null;
      quotes.set(symbol, tradeTickToQuote(tick));
    }
    return quotes;
  }

  private async buildContext(strategy: Strategy): Promise<StrategyContext> {
    const spec = strategy.spec;
    const quotes = this.streamQuotes(spec) ?? new Map((await this.broker.getQuotes(spec.symbols)).map((q) => [q.symbol, q]));
    const candles = new Map<string, Awaited<ReturnType<BrokerClient["getCandles"]>>>();
    for (const symbol of spec.symbols) {
      candles.set(symbol, await this.broker.getCandles(symbol, spec.candleInterval ?? "1d", spec.candleLimit ?? 30));
    }
    return new StrategyContext(
      new Date(),
      quotes,
      candles,
      await this.broker.getAccount(),
      new Map((await this.broker.getHoldings()).map((h) => [h.symbol, h])),
      (await this.broker.getOrders()).filter((o) => isOpenStatus(o.status)),
      await this.broker.getBuyingPower(),
      this.orderBooksFor(spec),
    );
  }

  /** spec.orderBook 전략의 심볼 중 스트림이 한 번이라도 준 호가창 */
  private orderBooksFor(spec: Strategy["spec"]): Map<string, OrderBookTick> {
    const books = new Map<string, OrderBookTick>();
    if (!spec.orderBook) return books;
    for (const symbol of spec.symbols) {
      const book = this.latestOrderBooks.get(symbol);
      if (book) books.set(symbol, book);
    }
    return books;
  }
}

export interface PnlReport {
  timestamp: string;
  accountId: string;
  currency: string;
  cash: Decimal;
  portfolioValue: Decimal;
  totalMarketValue: Decimal;
  totalUnrealizedPnl: Decimal;
  totalReturnRate: Decimal | null;
  holdings: Holding[];
}

/** 계좌 수익률 리포트. */
export async function pnlReport(broker: BrokerClient, initialCapital?: Decimal): Promise<PnlReport> {
  const account = await broker.getAccount();
  const holdings = await broker.getHoldings();
  const zero = new Decimal(0);
  const totalMv = holdings.reduce((acc, h) => acc.plus(h.marketValue ?? zero), zero);
  // 평가손익 합계는 계좌 통화 종목만 — 외화 종목 손익(currency 가 다름)은 단위가 달라 각 holding 에만 있다
  const totalPnl = holdings
    .filter((h) => !h.currency || h.currency === account.currency)
    .reduce((acc, h) => acc.plus(h.unrealizedPnl ?? zero), zero);
  const totalReturn = initialCapital?.gt(0)
    ? account.portfolioValue.minus(initialCapital).div(initialCapital)
    : null;
  return {
    timestamp: new Date().toISOString(),
    accountId: account.accountId,
    currency: account.currency,
    cash: account.cash,
    portfolioValue: account.portfolioValue,
    totalMarketValue: totalMv,
    totalUnrealizedPnl: totalPnl,
    totalReturnRate: totalReturn,
    holdings,
  };
}
