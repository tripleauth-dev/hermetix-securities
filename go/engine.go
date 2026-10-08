package hermetix

// 전략 실행 엔진 - 매 틱: 장시간 확인 -> 스냅샷 -> 브라켓 점검 -> 전략 호출 -> 시그널 실행.

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

// MarketCalendar - 개장 판단, 브로커 캘린더를 6시간 캐시.
type MarketCalendar struct {
	broker   BrokerClient
	mu       sync.Mutex
	cache    []MarketDay
	cachedAt time.Time
}

func NewMarketCalendar(broker BrokerClient) *MarketCalendar {
	return &MarketCalendar{broker: broker}
}

func (m *MarketCalendar) IsRegularOpen(now time.Time) (bool, error) {
	days, err := m.calendar()
	if err != nil {
		return false, err
	}
	if len(days) == 0 {
		return false, nil
	}
	loc, err := time.LoadLocation(days[0].Timezone)
	if err != nil {
		return false, err
	}
	local := now.In(loc)
	date := local.Format("2006-01-02")
	for _, day := range days {
		if day.Date != date {
			continue
		}
		if !day.Open || day.Regular == nil {
			return false, nil
		}
		hhmm := local.Format("15:04")
		return day.Regular.Start <= hhmm && hhmm < day.Regular.End, nil
	}
	return false, nil
}

func (m *MarketCalendar) calendar() ([]MarketDay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cache == nil || time.Since(m.cachedAt) > 6*time.Hour {
		days, err := m.broker.GetCalendar()
		if err != nil {
			return nil, err
		}
		m.cache = days
		m.cachedAt = time.Now()
		log.Printf("INFO hermetix market calendar refreshed / days=%d", len(days))
	}
	return m.cache, nil
}

// TradingGuard - 비상정지: 연속 실패 임계치 도달 시 미체결 전량 취소 + 신규 주문 차단.
type TradingGuard struct {
	broker      BrokerClient
	maxFailures int
	mu          sync.Mutex
	failures    int
	halted      bool
}

func NewTradingGuard(broker BrokerClient, maxFailures int) *TradingGuard {
	if maxFailures <= 0 {
		maxFailures = 5
	}
	return &TradingGuard{broker: broker, maxFailures: maxFailures}
}

func (g *TradingGuard) IsHalted() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.halted
}

func (g *TradingGuard) RecordSuccess() {
	g.mu.Lock()
	g.failures = 0
	g.mu.Unlock()
}

func (g *TradingGuard) RecordFailure(cause error) {
	g.mu.Lock()
	g.failures++
	count := g.failures
	g.mu.Unlock()
	log.Printf("WARN hermetix engine failure %d/%d - %v", count, g.maxFailures, cause)
	if count >= g.maxFailures {
		g.Halt(fmt.Sprintf("연속 실패 %d회", count))
	}
}

func (g *TradingGuard) Halt(reason string) {
	g.mu.Lock()
	if g.halted {
		g.mu.Unlock()
		return
	}
	g.halted = true
	g.mu.Unlock()
	log.Printf("ERROR hermetix TRADING HALTED / %s - 미체결 전량 취소", reason)
	orders, err := g.broker.GetOrders()
	if err != nil {
		log.Printf("ERROR hermetix halt: open order lookup failed: %v", err)
		return
	}
	for _, order := range orders {
		if !order.Status.IsOpen() {
			continue
		}
		if _, err := g.broker.CancelOrder(order.OrderID); err != nil {
			log.Printf("ERROR hermetix halt-cancel failed / %s: %v", order.OrderID, err)
		} else {
			log.Printf("INFO hermetix halt-cancel ok / %s", order.OrderID)
		}
	}
}

func (g *TradingGuard) Resume() {
	g.mu.Lock()
	g.failures = 0
	g.halted = false
	g.mu.Unlock()
	log.Printf("INFO hermetix trading resumed")
}

type bracket struct {
	entryOrderID   string
	symbol         string
	quantity       decimal.Decimal
	takeProfit     *decimal.Decimal
	stopLoss       *decimal.Decimal
	active         bool
	filledQuantity decimal.Decimal // 주문 통보로 누적된 체결량
}

// BracketMonitor - 소프트웨어 익절/손절. 상태는 메모리에만 (재시작 시 소실).
type BracketMonitor struct {
	broker   BrokerClient
	mu       sync.Mutex
	brackets map[string]*bracket
}

func NewBracketMonitor(broker BrokerClient) *BracketMonitor {
	return &BracketMonitor{broker: broker, brackets: map[string]*bracket{}}
}

func (b *BracketMonitor) Register(entryOrderID, symbol string, quantity decimal.Decimal,
	takeProfit, stopLoss *decimal.Decimal) {
	if takeProfit == nil && stopLoss == nil {
		return
	}
	b.mu.Lock()
	b.brackets[entryOrderID] = &bracket{entryOrderID: entryOrderID, symbol: symbol, quantity: quantity, takeProfit: takeProfit, stopLoss: stopLoss}
	b.mu.Unlock()
	log.Printf("INFO hermetix bracket registered / %s %s qty=%s", entryOrderID, symbol, quantity)
}

func (b *BracketMonitor) Check(ctx *StrategyContext) []Signal {
	b.mu.Lock()
	snapshot := make([]*bracket, 0, len(b.brackets))
	for _, br := range b.brackets {
		snapshot = append(snapshot, br)
	}
	b.mu.Unlock()

	signals := make([]Signal, 0)
	for _, br := range snapshot {
		if !br.active && !b.resolveEntry(br) {
			continue
		}
		quote, ok := ctx.Quote(br.symbol)
		if !ok {
			continue
		}
		tpHit := br.takeProfit != nil && quote.Price.GreaterThanOrEqual(*br.takeProfit)
		slHit := br.stopLoss != nil && quote.Price.LessThanOrEqual(*br.stopLoss)
		if !tpHit && !slHit {
			continue
		}
		b.mu.Lock()
		delete(b.brackets, br.entryOrderID)
		b.mu.Unlock()

		held := decimal.Zero
		if h, ok := ctx.Holding(br.symbol); ok {
			held = h.Quantity
		}
		qty := decimal.Min(br.quantity, held)
		if !qty.IsPositive() {
			log.Printf("WARN hermetix bracket hit but no holdings / %s", br.symbol)
			continue
		}
		kind := "STOP-LOSS"
		if tpHit {
			kind = "TAKE-PROFIT"
		}
		log.Printf("INFO hermetix bracket %s / %s price=%s", kind, br.symbol, quote.Price)
		signals = append(signals, SellSignal{Symbol: br.symbol, Quantity: qty})
	}
	return signals
}

func (b *BracketMonitor) resolveEntry(br *bracket) bool {
	order, err := b.broker.GetOrder(br.entryOrderID)
	if err != nil {
		log.Printf("WARN hermetix bracket entry lookup failed / %s: %v", br.entryOrderID, err)
		return false
	}
	if order.Status == Filled {
		br.active = true
		log.Printf("INFO hermetix bracket activated / entry filled %s", br.entryOrderID)
		return true
	}
	if !order.Status.IsOpen() {
		b.mu.Lock()
		delete(b.brackets, br.entryOrderID)
		b.mu.Unlock()
		log.Printf("INFO hermetix bracket dropped / entry %s %s", order.Status, br.entryOrderID)
	}
	return false
}

// OnOrderEvent - 주문 통보로 진입 주문 상태를 바로 반영한다 (서버 조회 없이). 체결은 누적해 주문 수량을 채우면 활성화,
// 취소·거부는 폐기. 통보가 없는 브로커에서는 기존처럼 틱마다 resolveEntry 가 조회한다.
func (b *BracketMonitor) OnOrderEvent(event OrderEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, br := range b.brackets {
		if !event.OrderIDMatches(id) {
			continue
		}
		switch event.Type {
		case OrderFilled:
			if event.Quantity != nil {
				br.filledQuantity = br.filledQuantity.Add(*event.Quantity)
			}
			if !br.active && br.filledQuantity.GreaterThanOrEqual(br.quantity) {
				br.active = true
				log.Printf("INFO hermetix bracket activated / entry filled by event %s", id)
			}
		case OrderCanceled, OrderRejected:
			delete(b.brackets, id)
			log.Printf("INFO hermetix bracket dropped / entry %s by event %s", event.Type, id)
		}
		return
	}
}

func (b *BracketMonitor) ActiveCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.brackets)
}

// OrderExecutor - Signal -> 주문 실행. 매도 클램프(공매도 방지), 멱등키(지원 브로커만).
// RiskGuard - 주문 금액 상한. 실전투자에서 봇 폭주 손실 규모를 제한한다 (모의에서도 설정하면 적용).
// MaxOrderValue: 주문 1건 추정 금액(수량 × 지정가 또는 현재가) 상한. MaxDailyOrderValue: 하루(UTC) 누적 상한(매수·매도 합산).
// 추정 금액을 알 수 없으면 상한이 설정된 경우 거부한다. 누적치는 메모리에만 있다.
type RiskGuard struct {
	maxOrderValue      *decimal.Decimal
	maxDailyOrderValue *decimal.Decimal
	now                func() time.Time
	mu                 sync.Mutex
	day                string
	dailyTotal         decimal.Decimal
}

func NewRiskGuard(maxOrderValue, maxDailyOrderValue *decimal.Decimal) *RiskGuard {
	g := &RiskGuard{maxOrderValue: maxOrderValue, maxDailyOrderValue: maxDailyOrderValue, now: time.Now}
	g.day = g.today()
	return g
}

func (g *RiskGuard) IsActive() bool { return g.maxOrderValue != nil || g.maxDailyOrderValue != nil }

// TryReserve - 허용하면 일일 누적에 반영하고 "", 거부하면 사유. 누적은 제출 전에 잡는다 (보수적).
func (g *RiskGuard) TryReserve(symbol string, quantity decimal.Decimal, price *decimal.Decimal) string {
	if !g.IsActive() {
		return ""
	}
	if price == nil || !price.IsPositive() {
		return fmt.Sprintf("가격을 알 수 없어 주문 금액 상한을 검증할 수 없습니다 (symbol=%s)", symbol)
	}
	value := quantity.Mul(*price)
	if g.maxOrderValue != nil && value.GreaterThan(*g.maxOrderValue) {
		return fmt.Sprintf("주문 금액 %s 이(가) 1건 상한 %s 을(를) 초과합니다 (symbol=%s)", value, g.maxOrderValue, symbol)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.rollDay()
	if g.maxDailyOrderValue != nil && g.dailyTotal.Add(value).GreaterThan(*g.maxDailyOrderValue) {
		return fmt.Sprintf("일일 누적 주문 금액 %s 이(가) 상한 %s 을(를) 초과합니다 (오늘 누적=%s)", g.dailyTotal.Add(value), g.maxDailyOrderValue, g.dailyTotal)
	}
	g.dailyTotal = g.dailyTotal.Add(value)
	return ""
}

func (g *RiskGuard) DailyTotal() decimal.Decimal {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.rollDay()
	return g.dailyTotal
}

func (g *RiskGuard) today() string { return g.now().UTC().Format("2006-01-02") }

func (g *RiskGuard) rollDay() {
	if today := g.today(); today != g.day {
		g.day = today
		g.dailyTotal = decimal.Zero
	}
}

type OrderExecutor struct {
	broker   BrokerClient
	brackets *BracketMonitor
	guard    *TradingGuard
	risk     *RiskGuard
}

func NewOrderExecutor(broker BrokerClient, brackets *BracketMonitor, guard *TradingGuard) *OrderExecutor {
	return &OrderExecutor{broker, brackets, guard, NewRiskGuard(nil, nil)}
}

// WithRisk - 주문 금액 상한 적용.
func (e *OrderExecutor) WithRisk(risk *RiskGuard) *OrderExecutor {
	e.risk = risk
	return e
}

// withinRisk - 추정 금액 = 수량 × (지정가 or 현재가). 상한을 넘으면 경고 후 false.
func (e *OrderExecutor) withinRisk(strategyName, symbol string, quantity decimal.Decimal, limitPrice *decimal.Decimal, ctx *StrategyContext) bool {
	price := limitPrice
	if price == nil {
		if q, ok := ctx.Quote(symbol); ok {
			p := q.Price
			price = &p
		}
	}
	if rejection := e.risk.TryReserve(symbol, quantity, price); rejection != "" {
		log.Printf("WARN hermetix [%s] 주문 금액 상한으로 시그널 스킵: %s", strategyName, rejection)
		return false
	}
	return true
}

func (e *OrderExecutor) Execute(strategyName string, signals []Signal, ctx *StrategyContext) {
	for _, signal := range signals {
		if e.guard.IsHalted() {
			log.Printf("WARN hermetix [%s] halted - signal skipped", strategyName)
			continue
		}
		var err error
		switch s := signal.(type) {
		case BuySignal:
			err = e.buy(strategyName, s, ctx)
		case SellSignal:
			err = e.sell(strategyName, s, ctx)
		case CancelSignal:
			var order Order
			if order, err = e.broker.CancelOrder(s.OrderID); err == nil {
				log.Printf("INFO hermetix [%s] CANCEL / %s -> %s", strategyName, s.OrderID, order.Status)
			}
		}
		var insufficient *InsufficientFundsError
		if errors.As(err, &insufficient) {
			log.Printf("WARN hermetix [%s] 주문가능금액 부족으로 시그널 스킵", strategyName)
		} else if err != nil {
			log.Printf("ERROR hermetix [%s] signal 실행 실패: %v", strategyName, err)
		}
	}
}

func (e *OrderExecutor) buy(strategyName string, signal BuySignal, ctx *StrategyContext) error {
	if !e.withinRisk(strategyName, signal.Symbol, signal.Quantity, signal.LimitPrice, ctx) {
		return nil
	}
	orderType := signal.OrderType
	if orderType == "" {
		orderType = Market
	}
	tif := signal.TimeInForce
	if tif == "" {
		tif = Day
	}
	order, err := e.broker.CreateOrder(CreateOrderRequest{
		Symbol: signal.Symbol, Side: Buy, OrderType: orderType,
		Quantity: signal.Quantity, LimitPrice: signal.LimitPrice, TimeInForce: tif,
		ClientOrderID: e.clientOrderID(strategyName),
	})
	if err != nil {
		return err
	}
	log.Printf("INFO hermetix [%s] BUY 접수 / %s qty=%s orderId=%s",
		strategyName, signal.Symbol, signal.Quantity, order.OrderID)
	e.brackets.Register(order.OrderID, signal.Symbol, signal.Quantity, signal.TakeProfitPrice, signal.StopLossPrice)
	return nil
}

func (e *OrderExecutor) sell(strategyName string, signal SellSignal, ctx *StrategyContext) error {
	held := decimal.Zero
	if h, ok := ctx.Holding(signal.Symbol); ok {
		held = h.Quantity
	}
	qty := decimal.Min(signal.Quantity, held)
	if !qty.IsPositive() {
		log.Printf("WARN hermetix [%s] SELL 스킵 / %s 보유 수량 없음", strategyName, signal.Symbol)
		return nil
	}
	if !e.withinRisk(strategyName, signal.Symbol, qty, signal.LimitPrice, ctx) {
		return nil
	}
	orderType := signal.OrderType
	if orderType == "" {
		orderType = Market
	}
	tif := signal.TimeInForce
	if tif == "" {
		tif = Day
	}
	order, err := e.broker.CreateOrder(CreateOrderRequest{
		Symbol: signal.Symbol, Side: Sell, OrderType: orderType,
		Quantity: qty, LimitPrice: signal.LimitPrice, TimeInForce: tif,
		ClientOrderID: e.clientOrderID(strategyName),
	})
	if err != nil {
		return err
	}
	log.Printf("INFO hermetix [%s] SELL 접수 / %s qty=%s orderId=%s", strategyName, signal.Symbol, qty, order.OrderID)
	return nil
}

func (e *OrderExecutor) clientOrderID(strategyName string) string {
	if !e.broker.Capabilities().ClientOrderID {
		return ""
	}
	return fmt.Sprintf("%s-%08x", strategyName, rand.Uint32())
}

// StrategyEngine - 등록된 전략들을 각자의 PollInterval 로 순차 호출한다.
//
// 실시간 트리거(TriggerOnTrade): 브로커가 StreamingBrokerClient 이고 StreamTrades 를 선언하면 Run 시작 시 전략 심볼의
// 체결가 스트림을 구독하고, 틱마다 같은 단일 루프 고루틴에 tick 을 넣는다. 대기 중인 틱이 있으면 합치고(pending),
// 직전 틱 종료 후 MinTickInterval 이 지나야 다음을 돌린다 (실행 직전에 다시 확인). 폴링은 안전망으로 계속 돈다.
// 스트림 틱이 모든 심볼을 덮으면 quotes REST 호출 대신 마지막 틱을 현재가로 쓴다.
type StrategyEngine struct {
	Broker     BrokerClient
	Guard      *TradingGuard
	Brackets   *BracketMonitor
	Executor   *OrderExecutor
	Calendar   *MarketCalendar
	Strategies []Strategy
	stop       chan struct{}
	stopOnce   sync.Once

	// 스트림 상태 — 스트림 고루틴과 루프 고루틴이 공유한다
	streamMu      sync.Mutex
	stream        MarketStream
	latestTrades  map[string]TradeTick     // 심볼(요청 표기) → 마지막 체결 틱
	latestBooks   map[string]OrderBookTick // 심볼(요청 표기) → 마지막 호가창 (OrderBook 전략만)
	orderEventsOn bool                     // 주문 통보 구독 여부 — 한 번만
	pending       map[string]bool          // 전략 이름 → 스케줄 대기 중인 스트림 틱이 있는지 (합치기용)
	lastTickEnded map[string]time.Time     // 전략 이름 → 직전 tick 종료 시각
	wake          chan struct{}            // 스트림 틱 도착 신호 (버퍼 1 — 몰려도 한 번만 깨운다)
}

// EngineOptions - 실전 게이트와 주문 금액 상한.
type EngineOptions struct {
	// 브로커가 Live 환경이면 true 여야 스케줄한다 (실전 명시 동의)
	LiveTradingEnabled bool
	// 주문 1건 추정 금액 상한 (브로커 통화)
	MaxOrderValue *decimal.Decimal
	// 하루(UTC) 누적 주문 금액 상한
	MaxDailyOrderValue     *decimal.Decimal
	MaxConsecutiveFailures int
}

func NewStrategyEngine(broker BrokerClient, strategies []Strategy) *StrategyEngine {
	return NewStrategyEngineWithOptions(broker, strategies, EngineOptions{})
}

func NewStrategyEngineWithOptions(broker BrokerClient, strategies []Strategy, opts EngineOptions) *StrategyEngine {
	guard := NewTradingGuard(broker, opts.MaxConsecutiveFailures)
	brackets := NewBracketMonitor(broker)
	risk := NewRiskGuard(opts.MaxOrderValue, opts.MaxDailyOrderValue)
	caps := broker.Capabilities()
	environment := broker.Environment()
	if environment == "" {
		environment = Paper
	}

	accepted := make([]Strategy, 0, len(strategies))
	names := make([]string, 0, len(strategies))
	switch {
	case !caps.SupportsEnvironment(environment):
		log.Printf("ERROR hermetix 브로커 '%s' 는 %s 환경을 지원하지 않습니다 - 엔진을 시작하지 않습니다", caps.BrokerID, environment)
		strategies = nil
	case environment == Live && !opts.LiveTradingEnabled:
		log.Printf("ERROR hermetix 브로커 '%s' 가 실전투자(LIVE)로 설정돼 있지만 LiveTradingEnabled 가 없습니다 - 엔진을 시작하지 않습니다. 실제 돈으로 거래하려면 명시하세요.", caps.BrokerID)
		strategies = nil
	case environment == Live:
		log.Printf("WARN hermetix ***** 실전투자(LIVE) 모드 - 주문이 실제 계좌에서 체결됩니다. 주문 금액 상한(MaxOrderValue 등) 설정을 권장합니다 *****")
	}
	for _, strategy := range strategies {
		spec := strategy.Spec()
		// capability 검증 - 미지원 조합은 스케줄하지 않는다 (fail-fast)
		if !caps.CandleIntervals[spec.candleInterval()] {
			log.Printf("ERROR hermetix [%s] 스케줄 제외: 브로커 '%s' 는 %s 캔들을 지원하지 않습니다",
				spec.Name, caps.BrokerID, spec.candleInterval())
			continue
		}
		accepted = append(accepted, strategy)
		names = append(names, spec.Name)
	}
	log.Printf("INFO hermetix broker=%s environment=%s market=%s / strategies=%v", caps.BrokerID, environment, caps.Market, names)

	return &StrategyEngine{
		Broker: broker, Guard: guard, Brackets: brackets,
		Executor:      NewOrderExecutor(broker, brackets, guard).WithRisk(risk),
		Calendar:      NewMarketCalendar(broker),
		Strategies:    accepted,
		stop:          make(chan struct{}),
		latestTrades:  map[string]TradeTick{},
		latestBooks:   map[string]OrderBookTick{},
		pending:       map[string]bool{},
		lastTickEnded: map[string]time.Time{},
		wake:          make(chan struct{}, 1),
	}
}

// Run - 블로킹 실행 루프. Stop() 으로 종료. TriggerOnTrade 전략이 있으면 시작 시 체결가 스트림을 붙인다.
func (e *StrategyEngine) Run() {
	e.attachStreams()
	nextRun := map[string]time.Time{}
	for {
		select {
		case <-e.stop:
			return
		default:
		}
		sleep := time.Second
		for _, strategy := range e.Strategies {
			spec := strategy.Spec()
			if time.Now().After(nextRun[spec.Name]) {
				e.runTick(strategy)
				nextRun[spec.Name] = time.Now().Add(spec.pollInterval())
			}
			// 스트림이 요청한 tick — 직전 종료 후 MinTickInterval 이 지났을 때만 (실행 직전 재확인)
			if e.isPending(spec.Name) {
				if remaining := e.remainingInterval(spec); remaining > 0 {
					if remaining < sleep {
						sleep = remaining
					}
				} else {
					e.clearPending(spec.Name)
					e.runTick(strategy)
				}
			}
		}
		select {
		case <-e.stop:
			return
		case <-e.wake:
		case <-time.After(sleep):
		}
	}
}

// Stop - 루프를 멈추고 스트림을 닫는다.
func (e *StrategyEngine) Stop() {
	e.stopOnce.Do(func() {
		close(e.stop)
		e.streamMu.Lock()
		stream := e.stream
		e.streamMu.Unlock()
		if stream != nil {
			_ = stream.Close()
		}
		FlushTelemetry() // 종료 전에 남은 사용량 버킷을 보낸다 (docs/telemetry.md)
	})
}

// StreamConnected - 체결가 스트림이 열려 있고 로그인까지 끝났는지 (상태 확인용).
func (e *StrategyEngine) StreamConnected() bool {
	e.streamMu.Lock()
	stream := e.stream
	e.streamMu.Unlock()
	return stream != nil && stream.IsConnected()
}

// streamFor - 브로커가 채널을 제공하면 공유 스트림을 (필요 시 열어) 돌려주고, 아니면 nil.
func (e *StrategyEngine) streamFor(channel StreamChannel) MarketStream {
	streaming, ok := e.Broker.(StreamingBrokerClient)
	if !ok || !e.Broker.Capabilities().HasStream(channel) {
		return nil
	}
	e.streamMu.Lock()
	defer e.streamMu.Unlock()
	if e.stream == nil {
		e.stream = streaming.OpenStream()
		e.stream.Connect()
	}
	return e.stream
}

// attachStreams - TriggerOnTrade 전략은 체결가 스트림에, OrderBook 전략은 호가 스트림에 붙이고, 브로커가 주문 통보를 제공하면 구독한다.
// 브로커가 지원하지 않으면 경고만 남기고 폴링으로 둔다.
func (e *StrategyEngine) attachStreams() {
	brokerID := e.Broker.Capabilities().BrokerID
	for _, strategy := range e.Strategies {
		spec := strategy.Spec()
		if spec.Trigger == TriggerOnTrade {
			if stream := e.streamFor(StreamTrades); stream == nil {
				log.Printf("WARN hermetix [%s] Trigger=ON_TRADE 이지만 브로커 '%s' 는 체결가 스트림을 제공하지 않습니다 - PollInterval=%s 폴링으로 동작합니다",
					spec.Name, brokerID, spec.pollInterval())
			} else {
				name := spec.Name
				stream.SubscribeTrades(spec.Symbols, func(tick TradeTick) {
					e.streamMu.Lock()
					e.latestTrades[tick.Symbol] = tick
					e.streamMu.Unlock()
					e.requestTick(name)
				})
				log.Printf("INFO hermetix [%s] trade stream attached / symbols=%v minTickInterval=%s", spec.Name, spec.Symbols, spec.minTickInterval())
			}
		}
		if spec.OrderBook {
			if stream := e.streamFor(StreamOrderBook); stream == nil {
				log.Printf("WARN hermetix [%s] OrderBook=true 이지만 브로커 '%s' 는 호가 스트림을 제공하지 않습니다 - 컨텍스트의 OrderBook 은 비어 있습니다", spec.Name, brokerID)
			} else if err := stream.SubscribeOrderBook(spec.Symbols, func(tick OrderBookTick) {
				e.streamMu.Lock()
				e.latestBooks[tick.Symbol] = tick
				e.streamMu.Unlock()
			}); err != nil {
				log.Printf("WARN hermetix [%s] 호가 스트림을 구독하지 못했습니다: %v", spec.Name, err)
			} else {
				log.Printf("INFO hermetix [%s] order book stream attached / symbols=%v", spec.Name, spec.Symbols)
			}
		}
	}
	e.attachOrderEvents()
}

// attachOrderEvents - 주문 통보를 구독해 어댑터 추적(OrderEventApplier)과 브라켓(BracketMonitor.OnOrderEvent)에 반영한다.
// 브로커가 제공하면 항상 붙인다 — 구독 실패(KIS HTS ID 미설정 등)는 경고만 남기고 폴링 판정으로 둔다.
func (e *StrategyEngine) attachOrderEvents() {
	e.streamMu.Lock()
	already := e.orderEventsOn
	e.streamMu.Unlock()
	if already {
		return
	}
	stream := e.streamFor(StreamOrderEvents)
	if stream == nil {
		return
	}
	if err := stream.SubscribeOrderEvents(e.onOrderEvent); err != nil {
		log.Printf("WARN hermetix 주문 통보 스트림을 구독하지 못했습니다 - 체결 판정은 폴링으로 계속합니다: %v", err)
		return
	}
	e.streamMu.Lock()
	e.orderEventsOn = true
	e.streamMu.Unlock()
	log.Printf("INFO hermetix order event stream attached")
}

func (e *StrategyEngine) onOrderEvent(event OrderEvent) {
	side := ""
	if event.Side != nil {
		side = string(*event.Side)
	}
	log.Printf("INFO hermetix order event / %s order=%s %s %s qty=%v price=%v", event.Type, event.OrderID, event.Symbol, side, event.Quantity, event.Price)
	if applier, ok := e.Broker.(OrderEventApplier); ok {
		safeCall("engine", "applyOrderEvent "+event.OrderID, func() { applier.ApplyOrderEvent(event) })
	}
	safeCall("engine", "bracket onOrderEvent "+event.OrderID, func() { e.Brackets.OnOrderEvent(event) })
}

// requestTick - 스트림 틱으로 tick 을 요청한다. 이미 대기 중이면 합친다. 스트림 고루틴에서 호출되므로 신호만 보내고 돌아온다.
func (e *StrategyEngine) requestTick(strategyName string) {
	e.streamMu.Lock()
	already := e.pending[strategyName]
	e.pending[strategyName] = true
	e.streamMu.Unlock()
	if already {
		return
	}
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *StrategyEngine) isPending(strategyName string) bool {
	e.streamMu.Lock()
	defer e.streamMu.Unlock()
	return e.pending[strategyName]
}

func (e *StrategyEngine) clearPending(strategyName string) {
	e.streamMu.Lock()
	delete(e.pending, strategyName)
	e.streamMu.Unlock()
}

// remainingInterval - 직전 tick 종료 후 MinTickInterval 까지 남은 시간 (0 이면 바로 실행 가능).
func (e *StrategyEngine) remainingInterval(spec StrategySpec) time.Duration {
	e.streamMu.Lock()
	ended := e.lastTickEnded[spec.Name]
	e.streamMu.Unlock()
	if ended.IsZero() {
		return 0
	}
	if remaining := spec.minTickInterval() - time.Since(ended); remaining > 0 {
		return remaining
	}
	return 0
}

// runTick - Tick + 종료 시각 기록 (폴링·스트림 공통).
func (e *StrategyEngine) runTick(strategy Strategy) {
	name := strategy.Spec().Name
	defer func() {
		e.streamMu.Lock()
		e.lastTickEnded[name] = time.Now()
		e.streamMu.Unlock()
	}()
	e.Tick(strategy)
}

// streamQuotes - TriggerOnTrade 전략의 모든 심볼에 스트림 틱이 있으면 그것을 현재가로 (REST quotes 1회 절약). 하나라도 없으면 nil → REST.
func (e *StrategyEngine) streamQuotes(spec StrategySpec) map[string]Quote {
	if spec.Trigger != TriggerOnTrade {
		return nil
	}
	e.streamMu.Lock()
	defer e.streamMu.Unlock()
	if e.stream == nil {
		return nil
	}
	quotes := make(map[string]Quote, len(spec.Symbols))
	for _, symbol := range spec.Symbols {
		tick, ok := e.latestTrades[symbol]
		if !ok {
			return nil
		}
		quotes[symbol] = tick.ToQuote()
	}
	return quotes
}

func (e *StrategyEngine) Tick(strategy Strategy) {
	spec := strategy.Spec()
	err := e.tick(strategy)
	if err == nil {
		e.Guard.RecordSuccess()
		return
	}
	var marketClosed *MarketClosedError
	var rateLimited *RateLimitError
	switch {
	case errors.As(err, &marketClosed): // 휴장 - 실패 아님
	case errors.As(err, &rateLimited):
		log.Printf("WARN hermetix [%s] rate limited - %v", spec.Name, err)
	default:
		log.Printf("ERROR hermetix [%s] tick failed: %v", spec.Name, err)
		e.Guard.RecordFailure(err)
	}
}

func (e *StrategyEngine) tick(strategy Strategy) error {
	spec := strategy.Spec()
	if e.Guard.IsHalted() {
		return nil
	}
	if spec.regularHoursOnly() {
		open, err := e.Calendar.IsRegularOpen(time.Now())
		if err != nil {
			return err
		}
		if !open {
			return nil
		}
	}

	ctx, err := e.buildContext(strategy)
	if err != nil {
		return err
	}

	if bracketSignals := e.Brackets.Check(ctx); len(bracketSignals) > 0 { // 익절/손절 우선
		e.Executor.Execute(spec.Name, bracketSignals, ctx)
	}

	signals, err := strategy.Decide(ctx)
	if err != nil {
		return err
	}
	if len(signals) > 0 {
		e.Executor.Execute(spec.Name, signals, ctx)
	}
	return nil
}

func (e *StrategyEngine) buildContext(strategy Strategy) (*StrategyContext, error) {
	spec := strategy.Spec()
	quoteMap := e.streamQuotes(spec)
	if quoteMap == nil {
		quotes, err := e.Broker.GetQuotes(spec.Symbols)
		if err != nil {
			return nil, err
		}
		quoteMap = map[string]Quote{}
		for _, q := range quotes {
			quoteMap[q.Symbol] = q
		}
	}
	candles := map[string][]Candle{}
	for _, symbol := range spec.Symbols {
		list, err := e.Broker.GetCandles(symbol, spec.candleInterval(), spec.candleLimit())
		if err != nil {
			return nil, err
		}
		candles[symbol] = list
	}
	account, err := e.Broker.GetAccount()
	if err != nil {
		return nil, err
	}
	holdings, err := e.Broker.GetHoldings()
	if err != nil {
		return nil, err
	}
	orders, err := e.Broker.GetOrders()
	if err != nil {
		return nil, err
	}
	power, err := e.Broker.GetBuyingPower()
	if err != nil {
		return nil, err
	}

	holdingMap := map[string]Holding{}
	for _, h := range holdings {
		holdingMap[h.Symbol] = h
	}
	openOrders := make([]Order, 0)
	for _, o := range orders {
		if o.Status.IsOpen() {
			openOrders = append(openOrders, o)
		}
	}
	books := map[string]OrderBookTick{}
	if spec.OrderBook {
		e.streamMu.Lock()
		for _, symbol := range spec.Symbols {
			if book, ok := e.latestBooks[symbol]; ok {
				books[symbol] = book
			}
		}
		e.streamMu.Unlock()
	}
	return &StrategyContext{
		Now: time.Now(), Quotes: quoteMap, Candles: candles,
		Account: account, Holdings: holdingMap, OpenOrders: openOrders, BuyingPower: power,
		OrderBooks: books,
	}, nil
}

// PnlReport - 계좌 수익률 리포트.
type PnlReport struct {
	Timestamp          time.Time
	AccountID          string
	Currency           string
	Cash               decimal.Decimal
	PortfolioValue     decimal.Decimal
	TotalMarketValue   decimal.Decimal
	TotalUnrealizedPnl decimal.Decimal
	TotalReturnRate    *decimal.Decimal
	Holdings           []Holding
}

func Pnl(broker BrokerClient, initialCapital *decimal.Decimal) (PnlReport, error) {
	account, err := broker.GetAccount()
	if err != nil {
		return PnlReport{}, err
	}
	holdings, err := broker.GetHoldings()
	if err != nil {
		return PnlReport{}, err
	}
	totalMv, totalPnl := decimal.Zero, decimal.Zero
	for _, h := range holdings {
		if h.MarketValue != nil {
			totalMv = totalMv.Add(*h.MarketValue)
		}
		// 평가손익 합계는 계좌 통화 종목만 — 외화 종목 손익은 단위가 달라 각 Holding 에만 있다
		if h.UnrealizedPnl != nil && (h.Currency == "" || h.Currency == account.Currency) {
			totalPnl = totalPnl.Add(*h.UnrealizedPnl)
		}
	}
	report := PnlReport{
		Timestamp: time.Now(), AccountID: account.AccountID, Currency: account.Currency,
		Cash: account.Cash, PortfolioValue: account.PortfolioValue,
		TotalMarketValue: totalMv, TotalUnrealizedPnl: totalPnl, Holdings: holdings,
	}
	if initialCapital != nil && initialCapital.IsPositive() {
		rate := account.PortfolioValue.Sub(*initialCapital).Div(*initialCapital)
		report.TotalReturnRate = &rate
	}
	return report, nil
}
