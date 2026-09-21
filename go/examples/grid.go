package examples

// 목표가 스캘핑 전략 (다중 종목, 캔들 미사용 - KRX 브로커 호환).
// 딥 지정가 매수(추격) -> 목표가 매도(GTC) -> 감쇠(count--) -> 수익권 시장가 청산.

import (
	"time"

	"github.com/shopspring/decimal"
	hermetix "github.com/tripleauth-dev/hermetix-securities/go"
)

type GridStrategy struct {
	Symbols       []string
	BuyDipRate    decimal.Decimal
	TargetRate    decimal.Decimal
	MaxDecayCount int
	DecayMinutes  int
	ChaseRate     decimal.Decimal
	BudgetRatio   decimal.Decimal
	PollInterval  time.Duration

	DecayCount   map[string]int
	SellPlacedAt map[string]time.Time
}

func NewGridStrategy() *GridStrategy {
	return &GridStrategy{
		Symbols:    []string{"AAPL"},
		BuyDipRate: decimal.NewFromFloat(0.003), TargetRate: decimal.NewFromFloat(0.01),
		MaxDecayCount: 3, DecayMinutes: 30,
		ChaseRate: decimal.NewFromFloat(0.005), BudgetRatio: decimal.NewFromFloat(0.5),
		PollInterval: 30 * time.Second,
		DecayCount:   map[string]int{}, SellPlacedAt: map[string]time.Time{},
	}
}

func (s *GridStrategy) Spec() hermetix.StrategySpec {
	return hermetix.StrategySpec{
		Name: "grid", Symbols: s.Symbols,
		CandleInterval: hermetix.Day1, CandleLimit: 2, // 캔들 미사용
		PollInterval: s.PollInterval,
	}
}

func (s *GridStrategy) Decide(ctx *hermetix.StrategyContext) ([]hermetix.Signal, error) {
	signals := make([]hermetix.Signal, 0)
	for _, symbol := range s.Symbols {
		signals = append(signals, s.decideSymbol(symbol, ctx)...)
	}
	return signals, nil
}

func (s *GridStrategy) decideSymbol(symbol string, ctx *hermetix.StrategyContext) []hermetix.Signal {
	quote, ok := ctx.Quote(symbol)
	if !ok {
		return nil
	}
	if ctx.HasPosition(symbol) {
		return s.decideSell(symbol, quote.Price, ctx)
	}
	s.DecayCount[symbol] = s.MaxDecayCount
	delete(s.SellPlacedAt, symbol)
	return s.decideBuy(symbol, quote.Price, ctx)
}

func (s *GridStrategy) decideBuy(symbol string, price decimal.Decimal, ctx *hermetix.StrategyContext) []hermetix.Signal {
	desired := price.Mul(decimal.NewFromInt(1).Sub(s.BuyDipRate)).Round(2)
	for _, order := range ctx.OpenOrdersOf(symbol) {
		if order.Side != hermetix.Buy {
			continue
		}
		if order.LimitPrice != nil &&
			desired.GreaterThan(order.LimitPrice.Mul(decimal.NewFromInt(1).Add(s.ChaseRate))) {
			return []hermetix.Signal{hermetix.CancelSignal{OrderID: order.OrderID}} // 가격 이탈 - 추격
		}
		return nil
	}
	budget := ctx.BuyingPower.Mul(s.BudgetRatio).Div(decimal.NewFromInt(int64(len(s.Symbols))))
	quantity := budget.Div(desired).Floor()
	if quantity.LessThan(decimal.NewFromInt(1)) {
		return nil
	}
	return []hermetix.Signal{hermetix.BuySignal{
		Symbol: symbol, Quantity: quantity,
		OrderType: hermetix.Limit, LimitPrice: &desired, TimeInForce: hermetix.Day,
	}}
}

func (s *GridStrategy) decideSell(symbol string, price decimal.Decimal, ctx *hermetix.StrategyContext) []hermetix.Signal {
	holding, ok := ctx.Holding(symbol)
	if !ok {
		return nil
	}
	entry := holding.AvgEntryPrice
	count, exists := s.DecayCount[symbol]
	if !exists {
		count = s.MaxDecayCount
		s.DecayCount[symbol] = count
	}

	for _, order := range ctx.OpenOrdersOf(symbol) {
		if order.Side != hermetix.Sell {
			continue
		}
		placedAt, ok := s.SellPlacedAt[symbol]
		if !ok {
			placedAt = ctx.Now
			s.SellPlacedAt[symbol] = placedAt
		}
		if count > 0 && ctx.Now.Sub(placedAt) >= time.Duration(s.DecayMinutes)*time.Minute {
			s.DecayCount[symbol] = count - 1 // 목표 감쇠
			delete(s.SellPlacedAt, symbol)
			return []hermetix.Signal{hermetix.CancelSignal{OrderID: order.OrderID}}
		}
		return nil
	}

	if count <= 0 && price.GreaterThanOrEqual(entry) { // 수익권 - 즉시 시장가 청산
		return []hermetix.Signal{hermetix.SellSignal{Symbol: symbol, Quantity: holding.Quantity}}
	}

	if count < 0 {
		count = 0
	}
	sellPrice := entry.Mul(decimal.NewFromInt(1).Add(s.TargetRate.Mul(decimal.NewFromInt(int64(count))))).Round(2)
	s.SellPlacedAt[symbol] = ctx.Now
	return []hermetix.Signal{hermetix.SellSignal{
		Symbol: symbol, Quantity: holding.Quantity,
		OrderType: hermetix.Limit, LimitPrice: &sellPrice, TimeInForce: hermetix.GTC,
	}}
}
