// Package examples - 공식 전략 3종의 Go 구현 (다른 언어 구현과 동일 로직).
package examples

// Larry Williams 식 변동성 돌파 전략 (롱 온리, 다중 종목).
// 직전 완성 캔들 몸통 >= 평균 몸통 x multiplier 인 양봉 -> 시장가 매수 (손절=시가, 브라켓 위임).

import (
	"time"

	"github.com/shopspring/decimal"
	hermetix "github.com/tripleauth-dev/hermetix-securities/go"
)

type LarryStrategy struct {
	Symbols        []string
	Lookback       int
	Multiplier     decimal.Decimal
	ExpireHours    int
	BudgetRatio    decimal.Decimal
	CandleInterval hermetix.CandleInterval

	lastEvaluated map[string]time.Time
	entryAt       map[string]time.Time
}

func NewLarryStrategy() *LarryStrategy {
	return &LarryStrategy{
		Symbols: []string{"AAPL"}, Lookback: 24,
		Multiplier: decimal.NewFromFloat(1.2), ExpireHours: 48,
		BudgetRatio: decimal.NewFromFloat(0.5), CandleInterval: hermetix.Hour1,
		lastEvaluated: map[string]time.Time{}, entryAt: map[string]time.Time{},
	}
}

func (s *LarryStrategy) Spec() hermetix.StrategySpec {
	return hermetix.StrategySpec{
		Name: "larry", Symbols: s.Symbols,
		CandleInterval: s.CandleInterval, CandleLimit: s.Lookback + 2,
	}
}

func (s *LarryStrategy) Decide(ctx *hermetix.StrategyContext) ([]hermetix.Signal, error) {
	signals := make([]hermetix.Signal, 0)
	for _, symbol := range s.Symbols {
		signals = append(signals, s.decideSymbol(symbol, ctx)...)
	}
	return signals, nil
}

func (s *LarryStrategy) decideSymbol(symbol string, ctx *hermetix.StrategyContext) []hermetix.Signal {
	if ctx.HasPosition(symbol) {
		return s.decideExit(symbol, ctx)
	}
	delete(s.entryAt, symbol)
	if ctx.HasOpenOrder(symbol) {
		return nil
	}
	return s.decideEntry(symbol, ctx)
}

func (s *LarryStrategy) decideEntry(symbol string, ctx *hermetix.StrategyContext) []hermetix.Signal {
	candles := ctx.CandlesOf(symbol)
	if len(candles) < s.Lookback+2 {
		return nil
	}
	completed := candles[:len(candles)-1] // 마지막 캔들은 진행 중
	target := completed[len(completed)-1]
	if s.lastEvaluated[symbol].Equal(target.Timestamp) {
		return nil // 같은 캔들로 중복 진입 방지
	}
	s.lastEvaluated[symbol] = target.Timestamp

	history := completed[len(completed)-1-s.Lookback : len(completed)-1]
	sum := decimal.Zero
	for _, c := range history {
		sum = sum.Add(c.Open.Sub(c.Close).Abs())
	}
	avgBody := sum.Div(decimal.NewFromInt(int64(len(history))))
	if target.Open.Sub(target.Close).Abs().LessThan(avgBody.Mul(s.Multiplier)) {
		return nil
	}
	if target.Open.GreaterThanOrEqual(target.Close) {
		return nil // 롱 온리 - 음봉 스킵
	}

	quote, ok := ctx.Quote(symbol)
	if !ok {
		return nil
	}
	budget := ctx.BuyingPower.Mul(s.BudgetRatio).Div(decimal.NewFromInt(int64(len(s.Symbols))))
	quantity := budget.Div(quote.Price).Floor()
	if quantity.LessThan(decimal.NewFromInt(1)) {
		return nil
	}

	s.entryAt[symbol] = ctx.Now
	stopLoss := target.Open
	return []hermetix.Signal{hermetix.BuySignal{
		Symbol: symbol, Quantity: quantity, StopLossPrice: &stopLoss,
	}}
}

func (s *LarryStrategy) decideExit(symbol string, ctx *hermetix.StrategyContext) []hermetix.Signal {
	openedAt, ok := s.entryAt[symbol]
	if !ok {
		openedAt = ctx.Now // 재시작 시 만료 클록 재시작
		s.entryAt[symbol] = openedAt
	}
	if ctx.Now.Sub(openedAt) < time.Duration(s.ExpireHours)*time.Hour {
		return nil
	}
	holding, ok := ctx.Holding(symbol)
	if !ok {
		return nil
	}
	delete(s.entryAt, symbol)
	return []hermetix.Signal{hermetix.SellSignal{Symbol: symbol, Quantity: holding.Quantity}}
}
