package examples

// WMA 추세선 돌파 전략 (롱 온리, 다중 종목).
// 예측고점+갭 돌파 & 상승 추세 -> 시장가 매수 (익절 +ProfitRate, 손절 지지선 - 브라켓 위임).

import (
	"time"

	"github.com/shopspring/decimal"
	hermetix "github.com/tripleauth-dev/hermetix-securities/go"
)

type TrendLine struct {
	HighPrice, LowPrice             decimal.Decimal
	HighInclination, LowInclination decimal.Decimal
	AvgVolume                       decimal.Decimal
}

func (t TrendLine) Gap() decimal.Decimal          { return t.HighPrice.Sub(t.LowPrice) }
func (t TrendLine) BreakoutLine() decimal.Decimal { return t.HighPrice.Add(t.Gap()) }
func (t TrendLine) SupportLine() decimal.Decimal  { return t.LowPrice.Sub(t.Gap()) }

func TrendLineOf(candles []hermetix.Candle) TrendLine {
	weight := decimal.Zero
	volumeWeight := decimal.NewFromInt(1)
	highInc, lowInc := decimal.Zero, decimal.Zero
	avgVolume := decimal.NewFromInt(candles[0].Volume)
	for i := 1; i < len(candles); i++ {
		w := decimal.NewFromInt(int64(i))
		weight = weight.Add(w)
		volumeWeight = volumeWeight.Add(decimal.NewFromInt(int64(i + 1)))
		highInc = highInc.Add(candles[i].High.Sub(candles[i-1].High).Mul(w))
		lowInc = lowInc.Add(candles[i].Low.Sub(candles[i-1].Low).Mul(w))
		avgVolume = avgVolume.Add(decimal.NewFromInt(candles[i].Volume).Mul(decimal.NewFromInt(int64(i + 1))))
	}
	highInc = highInc.Div(weight)
	lowInc = lowInc.Div(weight)
	return TrendLine{
		HighPrice:       candles[len(candles)-1].High.Add(highInc),
		LowPrice:        candles[len(candles)-1].Low.Add(lowInc),
		HighInclination: highInc, LowInclination: lowInc,
		AvgVolume: avgVolume.Div(volumeWeight),
	}
}

type TrendBreakoutStrategy struct {
	Symbols        []string
	Lookback       int
	ProfitRate     decimal.Decimal
	VolumeRatio    decimal.Decimal
	ExpireHours    int
	BudgetRatio    decimal.Decimal
	CandleInterval hermetix.CandleInterval

	lastEntryCandle map[string]time.Time
	entryAt         map[string]time.Time
}

func NewTrendBreakoutStrategy() *TrendBreakoutStrategy {
	return &TrendBreakoutStrategy{
		Symbols: []string{"AAPL"}, Lookback: 120,
		ProfitRate: decimal.NewFromFloat(0.04), VolumeRatio: decimal.Zero,
		ExpireHours: 12, BudgetRatio: decimal.NewFromFloat(0.5), CandleInterval: hermetix.Hour1,
		lastEntryCandle: map[string]time.Time{}, entryAt: map[string]time.Time{},
	}
}

func (s *TrendBreakoutStrategy) Spec() hermetix.StrategySpec {
	return hermetix.StrategySpec{
		Name: "trend-breakout", Symbols: s.Symbols,
		CandleInterval: s.CandleInterval, CandleLimit: s.Lookback + 1,
	}
}

func (s *TrendBreakoutStrategy) Decide(ctx *hermetix.StrategyContext) ([]hermetix.Signal, error) {
	signals := make([]hermetix.Signal, 0)
	for _, symbol := range s.Symbols {
		signals = append(signals, s.decideSymbol(symbol, ctx)...)
	}
	return signals, nil
}

func (s *TrendBreakoutStrategy) decideSymbol(symbol string, ctx *hermetix.StrategyContext) []hermetix.Signal {
	if ctx.HasPosition(symbol) {
		return s.decideExit(symbol, ctx)
	}
	delete(s.entryAt, symbol)
	if ctx.HasOpenOrder(symbol) {
		return nil
	}
	return s.decideEntry(symbol, ctx)
}

func (s *TrendBreakoutStrategy) decideEntry(symbol string, ctx *hermetix.StrategyContext) []hermetix.Signal {
	candles := ctx.CandlesOf(symbol)
	if len(candles) < s.Lookback+1 {
		return nil
	}
	completed := candles[:len(candles)-1]
	inProgress := candles[len(candles)-1]
	if s.lastEntryCandle[symbol].Equal(inProgress.Timestamp) {
		return nil // 같은 시간봉 구간 재진입 방지
	}

	trend := TrendLineOf(completed[len(completed)-s.Lookback:])
	if !trend.HighInclination.IsPositive() {
		return nil // 상승 추세만
	}
	quote, ok := ctx.Quote(symbol)
	if !ok || quote.Price.LessThan(trend.BreakoutLine()) {
		return nil
	}
	if s.VolumeRatio.IsPositive() &&
		decimal.NewFromInt(inProgress.Volume).LessThan(trend.AvgVolume.Mul(s.VolumeRatio)) {
		return nil
	}

	budget := ctx.BuyingPower.Mul(s.BudgetRatio).Div(decimal.NewFromInt(int64(len(s.Symbols))))
	quantity := budget.Div(quote.Price).Floor()
	if quantity.LessThan(decimal.NewFromInt(1)) {
		return nil
	}

	s.lastEntryCandle[symbol] = inProgress.Timestamp
	s.entryAt[symbol] = ctx.Now
	takeProfit := quote.Price.Mul(decimal.NewFromInt(1).Add(s.ProfitRate)).Round(2)
	stopLoss := trend.SupportLine().Round(2)
	return []hermetix.Signal{hermetix.BuySignal{
		Symbol: symbol, Quantity: quantity,
		TakeProfitPrice: &takeProfit, StopLossPrice: &stopLoss,
	}}
}

func (s *TrendBreakoutStrategy) decideExit(symbol string, ctx *hermetix.StrategyContext) []hermetix.Signal {
	openedAt, ok := s.entryAt[symbol]
	if !ok {
		openedAt = ctx.Now
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
