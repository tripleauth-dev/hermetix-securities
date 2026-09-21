package examples

// 예제 전략 3종 핵심 시나리오 - 다른 언어 구현과 동일 정답지.

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
	hermetix "github.com/tripleauth-dev/hermetix-securities/go"
)

func candle(hourOffset int, open, close string, highLow ...string) hermetix.Candle {
	o, _ := decimal.NewFromString(open)
	c, _ := decimal.NewFromString(close)
	high, low := decimal.Max(o, c), decimal.Min(o, c)
	if len(highLow) == 2 {
		high, _ = decimal.NewFromString(highLow[0])
		low, _ = decimal.NewFromString(highLow[1])
	}
	return hermetix.Candle{
		Timestamp: time.Date(2026, 8, 6, hourOffset, 0, 0, 0, time.UTC),
		Open:      o, High: high, Low: low, Close: c, Volume: 1000,
	}
}

type ctxOpts struct {
	candles    []hermetix.Candle
	price      string
	heldQty    string
	avgEntry   string
	openOrders []hermetix.Order
	now        time.Time
}

func makeCtx(opts ctxOpts) *hermetix.StrategyContext {
	now := opts.now
	if now.IsZero() {
		now = time.Now()
	}
	ctx := &hermetix.StrategyContext{
		Now: now, Quotes: map[string]hermetix.Quote{}, Candles: map[string][]hermetix.Candle{},
		Holdings: map[string]hermetix.Holding{}, OpenOrders: opts.openOrders,
		BuyingPower: decimal.NewFromInt(10000),
	}
	if opts.price != "" {
		p, _ := decimal.NewFromString(opts.price)
		ctx.Quotes["AAPL"] = hermetix.Quote{Symbol: "AAPL", Price: p, Timestamp: now}
	}
	if opts.candles != nil {
		ctx.Candles["AAPL"] = opts.candles
	}
	if opts.heldQty != "" {
		q, _ := decimal.NewFromString(opts.heldQty)
		entry := decimal.NewFromInt(100)
		if opts.avgEntry != "" {
			entry, _ = decimal.NewFromString(opts.avgEntry)
		}
		ctx.Holdings["AAPL"] = hermetix.Holding{Symbol: "AAPL", Quantity: q, AvgEntryPrice: entry}
	}
	return ctx
}

func larryCandles(targetOpen, targetClose string) []hermetix.Candle {
	candles := make([]hermetix.Candle, 0, 7)
	for i := 0; i < 5; i++ {
		candles = append(candles, candle(i, "100", "101"))
	}
	candles = append(candles, candle(5, targetOpen, targetClose))
	candles = append(candles, candle(6, targetClose, targetClose))
	return candles
}

func newTestLarry() *LarryStrategy {
	s := NewLarryStrategy()
	s.Symbols = []string{"AAPL"}
	s.Lookback = 5
	return s
}

func TestLarryEntryWithStopAtOpen(t *testing.T) {
	s := newTestLarry()
	signals, _ := s.Decide(makeCtx(ctxOpts{candles: larryCandles("100", "108"), price: "108"}))
	if len(signals) != 1 {
		t.Fatalf("signals = %d", len(signals))
	}
	buySignal := signals[0].(hermetix.BuySignal)
	if buySignal.StopLossPrice.String() != "100" {
		t.Fatalf("stopLoss = %s", buySignal.StopLossPrice)
	}
	if buySignal.Quantity.String() != "46" { // 10000*0.5/108
		t.Fatalf("quantity = %s", buySignal.Quantity)
	}
}

func TestLarrySkipsBearishAndDuplicate(t *testing.T) {
	s := newTestLarry()
	if signals, _ := s.Decide(makeCtx(ctxOpts{candles: larryCandles("108", "100"), price: "100"})); len(signals) != 0 {
		t.Fatal("bearish should be skipped")
	}
	s2 := newTestLarry()
	data := larryCandles("100", "108")
	if signals, _ := s2.Decide(makeCtx(ctxOpts{candles: data, price: "108"})); len(signals) != 1 {
		t.Fatal("first entry expected")
	}
	if signals, _ := s2.Decide(makeCtx(ctxOpts{candles: data, price: "108"})); len(signals) != 0 {
		t.Fatal("duplicate entry should be skipped")
	}
}

func TestLarryExpireExit(t *testing.T) {
	s := newTestLarry()
	now := time.Now()
	s.entryAt["AAPL"] = now.Add(-49 * time.Hour)
	signals, _ := s.Decide(makeCtx(ctxOpts{candles: larryCandles("100", "101"), heldQty: "46", now: now}))
	if len(signals) != 1 {
		t.Fatalf("signals = %d", len(signals))
	}
	if _, ok := signals[0].(hermetix.SellSignal); !ok {
		t.Fatal("expected sell")
	}
}

func trendCandles(count int, startHigh, step float64) []hermetix.Candle {
	candles := make([]hermetix.Candle, 0, count+1)
	for i := 0; i <= count; i++ {
		high := startHigh + step*float64(i)
		candles = append(candles, candle(i,
			decimal.NewFromFloat(high-2).String(), decimal.NewFromFloat(high-1).String(),
			decimal.NewFromFloat(high).String(), decimal.NewFromFloat(high-4).String()))
	}
	return candles
}

func newTestTrend() *TrendBreakoutStrategy {
	s := NewTrendBreakoutStrategy()
	s.Symbols = []string{"AAPL"}
	s.Lookback = 10
	return s
}

func TestTrendEntryOnBreakout(t *testing.T) {
	s := newTestTrend()
	data := trendCandles(10, 100, 1)
	trend := TrendLineOf(data[len(data)-1-10 : len(data)-1])
	price := trend.BreakoutLine().Add(decimal.NewFromInt(1))
	signals, _ := s.Decide(makeCtx(ctxOpts{candles: data, price: price.String()}))
	if len(signals) != 1 {
		t.Fatalf("signals = %d", len(signals))
	}
	buySignal := signals[0].(hermetix.BuySignal)
	if buySignal.TakeProfitPrice == nil || buySignal.StopLossPrice == nil {
		t.Fatal("bracket prices expected")
	}
}

func TestTrendSkipsDowntrend(t *testing.T) {
	s := newTestTrend()
	if signals, _ := s.Decide(makeCtx(ctxOpts{candles: trendCandles(10, 110, -1), price: "999"})); len(signals) != 0 {
		t.Fatal("downtrend should be skipped")
	}
}

func openOrder(side hermetix.OrderSide, limit string) hermetix.Order {
	p, _ := decimal.NewFromString(limit)
	return hermetix.Order{OrderID: "ord_1", Status: hermetix.Submitted, Symbol: "AAPL",
		Side: side, OrderType: hermetix.Limit, LimitPrice: &p}
}

func newTestGrid() *GridStrategy {
	s := NewGridStrategy()
	s.Symbols = []string{"AAPL"}
	return s
}

func TestGridDipBuy(t *testing.T) {
	s := newTestGrid()
	signals, _ := s.Decide(makeCtx(ctxOpts{price: "100"}))
	buySignal := signals[0].(hermetix.BuySignal)
	if buySignal.LimitPrice.String() != "99.7" {
		t.Fatalf("limit = %s", buySignal.LimitPrice)
	}
}

func TestGridChase(t *testing.T) {
	s := newTestGrid()
	signals, _ := s.Decide(makeCtx(ctxOpts{price: "102",
		openOrders: []hermetix.Order{openOrder(hermetix.Buy, "99.70")}}))
	if _, ok := signals[0].(hermetix.CancelSignal); !ok {
		t.Fatal("expected cancel (chase)")
	}
}

func TestGridSellDecayAndMarketExit(t *testing.T) {
	s := newTestGrid()
	signals, _ := s.Decide(makeCtx(ctxOpts{price: "100", heldQty: "50"}))
	sellSignal := signals[0].(hermetix.SellSignal)
	if sellSignal.LimitPrice.String() != "103" { // 1 + 0.01*3
		t.Fatalf("target = %s", sellSignal.LimitPrice)
	}

	now := time.Now()
	s.DecayCount["AAPL"] = 1
	s.SellPlacedAt["AAPL"] = now.Add(-31 * time.Minute)
	signals, _ = s.Decide(makeCtx(ctxOpts{price: "100", heldQty: "50",
		openOrders: []hermetix.Order{openOrder(hermetix.Sell, "101")}, now: now}))
	if _, ok := signals[0].(hermetix.CancelSignal); !ok {
		t.Fatal("expected decay cancel")
	}
	if s.DecayCount["AAPL"] != 0 {
		t.Fatalf("count = %d", s.DecayCount["AAPL"])
	}

	signals, _ = s.Decide(makeCtx(ctxOpts{price: "100.5", heldQty: "50", avgEntry: "100", now: now}))
	sellSignal = signals[0].(hermetix.SellSignal)
	if sellSignal.OrderType == hermetix.Limit {
		t.Fatal("expected market exit when profitable and count=0")
	}
}
