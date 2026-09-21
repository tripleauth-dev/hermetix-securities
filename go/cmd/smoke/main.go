// 실서버 스모크 (수동 실행 전용):
//
//	go run ./cmd/smoke next|kis|kiwoom   (환경변수로 키 주입)
package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/shopspring/decimal"
	hermetix "github.com/tripleauth-dev/hermetix-securities/go"
)

func main() {
	name := "next"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	if err := run(name); err != nil {
		fmt.Println("SMOKE FAILED:", err)
		os.Exit(1)
	}
	fmt.Println("SMOKE OK")
}

func run(name string) error {
	var client hermetix.BrokerClient
	symbol := "005930"
	var farPrice *decimal.Decimal
	switch name {
	case "next":
		client = hermetix.NewNextClient(os.Getenv("NEXT_CLIENT_ID"), os.Getenv("NEXT_CLIENT_SECRET"))
		symbol = "AAPL"
		p := decimal.NewFromInt(150)
		farPrice = &p
	case "kis":
		client = hermetix.NewKisClient(os.Getenv("KIS_APPKEY"), os.Getenv("KIS_APPSECRET"), os.Getenv("KIS_CANO"))
	case "kiwoom":
		client = hermetix.NewKiwoomClient(os.Getenv("KIWOOM_APPKEY"), os.Getenv("KIWOOM_SECRETKEY"))
	default:
		return fmt.Errorf("unknown broker: %s", name)
	}
	fmt.Printf("== %s (%s) ==\n", name, client.Capabilities().Market)

	quotes, err := client.GetQuotes([]string{symbol})
	if err != nil {
		return err
	}
	if !quotes[0].Price.IsPositive() {
		return fmt.Errorf("price <= 0")
	}
	fmt.Printf("현재가 %s: %s / 등락률 %v\n", symbol, quotes[0].Price, quotes[0].ChangeRate)

	candles, err := client.GetCandles(symbol, hermetix.Day1, 30)
	if err != nil {
		return err
	}
	if len(candles) < 20 || !candles[0].Timestamp.Before(candles[len(candles)-1].Timestamp) {
		return fmt.Errorf("candles bad")
	}
	fmt.Printf("일봉 %d개 / 최신 종가 %s\n", len(candles), candles[len(candles)-1].Close)

	open, err := hermetix.NewMarketCalendar(client).IsRegularOpen(quotes[0].Timestamp)
	if err != nil {
		return err
	}
	fmt.Printf("정규장 open = %v\n", open)

	account, err := client.GetAccount()
	if err != nil {
		return err
	}
	if !account.Cash.IsPositive() {
		return fmt.Errorf("cash <= 0")
	}
	fmt.Printf("예수금 %s / 총평가 %s\n", account.Cash, account.PortfolioValue)

	holdings, err := client.GetHoldings()
	if err != nil {
		return err
	}
	fmt.Printf("보유 %d종목\n", len(holdings))

	power, err := client.GetBuyingPower()
	if err != nil {
		return err
	}
	if !power.IsPositive() {
		return fmt.Errorf("buying power <= 0")
	}
	fmt.Printf("주문가능 %s\n", power)

	if _, err = client.GetOrders(); err != nil {
		return err
	}
	if _, err = client.GetFills(); err != nil {
		return err
	}

	// 주문 사이클 - 시장가와 먼 지정가 매수 후 취소
	price := quotes[0].Price.Mul(decimal.NewFromFloat(0.8))
	if farPrice != nil {
		price = *farPrice
	}
	order, err := client.CreateOrder(hermetix.CreateOrderRequest{
		Symbol: symbol, Side: hermetix.Buy, OrderType: hermetix.Limit,
		Quantity: decimal.NewFromInt(1), LimitPrice: &price,
	})
	var brokerErr *hermetix.BrokerAPIError
	var marketClosed *hermetix.MarketClosedError
	if errors.As(err, &marketClosed) || errors.As(err, &brokerErr) {
		fmt.Printf("주문 스킵: %v\n", err)
	} else if err != nil {
		return err
	} else {
		fmt.Printf("주문 접수: %s\n", order.OrderID)
		detail, err := client.GetOrder(order.OrderID)
		if err != nil {
			return err
		}
		fmt.Printf("주문 조회: %s\n", detail.Status)
		canceled, err := client.CancelOrder(order.OrderID)
		if err != nil {
			return err
		}
		fmt.Printf("주문 취소: %s\n", canceled.Status)
	}

	report, err := hermetix.Pnl(client, nil)
	if err != nil {
		return err
	}
	fmt.Printf("PnL: portfolio=%s unrealized=%s\n", report.PortfolioValue, report.TotalUnrealizedPnl)
	return nil
}
