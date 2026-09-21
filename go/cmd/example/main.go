// 예제 전략 실행기:
//
//	HERMETIX_BROKER=next NEXT_CLIENT_ID=... go run ./cmd/example larry|trend|grid
package main

import (
	"fmt"
	"os"

	hermetix "github.com/tripleauth-dev/hermetix-securities/go"
	"github.com/tripleauth-dev/hermetix-securities/go/examples"
)

func main() {
	name := "larry"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	var strategy hermetix.Strategy
	switch name {
	case "larry":
		strategy = examples.NewLarryStrategy()
	case "trend":
		strategy = examples.NewTrendBreakoutStrategy()
	case "grid":
		strategy = examples.NewGridStrategy()
	default:
		fmt.Println("usage: example larry|trend|grid")
		os.Exit(1)
	}

	var broker hermetix.BrokerClient
	switch os.Getenv("HERMETIX_BROKER") {
	case "", "next":
		broker = hermetix.NewNextClient(os.Getenv("NEXT_CLIENT_ID"), os.Getenv("NEXT_CLIENT_SECRET"))
	case "kis":
		broker = hermetix.NewKisClient(os.Getenv("KIS_APPKEY"), os.Getenv("KIS_APPSECRET"), os.Getenv("KIS_CANO"))
	case "kiwoom":
		broker = hermetix.NewKiwoomClient(os.Getenv("KIWOOM_APPKEY"), os.Getenv("KIWOOM_SECRETKEY"))
	default:
		fmt.Println("unknown HERMETIX_BROKER")
		os.Exit(1)
	}

	hermetix.NewStrategyEngine(broker, []hermetix.Strategy{strategy}).Run()
}
