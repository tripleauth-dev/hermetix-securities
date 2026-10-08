// Package hermetix - 증권사 모의투자 통합 트레이딩 프레임워크 (Go).
//
// 금액/수량은 전부 decimal.Decimal - float 를 절대 섞지 말 것.
package hermetix

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

type CandleInterval string

const (
	Min1  CandleInterval = "1m"
	Min5  CandleInterval = "5m"
	Hour1 CandleInterval = "1h"
	Day1  CandleInterval = "1d"
)

// TradingEnvironment - 거래 환경. Paper=모의투자(기본), Live=실전 — 엔진은 LiveTradingEnabled 없이는 Live 를 기동하지 않는다.
// 키는 항상 사용자 기기에서만 쓰인다.
type TradingEnvironment string

const (
	Paper TradingEnvironment = "PAPER"
	Live  TradingEnvironment = "LIVE"
)

var marketPrefix = regexp.MustCompile(`^([A-Z]{2,6}):(.+)$`)

// ParseSymbol - `MARKET:CODE` 표기를 (market, code) 로 나눈다. 접두가 없으면 market="".
func ParseSymbol(symbol string) (market, code string) {
	if m := marketPrefix.FindStringSubmatch(symbol); m != nil {
		return m[1], m[2]
	}
	return "", symbol
}

// SymbolCode - 접두를 뗀 브로커 심볼 코드.
func SymbolCode(symbol string) string {
	_, code := ParseSymbol(symbol)
	return code
}

// SymbolsMatch - 코드가 같고, 둘 다 시장을 명시했다면 시장도 같아야 한다.
func SymbolsMatch(a, b string) bool {
	ma, ca := ParseSymbol(a)
	mb, cb := ParseSymbol(b)
	return ca == cb && (ma == "" || mb == "" || ma == mb)
}

type OrderSide string

const (
	Buy  OrderSide = "BUY"
	Sell OrderSide = "SELL"
)

type OrderType string

const (
	Market OrderType = "MARKET"
	Limit  OrderType = "LIMIT"
)

type TimeInForce string

const (
	Day TimeInForce = "DAY"
	GTC TimeInForce = "GTC"
)

// OrderStatus 는 주문 상태다 (넥스트증권 공개 스펙 v1.3 부록 D 7종 + Unknown 폴백).
// PendingCancel 은 취소 접수 후 미확정 — 원주문이 체결될 수 있으므로 OPEN 으로 분류한다.
type OrderStatus string

const (
	Submitted       OrderStatus = "SUBMITTED"
	PartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	PendingCancel   OrderStatus = "PENDING_CANCEL"
	Filled          OrderStatus = "FILLED"
	Canceled        OrderStatus = "CANCELED"
	Rejected        OrderStatus = "REJECTED"
	Expired         OrderStatus = "EXPIRED"
	Unknown         OrderStatus = "UNKNOWN"
)

func (s OrderStatus) IsOpen() bool {
	return s == Submitted || s == PartiallyFilled || s == PendingCancel
}

type Quote struct {
	Symbol     string
	Price      decimal.Decimal
	BidPrice   *decimal.Decimal
	AskPrice   *decimal.Decimal
	Volume     int64
	Change     *decimal.Decimal
	ChangeRate *decimal.Decimal
	Timestamp  time.Time
}

type Candle struct {
	Timestamp time.Time
	Open      decimal.Decimal
	High      decimal.Decimal
	Low       decimal.Decimal
	Close     decimal.Decimal
	Volume    int64
}

type SessionHours struct {
	Start string // "09:00"
	End   string // "15:30"
}

type MarketDay struct {
	Date     string // "2026-08-06"
	Open     bool
	Regular  *SessionHours
	Timezone string
	Holiday  string
}

type Account struct {
	AccountID      string
	Currency       string
	Cash           decimal.Decimal
	PortfolioValue decimal.Decimal
	Status         string
}

type Holding struct {
	Symbol            string
	Quantity          decimal.Decimal
	AvgEntryPrice     decimal.Decimal
	CurrentPrice      *decimal.Decimal
	MarketValue       *decimal.Decimal
	UnrealizedPnl     *decimal.Decimal
	UnrealizedPnlRate *decimal.Decimal
	// Currency 는 종목 거래 통화 — AvgEntryPrice·CurrentPrice·UnrealizedPnl 의 단위. 빈 문자열이면 계좌 통화.
	// MarketValue 는 항상 계좌 통화 (계좌 총평가에 더하는 값)
	Currency string
}

type CreateOrderRequest struct {
	Symbol        string
	Side          OrderSide
	OrderType     OrderType
	Quantity      decimal.Decimal
	LimitPrice    *decimal.Decimal
	TimeInForce   TimeInForce
	ClientOrderID string
}

type Order struct {
	OrderID        string
	Status         OrderStatus
	Symbol         string
	Side           OrderSide
	OrderType      OrderType
	Quantity       *decimal.Decimal
	LimitPrice     *decimal.Decimal
	FilledQuantity *decimal.Decimal
	AvgFillPrice   *decimal.Decimal
	ClientOrderID  string
	SubmittedAt    *time.Time
	CanceledAt     *time.Time
}

type Fill struct {
	FillID   string
	OrderID  string
	Symbol   string
	Side     OrderSide
	Quantity *decimal.Decimal
	Price    *decimal.Decimal
}

// StreamChannel - 브로커가 제공하는 실시간 스트림 채널. BrokerCapabilities.Streams 로 선언한다.
type StreamChannel string

const (
	// StreamTrades - 체결가 스트림. 체결이 일어날 때마다 TradeTick 을 밀어준다.
	StreamTrades StreamChannel = "TRADES"
	// StreamOrderBook - 호가 스트림. 호가창이 바뀔 때마다 OrderBookTick (10단계).
	StreamOrderBook StreamChannel = "ORDER_BOOK"
	// StreamOrderEvents - 내 주문의 접수·체결·취소·거부 통보 (OrderEvent).
	StreamOrderEvents StreamChannel = "ORDER_EVENTS"
)

// OrderBookLevel - 호가 한 단계.
type OrderBookLevel struct {
	Price    decimal.Decimal
	Quantity decimal.Decimal
}

// OrderBookTick - 호가창 스냅샷. Asks/Bids 는 최우선(1호가)부터 순서대로, 브로커가 주는 만큼(보통 10단계). 심볼은 구독 요청 표기 그대로.
type OrderBookTick struct {
	Symbol           string
	Timestamp        time.Time
	Asks             []OrderBookLevel
	Bids             []OrderBookLevel
	TotalAskQuantity *decimal.Decimal
	TotalBidQuantity *decimal.Decimal
}

// BestAsk - 최우선 매도호가 (없으면 false).
func (t OrderBookTick) BestAsk() (OrderBookLevel, bool) {
	if len(t.Asks) == 0 {
		return OrderBookLevel{}, false
	}
	return t.Asks[0], true
}

// BestBid - 최우선 매수호가 (없으면 false).
func (t OrderBookTick) BestBid() (OrderBookLevel, bool) {
	if len(t.Bids) == 0 {
		return OrderBookLevel{}, false
	}
	return t.Bids[0], true
}

// OrderEventType - 주문 통보 종류.
type OrderEventType string

const (
	OrderAccepted OrderEventType = "ACCEPTED" // 주문 접수
	OrderFilled   OrderEventType = "FILLED"   // 체결 (부분 체결 포함 — OrderEvent.Quantity 가 이번 체결량)
	OrderCanceled OrderEventType = "CANCELED" // 취소 확인
	OrderModified OrderEventType = "MODIFIED" // 정정 확인
	OrderRejected OrderEventType = "REJECTED" // 거부
)

// OrderEvent - 내 주문 통보 1건.
//   - OrderID 는 브로커 주문번호. 브로커에 따라 REST 응답과 자릿수(0 패딩)가 다를 수 있어 비교는 OrderIDMatches 로 한다
//   - Quantity/Price 는 이벤트 종류에 따라 체결량·체결가(FILLED) 또는 주문량·주문가(그 외)
//   - RemainingQuantity 는 브로커가 주는 경우만 (키움 902). KIS 통보에는 없다
type OrderEvent struct {
	OrderID           string
	Type              OrderEventType
	Timestamp         time.Time
	Symbol            string
	Side              *OrderSide
	Quantity          *decimal.Decimal
	Price             *decimal.Decimal
	RemainingQuantity *decimal.Decimal
	OriginalOrderID   string
	Reason            string
}

// NormalizeOrderID - 앞자리 0 패딩을 무시한 주문번호 (KIS 통보 10자리 vs REST ODNO 7자리 등).
func NormalizeOrderID(id string) string {
	normalized := strings.TrimLeft(strings.TrimSpace(id), "0")
	if normalized == "" {
		return "0"
	}
	return normalized
}

// OrderIDMatches - 0 패딩 차이를 무시한 주문번호 비교.
func (e OrderEvent) OrderIDMatches(other string) bool {
	return NormalizeOrderID(e.OrderID) == NormalizeOrderID(other)
}

// TradeTick - 체결 1건. 브로커 프레임을 공통 모델로 정규화한 것.
//   - Symbol 은 구독 요청 표기 그대로 돌려준다 (KRX:005930 으로 구독하면 KRX:005930)
//   - Quantity 는 이 체결의 수량, CumulativeVolume 은 당일 누적 거래량
//   - 호가·등락은 프레임에 있으면 채우고 없으면 nil
type TradeTick struct {
	Symbol           string
	Price            decimal.Decimal
	Quantity         decimal.Decimal
	Timestamp        time.Time
	BidPrice         *decimal.Decimal
	AskPrice         *decimal.Decimal
	CumulativeVolume *int64
	Change           *decimal.Decimal
	ChangeRate       *decimal.Decimal
}

// ToQuote - 스트림 틱을 REST 현재가와 같은 모양으로 (엔진이 quotes 호출을 아낄 때).
func (t TradeTick) ToQuote() Quote {
	var volume int64
	if t.CumulativeVolume != nil {
		volume = *t.CumulativeVolume
	}
	return Quote{
		Symbol: t.Symbol, Price: t.Price, BidPrice: t.BidPrice, AskPrice: t.AskPrice,
		Volume: volume, Change: t.Change, ChangeRate: t.ChangeRate, Timestamp: t.Timestamp,
	}
}

// BrokerCapabilities - 브로커가 지원하는 기능의 코드 선언. 실측으로 확인한 것만 true.
type BrokerCapabilities struct {
	BrokerID         string
	Market           string // "US" | "KRX"
	Currency         string
	CandleIntervals  map[CandleInterval]bool
	ClientOrderID    bool
	NativeBracket    bool
	FractionalShares bool
	// false 면 어댑터가 메모리 추적 (재시작 시 추적 소실)
	ServerOpenOrders bool
	// 지원 거래 환경. nil 이면 Paper 만. 실전(Live)은 실측으로 확인한 어댑터만 선언
	Environments map[TradingEnvironment]bool
	// 한 계좌로 다룰 수 있는 시장 목록 (MARKET:CODE 접두 허용 값). nil 이면 {Market}
	Markets map[string]bool
	// 실시간 스트림 채널. nil 이면 폴링만 가능. 선언한 어댑터는 StreamingBrokerClient 를 구현해야 한다 — 엔진은 둘 다 확인한다
	Streams []StreamChannel
}

// HasStream - 선언된 실시간 채널인지.
func (c BrokerCapabilities) HasStream(channel StreamChannel) bool {
	for _, ch := range c.Streams {
		if ch == channel {
			return true
		}
	}
	return false
}

// SupportsEnvironment - 선언된 환경인지 (nil 이면 Paper 만).
func (c BrokerCapabilities) SupportsEnvironment(env TradingEnvironment) bool {
	if c.Environments == nil {
		return env == Paper
	}
	return c.Environments[env]
}

// SymbolCode - 심볼의 시장 접두가 지원 시장인지 확인하고 브로커 코드를 돌려준다.
func (c BrokerCapabilities) SymbolCode(symbol string) (string, error) {
	market, code := ParseSymbol(symbol)
	if market != "" && market != c.Market && !c.Markets[market] {
		return "", fmt.Errorf("브로커 '%s' 는 시장 '%s' 을 지원하지 않습니다: %s", c.BrokerID, market, symbol)
	}
	return code, nil
}
