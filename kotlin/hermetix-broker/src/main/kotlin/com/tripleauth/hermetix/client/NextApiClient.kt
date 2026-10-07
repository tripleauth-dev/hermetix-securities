package com.tripleauth.hermetix.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.broker.BrokerApiException
import com.tripleauth.hermetix.broker.BrokerCapabilities
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.InsufficientFundsError
import com.tripleauth.hermetix.broker.InvalidOrderError
import com.tripleauth.hermetix.broker.MarketClosedError
import com.tripleauth.hermetix.broker.OrderNotFoundError
import com.tripleauth.hermetix.broker.RateLimitError
import com.tripleauth.hermetix.broker.RateLimiter
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.broker.UsageTelemetry
import com.tripleauth.hermetix.broker.symbolCode
import com.tripleauth.hermetix.client.dto.AccountResponse
import com.tripleauth.hermetix.client.dto.ApiError
import com.tripleauth.hermetix.client.dto.ApiErrorEnvelope
import com.tripleauth.hermetix.client.dto.BuyingPowerResponse
import com.tripleauth.hermetix.client.dto.CalendarResponse
import com.tripleauth.hermetix.client.dto.Candle
import com.tripleauth.hermetix.client.dto.CandleInterval
import com.tripleauth.hermetix.client.dto.CandlesResponse
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.ExchangeRateResponse
import com.tripleauth.hermetix.client.dto.Fill
import com.tripleauth.hermetix.client.dto.FillsResponse
import com.tripleauth.hermetix.client.dto.Holding
import com.tripleauth.hermetix.client.dto.HoldingsResponse
import com.tripleauth.hermetix.client.dto.HoldingsSummary
import com.tripleauth.hermetix.client.dto.Instrument
import com.tripleauth.hermetix.client.dto.InstrumentDetailResponse
import com.tripleauth.hermetix.client.dto.InstrumentsResponse
import com.tripleauth.hermetix.client.dto.MarketDay
import com.tripleauth.hermetix.client.dto.MarketSessions
import com.tripleauth.hermetix.client.dto.NextAccountResponse
import com.tripleauth.hermetix.client.dto.NextBuyingPowerResponse
import com.tripleauth.hermetix.client.dto.NextCalendarDay
import com.tripleauth.hermetix.client.dto.NextCalendarResponse
import com.tripleauth.hermetix.client.dto.NextCandlesResponse
import com.tripleauth.hermetix.client.dto.NextFillsResponse
import com.tripleauth.hermetix.client.dto.NextHoldingsResponse
import com.tripleauth.hermetix.client.dto.NextInstrument
import com.tripleauth.hermetix.client.dto.NextInstrumentsResponse
import com.tripleauth.hermetix.client.dto.NextOrderRequest
import com.tripleauth.hermetix.client.dto.NextOrderResponse
import com.tripleauth.hermetix.client.dto.NextOrdersResponse
import com.tripleauth.hermetix.client.dto.NextQuotesResponse
import com.tripleauth.hermetix.client.dto.NextTime
import com.tripleauth.hermetix.client.dto.OrderResponse
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderStatus
import com.tripleauth.hermetix.client.dto.OrderType
import com.tripleauth.hermetix.client.dto.OrdersResponse
import com.tripleauth.hermetix.client.dto.PreviewOrderRequest
import com.tripleauth.hermetix.client.dto.PreviewOrderResponse
import com.tripleauth.hermetix.client.dto.Quote
import com.tripleauth.hermetix.client.dto.QuotesResponse
import com.tripleauth.hermetix.client.dto.SessionHours
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.web.client.RestClient
import org.springframework.web.util.UriBuilder
import java.math.BigDecimal
import java.math.RoundingMode
import java.net.URI
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * 넥스트증권 OpenAPI 클라이언트 — 공개 스펙 **v1.3** 기준.
 *
 * v1.3 응답(quotes `outcome`, 캔들 `time`, 계좌 `cashAmount`, 보유 `averageBuyPrice`, 캘린더 `status`+`sessions[]` …)을
 * 브로커 중립 공통 모델(Quote/Candle/Holding/OrderResponse …)로 정규화한다. 공통 모델은 전략 SPI 가 보는 타입이므로
 * 서버 스펙이 바뀌어도 이 어댑터 안에서만 흡수한다 (KIS/키움 어댑터와 같은 방식).
 *
 * 규약:
 * - 모든 호출은 Bearer 토큰을 자동 첨부하며, 401 발생 시 거부된 토큰을 버리고 1회 재발급 후 재시도한다 ([BrokerTokenManager])
 * - 429 는 `Retry-After` 만큼 기다렸다가 최대 2회 재시도한다 ([RateLimiter]) — 쓰로틀은 없다 (초당 한도가 넉넉함)
 * - 모든 고객 API 에 `X-Request-Id` 를 붙인다 (토큰 발급 외 전 API 필수 — 누락 시 400 `request-id-required`)
 * - 계좌·자산·주문 API 는 `X-Next-Account-Id` 헤더를 자동으로 붙인다 (구 `X-Nextsecurities-Account` 에서 개명.
 *   토큰의 계좌와 불일치 시 403 `account-mismatch`)
 * - 시각은 ISO 8601 · KST (오프셋 생략 시 KST) → 공통 모델의 `Instant` 로 변환
 * - 등락률·손익률은 서버가 % 단위로 주므로 공통 모델 규약(비율)에 맞춰 100 으로 나눈다
 */
class NextApiClient internal constructor(
    private val properties: NextApiProperties,
    private val tokenManager: TokenManager,
    private val objectMapper: ObjectMapper,
    sleeper: (Long) -> Unit,
) : BrokerClient {

    constructor(properties: NextApiProperties, tokenManager: TokenManager, objectMapper: ObjectMapper) :
        this(properties, tokenManager, objectMapper, Thread::sleep)

    private val logger = KotlinLogging.logger { }

    override val capabilities = BrokerCapabilities(
        brokerId = "next",
        market = "US",
        currency = "USD",
        // v1.3 캔들 주기는 1m · 1d 두 가지
        candleIntervals = setOf(CandleInterval.MIN_1, CandleInterval.DAY_1),
        clientOrderId = true,
        nativeBracket = false, // 서버 /v2/orders/advanced(BRACKET) 연동 전까지 소프트웨어 브라켓 사용
        fractionalShares = false, // v1.3 주문 수량은 정수만 허용
        environments = setOf(TradingEnvironment.PAPER, TradingEnvironment.LIVE), // 키 프리픽스로 결정 (pk_test_ / pk_live_)
    )

    override val environment: TradingEnvironment = properties.environment


    private val usage = UsageTelemetry.forBroker(capabilities.brokerId, environment)

    init {
        tokenManager.usage = usage
    }

    init {
        // 환경은 키 프리픽스가 결정한다 — 설정과 어긋나면 기동 실패 (실전 키를 모의로 착각하는 사고 방지)
        val expectedPrefix = if (properties.environment == TradingEnvironment.LIVE) "pk_live_" else "pk_test_"
        check(!properties.clientId.startsWith("pk_") || properties.clientId.startsWith(expectedPrefix)) {
            "hermetix.next.environment=${properties.environment} 인데 client-id 가 '$expectedPrefix' 로 시작하지 않습니다 " +
                "(모의=pk_test_, 실전=pk_live_). 키와 환경 설정을 맞추세요."
        }
    }

    private val restClient = RestClient.builder()
        .baseUrl(properties.baseUrl)
        .build()

    private val limiter = RateLimiter(minIntervalMillis = 0, maxRetries = 2, backoffMillis = { attempt -> 1000L * attempt }, sleeper = sleeper)

    // ------------------------------------------------------------------ market

    override fun getQuotes(symbols: List<String>): QuotesResponse = usage.measure("quotes") {
        val codes = symbols.map { capabilities.symbolCode(it) }
        val raw: NextQuotesResponse = get { it.path("/v1/market/quotes").queryParam("symbols", codes.joinToString(",")).build() }
        val requestedByCode = symbols.associateBy { capabilities.symbolCode(it) }
        val quotes = raw.quotes.mapNotNull { q ->
            // NOT_FOUND / NO_DATA 는 개별 종목의 정상 결과 — 가격이 없으므로 공통 모델에서는 제외한다 (context.quote() 가 null)
            if (q.outcome != "OK" || q.price == null) {
                logger.warn { "quote skipped / ${q.symbol} outcome=${q.outcome} session=${q.session}" }
                return@mapNotNull null
            }
            Quote(
                symbol = requestedByCode[q.symbol] ?: q.symbol, // 요청받은 표기(시장 접두 포함)로 돌려준다
                price = q.price,
                bidPrice = q.bidPrice,
                askPrice = q.askPrice,
                volume = q.volume ?: 0L,
                change = q.change,
                changeRate = q.changeRate?.percentToRate(),
                timestamp = NextTime.parseInstant(q.lastTradeAt) ?: NextTime.parseInstant(q.requestedAt) ?: Instant.now(),
            )
        }
        return QuotesResponse(quotes)
    }

    override fun getCandles(symbol: String, interval: CandleInterval, limit: Int?): CandlesResponse = usage.measure("candles") {
        val code = capabilities.symbolCode(symbol)
        val raw: NextCandlesResponse = get {
            it.path("/v1/market/candles")
                .queryParam("symbol", code)
                .queryParam("interval", interval.value)
                .apply { if (limit != null) queryParam("limit", limit) }
                .build()
        }
        return CandlesResponse(
            symbol = symbol,
            interval = raw.interval ?: interval.value,
            candles = raw.candles.map { c ->
                Candle(
                    timestamp = NextTime.parseInstant(c.time) ?: Instant.EPOCH,
                    open = c.open, high = c.high, low = c.low, close = c.close,
                    volume = c.volume ?: 0L,
                )
            },
        )
    }

    override fun getCalendar(): CalendarResponse = usage.measure("calendar") {
        val raw: NextCalendarResponse = get { it.path("/v1/market/calendar").build() }
        return CalendarResponse(raw.calendar.map { toMarketDay(it) })
    }

    fun getExchangeRate(base: String = "USD", quote: String = "KRW"): ExchangeRateResponse =
        get { it.path("/v1/market/exchange-rate").queryParam("base", base).queryParam("quote", quote).build() }

    fun getInstruments(search: String? = null, limit: Int? = null): InstrumentsResponse {
        val raw: NextInstrumentsResponse = get {
            it.path("/v1/instruments")
                .apply { if (search != null) queryParam("search", search) }
                .apply { if (limit != null) queryParam("limit", limit) }
                .build()
        }
        return InstrumentsResponse(raw.instruments.map { it.toInstrument() })
    }

    fun getInstrument(symbol: String): InstrumentDetailResponse {
        val raw: NextInstrument = get { it.path("/v1/instruments/{symbol}").build(symbol) }
        return InstrumentDetailResponse(
            symbol = raw.symbol, name = raw.name, type = raw.type, exchange = raw.exchange, currency = raw.currency,
            tradable = raw.tradable == "TRADABLE", tradableStatus = raw.tradable, dayMarketTradable = raw.dayMarketTradable,
        )
    }

    // ----------------------------------------------------------------- account

    /**
     * v1.3 계좌 응답에는 예수금(`cashAmount`)만 있다. 공통 모델의 `portfolioValue`(총평가)는
     * 예수금 + 보유 평가금액 합계로 계산한다 — 보유 조회 1회가 추가된다.
     */
    override fun getAccount(): AccountResponse = usage.measure("account") {
        val raw: NextAccountResponse = get(account = true) { it.path("/v1/account").build() }
        val holdings = getHoldings()
        val totalMarketValue = holdings.summary?.totalMarketValue ?: BigDecimal.ZERO
        return AccountResponse(
            accountId = raw.accountId,
            name = null,
            currency = raw.currency ?: "USD",
            cash = raw.cashAmount,
            portfolioValue = raw.cashAmount + totalMarketValue,
            status = "ACTIVE",
        )
    }

    override fun getHoldings(): HoldingsResponse = usage.measure("holdings") {
        val raw: NextHoldingsResponse = get(account = true) { it.path("/v1/account/holdings").build() }
        val holdings = raw.holdings.map { h ->
            Holding(
                symbol = h.symbol,
                quantity = h.quantity,
                avgEntryPrice = h.averageBuyPrice,
                currentPrice = h.currentPrice,
                marketValue = h.evaluationAmount,
                unrealizedPnl = h.evaluationPnl,
                unrealizedPnlRate = h.evaluationPnlRate?.percentToRate(),
            )
        }
        return HoldingsResponse(
            holdings = holdings,
            summary = HoldingsSummary(
                totalMarketValue = holdings.fold(BigDecimal.ZERO) { acc, h -> acc + (h.marketValue ?: BigDecimal.ZERO) },
                totalUnrealizedPnl = holdings.fold(BigDecimal.ZERO) { acc, h -> acc + (h.unrealizedPnl ?: BigDecimal.ZERO) },
            ),
        )
    }

    override fun getBuyingPower(): BuyingPowerResponse = usage.measure("buying_power") {
        val raw: NextBuyingPowerResponse = get(account = true) { it.path("/v1/account/buying-power").build() }
        return BuyingPowerResponse(accountId = raw.accountId, currency = raw.currency ?: "USD", buyingPower = raw.buyingPower)
    }

    // ------------------------------------------------------------------ orders

    fun previewOrder(request: PreviewOrderRequest): PreviewOrderResponse =
        exchange(
            HttpMethod.POST, { it.path("/v1/orders/preview").build() },
            NextOrderRequest(
                clientOrderId = UUID.randomUUID().toString(), // 미리보기에서는 무시되지만 본문 계약상 필수
                market = MARKET,
                symbol = capabilities.symbolCode(request.symbol),
                side = request.side.name,
                orderType = request.orderType.name,
                quantity = request.quantity,
                limitPrice = request.limitPrice,
                timeInForce = request.timeInForce.name,
            ),
            account = true,
        )

    override fun createOrder(request: CreateOrderRequest): OrderResponse = usage.measure("create_order") {
        val raw: NextOrderResponse = exchange(
            HttpMethod.POST, { it.path("/v1/orders").build() },
            NextOrderRequest(
                // v1.3: clientOrderId 는 필수 멱등키 — 호출자가 안 주면 어댑터가 UUID 를 만든다
                clientOrderId = request.clientOrderId ?: UUID.randomUUID().toString(),
                market = MARKET,
                symbol = capabilities.symbolCode(request.symbol),
                side = request.side.name,
                orderType = request.orderType.name,
                quantity = request.quantity,
                limitPrice = request.limitPrice,
                timeInForce = request.timeInForce.name,
            ),
            account = true,
        )
        return raw.toOrderResponse()
    }

    override fun getOrders(): OrdersResponse = usage.measure("get_orders") {
        val raw: NextOrdersResponse = get(account = true) { it.path("/v1/orders").build() }
        return OrdersResponse(raw.orders.map { it.toOrderResponse() })
    }

    override fun getOrder(orderId: String): OrderResponse = usage.measure("get_order") {
        val raw: NextOrderResponse = get(account = true) { it.path("/v1/orders/{orderId}").build(orderId) }
        return raw.toOrderResponse()
    }

    override fun cancelOrder(orderId: String): OrderResponse = usage.measure("cancel_order") {
        val raw: NextOrderResponse = exchange(HttpMethod.DELETE, { it.path("/v1/orders/{orderId}").build(orderId) }, null, account = true)
        return raw.toOrderResponse()
    }

    override fun getFills(): FillsResponse = usage.measure("fills") {
        val raw: NextFillsResponse = get(account = true) { it.path("/v1/orders/fills").build() }
        return FillsResponse(
            raw.fills.map { f ->
                Fill(
                    fillId = null, // v1.3: 원장이 체결 ID 를 발급하지 않는다
                    orderId = f.orderId,
                    symbol = f.symbol,
                    side = f.side?.let { runCatching { OrderSide.valueOf(it) }.getOrNull() },
                    quantity = f.quantity,
                    price = f.price,
                    amount = f.amount,
                    timestamp = NextTime.parseInstant(f.filledAt),
                )
            },
        )
    }

    // --------------------------------------------------------------- mapping

    private fun NextOrderResponse.toOrderResponse(): OrderResponse =
        OrderResponse(
            orderId = orderId,
            clientOrderId = requestId,
            status = status?.let { runCatching { OrderStatus.valueOf(it) }.getOrNull() } ?: OrderStatus.UNKNOWN,
            symbol = symbol,
            side = side?.let { runCatching { OrderSide.valueOf(it) }.getOrNull() },
            orderType = orderType?.let { runCatching { OrderType.valueOf(it.uppercase()) }.getOrNull() },
            quantity = quantity,
            notional = amount,
            limitPrice = limitPrice,
            filledQuantity = filledQuantity,
            avgFillPrice = avgFillPrice,
            submittedAt = NextTime.parseInstant(requestedAt),
            filledAt = null,
            canceledAt = null,
            rejectReason = rejectReason,
        )

    /**
     * v1.3 캘린더: `date` 는 거래소 현지 일자, 세션 시각은 KST 의 ISO 8601. 공통 모델은 "현지 타임존 + HH:mm" 이므로
     * 세션 시각을 뉴욕 현지 시각으로 바꿔 담는다 — 엔진의 개장 판단 로직은 그대로 동작한다.
     */
    private fun toMarketDay(day: NextCalendarDay): MarketDay {
        fun session(type: String): SessionHours? {
            val s = day.sessions.firstOrNull { it.type == type } ?: return null
            val open = NextTime.parseInstant(s.open) ?: return null
            val close = NextTime.parseInstant(s.close) ?: return null
            return SessionHours(start = open.toNewYorkClock(), end = close.toNewYorkClock())
        }

        val open = day.status != null && day.status != "CLOSED"
        val sessions = MarketSessions(preMarket = session("PRE"), regular = session("REGULAR"), afterHours = session("AFTER"))
        return MarketDay(
            date = day.date,
            open = open && sessions.regular != null,
            sessions = if (open) sessions else null,
            timezone = NextTime.NEW_YORK.id,
            holiday = day.holidayName,
        )
    }

    private fun Instant.toNewYorkClock(): String = atZone(NextTime.NEW_YORK).toLocalTime().format(HH_MM)

    private fun BigDecimal.percentToRate(): BigDecimal = divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN)

    private fun NextInstrument.toInstrument() = Instrument(
        symbol = symbol, name = name, type = type, exchange = exchange, currency = currency,
        tradable = tradable == "TRADABLE", tradableStatus = tradable, dayMarketTradable = dayMarketTradable,
    )

    // ---------------------------------------------------------------- internal

    private inline fun <reified T : Any> get(
        account: Boolean = false,
        noinline uri: (UriBuilder) -> URI,
    ): T = exchange(HttpMethod.GET, uri, null, account)

    private inline fun <reified T : Any> exchange(
        method: HttpMethod,
        noinline uri: (UriBuilder) -> URI,
        body: Any?,
        account: Boolean = false,
    ): T = executeWithRetry { token ->
        restClient.method(method)
            .uri(uri)
            .apply {
                header(REQUEST_ID_HEADER, newRequestId())
                header("Authorization", "Bearer $token")
                if (account && properties.accountId.isNotBlank()) header(ACCOUNT_HEADER, properties.accountId)
                if (body != null) {
                    contentType(MediaType.APPLICATION_JSON)
                    body(body)
                }
            }
            .exchange { _, res ->
                val bytes = res.body.readAllBytes()
                if (res.statusCode.is2xxSuccessful) {
                    objectMapper.readValue(bytes, T::class.java)
                } else {
                    val error = runCatching { objectMapper.readValue(bytes, ApiErrorEnvelope::class.java).error }.getOrNull()
                    val retryAfter = res.headers.getFirst("Retry-After")?.trim()?.toLongOrNull()
                    throw mapError(res.statusCode.value(), error, retryAfter)
                }
            }!!
    }

    /**
     * v1.3 에러 엔벨로프 `{error:{type, code, message, param, requestId, data, docUrl}}` 를 타입화된 예외로 변환한다.
     * `type` ↔ HTTP: validation(400) · authentication(401) · permission(403) · not_found(404) · conflict(409) ·
     * business_rule(422) · locked(423, 킬스위치 `trading-halted`) · rate_limit(429) · server(5xx)
     */
    private fun mapError(status: Int, error: ApiError?, retryAfterSeconds: Long? = null): BrokerApiException {
        val code = error?.code
        val message = "Next(${code}) ${error?.message ?: ""} requestId=${error?.requestId}"
        return when {
            status == 401 || error?.type == "authentication" -> AuthError(status, code, message)
            status == 429 -> RateLimitError(status, code, message, retryAfterSeconds)
            // 조회전용 키·계좌 불일치 — 인증 재시도로 풀리지 않는 권한 문제
            error?.type == "permission" -> BrokerApiException(status, code, message)
            code == "order-not-found" -> OrderNotFoundError(code, message)
            code?.contains("insufficient") == true -> InsufficientFundsError(status, code, message)
            // 킬스위치(423) 또는 장 마감 — 주문이 불가능한 상태이므로 틱을 조용히 건너뛴다
            code == "trading-halted" || code?.contains("market-closed") == true -> MarketClosedError(status, code, message)
            error?.type == "validation" || error?.type == "business_rule" -> InvalidOrderError(status, code, message)
            else -> BrokerApiException(status, code, message)
        }
    }

    /** 토큰 거부(만료) 시 1회 재발급 후 재시도는 [BrokerTokenManager.call] */
    private fun <T> executeWithRetry(call: (String) -> T): T = limiter.execute("next") { tokenManager.tokens.call(call) }

    companion object {
        const val ACCOUNT_HEADER = "X-Next-Account-Id"
        const val REQUEST_ID_HEADER = "X-Request-Id"
        private const val MARKET = "US"
        private val HH_MM: DateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm")

        /** v1.3 규칙: 영숫자·`.`·`_`·`-` 만 허용, 최대 64자. `hmx-` 프리픽스 + UUID(36자) = 40자 */
        fun newRequestId(): String = "hmx-${UUID.randomUUID()}"
    }
}
