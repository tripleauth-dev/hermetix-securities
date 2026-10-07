package com.tripleauth.hermetix.client.toss

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.broker.BrokerApiException
import com.tripleauth.hermetix.broker.BrokerCapabilities
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.InsufficientFundsError
import com.tripleauth.hermetix.broker.InvalidOrderError
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.KrxTick
import com.tripleauth.hermetix.broker.MarketClosedError
import com.tripleauth.hermetix.broker.MarketStream
import com.tripleauth.hermetix.broker.MarketSymbol
import com.tripleauth.hermetix.broker.OrderNotFoundError
import com.tripleauth.hermetix.broker.RateLimitError
import com.tripleauth.hermetix.broker.RateLimiter
import com.tripleauth.hermetix.broker.StreamChannel
import com.tripleauth.hermetix.broker.StreamingBrokerClient
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.broker.UsageTelemetry
import com.tripleauth.hermetix.broker.symbolCode
import com.tripleauth.hermetix.client.BrokerTokenManager
import com.tripleauth.hermetix.client.dto.AccountResponse
import com.tripleauth.hermetix.client.dto.BuyingPowerResponse
import com.tripleauth.hermetix.client.dto.CalendarResponse
import com.tripleauth.hermetix.client.dto.Candle
import com.tripleauth.hermetix.client.dto.CandleInterval
import com.tripleauth.hermetix.client.dto.CandlesResponse
import com.tripleauth.hermetix.client.dto.CreateOrderRequest
import com.tripleauth.hermetix.client.dto.Fill
import com.tripleauth.hermetix.client.dto.FillsResponse
import com.tripleauth.hermetix.client.dto.Holding
import com.tripleauth.hermetix.client.dto.HoldingsResponse
import com.tripleauth.hermetix.client.dto.HoldingsSummary
import com.tripleauth.hermetix.client.dto.OrderResponse
import com.tripleauth.hermetix.client.dto.OrderSide
import com.tripleauth.hermetix.client.dto.OrderStatus
import com.tripleauth.hermetix.client.dto.OrderType
import com.tripleauth.hermetix.client.dto.OrdersResponse
import com.tripleauth.hermetix.client.dto.Quote
import com.tripleauth.hermetix.client.dto.QuotesResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.http.HttpMethod
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClient
import java.math.BigDecimal
import java.time.Clock
import java.time.Instant
import java.time.OffsetDateTime
import java.util.UUID

/**
 * **토스증권 Open API** 어댑터 (REST v1.2.15). `hermetix.broker: toss` 로 활성화한다.
 *
 * ⚠️ **실전 전용 · 실측 전** — 토스증권은 모의투자 샌드박스가 없다. 공식 OpenAPI 문서(`openapi.tossinvest.com/openapi-docs`)로
 * 구현했고 문서 정확도는 높지만, 실계좌 소액 검증 전까지 상태는 "미검증" 이다. 반드시 `hermetix.live.enabled=true` 와
 * 주문 금액 상한(`hermetix.risk.*`)을 설정하고 소액으로 시작하라.
 *
 * 규약 (공식 문서 기준):
 * - `/api/v1/…` + `Authorization: Bearer`. 계좌·자산·주문 API 는 `X-Tossinvest-Account: {accountSeq}` 헤더 필수
 * - 성공 `{"result": …}`, 에러 `{"error": {requestId, code, message, data}}`. 토큰 발급만 OAuth 표준 `{error, error_description}`
 * - client 당 유효 토큰 1개 — 재발급하면 이전 토큰이 즉시 무효(`token-revoked`). 같은 키의 프로세스끼리는 파일 캐시로 토큰을 나눠 쓴다 ([BrokerTokenManager])
 * - 429 는 `Retry-After` + `X-RateLimit-…`. 그룹별 초당 한도(자산 5, 주문 10, 주문정보 6, 계좌 1)
 * - 한 계좌로 KRX·미국 주식을 모두 다룬다 — 보유·주문의 심볼은 `KRX:005930` / `US:AAPL` 로 접두를 붙여 돌려준다
 *
 * 공통 모델과의 차이 (문서상 확정):
 * - 시세(`/prices`)에 등락·거래량이 없다 → `change`/`changeRate` null, `volume` 0
 * - 예수금 엔드포인트가 없다 → `cash` 는 KRW 매수가능금액, `portfolioValue` 는 그 값 + 보유 원화환산 평가금액 합
 * - 체결 내역 엔드포인트가 없다 → 종료 주문의 `execution` 집계를 체결로 돌려준다 (fillId 없음)
 * - 취소·정정은 **새 orderId** 를 발급한다 → `cancelOrder` 는 원주문 ID 를 유지하고 PENDING_CANCEL 로 응답
 * - 캘린더는 KRX 합성(`KrxCalendar`) — 미국 종목만 다루는 전략은 `regularHoursOnly=false` 로 두고 직접 판단해야 한다
 */
class TossApiClient internal constructor(
    private val properties: TossApiProperties,
    private val objectMapper: ObjectMapper,
    clock: Clock,
    sleeper: (Long) -> Unit,
) : StreamingBrokerClient {

    constructor(properties: TossApiProperties, objectMapper: ObjectMapper) : this(properties, objectMapper, Clock.systemUTC(), Thread::sleep)

    private val logger = KotlinLogging.logger { }

    override val capabilities = BrokerCapabilities(
        brokerId = "toss",
        market = "KRX",
        currency = "KRW",
        candleIntervals = setOf(CandleInterval.MIN_1, CandleInterval.DAY_1),
        clientOrderId = true, // 10분 멱등키 (≤36자, 영숫자·-·_)
        nativeBracket = false,
        fractionalShares = false, // 미국 시장가 매도·금액 매수만 소수 허용 — 공통 모델에서는 미지원으로 선언
        serverOpenOrders = true,
        environments = setOf(TradingEnvironment.LIVE), // 모의투자 샌드박스 없음
        markets = setOf("KRX", "US"),
        // 웹소켓 trade:{kr,us} / orderbook:{kr,us} / personal:order — AsyncAPI 1.2.2 기반, 실측 전
        streams = setOf(StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS),
    )

    override val environment: TradingEnvironment = properties.environment


    private val usage = UsageTelemetry.forBroker(capabilities.brokerId, environment)

    private val restClient = RestClient.builder().baseUrl(properties.baseUrl).build()

    private val limiter = RateLimiter(
        minIntervalMillis = properties.throttleMillis,
        maxRetries = 3,
        backoffMillis = { attempt -> 1000L shl (attempt - 1) }, // 1s → 2s → 4s (문서 권장)
        sleeper = sleeper,
    )

    /**
     * 토큰 수명주기 — 메모리 → 파일 캐시 → 발급, 발급 실패 60초 쿨다운, 거부 토큰 폐기 후 1회 재시도.
     * 재발급이 이전 토큰을 무효로 만들므로 파일 캐시로 같은 키의 프로세스끼리 토큰을 나눠 쓴다
     */
    private val tokens = BrokerTokenManager("toss", properties.clientId, properties.tokenRefreshMarginSeconds, clock) { issueToken(clock) }

    @Volatile
    private var resolvedAccountSeq: String = properties.accountSeq

    /** 계좌 조회 전용 잠금 — 토큰 잠금과 분리 (계좌 조회 호출이 토큰 발급을 거친다) */
    private val accountLock = Any()

    // ------------------------------------------------------------------ market

    override fun getQuotes(symbols: List<String>): QuotesResponse = usage.measure("quotes") {
        val requestedByCode = symbols.associateBy { capabilities.symbolCode(it) }
        val result = call(HttpMethod.GET, "/api/v1/prices?symbols=${requestedByCode.keys.joinToString(",")}")
        val quotes = result.mapNotNull { q ->
            val price = q.decimalOrNull("lastPrice") ?: return@mapNotNull null
            Quote(
                symbol = requestedByCode[q.path("symbol").asText()] ?: q.path("symbol").asText(),
                price = price,
                bidPrice = null,
                askPrice = null,
                volume = 0L, // /prices 에 거래량 없음
                change = null,
                changeRate = null,
                timestamp = parseInstant(q.path("timestamp").asText(null)) ?: Instant.now(),
            )
        }
        return QuotesResponse(quotes)
    }

    override fun getCandles(symbol: String, interval: CandleInterval, limit: Int?): CandlesResponse = usage.measure("candles") {
        require(interval in capabilities.candleIntervals) { "토스 어댑터는 1m/1d 캔들만 지원합니다 (${interval.value})" }
        val count = (limit ?: 100).coerceIn(1, 200)
        val result = call(HttpMethod.GET, "/api/v1/candles?symbol=${capabilities.symbolCode(symbol)}&interval=${interval.value}&count=$count")
        // 최신순 → 공통 모델은 과거→최신
        val candles = result.path("candles").mapNotNull { c ->
            val ts = parseInstant(c.path("timestamp").asText(null)) ?: return@mapNotNull null
            Candle(
                timestamp = ts,
                open = c.decimal("openPrice"), high = c.decimal("highPrice"), low = c.decimal("lowPrice"), close = c.decimal("closePrice"),
                volume = c.decimalOrNull("volume")?.toLong() ?: 0L,
            )
        }.sortedBy { it.timestamp }
        return CandlesResponse(symbol = symbol, interval = interval.value, candles = candles)
    }

    override fun getCalendar(): CalendarResponse = usage.measure("calendar") { KrxCalendar.synthesize() }

    // ----------------------------------------------------------------- account

    override fun getAccount(): AccountResponse = usage.measure("account") {
        val cash = buyingPower("KRW")
        val holdings = getHoldings()
        return AccountResponse(
            accountId = accountSeq(),
            name = null,
            currency = "KRW",
            cash = cash,
            portfolioValue = cash + (holdings.summary?.totalMarketValue ?: BigDecimal.ZERO),
            status = "ACTIVE",
        )
    }

    override fun getHoldings(): HoldingsResponse = usage.measure("holdings") {
        val result = call(HttpMethod.GET, "/api/v1/holdings", account = true)
        val holdings = result.path("items").mapNotNull { h ->
            val quantity = h.decimalOrNull("quantity") ?: return@mapNotNull null
            if (quantity.signum() <= 0) return@mapNotNull null
            Holding(
                symbol = prefixed(h.path("symbol").asText(), if (h.path("marketCountry").asText() == "US") "US" else "KRX"),
                quantity = quantity,
                avgEntryPrice = h.decimalOrNull("averagePurchasePrice") ?: BigDecimal.ZERO, // 종목 통화 기준
                currentPrice = h.decimalOrNull("lastPrice"),
                marketValue = h.path("marketValue").path("amount").decimalOrNull("krw"),   // 원화환산
                unrealizedPnl = h.path("profitLoss").path("amount").decimalOrNull("krw"),  // 원화환산
                unrealizedPnlRate = h.path("profitLoss").decimalOrNull("rate"),            // 이미 소수 비율
            )
        }
        return HoldingsResponse(
            holdings = holdings,
            summary = HoldingsSummary(
                totalMarketValue = result.path("marketValue").path("amount").decimalOrNull("krw")
                    ?: holdings.fold(BigDecimal.ZERO) { acc, h -> acc + (h.marketValue ?: BigDecimal.ZERO) },
                totalUnrealizedPnl = result.path("profitLoss").path("amount").decimalOrNull("krw"),
            ),
        )
    }

    override fun getBuyingPower(): BuyingPowerResponse = usage.measure("buying_power") {
        
        BuyingPowerResponse(accountId = accountSeq(), currency = "KRW", buyingPower = buyingPower("KRW"))
    }

    // ------------------------------------------------------------------ orders

    override fun createOrder(request: CreateOrderRequest): OrderResponse = usage.measure("create_order") {
        require(request.orderType == OrderType.LIMIT || request.orderType == OrderType.MARKET) { "토스 어댑터는 LIMIT/MARKET 주문만 지원합니다" }
        val parsed = MarketSymbol.parse(request.symbol)
        val code = capabilities.symbolCode(request.symbol)
        val isKrx = (parsed.market ?: "KRX") == "KRX"
        val body = mutableMapOf<String, Any>(
            "clientOrderId" to (request.clientOrderId ?: UUID.randomUUID().toString()),
            "symbol" to code,
            "side" to request.side.name,
            "orderType" to request.orderType.name,
            "timeInForce" to "DAY",
            "quantity" to request.quantity.toPlainString(),
            "confirmHighValueOrder" to false,
        )
        if (request.orderType == OrderType.LIMIT) {
            val price = request.limitPrice!!
            // KRX 지정가는 정수·호가단위, 미국은 소수 그대로
            body["price"] = if (isKrx) KrxTick.round(price).toPlainString() else price.toPlainString()
        }
        val result = call(HttpMethod.POST, "/api/v1/orders", body = body, account = true)
        val orderId = result.path("orderId").asText("").trim()
        if (orderId.isBlank()) throw BrokerApiException(200, null, "토스 주문 응답에 orderId 가 없습니다")
        return OrderResponse(
            orderId = orderId,
            clientOrderId = result.path("clientOrderId").asText(null) ?: request.clientOrderId,
            status = OrderStatus.SUBMITTED, // 생성 응답에는 상태가 없다 — 접수로 간주하고 상세 조회로 확인
            symbol = prefixed(code, if (isKrx) "KRX" else "US"),
            side = request.side,
            orderType = request.orderType,
            quantity = request.quantity,
            limitPrice = request.limitPrice,
            filledQuantity = BigDecimal.ZERO,
            submittedAt = Instant.now(),
        )
    }

    override fun getOrders(): OrdersResponse = usage.measure("get_orders") {
        
        OrdersResponse(call(HttpMethod.GET, "/api/v1/orders?status=OPEN", account = true).path("orders").map { it.toOrderResponse() })
    }

    override fun getOrder(orderId: String): OrderResponse = usage.measure("get_order") {
        
        call(HttpMethod.GET, "/api/v1/orders/$orderId", account = true).toOrderResponse()
    }

    override fun cancelOrder(orderId: String): OrderResponse = usage.measure("cancel_order") {
        // 취소는 새 orderId 를 발급한다 — 호출자에게는 원주문 ID 를 유지해 돌려준다
        val result = call(HttpMethod.POST, "/api/v1/orders/$orderId/cancel", body = emptyMap<String, Any>(), account = true)
        logger.debug { "toss cancel accepted / original=$orderId cancelOrderId=${result.path("orderId").asText()}" }
        return OrderResponse(orderId = orderId, status = OrderStatus.PENDING_CANCEL, canceledAt = Instant.now())
    }

    override fun getFills(): FillsResponse = usage.measure("fills") {
        // 체결 엔드포인트가 없다 — 종료 주문의 execution 집계를 주문 단위 체결로 돌려준다
        val orders = call(HttpMethod.GET, "/api/v1/orders?status=CLOSED", account = true).path("orders")
        val fills = orders.mapNotNull { o ->
            val exec = o.path("execution")
            val qty = exec.decimalOrNull("filledQuantity")?.takeIf { it.signum() > 0 } ?: return@mapNotNull null
            val price = exec.decimalOrNull("averageFilledPrice")
            Fill(
                fillId = null,
                orderId = o.path("orderId").asText(),
                symbol = prefixed(o.path("symbol").asText(), if (o.path("currency").asText() == "USD") "US" else "KRX"),
                side = o.path("side").asText(null)?.let { runCatching { OrderSide.valueOf(it) }.getOrNull() },
                quantity = qty,
                price = price,
                amount = exec.decimalOrNull("filledAmount") ?: if (price != null) qty.multiply(price) else null,
                timestamp = parseInstant(exec.path("filledAt").asText(null)),
            )
        }
        return FillsResponse(fills)
    }

    // ---------------------------------------------------------------- mapping

    private fun JsonNode.toOrderResponse(): OrderResponse {
        val exec = path("execution")
        return OrderResponse(
            orderId = path("orderId").asText(),
            clientOrderId = path("clientOrderId").asText(null),
            status = mapStatus(path("status").asText("")),
            symbol = prefixed(path("symbol").asText(), if (path("currency").asText() == "USD") "US" else "KRX"),
            side = path("side").asText(null)?.let { runCatching { OrderSide.valueOf(it) }.getOrNull() },
            orderType = path("orderType").asText(null)?.let { runCatching { OrderType.valueOf(it) }.getOrNull() },
            quantity = decimalOrNull("quantity"),
            notional = decimalOrNull("orderAmount"),
            limitPrice = decimalOrNull("price"),
            filledQuantity = exec.decimalOrNull("filledQuantity") ?: BigDecimal.ZERO,
            avgFillPrice = exec.decimalOrNull("averageFilledPrice"),
            submittedAt = parseInstant(path("orderedAt").asText(null)),
            filledAt = parseInstant(exec.path("filledAt").asText(null)),
            canceledAt = parseInstant(path("canceledAt").asText(null)),
        )
    }

    private fun prefixed(code: String, market: String): String = "$market:$code"

    private fun buyingPower(currency: String): BigDecimal =
        call(HttpMethod.GET, "/api/v1/buying-power?currency=$currency", account = true).decimal("cashBuyingPower")

    // ------------------------------------------------------------------ stream

    /** 웹소켓은 REST 와 같은 토큰을 핸드셰이크 헤더에 싣고, 주문 이벤트 구독은 [accountSeq] 로 계좌를 고른다 */
    override fun openStream(): MarketStream = TossMarketStream(properties, objectMapper, ::token, ::accountSeq, usage = usage)

    /** 계좌 순번 — 설정이 비어 있으면 `/api/v1/accounts` 의 첫 BROKERAGE 계좌 */
    internal fun accountSeq(): String {
        resolvedAccountSeq.takeIf { it.isNotBlank() }?.let { return it }
        synchronized(accountLock) {
            resolvedAccountSeq.takeIf { it.isNotBlank() }?.let { return it }
            val accounts = call(HttpMethod.GET, "/api/v1/accounts").toList()
            val picked = accounts.firstOrNull { it.path("accountType").asText() == "BROKERAGE" } ?: accounts.firstOrNull()
                ?: throw BrokerApiException(200, null, "토스 계좌 목록이 비어 있습니다")
            resolvedAccountSeq = picked.path("accountSeq").asText()
            logger.info { "toss account resolved / accountSeq=$resolvedAccountSeq accountNo=${picked.path("accountNo").asText()}" }
            return resolvedAccountSeq
        }
    }

    // ---------------------------------------------------------------- internal

    /** 성공 envelope 의 `result` 를 돌려준다 */
    private fun call(method: HttpMethod, path: String, body: Any? = null, account: Boolean = false): JsonNode =
        limiter.execute("toss $path") { tokens.call { token -> callOnce(method, path, body, account, token) } }

    private fun callOnce(method: HttpMethod, path: String, body: Any?, account: Boolean, token: String): JsonNode {
        val accountHeader = if (account) accountSeq() else null
        return restClient.method(method)
            .uri(path)
            .headers { headers ->
                headers.set("Authorization", "Bearer $token")
                if (accountHeader != null) headers.set("X-Tossinvest-Account", accountHeader)
            }
            .apply {
                if (body != null) {
                    contentType(MediaType.APPLICATION_JSON)
                    body(objectMapper.writeValueAsString(body))
                }
            }
            .exchange { _, res ->
                val bytes = res.body.readAllBytes()
                val node = runCatching { objectMapper.readTree(bytes) }.getOrNull() ?: objectMapper.createObjectNode()
                if (!res.statusCode.is2xxSuccessful) {
                    val error = node.path("error")
                    val code = error.path("code").asText("")
                    val msg = "Toss($path) [$code] ${error.path("message").asText("")} requestId=${error.path("requestId").asText("")}".trim()
                    val status = res.statusCode.value()
                    val retryAfter = res.headers.getFirst("Retry-After")?.trim()?.toLongOrNull()
                    throw when {
                        status == 429 -> RateLimitError(status, code, msg, retryAfter)
                        status == 401 -> AuthError(status, code, msg)
                        code == "order-not-found" -> OrderNotFoundError(code, msg)
                        code == "insufficient-buying-power" -> InsufficientFundsError(status, code, msg)
                        code == "order-hours-closed" -> MarketClosedError(status, code, msg)
                        status == 400 || status == 409 || status == 422 -> InvalidOrderError(status, code, msg)
                        else -> BrokerApiException(status, code, msg)
                    }
                }
                node.path("result")
            }!!
    }

    private fun token(): String = tokens.get()

    /** 토큰 발급 — 429 는 서버 `Retry-After`(없으면 60초) 의 [RateLimitError] */
    private fun issueToken(clock: Clock): Pair<String, Instant> = usage.measure("auth") {
        limiter.throttle()
        val form = LinkedMultiValueMap<String, String>().apply {
            add("grant_type", "client_credentials")
            add("client_id", properties.clientId)
            add("client_secret", properties.clientSecret)
        }
        val node = restClient.post()
            .uri("/oauth2/token")
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .body(form)
            .exchange { _, res ->
                val n = runCatching { objectMapper.readTree(res.body.readAllBytes()) }.getOrNull() ?: objectMapper.createObjectNode()
                if (!res.statusCode.is2xxSuccessful || !n.hasNonNull("access_token")) {
                    val status = res.statusCode.value()
                    // OAuth 표준 {error, error_description} 또는 플랫폼 엔벨로프 {error: {code, message}}
                    val error = n.path("error")
                    val code = if (error.isObject) error.path("code").asText(null) else error.asText(null)
                    val detail = n.path("error_description").asText("").ifBlank { if (error.isObject) error.path("message").asText("") else "" }
                    val msg = "토스 토큰 발급 실패($code): $detail"
                    throw if (status == 429) {
                        val retryAfter = res.headers.getFirst("Retry-After")?.trim()?.toLongOrNull()
                        RateLimitError(status, code, msg, retryAfter ?: BrokerTokenManager.FAILURE_COOLDOWN_SECONDS)
                    } else {
                        AuthError(status, code, msg)
                    }
                }
                n
            }

        val expiresIn = node.path("expires_in").asLong(86400)
        logger.info { "toss token issued / expiresIn=${expiresIn}s (이전 토큰은 무효화됨)" }
        node.path("access_token").asText() to clock.instant().plusSeconds(expiresIn)
    }

    private fun parseInstant(text: String?): Instant? =
        text?.takeIf { it.isNotBlank() }?.let { runCatching { OffsetDateTime.parse(it).toInstant() }.getOrNull() }

    private fun JsonNode.decimal(field: String): BigDecimal = decimalOrNull(field) ?: BigDecimal.ZERO

    private fun JsonNode.decimalOrNull(field: String): BigDecimal? =
        path(field).takeIf { !it.isMissingNode && !it.isNull }?.asText("")?.trim()?.takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

    companion object {
        /** 토스 주문 상태 → 공통 상태. 취소/정정 거부는 별도 주문 레코드라 REJECTED, REPLACED 는 원주문 종료 */
        fun mapStatus(raw: String): OrderStatus = when (raw) {
            "PENDING", "PENDING_REPLACE" -> OrderStatus.SUBMITTED
            "PENDING_CANCEL" -> OrderStatus.PENDING_CANCEL
            "PARTIAL_FILLED" -> OrderStatus.PARTIALLY_FILLED
            "FILLED" -> OrderStatus.FILLED
            "CANCELED", "REPLACED" -> OrderStatus.CANCELED
            "REJECTED", "CANCEL_REJECTED", "REPLACE_REJECTED" -> OrderStatus.REJECTED
            else -> OrderStatus.UNKNOWN
        }
    }
}
