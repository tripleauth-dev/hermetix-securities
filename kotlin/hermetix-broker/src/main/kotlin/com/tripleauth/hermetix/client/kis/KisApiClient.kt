package com.tripleauth.hermetix.client.kis

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.broker.BrokerApiException
import com.tripleauth.hermetix.broker.BrokerCapabilities
import com.tripleauth.hermetix.broker.MarketClosedError
import com.tripleauth.hermetix.broker.OrderNotFoundError
import com.tripleauth.hermetix.broker.RateLimitError
import com.tripleauth.hermetix.broker.RateLimiter
import com.tripleauth.hermetix.broker.MarketStream
import com.tripleauth.hermetix.broker.OrderEvent
import com.tripleauth.hermetix.broker.OrderEventType
import com.tripleauth.hermetix.broker.StreamChannel
import com.tripleauth.hermetix.broker.StreamingBrokerClient
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.KrxTick
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
import org.springframework.web.client.RestClient
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneId
import java.time.format.DateTimeFormatter

/**
 * 한국투자증권(KIS) 어댑터. `hermetix.broker: kis` 로 활성화한다.
 *
 * 실측 기반 구현 (모의투자 도메인, 2026-08):
 * - 응답 엔벨로프: rt_cd("0"=성공) / msg_cd / msg1 / output*
 * - 서버는 초당 요청 제한이 있어 모든 호출에 최소 간격 쓰로틀을 건다 (모의 2건/s, 실전 20건/s)
 * - 계좌 TR-ID 는 환경 프리픽스가 다르다 — 모의 V (VTTC8434R…), 실전 T (TTTC8434R…). 시세 TR(FHKST…)은 공통
 * - 실전(`hermetix.kis.environment: live`)은 호스트 openapi.koreainvestment.com:9443 을 쓴다.
 *   실전 미체결/체결 조회 TR 은 아직 연동하지 않아 주문 추적은 모의와 같은 메모리 방식이다
 *
 * 제약 (문서화된 트레이드오프):
 * - 캔들은 일봉(DAY_1)만 지원한다 — KIS 분봉 API 는 당일 데이터만 제공해 lookback 전략에 부적합
 * - 캘린더는 KRX 정규장(평일 09:00-15:30 KST)을 합성한다 — 공휴일은 개장일로 보이지만
 *   주문 시 서버가 거부하므로 안전에는 문제 없다
 * - clientOrderId 미지원 (KIS 에 대응 개념 없음 — 무시된다)
 * - **모의 서버는 미체결/체결 주문 조회를 제공하지 않는다** (일별주문체결 TR 이 항상 빈 목록,
 *   정정취소가능조회 TR 은 미제공 — 2026-08 실측). 따라서 주문은 어댑터가 메모리에서 추적하고,
 *   체결 판정은 보유 수량 변화로 근사한다. 앱 재시작 시 추적이 끊긴다 (재시작 후 잔여 미체결 주의)
 * - 주문 취소는 지점번호 없이 ODNO 만으로 동작한다 (실측 검증)
 */
class KisApiClient internal constructor(
    private val properties: KisApiProperties,
    private val objectMapper: ObjectMapper,
    clock: Clock,
    sleeper: (Long) -> Unit,
) : StreamingBrokerClient {

    constructor(properties: KisApiProperties, objectMapper: ObjectMapper) : this(properties, objectMapper, Clock.systemUTC(), Thread::sleep)

    private val logger = KotlinLogging.logger { }

    override val capabilities = BrokerCapabilities(
        brokerId = "kis",
        market = "KRX",
        currency = "KRW",
        candleIntervals = setOf(CandleInterval.DAY_1),
        clientOrderId = false,
        nativeBracket = false,
        fractionalShares = false,
        serverOpenOrders = false, // 모의 서버가 주문 조회를 제공하지 않음 - 어댑터 내부 추적
        environments = setOf(TradingEnvironment.PAPER, TradingEnvironment.LIVE),
        // H0STCNT0 체결가·H0STASP0 호가 — 2026-09 모의 실측. H0STCNI9 주문 통보 — 문서 기반 (HTS ID 필요)
        streams = setOf(StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS),
    )

    override val environment: TradingEnvironment = properties.environment


    private val usage = UsageTelemetry.forBroker(capabilities.brokerId, environment)

    /** 어댑터 내부 주문 추적 (모의 서버가 주문 조회 미제공) */
    private val trackedOrders = java.util.concurrent.ConcurrentHashMap<String, TrackedOrder>()

    private val restClient = RestClient.builder()
        .baseUrl(properties.resolvedBaseUrl())
        .build()

    /** 초당 요청 제한 — 쓰로틀 + EGW00201 백오프 재시도 */
    private val limiter = RateLimiter(
        minIntervalMillis = properties.resolvedThrottleMillis(),
        maxRetries = 3,
        backoffMillis = { attempt -> 1000L * attempt },
        sleeper = sleeper,
    )

    /** 토큰 수명주기 — 메모리 → 파일 캐시 → 발급, 발급 실패 60초 쿨다운, 거부 토큰 폐기 후 1회 재시도 */
    private val tokens = BrokerTokenManager("kis", properties.appkey, properties.tokenRefreshMarginSeconds, clock) { issueToken(clock) }

    // ------------------------------------------------------------------ market

    override fun getQuotes(symbols: List<String>): QuotesResponse = usage.measure("quotes") {
        val quotes = symbols.map { symbol ->
            val output = call(
                HttpMethod.GET, "/uapi/domestic-stock/v1/quotations/inquire-price", "FHKST01010100",
                query = mapOf("FID_COND_MRKT_DIV_CODE" to "J", "FID_INPUT_ISCD" to capabilities.symbolCode(symbol)),
            ).path("output")

            Quote(
                symbol = symbol,
                price = output.decimal("stck_prpr"),
                bidPrice = null,
                askPrice = null,
                volume = output.path("acml_vol").asText("0").toLong(),
                change = output.decimalOrNull("prdy_vrss"),
                // KIS 는 % 단위(-8.76), 공통 모델은 비율(-0.0876)
                changeRate = output.decimalOrNull("prdy_ctrt")?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
                timestamp = Instant.now(),
            )
        }
        return QuotesResponse(quotes)
    }

    override fun getCandles(symbol: String, interval: CandleInterval, limit: Int?): CandlesResponse = usage.measure("candles") {
        require(interval == CandleInterval.DAY_1) {
            "KIS 어댑터는 일봉(DAY_1)만 지원합니다. 분/시간봉 API 는 당일 데이터만 제공되어 lookback 전략에 사용할 수 없습니다."
        }

        val count = limit ?: 30
        val today = LocalDate.now(KST)
        // 휴장일 감안해 여유 있게 조회 (달력일 기준 약 1.6배)
        val start = today.minusDays((count * 16L / 10) + 10)

        val rows = call(
            HttpMethod.GET, "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice", "FHKST03010100",
            query = mapOf(
                "FID_COND_MRKT_DIV_CODE" to "J", "FID_INPUT_ISCD" to capabilities.symbolCode(symbol),
                "FID_INPUT_DATE_1" to start.format(DATE), "FID_INPUT_DATE_2" to today.format(DATE),
                "FID_PERIOD_DIV_CODE" to "D", "FID_ORG_ADJ_PRC" to "0",
            ),
        ).path("output2")

        // KIS 는 최신순 → 공통 모델은 과거→최신
        val candles = rows.mapNotNull { row ->
            val date = row.path("stck_bsop_date").asText("")
            if (date.isBlank()) return@mapNotNull null
            Candle(
                timestamp = LocalDate.parse(date, DATE).atStartOfDay(KST).toInstant(),
                open = row.decimal("stck_oprc"),
                high = row.decimal("stck_hgpr"),
                low = row.decimal("stck_lwpr"),
                close = row.decimal("stck_clpr"),
                volume = row.path("acml_vol").asText("0").toLong(),
            )
        }.sortedBy { it.timestamp }.takeLast(count)

        return CandlesResponse(symbol = symbol, interval = interval.value, candles = candles)
    }

    override fun getCalendar(): CalendarResponse = usage.measure("calendar") { KrxCalendar.synthesize() }

    // ----------------------------------------------------------------- account

    override fun getAccount(): AccountResponse = usage.measure("account") {
        val summary = balance().path("output2").firstOrNull()
            ?: throw BrokerApiException(200, null, "KIS 잔고 요약(output2)이 비어 있습니다")

        return AccountResponse(
            accountId = properties.cano,
            name = null,
            currency = "KRW",
            cash = summary.decimal("dnca_tot_amt"),
            portfolioValue = summary.decimal("tot_evlu_amt"),
            status = "ACTIVE",
        )
    }

    override fun getHoldings(): HoldingsResponse = usage.measure("holdings") {
        val holdings = balance().path("output1")
            .filter { it.path("hldg_qty").asText("0").toBigDecimal() > BigDecimal.ZERO }
            .map { row ->
                Holding(
                    symbol = row.path("pdno").asText(),
                    quantity = row.decimal("hldg_qty"),
                    avgEntryPrice = row.decimal("pchs_avg_pric"),
                    currentPrice = row.decimalOrNull("prpr"),
                    marketValue = row.decimalOrNull("evlu_amt"),
                    unrealizedPnl = row.decimalOrNull("evlu_pfls_amt"),
                    unrealizedPnlRate = row.decimalOrNull("evlu_pfls_rt")?.divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN),
                )
            }
        return HoldingsResponse(holdings)
    }

    override fun getBuyingPower(): BuyingPowerResponse = usage.measure("buying_power") {
        val output = call(
            HttpMethod.GET, "/uapi/domestic-stock/v1/trading/inquire-psbl-order", properties.tr("TTC8908R"),
            query = accountParams() + mapOf(
                "PDNO" to "005930", "ORD_UNPR" to "", "ORD_DVSN" to "01",
                "CMA_EVLU_AMT_ICLD_YN" to "N", "OVRS_ICLD_YN" to "N",
            ),
        ).path("output")

        return BuyingPowerResponse(
            accountId = properties.cano,
            currency = "KRW",
            buyingPower = output.decimal("ord_psbl_cash"),
        )
    }

    // ------------------------------------------------------------------ orders

    override fun createOrder(request: CreateOrderRequest): OrderResponse = usage.measure("create_order") {
        require(request.orderType == OrderType.LIMIT || request.orderType == OrderType.MARKET) {
            "KIS 어댑터는 LIMIT/MARKET 주문만 지원합니다"
        }

        val trId = properties.tr(if (request.side == OrderSide.BUY) "TTC0802U" else "TTC0801U")
        val ordDvsn = if (request.orderType == OrderType.LIMIT) "00" else "01"
        // KRX 호가단위 보정 - 맞지 않는 지정가는 거래소가 거부한다
        val price = if (request.orderType == OrderType.LIMIT) KrxTick.round(request.limitPrice!!).toPlainString() else "0"
        val code = capabilities.symbolCode(request.symbol)

        val output = call(
            HttpMethod.POST, "/uapi/domestic-stock/v1/trading/order-cash", trId,
            body = accountParams() + mapOf(
                "PDNO" to code,
                "ORD_DVSN" to ordDvsn,
                "ORD_QTY" to request.quantity.toPlainString(),
                "ORD_UNPR" to price,
            ),
        ).path("output")

        val order = OrderResponse(
            orderId = output.path("ODNO").asText(),
            clientOrderId = request.clientOrderId, // KIS 미지원 — 반환만 유지
            status = OrderStatus.SUBMITTED,
            symbol = code, // 보유/추적과 같은 단일 시장 표기(접두 없음)
            side = request.side,
            orderType = request.orderType,
            quantity = request.quantity,
            limitPrice = request.limitPrice,
            filledQuantity = BigDecimal.ZERO,
            submittedAt = Instant.now(),
        )

        trackedOrders[order.orderId] = TrackedOrder(order = order, baselineQty = holdingQty(code), date = LocalDate.now(KST))
        return order
    }

    override fun getOrders(): OrdersResponse = usage.measure("get_orders") {
        refreshTrackedOrders()
        return OrdersResponse(trackedOrders.values.filter { it.order.status.isOpen }.map { it.order })
    }

    override fun getOrder(orderId: String): OrderResponse = usage.measure("get_order") {
        refreshTrackedOrders()
        return trackedOrders[orderId]?.order
            ?: OrderResponse(orderId = orderId, status = OrderStatus.CANCELED) // 추적 밖(재시작 등) - 알 수 없어 취소로 간주
    }

    override fun cancelOrder(orderId: String): OrderResponse = usage.measure("cancel_order") {
        // 실측: 모의 서버는 지점번호 없이 ODNO 만으로 취소된다
        call(
            HttpMethod.POST, "/uapi/domestic-stock/v1/trading/order-rvsecncl", properties.tr("TTC0803U"),
            body = accountParams() + mapOf(
                "KRX_FWDG_ORD_ORGNO" to "",
                "ORGN_ODNO" to orderId,
                "ORD_DVSN" to "00",
                "RVSE_CNCL_DVSN_CD" to "02", // 취소
                "ORD_QTY" to "0",
                "ORD_UNPR" to "0",
                "QTY_ALL_ORD_YN" to "Y",
            ),
        )

        val canceled = OrderResponse(orderId = orderId, status = OrderStatus.CANCELED, canceledAt = Instant.now())
        trackedOrders[orderId]?.let { trackedOrders[orderId] = it.copy(order = it.order.copy(status = OrderStatus.CANCELED, canceledAt = Instant.now())) }
        return canceled
    }

    override fun getFills(): FillsResponse = usage.measure("fills") {
        // 모의 서버가 체결 내역 조회를 제공하지 않는다 - 추적 주문 중 체결 판정된 것으로 근사
        refreshTrackedOrders()
        val fills = trackedOrders.values
            .filter { it.order.status == OrderStatus.FILLED }
            .map { tracked ->
                Fill(
                    fillId = tracked.order.orderId,
                    orderId = tracked.order.orderId,
                    symbol = tracked.order.symbol,
                    side = tracked.order.side,
                    quantity = tracked.order.quantity,
                    price = tracked.order.limitPrice,
                    amount = null,
                    timestamp = null,
                )
            }
        return FillsResponse(fills)
    }

    // ---------------------------------------------------------------- internal

    private fun balance(): JsonNode = call(
        HttpMethod.GET, "/uapi/domestic-stock/v1/trading/inquire-balance", properties.tr("TTC8434R"),
        query = accountParams() + mapOf(
            "AFHR_FLPR_YN" to "N", "OFL_YN" to "", "INQR_DVSN" to "02", "UNPR_DVSN" to "01",
            "FUND_STTL_ICLD_YN" to "N", "FNCG_AMT_AUTO_RDPT_YN" to "N", "PRCS_DVSN" to "00",
            "CTX_AREA_FK100" to "", "CTX_AREA_NK100" to "",
        ),
    )

    /** 추적 중인 미체결 주문의 체결 여부를 보유 수량 변화로 판정한다 (모의 서버 제약의 근사) */
    private fun refreshTrackedOrders() {
        val open = trackedOrders.values.filter { it.order.status.isOpen }
        val today = LocalDate.now(KST)

        // DAY 주문 - 날짜가 바뀌면 소멸
        trackedOrders.values.filter { it.date != today }.forEach { trackedOrders.remove(it.order.orderId) }
        if (open.none { it.date == today }) return

        val holdings = getHoldings().holdings.associateBy { it.symbol }
        open.filter { it.date == today }.forEach { tracked ->
            val current = holdings[tracked.order.symbol]?.quantity ?: BigDecimal.ZERO
            val filled = when (tracked.order.side) {
                OrderSide.BUY -> current >= tracked.baselineQty + (tracked.order.quantity ?: BigDecimal.ZERO)
                OrderSide.SELL -> current <= tracked.baselineQty - (tracked.order.quantity ?: BigDecimal.ZERO)
                else -> false
            }
            if (filled) {
                trackedOrders[tracked.order.orderId] = tracked.copy(
                    order = tracked.order.copy(status = OrderStatus.FILLED, filledQuantity = tracked.order.quantity),
                )
            }
        }
    }

    private fun holdingQty(symbol: String): BigDecimal =
        getHoldings().holdings.firstOrNull { it.symbol == symbol }?.quantity ?: BigDecimal.ZERO

    private data class TrackedOrder(
        val order: OrderResponse,
        val baselineQty: BigDecimal,
        val date: LocalDate,
    )

    private fun accountParams(): Map<String, String> =
        mapOf("CANO" to properties.cano, "ACNT_PRDT_CD" to properties.acntPrdtCd)

    private fun call(
        method: HttpMethod,
        path: String,
        trId: String,
        query: Map<String, String> = emptyMap(),
        body: Map<String, String>? = null,
    ): JsonNode = limiter.execute("KIS $trId") { tokens.call { token -> callOnce(method, path, trId, token, query, body) } }

    private fun callOnce(
        method: HttpMethod,
        path: String,
        trId: String,
        token: String,
        query: Map<String, String> = emptyMap(),
        body: Map<String, String>? = null,
    ): JsonNode {
        val response = restClient.method(method)
            .uri { builder ->
                builder.path(path).apply { query.forEach { (k, v) -> queryParam(k, v) } }.build()
            }
            .headers { headers ->
                headers.set("authorization", "Bearer $token")
                headers.set("appkey", properties.appkey)
                headers.set("appsecret", properties.appsecret)
                headers.set("tr_id", trId)
                headers.set("custtype", properties.custtype)
            }
            .apply {
                if (body != null) {
                    contentType(MediaType.APPLICATION_JSON)
                    body(objectMapper.writeValueAsString(body))
                }
            }
            .exchange { _, res ->
                val bytes = res.body.readAllBytes()
                val node = runCatching { objectMapper.readTree(bytes) }.getOrNull()
                    ?: throw BrokerApiException(res.statusCode.value(), null, "KIS 응답 파싱 실패")
                if (!res.statusCode.is2xxSuccessful || node.path("rt_cd").asText("") != "0") {
                    val code = node.path("msg_cd").asText(null)
                    val msg = "KIS(${trId}) ${node.path("msg1").asText("")}".trim()
                    throw when {
                        code == "EGW00201" -> RateLimitError(res.statusCode.value(), code, msg)
                        msg.contains("장종료") || msg.contains("장운영일이 아닙") -> MarketClosedError(res.statusCode.value(), code, msg)
                        res.statusCode.value() == 401 || code in TOKEN_REJECTED_CODES -> AuthError(res.statusCode.value(), code, msg)
                        else -> BrokerApiException(res.statusCode.value(), code, msg)
                    }
                }
                node
            }!!

        return response
    }

    // ------------------------------------------------------------------ stream

    override fun openStream(): MarketStream = KisMarketStream(properties, objectMapper, ::approvalKey, usage)

    /**
     * 주문 통보를 메모리 추적에 반영한다 — 모의 서버가 주문 조회를 제공하지 않아 보유 수량 변화로 근사하던 체결 판정을
     * 통보가 오면 즉시 확정한다. 통보 주문번호는 10자리 0 패딩이라 [OrderEvent.orderIdMatches] 로 맞춘다.
     */
    override fun applyOrderEvent(event: OrderEvent) {
        val entry = trackedOrders.entries.firstOrNull { event.orderIdMatches(it.key) } ?: return
        val tracked = entry.value
        val order = tracked.order
        val updated = when (event.type) {
            OrderEventType.FILLED -> {
                val filled = (order.filledQuantity ?: BigDecimal.ZERO) + (event.quantity ?: BigDecimal.ZERO)
                val total = order.quantity ?: filled
                order.copy(
                    filledQuantity = filled,
                    avgFillPrice = event.price ?: order.avgFillPrice,
                    status = if (filled >= total) OrderStatus.FILLED else OrderStatus.PARTIALLY_FILLED,
                    filledAt = if (filled >= total) event.timestamp else order.filledAt,
                )
            }
            OrderEventType.CANCELED -> order.copy(status = OrderStatus.CANCELED, canceledAt = event.timestamp)
            OrderEventType.REJECTED -> order.copy(status = OrderStatus.REJECTED, rejectReason = event.reason)
            OrderEventType.ACCEPTED, OrderEventType.MODIFIED -> return
        }
        trackedOrders[entry.key] = tracked.copy(order = updated)
        logger.info { "KIS order event applied / ${entry.key} ${event.type} -> ${updated.status}" }
    }

    /**
     * 웹소켓 접속키 (`POST /oauth2/Approval`). 토큰과 달리 캐시하지 않는다 — 접속마다 새로 받아도 무방하고
     * 문서상 유효기간이 명시돼 있지 않다. 필드명이 REST 토큰(`appsecret`)과 달리 `secretkey` 인 점에 주의.
     */
    internal fun approvalKey(): String = usage.measure("auth") {
        limiter.throttle()
        val node = restClient.post()
            .uri("/oauth2/Approval")
            .contentType(MediaType.APPLICATION_JSON)
            .body(
                objectMapper.writeValueAsString(
                    mapOf("grant_type" to "client_credentials", "appkey" to properties.appkey, "secretkey" to properties.appsecret),
                ),
            )
            .exchange { _, res ->
                val n = objectMapper.readTree(res.body.readAllBytes())
                if (!res.statusCode.is2xxSuccessful || !n.hasNonNull("approval_key")) {
                    throw AuthError(res.statusCode.value(), n.path("error_code").asText(null), "KIS 웹소켓 접속키 발급 실패: ${n.path("error_description").asText("")}")
                }
                n
            }!!
        return node.path("approval_key").asText()
    }

    /** 토큰 발급 (`POST /oauth2/tokenP`) — 1분당 1회 제한, 초과 시 403 + `EGW00133` → [RateLimitError] */
    private fun issueToken(clock: Clock): Pair<String, Instant> = usage.measure("auth") {
        limiter.throttle()
        val node = restClient.post()
            .uri("/oauth2/tokenP")
            .contentType(MediaType.APPLICATION_JSON)
            .body(
                objectMapper.writeValueAsString(
                    mapOf("grant_type" to "client_credentials", "appkey" to properties.appkey, "appsecret" to properties.appsecret),
                ),
            )
            .exchange { _, res ->
                val n = runCatching { objectMapper.readTree(res.body.readAllBytes()) }.getOrNull() ?: objectMapper.createObjectNode()
                if (!res.statusCode.is2xxSuccessful || !n.hasNonNull("access_token")) {
                    val status = res.statusCode.value()
                    val code = n.path("error_code").asText(null)
                    val detail = n.path("error_description").asText("")
                    throw if (code == "EGW00133" || status == 429) {
                        RateLimitError(status, code, "KIS 토큰 발급 유량 초과($code): $detail (발급은 1분당 1회 제한)", BrokerTokenManager.FAILURE_COOLDOWN_SECONDS)
                    } else {
                        AuthError(status, code, "KIS 토큰 발급 실패($code): $detail")
                    }
                }
                n
            }

        val expiresIn = node.path("expires_in").asLong(86400)
        logger.info { "KIS token issued / expiresIn=${expiresIn}s" }
        node.path("access_token").asText() to clock.instant().plusSeconds(expiresIn)
    }

    private fun JsonNode.decimal(field: String): BigDecimal =
        path(field).asText("0").ifBlank { "0" }.toBigDecimal()

    private fun JsonNode.decimalOrNull(field: String): BigDecimal? =
        path(field).asText("").takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

    private fun JsonNode.firstOrNull(): JsonNode? = if (isArray && size() > 0) get(0) else null

    companion object {
        private val KST: ZoneId = KrxCalendar.KST
        private val DATE: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

        /** 업무 호출이 토큰을 거부 — EGW00121 유효하지 않은 token, EGW00123 기간이 만료된 token (HTTP 500 으로 온다) */
        private val TOKEN_REJECTED_CODES = setOf("EGW00121", "EGW00123")
    }
}
