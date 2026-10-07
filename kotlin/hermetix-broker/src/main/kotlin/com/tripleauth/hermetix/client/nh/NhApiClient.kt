package com.tripleauth.hermetix.client.nh

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.broker.BrokerApiException
import com.tripleauth.hermetix.broker.BrokerCapabilities
import com.tripleauth.hermetix.broker.MarketStream
import com.tripleauth.hermetix.broker.StreamChannel
import com.tripleauth.hermetix.broker.StreamingBrokerClient
import com.tripleauth.hermetix.broker.InsufficientFundsError
import com.tripleauth.hermetix.broker.InvalidOrderError
import com.tripleauth.hermetix.broker.KrxCalendar
import com.tripleauth.hermetix.broker.KrxTick
import com.tripleauth.hermetix.broker.OrderNotFoundError
import com.tripleauth.hermetix.broker.RateLimitError
import com.tripleauth.hermetix.broker.RateLimiter
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
import org.springframework.http.MediaType
import org.springframework.web.client.RestClient
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * NH투자증권 **NH PLUG** REST OpenAPI 어댑터. `hermetix.broker: nh` 로 활성화한다.
 *
 * ⚠️ **문서 기반 구현 (실측 전)** — 공식 Python SDK(`PLUG-OpenAPI/nhplug-sdk`, MIT)와 포털 OpenAPI 문서(2026-09-08 개정)에서
 * 엔드포인트·필드명·에러 코드를 역추적해 만들었다. 모의서버 실측으로 검증되기 전까지 상태는 "미검증" 이며,
 * 컨포먼스 픽스처(`conformance/fixtures/nh.json`)도 문서에서 재구성한 값이다.
 *
 * 규약 (SDK 기준):
 * - 모든 API 는 `POST`, 본문 `{"Input_0": {...}}`, 응답 `rsp_cd`/`rsp_msg` + `Output_0`(+`Output_1`). TR 헤더 없이 URL 경로로 식별
 * - 인증 헤더 `authorization: Bearer` + `x-client-id`/`x-client-secret` (포털은 Bearer 만 필요하다지만 SDK 는 셋 다 보낸다)
 * - 토큰은 운영 호스트 `/oauth2/token` 에서 **쿼리스트링** 파라미터로 발급 (24h), 모의·운영 양쪽에 사용
 * - HTTP 200 이어도 업무 오류가 올 수 있다 — `rsp_cd` ∈ {00000, 00166, 00221, 13578} 또는 `rsp_msg` 에 "완료" 면 성공 (SDK 판정식)
 * - 유량 제한 초당 5회 → 250ms 쓰로틀 + 429(`IGW4290x`, `Retry-After`) 재시도
 *
 * 문서로 확정하지 못한 점 (실측 필요):
 * - 주문 응답의 시장주문번호(`mkt_orr_no`)와 체결 조회의 통합주문번호(`itg_orr_no`)가 같은 값인지 → 같다고 가정하고 주문 ID 로 쓴다
 * - 체결구분(`ost_cns_dit`) 코드 의미가 문서마다 반대 → `0`(전체) 로 조회해 `ny_cns_qty` 로 미체결을 가른다
 * - 등락률(`prdy_ctrt`)·수익률(`pft_rt`) 단위는 % 로 추정 → 비율로 변환
 * - 응답 숫자가 문자열/숫자 중 무엇인지 → 양쪽 허용 파서
 */
class NhApiClient internal constructor(
    private val properties: NhApiProperties,
    private val objectMapper: ObjectMapper,
    clock: Clock,
    sleeper: (Long) -> Unit,
) : StreamingBrokerClient {

    constructor(properties: NhApiProperties, objectMapper: ObjectMapper) : this(properties, objectMapper, Clock.systemUTC(), Thread::sleep)

    private val logger = KotlinLogging.logger { }

    override val capabilities = BrokerCapabilities(
        brokerId = "nh",
        market = "KRX",
        currency = "KRW",
        candleIntervals = setOf(CandleInterval.DAY_1),
        clientOrderId = false,
        nativeBracket = false,
        fractionalShares = false,
        serverOpenOrders = true, // dailyOrderExecution 으로 당일 미체결 조회
        environments = setOf(TradingEnvironment.PAPER, TradingEnvironment.LIVE),
        // 웹소켓 체결(oc/nc/mc)·호가(ob/nb/mb)·주문 통보(d2/d3) — 문서 기반, 실측 전
        streams = setOf(StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS),
    )

    override val environment: TradingEnvironment = properties.environment


    private val usage = UsageTelemetry.forBroker(capabilities.brokerId, environment)

    /** 실시간 스트림 — 토큰은 REST 와 같은 캐시를 쓴다 (운영 호스트 발급, 모의·운영 공용) */
    override fun openStream(): MarketStream = NhMarketStream(properties, objectMapper, ::token, usage)

    private val restClient = RestClient.builder().baseUrl(properties.resolvedBaseUrl()).build()
    private val authClient = RestClient.builder().baseUrl(properties.authUrl).build()

    private val limiter = RateLimiter(
        minIntervalMillis = properties.throttleMillis,
        maxRetries = 3,
        backoffMillis = { attempt -> 1000L * attempt },
        sleeper = sleeper,
    )

    /** 토큰 수명주기 — 메모리 → 파일 캐시 → 발급, 발급 실패 60초 쿨다운, 거부 토큰 폐기 후 1회 재시도 */
    private val tokens = BrokerTokenManager("nh", properties.appKey, properties.tokenRefreshMarginSeconds, clock) { issueToken(clock) }

    @Volatile
    private var resolvedAccountNo: String = properties.accountNo

    /** 계좌 조회 전용 잠금 — 토큰 잠금과 분리 (계좌 조회 호출이 토큰 발급을 거친다) */
    private val accountLock = Any()

    // ------------------------------------------------------------------ market

    override fun getQuotes(symbols: List<String>): QuotesResponse = usage.measure("quotes") {
        val quotes = symbols.map { symbol ->
            val out = call("/krstock/quote/v1/currentPrice", mapOf("market_cd" to properties.marketCd, "iem_cd" to capabilities.symbolCode(symbol)))
                .path("Output_0")
            val sign = out.path("prdy_vrss_sign").asText("")
            val change = out.decimalOrNull("prdy_vrss")?.let { if (sign in FALLING_SIGNS && it.signum() > 0) it.negate() else it }
            val rate = out.decimalOrNull("prdy_ctrt")?.let { if (sign in FALLING_SIGNS && it.signum() > 0) it.negate() else it }
            Quote(
                symbol = symbol,
                price = out.decimal("stck_prpr"),
                bidPrice = out.decimalOrNull("bidp")?.takeIf { it.signum() > 0 },
                askPrice = out.decimalOrNull("askp")?.takeIf { it.signum() > 0 },
                volume = out.decimalOrNull("acml_vol")?.toLong() ?: 0L,
                change = change,
                changeRate = rate?.percentToRate(),
                timestamp = Instant.now(),
            )
        }
        return QuotesResponse(quotes)
    }

    override fun getCandles(symbol: String, interval: CandleInterval, limit: Int?): CandlesResponse = usage.measure("candles") {
        require(interval == CandleInterval.DAY_1) { "NH 어댑터는 일봉(DAY_1)만 지원합니다." }
        val count = limit ?: 30
        val rows = call(
            "/krstock/quote/v1/currentDaily",
            mapOf("market_cd" to properties.marketCd, "iem_cd" to capabilities.symbolCode(symbol), "array_cnt" to count.toString()),
        ).path("Output_0")

        // 문서상 최신일 우선(내림차순) → 공통 모델은 과거→최신
        val candles = rows.mapNotNull { row ->
            val date = parseDate(row.path("bsop_date").asText("")) ?: return@mapNotNull null
            Candle(
                timestamp = date.atStartOfDay(KST).toInstant(),
                open = row.decimal("stck_oprc"),
                high = row.decimal("stck_hgpr"),
                low = row.decimal("stck_lwpr"),
                close = row.decimal("stck_clpr"),
                volume = row.decimalOrNull("acml_vol")?.toLong() ?: 0L,
            )
        }.sortedBy { it.timestamp }.takeLast(count)

        return CandlesResponse(symbol = symbol, interval = interval.value, candles = candles)
    }

    override fun getCalendar(): CalendarResponse = usage.measure("calendar") { KrxCalendar.synthesize() }

    // ----------------------------------------------------------------- account

    override fun getAccount(): AccountResponse = usage.measure("account") {
        val summary = balance().path("Output_0")
        val cash = summary.decimal("dca")
        val portfolio = summary.decimalOrNull("tot_aet_amt")?.takeIf { it.signum() > 0 }
            ?: (cash + (summary.decimalOrNull("tot_eal_amt") ?: BigDecimal.ZERO))
        return AccountResponse(
            accountId = accountNo(),
            name = null,
            currency = "KRW",
            cash = cash,
            portfolioValue = portfolio,
            status = "ACTIVE",
        )
    }

    override fun getHoldings(): HoldingsResponse = usage.measure("holdings") {
        val holdings = balance().path("Output_1").mapNotNull { row ->
            val quantity = row.decimalOrNull("itg_bnc_qty") ?: return@mapNotNull null
            if (quantity.signum() <= 0) return@mapNotNull null
            Holding(
                symbol = normalizeCode(row.path("iem_cd").asText()),
                quantity = quantity,
                avgEntryPrice = row.decimalOrNull("phs_pr") ?: BigDecimal.ZERO,
                currentPrice = row.decimalOrNull("now_pr"),
                marketValue = row.decimalOrNull("eal_amt"),
                unrealizedPnl = row.decimalOrNull("eal_pls_amt"),
                unrealizedPnlRate = row.decimalOrNull("pft_rt")?.percentToRate(),
            )
        }
        return HoldingsResponse(holdings)
    }

    override fun getBuyingPower(): BuyingPowerResponse = usage.measure("buying_power") {
        val summary = balance().path("Output_0")
        return BuyingPowerResponse(
            accountId = accountNo(),
            currency = "KRW",
            buyingPower = summary.decimalOrNull("orr_pbl_amt") ?: summary.decimal("dca"),
        )
    }

    // ------------------------------------------------------------------ orders

    override fun createOrder(request: CreateOrderRequest): OrderResponse = usage.measure("create_order") {
        require(request.orderType == OrderType.LIMIT || request.orderType == OrderType.MARKET) { "NH 어댑터는 LIMIT/MARKET 주문만 지원합니다" }
        val code = capabilities.symbolCode(request.symbol)
        val path = if (request.side == OrderSide.BUY) "/krstock/order/v1/cashBuy" else "/krstock/order/v1/cashSell"
        val input = mutableMapOf<String, Any>(
            "act_no" to accountNo(),
            "iem_cd" to code,
            "orr_qty" to request.quantity.toPlainString().toLong(),
            "nmn_pr_tp_cd" to if (request.orderType == OrderType.LIMIT) "01" else "05",
            "orr_cnd_dit_cd" to "00",
            "ssl_nmn_pr_dit_cd" to "00",
            "rmt_mkt_cd" to properties.orderMarketCd,
            "sor_mkt_sli_yn" to "N",
        )
        if (request.orderType == OrderType.LIMIT) {
            // KRX 호가단위 보정 — 맞지 않는 지정가는 거래소가 거부한다
            input["orr_pr"] = KrxTick.round(request.limitPrice!!).toPlainString().toLong()
        }

        val out = call(path, input).path("Output_0")
        val orderId = out.path("mkt_orr_no").asText("").trim()
        if (orderId.isBlank()) throw BrokerApiException(200, null, "NH 주문 응답에 mkt_orr_no 가 없습니다")

        return OrderResponse(
            orderId = orderId,
            clientOrderId = request.clientOrderId, // NH 미지원 — 반환만 유지
            status = OrderStatus.SUBMITTED,
            symbol = code,
            side = request.side,
            orderType = request.orderType,
            quantity = request.quantity,
            limitPrice = request.limitPrice,
            filledQuantity = BigDecimal.ZERO,
            submittedAt = Instant.now(),
        )
    }

    override fun getOrders(): OrdersResponse = usage.measure("get_orders") {
        
        OrdersResponse(executionRows().map { it.toOrderResponse() }.filter { it.status.isOpen })
    }

    override fun getOrder(orderId: String): OrderResponse = usage.measure("get_order") {
        
        executionRows().firstOrNull { sameOrderNo(it.path("itg_orr_no").asText(), orderId) }?.toOrderResponse()
            ?: OrderResponse(orderId = orderId, status = OrderStatus.CANCELED) // 당일 조회에 없으면 종료된 것으로 간주
    }

    override fun cancelOrder(orderId: String): OrderResponse = usage.measure("cancel_order") {
        val row = executionRows().firstOrNull { sameOrderNo(it.path("itg_orr_no").asText(), orderId) }
            ?: throw OrderNotFoundError("order-not-found", "NH 당일 주문에서 찾을 수 없습니다: $orderId")
        // 취소는 원시장주문번호(org_mkt_orr_no)와 종목코드가 필요하다
        call(
            "/krstock/order/v1/cancel",
            mapOf(
                "act_no" to accountNo(),
                "org_mkt_orr_no" to orderId.trimStart('0').ifBlank { "0" }.toLong(),
                "all_pat_dit_cd" to "1", // 전량
                "iem_cd" to normalizeCode(row.path("iem_cd").asText()),
            ),
        )
        return OrderResponse(orderId = orderId, status = OrderStatus.CANCELED, canceledAt = Instant.now())
    }

    override fun getFills(): FillsResponse = usage.measure("fills") {
        val fills = executionRows()
            .filter { (it.decimalOrNull("tot_cns_qty") ?: BigDecimal.ZERO).signum() > 0 }
            .map { row ->
                Fill(
                    fillId = null, // 체결 단위 ID 없음 — 주문 단위 집계
                    orderId = row.path("itg_orr_no").asText().trim(),
                    symbol = normalizeCode(row.path("iem_cd").asText()),
                    side = sideOf(row),
                    quantity = row.decimalOrNull("tot_cns_qty"),
                    price = row.decimalOrNull("cns_avg_uit_pr"),
                    amount = row.decimalOrNull("cns_amt"),
                    timestamp = null,
                )
            }
        return FillsResponse(fills)
    }

    // ---------------------------------------------------------------- internal

    private fun balance(): JsonNode = call(
        "/krstock/inquiry/v1/balance",
        mapOf("act_no" to accountNo(), "bnc_bse_cd" to "5", "ltg_aot_dit_cd" to "9", "aet_bse" to "2", "qut_dit_cd" to properties.marketCd),
    )

    /** 당일 주문·체결 전체 (`ost_cns_dit=0`) — 문서마다 체결구분 코드 의미가 달라 전체를 받아 `ny_cns_qty` 로 가른다 */
    private fun executionRows(): List<JsonNode> = call(
        "/krstock/inquiry/v1/dailyOrderExecution",
        mapOf("orr_dt" to LocalDate.now(KST).format(DATE), "act_no" to accountNo(), "ost_cns_dit" to "0", "orr_mkt_cd" to "00"),
    ).path("Output_1").toList()

    private fun JsonNode.toOrderResponse(): OrderResponse {
        val ordQty = decimalOrNull("orr_qty") ?: BigDecimal.ZERO
        val filled = decimalOrNull("tot_cns_qty") ?: BigDecimal.ZERO
        val remaining = decimalOrNull("ny_cns_qty") ?: (ordQty - filled)
        val canceled = decimalOrNull("can_qty") ?: BigDecimal.ZERO
        val status = when {
            remaining.signum() > 0 && filled.signum() > 0 -> OrderStatus.PARTIALLY_FILLED
            remaining.signum() > 0 -> OrderStatus.SUBMITTED
            filled.signum() > 0 && filled >= ordQty -> OrderStatus.FILLED
            filled.signum() > 0 -> OrderStatus.PARTIALLY_FILLED
            canceled.signum() > 0 -> OrderStatus.CANCELED
            path("orr_rjt_rsn_cd_nm").asText("").isNotBlank() -> OrderStatus.REJECTED
            else -> OrderStatus.CANCELED
        }
        return OrderResponse(
            orderId = path("itg_orr_no").asText().trim(),
            status = status,
            symbol = normalizeCode(path("iem_cd").asText()),
            side = sideOf(this),
            orderType = if (path("nmn_pr_tp_cd_nm").asText("").contains("시장가")) OrderType.MARKET else OrderType.LIMIT,
            quantity = ordQty,
            limitPrice = decimalOrNull("orr_pr")?.takeIf { it.signum() > 0 },
            filledQuantity = filled,
            avgFillPrice = decimalOrNull("cns_avg_uit_pr")?.takeIf { it.signum() > 0 },
            rejectReason = path("orr_rjt_rsn_cd_nm").asText("").ifBlank { null },
        )
    }

    private fun sideOf(row: JsonNode): OrderSide = if (row.path("sby_dit_cd_nm").asText("").contains("매수")) OrderSide.BUY else OrderSide.SELL

    private fun sameOrderNo(a: String, b: String): Boolean = a.trim().trimStart('0') == b.trim().trimStart('0')

    /** 계좌번호 — 설정이 비어 있으면 `/n2/acctinfo` 에서 환경에 맞는 acct_type 의 첫 계좌를 고른다 */
    private fun accountNo(): String {
        resolvedAccountNo.takeIf { it.isNotBlank() }?.let { return it }
        synchronized(accountLock) {
            resolvedAccountNo.takeIf { it.isNotBlank() }?.let { return it }
            val expected = properties.expectedAcctType()
            val accounts = call("/n2/acctinfo", emptyMap()).path("Output_0").toList()
            val picked = accounts.firstOrNull { it.path("acct_type").asText() == expected }
                ?: throw BrokerApiException(200, null, "NH 계좌 목록에 ${properties.environment} 용 계좌(acct_type=$expected)가 없습니다: ${accounts.map { it.path("acct_no").asText() + "/" + it.path("acct_type").asText() }}")
            resolvedAccountNo = picked.path("acct_no").asText()
            logger.info { "NH account resolved / acct_no=$resolvedAccountNo acct_type=$expected" }
            return resolvedAccountNo
        }
    }

    private fun call(path: String, input: Map<String, Any>): JsonNode =
        limiter.execute("NH $path") { tokens.call { token -> callOnce(path, input, token) } }

    private fun callOnce(path: String, input: Map<String, Any>, token: String): JsonNode {
        val response = restClient.post()
            .uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { headers ->
                headers.set("authorization", "Bearer $token")
                headers.set("x-client-id", properties.appKey)
                headers.set("x-client-secret", properties.appSecret)
            }
            .body(objectMapper.writeValueAsString(mapOf("Input_0" to input)))
            .exchange { _, res ->
                val bytes = res.body.readAllBytes()
                val node = runCatching { objectMapper.readTree(bytes) }.getOrNull() ?: objectMapper.createObjectNode()
                val status = res.statusCode.value()
                val rspCd = node.path("rsp_cd").asText("")
                val rspMsg = node.path("rsp_msg").asText("")
                val gwCode = node.path("code").asText(node.path("error").path("code").asText(null)) ?: rspCd
                val msg = "NH($path) [${gwCode.ifBlank { rspCd }}] ${rspMsg.ifBlank { node.path("message").asText("") }}".trim()
                if (!res.statusCode.is2xxSuccessful) {
                    val retryAfter = res.headers.getFirst("Retry-After")?.trim()?.toLongOrNull()
                    throw when {
                        status == 429 || gwCode.startsWith("IGW429") -> RateLimitError(status, gwCode, msg, retryAfter)
                        status == 401 || gwCode.startsWith("IGW4004") || gwCode.startsWith("IGW4003") || gwCode == "IGW40051" -> AuthError(status, gwCode, msg)
                        status == 400 -> InvalidOrderError(status, gwCode, msg)
                        else -> BrokerApiException(status, gwCode, msg)
                    }
                }
                // HTTP 200 이어도 업무 오류가 올 수 있다 — SDK 판정식
                val ok = rspCd in SUCCESS_CODES || rspMsg.contains("완료")
                if (!ok) {
                    throw when {
                        rspMsg.contains("부족") -> InsufficientFundsError(status, rspCd, msg)
                        else -> BrokerApiException(status, rspCd, msg)
                    }
                }
                node
            }!!
        return response
    }

    internal fun token(): String = tokens.get()

    /** 토큰 발급 — 유량 초과(429·`IGW429*`)는 [RateLimitError] */
    private fun issueToken(clock: Clock): Pair<String, Instant> = usage.measure("auth") {
        limiter.throttle()
        // SDK 규약: 파라미터는 쿼리스트링, 본문 없음, content-type 은 form-urlencoded
        val node = authClient.post()
            .uri { b ->
                b.path("/oauth2/token")
                    .queryParam("appkey", properties.appKey)
                    .queryParam("appsecretkey", properties.appSecret)
                    .queryParam("grant_type", "client_credentials")
                    .queryParam("scope", "oob")
                    .build()
            }
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .exchange { _, res ->
                val n = runCatching { objectMapper.readTree(res.body.readAllBytes()) }.getOrNull() ?: objectMapper.createObjectNode()
                if (!res.statusCode.is2xxSuccessful || !n.hasNonNull("access_token")) {
                    val status = res.statusCode.value()
                    val code = n.path("code").asText(n.path("rsp_cd").asText("")).trim()
                    val msg = "NH 토큰 발급 실패($code): ${n.path("message").asText(n.path("rsp_msg").asText(""))}"
                    throw if (status == 429 || code.startsWith("IGW429")) {
                        RateLimitError(status, code, msg, BrokerTokenManager.FAILURE_COOLDOWN_SECONDS)
                    } else {
                        AuthError(status, code.ifBlank { null }, msg)
                    }
                }
                n
            }

        val expiresIn = node.path("expires_in").asLong(86400)
        logger.info { "NH token issued / expiresIn=${expiresIn}s" }
        node.path("access_token").asText() to clock.instant().plusSeconds(expiresIn)
    }

    private fun BigDecimal.percentToRate(): BigDecimal = divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN)

    /** 숫자/문자열 어느 쪽으로 와도 파싱 — 문서상 와이어 타입이 API 마다 다르다 */
    private fun JsonNode.decimal(field: String): BigDecimal = decimalOrNull(field) ?: BigDecimal.ZERO

    private fun JsonNode.decimalOrNull(field: String): BigDecimal? =
        path(field).takeIf { !it.isMissingNode && !it.isNull }?.asText("")?.trim()?.replace(",", "")?.takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

    private fun parseDate(raw: String): LocalDate? {
        val text = raw.trim()
        return when {
            text.length == 8 && text.all { it.isDigit() } -> runCatching { LocalDate.parse(text, DATE) }.getOrNull()
            text.length == 10 && text[4] == '-' -> runCatching { LocalDate.parse(text) }.getOrNull()
            text.length == 8 && text[2] == '/' -> runCatching { LocalDate.parse("20$text", DateTimeFormatter.ofPattern("yyyy/MM/dd")) }.getOrNull() // 문서 표기 "YY/MM/DD"
            else -> null
        }
    }

    companion object {
        private val KST = KrxCalendar.KST
        private val DATE: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        /** SDK 가 성공으로 보는 rsp_cd — 00166(잔고) 00221(주문가능) 13578(조회 내역 없음) */
        private val SUCCESS_CODES = setOf("00000", "00166", "00221", "13578", "00165", "00218")
        /** prdy_vrss_sign 하락 계열 — 4/8 하한, 5/9 하락 */
        internal val FALLING_SIGNS = setOf("4", "5", "8", "9")

        /** 계좌·주문 API 의 iem_cd 는 길이 12(선행 0)일 수 있고 A 접두가 붙을 수 있다 → 6자리 코드로 정규화 */
        fun normalizeCode(raw: String): String {
            val text = raw.trim().removePrefix("A")
            return if (text.length > 6 && text.all { it.isDigit() }) text.takeLast(6) else text
        }
    }
}
