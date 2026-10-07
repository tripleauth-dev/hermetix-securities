package com.tripleauth.hermetix.client.db

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
import com.tripleauth.hermetix.broker.MarketClosedError
import com.tripleauth.hermetix.broker.OrderNotFoundError
import com.tripleauth.hermetix.broker.RateLimitError
import com.tripleauth.hermetix.broker.RateLimiter
import com.tripleauth.hermetix.broker.TradingEnvironment
import com.tripleauth.hermetix.broker.UsageTelemetry
import com.tripleauth.hermetix.broker.symbolCode
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
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClient
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * **DB증권** REST OpenAPI 어댑터. `hermetix.broker: db` 로 활성화한다.
 *
 * ⚠️ **문서 기반 구현 (실측 전)** — 공식 SDK(`DBsecurities/dbsec-open-api`)의 소스·예제 docstring 과 포털 문서에서
 * 엔드포인트·필드명·에러 코드를 역추적해 만들었다. 모의서버 실측으로 검증되기 전까지 상태는 "미검증" 이며,
 * 컨포먼스 픽스처(`conformance/fixtures/db.json`)도 문서에서 재구성한 값이다.
 *
 * 규약 (SDK 기준):
 * - 모든 API 는 `POST`, 본문 `{"In": {...}}`, 응답 `rsp_cd`/`rsp_msg` + `Out`(객체 또는 배열) / `Out1`. TR 코드는 문서용이고 경로로 식별
 * - 헤더 `authorization: Bearer` + `cont_yn`/`cont_key`(연속조회) + 법인만 `mac_address`. appkey 헤더 없음
 * - 토큰 `POST /oauth2/token` 은 form-urlencoded(`appsecretkey`), 24h, 발급 1분 1건(초과 시 403 + `IGW00201` → [RateLimitError]).
 *   만료 전 재발급은 같은 토큰을 돌려준다. 발급 토큰은 [DbTokenCache] 파일로 프로세스 간 재사용하고, 발급 실패 후 60초는 서버에 다시 묻지 않는다
 * - 운영/모의 같은 호스트 — 모의 키로만 분기. **HTTP 200 + `rsp_cd != 00000`** 이 업무 오류이며 이때 `Out` 이 없다
 * - 유량: 앱 20 TPS 이지만 잔고·체결 2 TPS, 예수금 1 TPS → 500ms 쓰로틀 + `IGW00201` 지수 백오프
 *
 * 문서로 확정하지 못한 점 (실측 필요): 응답 숫자의 JSON 타입(문자열/숫자 — 양쪽 허용), `IsuNo` 의 `A` 접두 여부,
 * 일봉 정렬(최신일 우선 추정), `PrdyVrss` 부호 포함 여부, 체결 조회의 미체결 잔량 필드(`MrcAbleQty` 우선)
 */
class DbApiClient internal constructor(
    private val properties: DbApiProperties,
    private val objectMapper: ObjectMapper,
    private val clock: Clock,
    sleeper: (Long) -> Unit,
) : StreamingBrokerClient {

    constructor(properties: DbApiProperties, objectMapper: ObjectMapper) : this(properties, objectMapper, Clock.systemUTC(), Thread::sleep)

    private val logger = KotlinLogging.logger { }

    override val capabilities = BrokerCapabilities(
        brokerId = "db",
        market = "KRX",
        currency = "KRW",
        candleIntervals = setOf(CandleInterval.DAY_1),
        clientOrderId = false,
        nativeBracket = false,
        fractionalShares = false,
        serverOpenOrders = true, // transaction-history 로 당일 미체결 조회
        environments = setOf(TradingEnvironment.PAPER, TradingEnvironment.LIVE),
        // 웹소켓 S00 체결·S01 호가·IS0/IS1 주문 통보 — 문서 기반, 실측 전
        streams = setOf(StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS),
    )

    override val environment: TradingEnvironment = properties.environment


    private val usage = UsageTelemetry.forBroker(capabilities.brokerId, environment)

    private val restClient = RestClient.builder().baseUrl(properties.baseUrl).build()

    private val limiter = RateLimiter(
        minIntervalMillis = properties.throttleMillis,
        maxRetries = 4,
        backoffMillis = { attempt -> 1000L shl (attempt - 1) }, // 1s → 2s → 4s → 8s (SDK 와 동일)
        sleeper = sleeper,
    )

    @Volatile
    private var cachedToken: Pair<String, Instant>? = null

    /** 마지막 발급 실패 (쿨다운 종료 시각, 오류) — 쿨다운 동안은 서버에 묻지 않고 같은 오류를 돌려준다 */
    private var tokenFailure: Pair<Instant, BrokerApiException>? = null

    // ------------------------------------------------------------------ market

    override fun getQuotes(symbols: List<String>): QuotesResponse = usage.measure("quotes") {
        val quotes = symbols.map { symbol ->
            val out = call(
                "/api/v1/quote/kr-stock/inquiry/price",
                mapOf("InputCondMrktDivCode" to properties.marketDivCode, "InputIscd1" to capabilities.symbolCode(symbol)),
            ).path("Out")
            Quote(
                symbol = symbol,
                price = out.decimal("Prpr"),
                bidPrice = out.decimalOrNull("Bidp1")?.takeIf { it.signum() > 0 },
                askPrice = out.decimalOrNull("Askp1")?.takeIf { it.signum() > 0 },
                volume = out.decimalOrNull("AcmlVol")?.toLong() ?: 0L,
                change = out.decimalOrNull("PrdyVrss"),
                changeRate = out.decimalOrNull("PrdyCtrt")?.percentToRate(),
                timestamp = Instant.now(),
            )
        }
        return QuotesResponse(quotes)
    }

    override fun getCandles(symbol: String, interval: CandleInterval, limit: Int?): CandlesResponse = usage.measure("candles") {
        require(interval == CandleInterval.DAY_1) { "DB 어댑터는 일봉(DAY_1)만 지원합니다." }
        val count = limit ?: 30
        val today = LocalDate.now(KST)
        // 휴장일 감안해 여유 있게 조회 (달력일 기준 약 1.6배)
        val start = today.minusDays((count * 16L / 10) + 10)

        val rows = call(
            "/api/v1/quote/kr-chart/day",
            mapOf(
                "InputOrgAdjPrc" to "1", "InputCondMrktDivCode" to properties.marketDivCode,
                "InputIscd1" to capabilities.symbolCode(symbol),
                "InputDate1" to start.format(DATE), "InputDate2" to today.format(DATE),
            ),
        ).path("Out")

        // 최신일 우선(추정) → 공통 모델은 과거→최신
        val candles = rows.mapNotNull { row ->
            val date = row.path("Date").asText("").trim()
            if (date.length != 8) return@mapNotNull null
            Candle(
                timestamp = LocalDate.parse(date, DATE).atStartOfDay(KST).toInstant(),
                open = row.decimal("Oprc"),
                high = row.decimal("Hprc"),
                low = row.decimal("Lprc"),
                close = row.decimal("Prpr"),
                // 2024 샘플에서는 AcmlVol, 2026 SDK 문서는 CntgVol — 둘 다 허용
                volume = (row.decimalOrNull("CntgVol") ?: row.decimalOrNull("AcmlVol"))?.toLong() ?: 0L,
            )
        }.sortedBy { it.timestamp }.takeLast(count)

        return CandlesResponse(symbol = symbol, interval = interval.value, candles = candles)
    }

    override fun getCalendar(): CalendarResponse = usage.measure("calendar") { KrxCalendar.synthesize() }

    // ----------------------------------------------------------------- account

    override fun getAccount(): AccountResponse = usage.measure("account") {
        val out = balance().path("Out")
        val portfolio = out.decimal("DpsastAmt") // 예탁자산 = 현금 + 평가
        val cash = out.decimalOrNull("Dps2") ?: (portfolio - (out.decimalOrNull("TotEvalAmt") ?: BigDecimal.ZERO))
        return AccountResponse(
            accountId = "db-${properties.environment.name.lowercase()}", // 계좌번호 필드 없음 — 앱키에 계좌 귀속
            name = null,
            currency = "KRW",
            cash = cash,
            portfolioValue = portfolio,
            status = "ACTIVE",
        )
    }

    override fun getHoldings(): HoldingsResponse = usage.measure("holdings") {
        val holdings = balance().path("Out1").mapNotNull { row ->
            val quantity = row.decimalOrNull("BalQty0") ?: row.decimalOrNull("BalQty") ?: return@mapNotNull null
            if (quantity.signum() <= 0) return@mapNotNull null
            Holding(
                symbol = normalizeCode(row.path("IsuNo").asText()),
                quantity = quantity,
                avgEntryPrice = row.decimalOrNull("ExecPrc") ?: BigDecimal.ZERO,
                currentPrice = row.decimalOrNull("NowPrc"),
                marketValue = row.decimalOrNull("EvalAmt"),
                unrealizedPnl = row.decimalOrNull("EvalPnlAmt"),
                unrealizedPnlRate = row.decimalOrNull("Ernrat")?.percentToRate(),
            )
        }
        return HoldingsResponse(holdings)
    }

    override fun getBuyingPower(): BuyingPowerResponse = usage.measure("buying_power") {
        // 종목 무관 예수금잔고 (예수금 API 는 1 TPS — 틱당 1회만 호출된다)
        val out = call("/api/v1/trading/kr-stock/inquiry/acnt-deposit", emptyMap()).path("Out1")
        return BuyingPowerResponse(
            accountId = "db-${properties.environment.name.lowercase()}",
            currency = "KRW",
            buyingPower = out.decimalOrNull("DpsBalAmt") ?: out.decimal("WthdwAbleAmt"),
        )
    }

    // ------------------------------------------------------------------ orders

    override fun createOrder(request: CreateOrderRequest): OrderResponse = usage.measure("create_order") {
        require(request.orderType == OrderType.LIMIT || request.orderType == OrderType.MARKET) { "DB 어댑터는 LIMIT/MARKET 주문만 지원합니다" }
        val code = capabilities.symbolCode(request.symbol)
        val out = call(
            "/api/v1/trading/kr-stock/order",
            mapOf(
                "IsuNo" to code,
                "TrchNo" to 1,
                "OrdQty" to request.quantity.toPlainString().toLong(),
                // KRX 호가단위 보정 — 맞지 않는 지정가는 거래소가 거부한다(2706)
                "OrdPrc" to if (request.orderType == OrderType.LIMIT) KrxTick.round(request.limitPrice!!).toPlainString().toLong() else 0L,
                "BnsTpCode" to if (request.side == OrderSide.BUY) "2" else "1",
                "OrdprcPtnCode" to if (request.orderType == OrderType.LIMIT) "00" else "03",
                "MgntrnCode" to "000",
                "LoanDt" to "00000000",
                "OrdCndiTpCode" to "0",
            ),
        ).path("Out")
        val orderId = out.path("OrdNo").asText("").trim()
        if (orderId.isBlank()) throw BrokerApiException(200, null, "DB 주문 응답에 OrdNo 가 없습니다")

        return OrderResponse(
            orderId = orderId,
            clientOrderId = request.clientOrderId, // DB 미지원 — 반환만 유지
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
        
        OrdersResponse(historyRows().map { it.toOrderResponse() }.filter { it.status.isOpen })
    }

    override fun getOrder(orderId: String): OrderResponse = usage.measure("get_order") {
        
        historyRows().firstOrNull { sameOrderNo(it.path("OrdNo").asText(), orderId) }?.toOrderResponse()
            ?: OrderResponse(orderId = orderId, status = OrderStatus.CANCELED) // 당일 조회에 없으면 종료된 것으로 간주
    }

    override fun cancelOrder(orderId: String): OrderResponse = usage.measure("cancel_order") {
        val row = historyRows().firstOrNull { sameOrderNo(it.path("OrdNo").asText(), orderId) }
            ?: throw OrderNotFoundError("order-not-found", "DB 당일 주문에서 찾을 수 없습니다: $orderId")
        val remaining = row.remainingQty()
        // 취소는 원주문번호·종목·수량이 필요하다 (잔량 전량)
        val out = call(
            "/api/v1/trading/kr-stock/order-cancel",
            mapOf(
                "OrgOrdNo" to orderId.trimStart('0').ifBlank { "0" }.toLong(),
                "IsuNo" to normalizeCode(row.path("IsuNo").asText()),
                "OrdQty" to remaining.toPlainString().toLong(),
            ),
        ).path("Out")
        logger.debug { "DB cancel accepted / orgOrdNo=$orderId cancelOrdNo=${out.path("OrdNo").asText()}" }
        return OrderResponse(orderId = orderId, status = OrderStatus.PENDING_CANCEL, canceledAt = Instant.now())
    }

    override fun getFills(): FillsResponse = usage.measure("fills") {
        val fills = historyRows()
            .filter { (it.decimalOrNull("AllExecQty") ?: BigDecimal.ZERO).signum() > 0 }
            .map { row ->
                val qty = row.decimalOrNull("AllExecQty")
                val price = row.decimalOrNull("AvrExecPrc")
                Fill(
                    fillId = null, // 체결 단위 ID 없음 — 주문 단위 집계
                    orderId = row.path("OrdNo").asText().trim(),
                    symbol = normalizeCode(row.path("IsuNo").asText()),
                    side = sideOf(row),
                    quantity = qty,
                    price = price,
                    amount = if (qty != null && price != null) qty.multiply(price) else null,
                    timestamp = null,
                )
            }
        return FillsResponse(fills)
    }

    // ---------------------------------------------------------------- internal

    private fun balance(): JsonNode = call("/api/v1/trading/kr-stock/inquiry/balance", mapOf("QryTpCode0" to "0"))

    /** 당일 주문·체결 전체 (`ExecYn=0`) */
    private fun historyRows(): List<JsonNode> = call(
        "/api/v1/trading/kr-stock/inquiry/transaction-history",
        mapOf("SorTpYn" to "2", "ExecYn" to "0", "TrdMktCode" to "0", "BnsTpCode" to "0", "IsuTpCode" to "0", "QryTp" to "0"),
    ).path("Out1").toList()

    private fun JsonNode.remainingQty(): BigDecimal =
        decimalOrNull("MrcAbleQty")
            ?: ((decimalOrNull("OrdQty") ?: BigDecimal.ZERO) - (decimalOrNull("AllExecQty") ?: BigDecimal.ZERO) - (decimalOrNull("MrcQty") ?: BigDecimal.ZERO))

    private fun JsonNode.toOrderResponse(): OrderResponse {
        val ordQty = decimalOrNull("OrdQty") ?: BigDecimal.ZERO
        val filled = decimalOrNull("AllExecQty") ?: BigDecimal.ZERO
        val remaining = remainingQty()
        val trx = path("OrdTrxPtnCode").asText("")
        val status = when {
            trx == "9" -> OrderStatus.PENDING_CANCEL // 취소중
            trx == "8" -> OrderStatus.CANCELED       // 취소확인
            remaining.signum() > 0 && filled.signum() > 0 -> OrderStatus.PARTIALLY_FILLED
            remaining.signum() > 0 -> OrderStatus.SUBMITTED
            filled.signum() > 0 && filled >= ordQty -> OrderStatus.FILLED
            filled.signum() > 0 -> OrderStatus.PARTIALLY_FILLED
            else -> OrderStatus.CANCELED
        }
        return OrderResponse(
            orderId = path("OrdNo").asText().trim(),
            status = status,
            symbol = normalizeCode(path("IsuNo").asText()),
            side = sideOf(this),
            orderType = if (path("OrdprcPtnCode").asText("") == "03") OrderType.MARKET else OrderType.LIMIT,
            quantity = ordQty,
            limitPrice = decimalOrNull("OrdPrc")?.takeIf { it.signum() > 0 },
            filledQuantity = filled,
            avgFillPrice = decimalOrNull("AvrExecPrc")?.takeIf { it.signum() > 0 },
        )
    }

    /** BnsTpCode: 1 매도 / 2 매수 */
    private fun sideOf(row: JsonNode): OrderSide = if (row.path("BnsTpCode").asText("").trim() == "2") OrderSide.BUY else OrderSide.SELL

    private fun sameOrderNo(a: String, b: String): Boolean = a.trim().trimStart('0') == b.trim().trimStart('0')

    private fun call(path: String, input: Map<String, Any>): JsonNode = limiter.execute("DB $path") { callOnce(path, input) }

    private fun callOnce(path: String, input: Map<String, Any>): JsonNode =
        restClient.post()
            .uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { headers ->
                headers.set("authorization", "Bearer ${token()}")
                headers.set("cont_yn", "N")
                headers.set("cont_key", "")
                if (properties.macAddress.isNotBlank()) headers.set("mac_address", properties.macAddress)
            }
            .body(objectMapper.writeValueAsString(mapOf("In" to input)))
            .exchange { _, res ->
                val bytes = res.body.readAllBytes()
                val node = runCatching { objectMapper.readTree(bytes) }.getOrNull() ?: objectMapper.createObjectNode()
                val status = res.statusCode.value()
                val code = node.path("rsp_cd").asText("").trim()
                val msg = "DB($path) [$code] ${node.path("rsp_msg").asText("")}".trim()
                if (!res.statusCode.is2xxSuccessful || code != "00000") {
                    val retryAfter = res.headers.getFirst("Retry-After")?.trim()?.toLongOrNull()
                    throw when {
                        code == "IGW00201" || status == 429 -> RateLimitError(status, code, msg, retryAfter)
                        code in AUTH_CODES || status == 401 -> AuthError(status, code, msg)
                        code in MARKET_CLOSED_CODES -> MarketClosedError(status, code, msg)
                        code in INSUFFICIENT_CODES || node.path("rsp_msg").asText("").contains("부족") -> InsufficientFundsError(status, code, msg)
                        code in INVALID_ORDER_CODES -> InvalidOrderError(status, code, msg)
                        code in ORDER_NOT_FOUND_CODES -> OrderNotFoundError(code, msg)
                        else -> BrokerApiException(status, code, msg)
                    }
                }
                node
            }!!

    // ------------------------------------------------------------------ stream

    /** 웹소켓은 REST 접근토큰을 매 구독 메시지 헤더에 싣는다 — 재접속 시 [token] 이 갱신한다 */
    override fun openStream(): MarketStream = DbMarketStream(properties, objectMapper, ::token, usage)

    private fun token(): String {
        val cached = cachedToken
        if (cached != null && cached.second.isAfter(clock.instant().plusSeconds(properties.tokenRefreshMarginSeconds))) return cached.first
        return refreshToken()
    }

    @Synchronized
    private fun refreshToken(): String {
        val now = clock.instant()
        cachedToken?.let { if (it.second.isAfter(now.plusSeconds(properties.tokenRefreshMarginSeconds))) return it.first }
        DbTokenCache.load(objectMapper, properties.appKey, now, properties.tokenRefreshMarginSeconds)?.let {
            cachedToken = it
            return it.first
        }
        tokenFailure?.let { (until, error) -> if (now.isBefore(until)) throw cooldownError(error, Duration.between(now, until)) }

        val issued = try {
            issueToken()
        } catch (e: BrokerApiException) {
            tokenFailure = clock.instant().plusSeconds(TOKEN_FAILURE_COOLDOWN_SECONDS) to e
            throw e
        }
        tokenFailure = null
        cachedToken = issued
        DbTokenCache.save(objectMapper, properties.appKey, issued.first, issued.second)
        return issued.first
    }

    private fun issueToken(): Pair<String, Instant> = usage.measure("auth") {
        limiter.throttle()
        val form = LinkedMultiValueMap<String, String>().apply {
            add("grant_type", "client_credentials")
            add("appkey", properties.appKey)
            add("appsecretkey", properties.appSecret) // JSON 이나 appsecret 으로 보내면 IGW00133
            add("scope", "oob")
        }
        val node = restClient.post()
            .uri("/oauth2/token")
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .body(form)
            .exchange { _, res ->
                val n = runCatching { objectMapper.readTree(res.body.readAllBytes()) }.getOrNull() ?: objectMapper.createObjectNode()
                if (!res.statusCode.is2xxSuccessful || !n.hasNonNull("access_token")) {
                    val status = res.statusCode.value()
                    val code = n.path("rsp_cd").asText(n.path("error").asText(null))
                    val detail = n.path("rsp_msg").asText(n.path("error_description").asText(""))
                    throw if (code == "IGW00201" || status == 429) {
                        RateLimitError(status, code, "DB 토큰 발급 유량 초과($code): $detail (발급은 1분당 1회 제한)", TOKEN_FAILURE_COOLDOWN_SECONDS)
                    } else {
                        AuthError(status, code, "DB 토큰 발급 실패($code): $detail")
                    }
                }
                n
            }!!

        val expiresIn = node.path("expires_in").asLong(86400)
        logger.info { "DB token issued / expiresIn=${expiresIn}s" }
        node.path("access_token").asText() to clock.instant().plusSeconds(expiresIn)
    }

    /** 쿨다운 중 재요청 — 서버에 묻지 않고 같은 종류의 오류를 남은 시간(올림 초)과 함께 돌려준다 */
    private fun cooldownError(failure: BrokerApiException, remaining: Duration): BrokerApiException =
        if (failure is RateLimitError) {
            RateLimitError(failure.httpStatus, failure.errorCode, failure.message, (remaining.toMillis() + 999) / 1000)
        } else {
            failure
        }

    private fun BigDecimal.percentToRate(): BigDecimal = divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN)

    /** 숫자/문자열 어느 쪽으로 와도 파싱 — 문서상 와이어 타입이 API 마다 다르다 */
    private fun JsonNode.decimal(field: String): BigDecimal = decimalOrNull(field) ?: BigDecimal.ZERO

    private fun JsonNode.decimalOrNull(field: String): BigDecimal? =
        path(field).takeIf { !it.isMissingNode && !it.isNull }?.asText("")?.trim()?.replace(",", "")?.takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

    companion object {
        private val KST = KrxCalendar.KST
        private val DATE: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        private val AUTH_CODES = setOf("IGW00121", "IGW00122", "IGW00123", "IGW40342")
        private val MARKET_CLOSED_CODES = setOf("2611", "3589", "3590", "3563")
        private val INSUFFICIENT_CODES = setOf("1584", "2714", "2752", "M100")
        private val INVALID_ORDER_CODES = setOf("2706", "3180", "3181")
        private val ORDER_NOT_FOUND_CODES = setOf("3056", "3416")

        /** 토큰 발급은 1분 1건 (초과 시 HTTP 403 + IGW00201). 발급에 실패하면 이 시간 동안 서버에 다시 묻지 않는다 */
        const val TOKEN_FAILURE_COOLDOWN_SECONDS = 60L

        /** 계좌·주문계 `IsuNo` 는 `A005930` 형태일 수 있다 → 6자리 코드로 정규화 */
        fun normalizeCode(raw: String): String {
            val text = raw.trim()
            return if (text.length == 7 && text[0] == 'A') text.substring(1) else text
        }
    }
}
