package com.tripleauth.hermetix.client.ls

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
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClient
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Clock
import java.time.Instant
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/**
 * **LS증권(구 이베스트투자증권) OPEN API** 어댑터. `hermetix.broker: ls` 로 활성화한다.
 *
 * ⚠️ **문서 기반 구현 (실측 전)** — 공식 포털 TR 문서와 커뮤니티 카탈로그(krsec, LsApiHelper, k-ebest-im, 공식 샘플)에서
 * 엔드포인트·TR 코드·필드명을 역추적했다. 모의서버 실측 전까지 상태는 "미검증" 이다.
 *
 * 규약 (문서 기준):
 * - 모든 API 는 `POST`, 경로는 기능군(`/stock/market-data`, `/stock/chart`, `/stock/accno`, `/stock/order`)이고 **TR 은 `tr_cd` 헤더**로 고른다
 * - 본문은 `{"<TR>InBlock": {...}}` (t-계열) 또는 `{"<TR>InBlock1": {...}}` (CSP-계열). 응답 `rsp_cd`("00000" 성공)/`rsp_msg` + OutBlock 들
 * - 헤더 `authorization: Bearer` + `tr_cont: N` + `tr_cont_key` (+ 법인만 `mac_address`). 계좌번호는 토큰(appkey)에 바인딩
 * - 토큰 `POST /oauth2/token` form(`appsecretkey`), 발급일 익일 07시까지 유효
 * - 실전/모의 같은 호스트 — 모의 appkey 로 서버가 라우팅. **모의 주문은 `IsuNo` 에 `A` 접두 필수** → 항상 `A`+코드
 * - HTTP 200 + `rsp_cd != 00000` 이 업무 오류. `IGW00201` 유량 초과(초당 TPS), `IGW00121/00123` 토큰 오류
 * - TR 별 TPS 가 다르다(시세 3, 차트 **1**, 계좌 2, 예수금 1, 주문 10, 취소 3) → 전역 500ms + 차트 전용 1100ms 쓰로틀
 *
 * 문서로 확정하지 못한 점 (실측 필요): 잔고 `expcode` 의 `A` 접두 여부, `sign` 코드 의미(xingAPI 관례 4·5 하락으로 가정),
 * 응답 숫자 타입(양쪽 허용), 장 마감 오류 코드(메시지로 판단), 미체결 `medosu` 표기("매수"/"매도" 가정)
 */
class LsApiClient internal constructor(
    private val properties: LsApiProperties,
    private val objectMapper: ObjectMapper,
    clock: Clock,
    sleeper: (Long) -> Unit,
) : StreamingBrokerClient {

    constructor(properties: LsApiProperties, objectMapper: ObjectMapper) : this(properties, objectMapper, Clock.systemUTC(), Thread::sleep)

    private val logger = KotlinLogging.logger { }

    override val capabilities = BrokerCapabilities(
        brokerId = "ls",
        market = "KRX",
        currency = "KRW",
        candleIntervals = setOf(CandleInterval.DAY_1),
        clientOrderId = false,
        nativeBracket = false,
        fractionalShares = false,
        serverOpenOrders = true, // t0425 미체결 조회
        environments = setOf(TradingEnvironment.PAPER, TradingEnvironment.LIVE),
        // 웹소켓 체결(S3_/K3_)·호가(H1_/HA_)·주문 통보(SC0~SC4) — 문서 기반, 실측 전
        streams = setOf(StreamChannel.TRADES, StreamChannel.ORDER_BOOK, StreamChannel.ORDER_EVENTS),
    )

    override val environment: TradingEnvironment = properties.environment


    private val usage = UsageTelemetry.forBroker(capabilities.brokerId, environment)

    private val restClient = RestClient.builder().baseUrl(properties.baseUrl).build()

    private val limiter = RateLimiter(properties.throttleMillis, maxRetries = 3, backoffMillis = { attempt -> 1000L * attempt }, sleeper = sleeper)
    /** 차트 TR 은 초당 1건 — 별도 간격 */
    private val chartLimiter = RateLimiter(properties.chartThrottleMillis, maxRetries = 0, sleeper = sleeper)

    /** 토큰 수명주기 — 메모리 → 파일 캐시 → 발급, 발급 실패 60초 쿨다운, 거부 토큰 폐기 후 1회 재시도 */
    private val tokens = BrokerTokenManager("ls", properties.appKey, properties.tokenRefreshMarginSeconds, clock) { issueToken(clock) }

    // ------------------------------------------------------------------ market

    override fun getQuotes(symbols: List<String>): QuotesResponse = usage.measure("quotes") {
        val quotes = symbols.map { symbol ->
            val out = call("/stock/market-data", "t1102", "t1102InBlock", mapOf("shcode" to capabilities.symbolCode(symbol), "exchgubun" to properties.exchGubun))
                .path("t1102OutBlock")
            val sign = out.path("sign").asText("")
            val falling = sign in FALLING_SIGNS
            Quote(
                symbol = symbol,
                price = out.decimal("price"),
                bidPrice = null, // t1102 에 호가 없음 (t1101 별도)
                askPrice = null,
                volume = out.decimalOrNull("volume")?.toLong() ?: 0L,
                change = out.decimalOrNull("change")?.let { if (falling && it.signum() > 0) it.negate() else it },
                changeRate = out.decimalOrNull("diff")?.let { if (falling && it.signum() > 0) it.negate() else it }?.percentToRate(),
                timestamp = Instant.now(),
            )
        }
        return QuotesResponse(quotes)
    }

    override fun getCandles(symbol: String, interval: CandleInterval, limit: Int?): CandlesResponse = usage.measure("candles") {
        require(interval == CandleInterval.DAY_1) { "LS 어댑터는 일봉(DAY_1)만 지원합니다." }
        val count = limit ?: 30
        val today = LocalDate.now(KST)
        val start = today.minusDays((count * 16L / 10) + 10)
        val rows = chartLimiter.execute("LS t8410") {
            call(
                "/stock/chart", "t8410", "t8410InBlock",
                mapOf(
                    "shcode" to capabilities.symbolCode(symbol), "gubun" to "2", "qrycnt" to count,
                    "sdate" to start.format(DATE), "edate" to today.format(DATE),
                    "cts_date" to "", "comp_yn" to "N", "sujung" to "Y",
                ),
            )
        }.path("t8410OutBlock1")

        // 문서상 과거→최신 오름차순이지만 정렬을 보장한다
        val candles = rows.mapNotNull { row ->
            val date = row.path("date").asText("").trim()
            if (date.length != 8) return@mapNotNull null
            Candle(
                timestamp = LocalDate.parse(date, DATE).atStartOfDay(KST).toInstant(),
                open = row.decimal("open"), high = row.decimal("high"), low = row.decimal("low"), close = row.decimal("close"),
                volume = row.decimalOrNull("jdiff_vol")?.toLong() ?: 0L,
            )
        }.sortedBy { it.timestamp }.takeLast(count)

        return CandlesResponse(symbol = symbol, interval = interval.value, candles = candles)
    }

    override fun getCalendar(): CalendarResponse = usage.measure("calendar") { KrxCalendar.synthesize() }

    // ----------------------------------------------------------------- account

    override fun getAccount(): AccountResponse = usage.measure("account") {
        val summary = balance().path("t0424OutBlock")
        val cash = summary.decimalOrNull("sunamt1") ?: BigDecimal.ZERO // 추정 D2 예수금
        val portfolio = summary.decimalOrNull("sunamt")?.takeIf { it.signum() > 0 } // 추정 순자산
            ?: (cash + (summary.decimalOrNull("tappamt") ?: BigDecimal.ZERO))
        return AccountResponse(
            accountId = "ls-${properties.environment.name.lowercase()}", // 계좌번호는 토큰에 바인딩 — 필드 없음
            name = null,
            currency = "KRW",
            cash = cash,
            portfolioValue = portfolio,
            status = "ACTIVE",
        )
    }

    override fun getHoldings(): HoldingsResponse = usage.measure("holdings") {
        val holdings = balance().path("t0424OutBlock1").mapNotNull { row ->
            val quantity = row.decimalOrNull("janqty") ?: return@mapNotNull null
            if (quantity.signum() <= 0) return@mapNotNull null
            Holding(
                symbol = normalizeCode(row.path("expcode").asText()),
                quantity = quantity,
                avgEntryPrice = row.decimalOrNull("pamt") ?: BigDecimal.ZERO,
                currentPrice = row.decimalOrNull("price"),
                marketValue = row.decimalOrNull("appamt"),
                unrealizedPnl = row.decimalOrNull("dtsunik"),
                unrealizedPnlRate = row.decimalOrNull("sunikrt")?.percentToRate(),
            )
        }
        return HoldingsResponse(holdings)
    }

    override fun getBuyingPower(): BuyingPowerResponse = usage.measure("buying_power") {
        val out = call("/stock/accno", "CSPAQ12200", "CSPAQ12200InBlock1", mapOf("BalCreTp" to "0")).path("CSPAQ12200OutBlock2")
        return BuyingPowerResponse(
            accountId = "ls-${properties.environment.name.lowercase()}",
            currency = "KRW",
            buyingPower = out.decimalOrNull("MnyOrdAbleAmt") ?: out.decimal("Dps"),
        )
    }

    // ------------------------------------------------------------------ orders

    override fun createOrder(request: CreateOrderRequest): OrderResponse = usage.measure("create_order") {
        require(request.orderType == OrderType.LIMIT || request.orderType == OrderType.MARKET) { "LS 어댑터는 LIMIT/MARKET 주문만 지원합니다" }
        val code = capabilities.symbolCode(request.symbol)
        val out = call(
            "/stock/order", "CSPAT00601", "CSPAT00601InBlock1",
            mapOf(
                "IsuNo" to "A$code", // 모의투자는 A 접두 필수, 실전은 둘 다 허용
                "OrdQty" to request.quantity.toPlainString().toLong(),
                "OrdPrc" to if (request.orderType == OrderType.LIMIT) KrxTick.round(request.limitPrice!!).toPlainString().toLong() else 0L,
                "BnsTpCode" to if (request.side == OrderSide.BUY) "2" else "1",
                "OrdprcPtnCode" to if (request.orderType == OrderType.LIMIT) "00" else "03",
                "MgntrnCode" to "000",
                "LoanDt" to "",
                "OrdCndiTpCode" to "0",
            ),
        ).path("CSPAT00601OutBlock2")
        val orderId = out.path("OrdNo").asText("").trim()
        if (orderId.isBlank() || orderId == "0") throw BrokerApiException(200, null, "LS 주문 응답에 OrdNo 가 없습니다")

        return OrderResponse(
            orderId = orderId,
            clientOrderId = request.clientOrderId, // LS 미지원 — 반환만 유지
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
        
        OrdersResponse(orderRows().map { it.toOrderResponse() }.filter { it.status.isOpen })
    }

    override fun getOrder(orderId: String): OrderResponse = usage.measure("get_order") {
        
        orderRows().firstOrNull { sameOrderNo(it.path("ordno").asText(), orderId) }?.toOrderResponse()
            ?: OrderResponse(orderId = orderId, status = OrderStatus.CANCELED) // 당일 조회에 없으면 종료로 간주
    }

    override fun cancelOrder(orderId: String): OrderResponse = usage.measure("cancel_order") {
        val row = orderRows().firstOrNull { sameOrderNo(it.path("ordno").asText(), orderId) }
            ?: throw OrderNotFoundError("order-not-found", "LS 당일 주문에서 찾을 수 없습니다: $orderId")
        val remaining = row.decimalOrNull("ordrem") ?: ((row.decimalOrNull("qty") ?: BigDecimal.ZERO) - (row.decimalOrNull("cheqty") ?: BigDecimal.ZERO))
        val out = call(
            "/stock/order", "CSPAT00801", "CSPAT00801InBlock1",
            mapOf(
                "OrgOrdNo" to orderId.trimStart('0').ifBlank { "0" }.toLong(),
                "IsuNo" to "A${normalizeCode(row.path("expcode").asText())}",
                "OrdQty" to remaining.toPlainString().toLong(),
            ),
        ).path("CSPAT00801OutBlock2")
        logger.debug { "LS cancel accepted / orgOrdNo=$orderId cancelOrdNo=${out.path("OrdNo").asText()}" }
        return OrderResponse(orderId = orderId, status = OrderStatus.PENDING_CANCEL, canceledAt = Instant.now())
    }

    override fun getFills(): FillsResponse = usage.measure("fills") {
        val fills = orderRows()
            .filter { (it.decimalOrNull("cheqty") ?: BigDecimal.ZERO).signum() > 0 }
            .map { row ->
                val qty = row.decimalOrNull("cheqty")
                val price = row.decimalOrNull("cheprice")
                Fill(
                    fillId = null, // 주문 단위 집계
                    orderId = row.path("ordno").asText().trim(),
                    symbol = normalizeCode(row.path("expcode").asText()),
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

    private fun balance(): JsonNode = call(
        "/stock/accno", "t0424", "t0424InBlock",
        mapOf("prcgb" to "1", "chegb" to "2", "dangb" to "0", "charge" to "1", "cts_expcode" to ""),
    )

    /** 당일 주문 전체(체결·미체결) — t0425 chegb=0 */
    private fun orderRows(): List<JsonNode> = call(
        "/stock/accno", "t0425", "t0425InBlock",
        mapOf("expcode" to "", "chegb" to "0", "medosu" to "0", "sortgb" to "1", "cts_ordno" to ""),
    ).path("t0425OutBlock1").toList()

    private fun JsonNode.toOrderResponse(): OrderResponse {
        val qty = decimalOrNull("qty") ?: BigDecimal.ZERO
        val filled = decimalOrNull("cheqty") ?: BigDecimal.ZERO
        val remaining = decimalOrNull("ordrem") ?: (qty - filled)
        val statusText = path("status").asText("")
        val status = when {
            statusText.contains("취소") && remaining.signum() > 0 -> OrderStatus.PENDING_CANCEL
            remaining.signum() > 0 && filled.signum() > 0 -> OrderStatus.PARTIALLY_FILLED
            remaining.signum() > 0 -> OrderStatus.SUBMITTED
            filled.signum() > 0 && filled >= qty -> OrderStatus.FILLED
            filled.signum() > 0 -> OrderStatus.PARTIALLY_FILLED
            statusText.contains("거부") -> OrderStatus.REJECTED
            else -> OrderStatus.CANCELED
        }
        return OrderResponse(
            orderId = path("ordno").asText().trim(),
            status = status,
            symbol = normalizeCode(path("expcode").asText()),
            side = sideOf(this),
            orderType = if (path("hogagb").asText("") == "03") OrderType.MARKET else OrderType.LIMIT,
            quantity = qty,
            limitPrice = decimalOrNull("price")?.takeIf { it.signum() > 0 },
            filledQuantity = filled,
            avgFillPrice = decimalOrNull("cheprice")?.takeIf { it.signum() > 0 },
        )
    }

    /** medosu: 한글 "매수"/"매도" 또는 코드 "2"/"1" — 둘 다 허용 */
    private fun sideOf(row: JsonNode): OrderSide {
        val text = row.path("medosu").asText("").trim()
        return if (text.contains("매수") || text == "2") OrderSide.BUY else OrderSide.SELL
    }

    private fun sameOrderNo(a: String, b: String): Boolean = a.trim().trimStart('0') == b.trim().trimStart('0')

    private fun call(path: String, trCd: String, inBlock: String, input: Map<String, Any>): JsonNode =
        limiter.execute("LS $trCd") { tokens.call { token -> callOnce(path, trCd, inBlock, input, token) } }

    private fun callOnce(path: String, trCd: String, inBlock: String, input: Map<String, Any>, token: String): JsonNode =
        restClient.post()
            .uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { headers ->
                headers.set("authorization", "Bearer $token")
                headers.set("tr_cd", trCd)
                headers.set("tr_cont", "N")
                headers.set("tr_cont_key", "")
                if (properties.macAddress.isNotBlank()) headers.set("mac_address", properties.macAddress)
            }
            .body(objectMapper.writeValueAsString(mapOf(inBlock to input)))
            .exchange { _, res ->
                val bytes = res.body.readAllBytes()
                val node = runCatching { objectMapper.readTree(bytes) }.getOrNull() ?: objectMapper.createObjectNode()
                val status = res.statusCode.value()
                val code = node.path("rsp_cd").asText(node.path("error_code").asText("")).trim()
                val rspMsg = node.path("rsp_msg").asText(node.path("error_description").asText(""))
                val msg = "LS($trCd) [$code] $rspMsg".trim()
                if (!res.statusCode.is2xxSuccessful || (node.has("rsp_cd") && code != "00000")) {
                    val retryAfter = res.headers.getFirst("Retry-After")?.trim()?.toLongOrNull()
                    throw when {
                        code == "IGW00201" || status == 429 -> RateLimitError(status, code, msg, retryAfter)
                        code in AUTH_CODES || status == 401 -> AuthError(status, code, msg)
                        rspMsg.contains("장종료") || rspMsg.contains("장운영") || rspMsg.contains("장 마감") || rspMsg.contains("장마감") -> MarketClosedError(status, code, msg)
                        rspMsg.contains("부족") -> InsufficientFundsError(status, code, msg)
                        rspMsg.contains("호가") || rspMsg.contains("단위") -> InvalidOrderError(status, code, msg)
                        rspMsg.contains("주문번호") || rspMsg.contains("원주문") -> OrderNotFoundError(code, msg)
                        else -> BrokerApiException(status, code, msg)
                    }
                }
                node
            }!!

    // ------------------------------------------------------------------ stream

    /** 실시간 스트림 — 매 메시지 헤더에 REST 토큰을 싣는다 (재접속 시 캐시/재발급 토큰) */
    override fun openStream(): MarketStream = LsMarketStream(properties, objectMapper, ::token, usage)

    private fun token(): String = tokens.get()

    /** 토큰 발급 — 유량 초과(`IGW00201`·429)는 [RateLimitError] */
    private fun issueToken(clock: Clock): Pair<String, Instant> = usage.measure("auth") {
        limiter.throttle()
        val form = LinkedMultiValueMap<String, String>().apply {
            add("grant_type", "client_credentials")
            add("appkey", properties.appKey)
            add("appsecretkey", properties.appSecret)
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
                    val code = n.path("error_code").asText(n.path("rsp_cd").asText(null))
                    val detail = n.path("error_description").asText(n.path("rsp_msg").asText(""))
                    throw if (code == "IGW00201" || status == 429) {
                        RateLimitError(status, code, "LS 토큰 발급 유량 초과($code): $detail", BrokerTokenManager.FAILURE_COOLDOWN_SECONDS)
                    } else {
                        AuthError(status, code, "LS 토큰 발급 실패($code): $detail")
                    }
                }
                n
            }

        val expiresIn = node.path("expires_in").asLong(86400)
        logger.info { "LS token issued / expiresIn=${expiresIn}s" }
        node.path("access_token").asText() to clock.instant().plusSeconds(expiresIn)
    }

    private fun BigDecimal.percentToRate(): BigDecimal = divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN)

    private fun JsonNode.decimal(field: String): BigDecimal = decimalOrNull(field) ?: BigDecimal.ZERO

    private fun JsonNode.decimalOrNull(field: String): BigDecimal? =
        path(field).takeIf { !it.isMissingNode && !it.isNull }?.asText("")?.trim()?.replace(",", "")?.takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

    companion object {
        private val KST = KrxCalendar.KST
        private val DATE: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        private val AUTH_CODES = setOf("IGW00121", "IGW00123")
        /** xingAPI 관례 sign: 1 상한 2 상승 3 보합 4 하한 5 하락 */
        private val FALLING_SIGNS = setOf("4", "5")

        /** 계좌 TR 의 종목코드는 `A005930` 형태(추정) → 6자리 코드 */
        fun normalizeCode(raw: String): String {
            val text = raw.trim()
            return if (text.length == 7 && text[0] == 'A') text.substring(1) else text
        }
    }
}
