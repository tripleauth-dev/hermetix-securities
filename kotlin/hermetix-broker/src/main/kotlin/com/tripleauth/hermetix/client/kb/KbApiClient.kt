package com.tripleauth.hermetix.client.kb

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
 * **KB증권 Open API** 어댑터 (개인 오픈베타). `hermetix.broker: kb` 로 활성화한다.
 *
 * ⚠️ **실전 전용 · 실측 전** — 모의투자가 없는 운영 단일 환경이다. 포털이 로그인 없이 공개한 전체 명세(TR 별 입출력 필드·샘플)와
 * 공식 GitHub 예제(`kbsecurities/kb-openapi`)로 구현했다. 실계좌 소액 검증 전까지 상태는 "미검증" 이며 오픈베타라 스펙이 바뀔 수 있다.
 *
 * 규약 (명세 기준):
 * - 모든 API 는 `POST /api/v1/{tr소문자}`, 본문·응답 모두 `{"dataHeader": {...}, "dataBody": {...}}` 봉투. TR 은 경로로 식별
 * - 헤더 `Authorization: bearer {token}` + `appKey`. 토큰은 `POST /oauth2/token` 의 dataBody(`appKey`,`appSecret`,`grantType`) 로 발급(24h)
 * - 성공은 `dataHeader.processFlag == "A"`; `"B"` 면 업무 오류(`processCode`/`processMessage`, `dataBody.o_msg`). HTTP 200 이어도 실패 가능
 * - 숫자는 zero-padded 문자열(`"000360000"`), 문자열은 고정길이 공백 패딩 → 모두 trim 후 파싱
 * - 계좌번호 필드가 없다 — appKey 에 계좌가 바인딩된다(추정). 유량은 게이트웨이 5초당 200건(추정) → 100ms 쓰로틀
 *
 * 문서로 확정하지 못한 점 (실측 필요): `bdy_cmpr_ccd` 부호 코드 의미(KIS 관례 4·5 하락으로 가정), 체결 조회 레코드 배열 이름(`Record1` 가정),
 * 체결 조회의 종목이 표준코드(`KR7005930003`)로만 와서 6자리 코드로 환산(ISIN 4~9번째 자리), 차트 TR 의 시장구분(KOSPI 기본), 오류코드 표(로그인 뒤)
 */
class KbApiClient internal constructor(
    private val properties: KbApiProperties,
    private val objectMapper: ObjectMapper,
    clock: Clock,
    sleeper: (Long) -> Unit,
) : BrokerClient {

    constructor(properties: KbApiProperties, objectMapper: ObjectMapper) : this(properties, objectMapper, Clock.systemUTC(), Thread::sleep)

    private val logger = KotlinLogging.logger { }

    override val capabilities = BrokerCapabilities(
        brokerId = "kb",
        market = "KRX",
        currency = "KRW",
        candleIntervals = setOf(CandleInterval.DAY_1),
        clientOrderId = false,
        nativeBracket = false,
        fractionalShares = false,
        serverOpenOrders = true, // SSQM2341 체결미체결 조회
        environments = setOf(TradingEnvironment.LIVE), // 모의투자 "추후 제공 예정"
        // streams 미선언 — 2026-09 개인 오픈베타 명세(95 TR)에 웹소켓/실시간 없음. 폴링 전용
    )

    override val environment: TradingEnvironment = properties.environment


    private val usage = UsageTelemetry.forBroker(capabilities.brokerId, environment)

    private val restClient = RestClient.builder().baseUrl(properties.baseUrl).build()

    private val limiter = RateLimiter(properties.throttleMillis, maxRetries = 3, backoffMillis = { attempt -> 1000L * attempt }, sleeper = sleeper)

    /** 토큰 수명주기 — 메모리 → 파일 캐시 → 발급, 발급 실패 60초 쿨다운, 거부 토큰 폐기 후 1회 재시도 */
    private val tokens = BrokerTokenManager("kb", properties.appKey, properties.tokenRefreshMarginSeconds, clock) { issueToken(clock) }

    // ------------------------------------------------------------------ market

    override fun getQuotes(symbols: List<String>): QuotesResponse = usage.measure("quotes") {
        val quotes = symbols.map { symbol ->
            val out = call("/api/v1/ivu10140", mapOf("excg_clsf" to properties.excgClsf, "shrt_cd" to capabilities.symbolCode(symbol)))
            val sign = out.text("bdy_cmpr_ccd")
            val falling = sign in FALLING_SIGNS
            Quote(
                symbol = symbol,
                price = out.decimal("now_prc"),
                bidPrice = out.decimalOrNull("b_sq1_askprc")?.takeIf { it.signum() > 0 },
                askPrice = out.decimalOrNull("s_sq1_askprc")?.takeIf { it.signum() > 0 },
                volume = out.decimalOrNull("acml_vlm")?.toLong() ?: 0L,
                change = out.decimalOrNull("bdy_cmpr")?.let { if (falling && it.signum() > 0) it.negate() else it },
                changeRate = out.decimalOrNull("up_dwn_r_p2")?.let { if (falling && it.signum() > 0) it.negate() else it }?.percentToRate(),
                timestamp = Instant.now(),
            )
        }
        return QuotesResponse(quotes)
    }

    override fun getCandles(symbol: String, interval: CandleInterval, limit: Int?): CandlesResponse = usage.measure("candles") {
        require(interval == CandleInterval.DAY_1) { "KB 어댑터는 일봉(DAY_1)만 지원합니다." }
        val count = limit ?: 30
        val out = call(
            "/api/v1/ivs11560",
            mapOf(
                "chrt_clsf" to "D", "inq_clsf" to "2", "strt_dy" to LocalDate.now(KST).format(DATE),
                "is_cd" to capabilities.symbolCode(symbol), "minute_tck_indx" to "일", "info_ccd" to "1",
                "mkt_clsf" to properties.chartMarketClsf, "inq_cnt" to count.toString(),
            ),
        )
        // 샘플상 최신일 우선 → 공통 모델은 과거→최신
        val candles = out.path("out2").mapNotNull { row ->
            val date = row.text("dt")
            if (date.length != 8) return@mapNotNull null
            Candle(
                timestamp = LocalDate.parse(date, DATE).atStartOfDay(KST).toInstant(),
                open = row.decimal("opn_prc_p2"), high = row.decimal("hgh_prc_p2"), low = row.decimal("lw_prc_p2"), close = row.decimal("cls_prc_p2"),
                volume = row.decimalOrNull("vlm")?.toLong() ?: 0L,
            )
        }.sortedBy { it.timestamp }.takeLast(count)
        return CandlesResponse(symbol = symbol, interval = interval.value, candles = candles)
    }

    override fun getCalendar(): CalendarResponse = usage.measure("calendar") { KrxCalendar.synthesize() }

    // ----------------------------------------------------------------- account

    override fun getAccount(): AccountResponse = usage.measure("account") {
        val out = balance()
        val cash = out.decimal("dy_tfnd") // 일예수금
        val portfolio = out.decimalOrNull("nt_asts_val_amt")?.takeIf { it.signum() > 0 } // 순자산평가금액
            ?: (cash + (out.decimalOrNull("val_amt_sum") ?: BigDecimal.ZERO))
        return AccountResponse(accountId = "kb-live", name = null, currency = "KRW", cash = cash, portfolioValue = portfolio, status = "ACTIVE")
    }

    override fun getHoldings(): HoldingsResponse = usage.measure("holdings") {
        val holdings = balance().path("Record1").mapNotNull { row ->
            val quantity = listOfNotNull(row.decimalOrNull("ec_q"), row.decimalOrNull("hld_q")).maxOrNull() ?: return@mapNotNull null
            if (quantity.signum() <= 0) return@mapNotNull null
            Holding(
                symbol = normalizeCode(row.text("is_cd")),
                quantity = quantity,
                avgEntryPrice = row.decimalOrNull("byng_avr_prc") ?: BigDecimal.ZERO,
                currentPrice = row.decimalOrNull("now_prc"),
                marketValue = row.decimalOrNull("val_amt"),
                unrealizedPnl = row.decimalOrNull("val_pl"),
                unrealizedPnlRate = row.decimalOrNull("val_yld")?.percentToRate(),
            )
        }
        return HoldingsResponse(holdings)
    }

    override fun getBuyingPower(): BuyingPowerResponse = usage.measure("buying_power") {
        val out = call("/api/v1/ssqm1802", mapOf("bnd_mktio_ccd" to "1", "is_no" to ""))
        return BuyingPowerResponse(accountId = "kb-live", currency = "KRW", buyingPower = out.decimalOrNull("ordr_psbl_csh") ?: out.decimal("ordr_psbl_tl_amt"))
    }

    // ------------------------------------------------------------------ orders

    override fun createOrder(request: CreateOrderRequest): OrderResponse = usage.measure("create_order") {
        require(request.orderType == OrderType.LIMIT || request.orderType == OrderType.MARKET) { "KB 어댑터는 LIMIT/MARKET 주문만 지원합니다" }
        val code = capabilities.symbolCode(request.symbol)
        val path = if (request.side == OrderSide.BUY) "/api/v1/ssam1802" else "/api/v1/ssam1801"
        val out = call(
            path,
            orderBody(
                jbClsf = if (request.side == OrderSide.BUY) "2" else "1",
                code = code,
                qty = request.quantity.toPlainString(),
                price = if (request.orderType == OrderType.LIMIT) KrxTick.round(request.limitPrice!!).toPlainString() else "0",
                ordrCcd = if (request.orderType == OrderType.LIMIT) "00" else "03",
            ),
        )
        val orderId = out.text("ordr_no")
        if (orderId.isBlank() || orderId.trimStart('0').isBlank()) throw BrokerApiException(200, null, "KB 주문 응답에 ordr_no 가 없습니다: ${out.text("o_msg")}")
        return OrderResponse(
            orderId = orderId,
            clientOrderId = request.clientOrderId, // KB 미지원 — 반환만 유지
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
        
        orderRows().firstOrNull { sameOrderNo(it.text("ordr_no"), orderId) }?.toOrderResponse()
            ?: OrderResponse(orderId = orderId, status = OrderStatus.CANCELED)
    }

    override fun cancelOrder(orderId: String): OrderResponse = usage.measure("cancel_order") {
        val row = orderRows().firstOrNull { sameOrderNo(it.text("ordr_no"), orderId) }
            ?: throw OrderNotFoundError("order-not-found", "KB 당일 주문에서 찾을 수 없습니다: $orderId")
        val remaining = row.decimalOrNull("nccls_q") ?: ((row.decimalOrNull("ordr_q") ?: BigDecimal.ZERO) - (row.decimalOrNull("tl_ccls_q") ?: BigDecimal.ZERO))
        val out = call(
            "/api/v1/ssam1806",
            orderBody(jbClsf = "4", code = normalizeCode(row.text("stnd_is_no").ifBlank { row.text("stnd_is_cd") }), qty = remaining.toPlainString(), price = "0", ordrCcd = "00") +
                mapOf("crct_clsf" to "2", "orgn_ordr_no" to orderId.padStart(10, '0')),
        )
        logger.debug { "KB cancel accepted / orgOrdNo=$orderId cancelOrdNo=${out.text("ordr_no")}" }
        return OrderResponse(orderId = orderId, status = OrderStatus.PENDING_CANCEL, canceledAt = Instant.now())
    }

    override fun getFills(): FillsResponse = usage.measure("fills") {
        val fills = orderRows()
            .filter { (it.decimalOrNull("tl_ccls_q") ?: BigDecimal.ZERO).signum() > 0 }
            .map { row ->
                val qty = row.decimalOrNull("tl_ccls_q")
                val price = row.decimalOrNull("ccls_uprc")?.takeIf { it.signum() > 0 }
                Fill(
                    fillId = null,
                    orderId = row.text("ordr_no"),
                    symbol = normalizeCode(row.text("stnd_is_no").ifBlank { row.text("stnd_is_cd") }),
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

    private fun orderBody(jbClsf: String, code: String, qty: String, price: String, ordrCcd: String): Map<String, String> = mapOf(
        "mkt_tm_clsf" to "1", "ordr_jb_clsf" to jbClsf, "s_clsf" to "", "is_cd" to code,
        "ordr_q" to qty, "ordr_uprc" to price, "ordr_ccd" to ordrCcd, "crdt_typ_cd" to "00",
        "ln_dt" to "", "crct_clsf" to "", "orgn_ordr_no" to "", "gtc_ccd" to "", "ordr_mng_no" to "",
        "spclz_ordr_ccd" to "", "acct_cd" to "", "sor_ordr_ccd" to properties.sorOrderCcd, "stpd_prc" to "",
    )

    private fun balance(): JsonNode = call("/api/v1/ssqm2952", mapOf("excg_mktpr_ccd" to ""))

    /** 당일 주문 전체 (SSQM2341 ccls_clsf=0) */
    private fun orderRows(): List<JsonNode> = call(
        "/api/v1/ssqm2341",
        mapOf(
            "inq_clsf" to "9", "ccls_clsf" to "0", "ordr_dt" to LocalDate.now(KST).format(DATE), "is_cd" to "", "ordr_no" to "",
            "mthr_ordr_no" to "", "orgn_ordr_no" to "", "s_ccls_amt" to "", "b_ccls_amt" to "", "s_ccls_q" to "", "b_ccls_q" to "",
            "ac_nm" to "", "is_nm" to "", "cn_clsf" to "", "nxt_key" to "",
        ),
    ).path("Record1").toList()

    private fun JsonNode.toOrderResponse(): OrderResponse {
        val qty = decimalOrNull("ordr_q") ?: BigDecimal.ZERO
        val filled = decimalOrNull("tl_ccls_q") ?: BigDecimal.ZERO
        val remaining = decimalOrNull("nccls_q") ?: (qty - filled)
        val cancelText = text("crct_cncl_ccd")
        val reject = text("rfsl_rsn_nm")
        val status = when {
            cancelText.contains("취소") && remaining.signum() > 0 -> OrderStatus.PENDING_CANCEL
            remaining.signum() > 0 && filled.signum() > 0 -> OrderStatus.PARTIALLY_FILLED
            remaining.signum() > 0 -> OrderStatus.SUBMITTED
            filled.signum() > 0 && filled >= qty -> OrderStatus.FILLED
            filled.signum() > 0 -> OrderStatus.PARTIALLY_FILLED
            reject.isNotBlank() -> OrderStatus.REJECTED
            else -> OrderStatus.CANCELED
        }
        return OrderResponse(
            orderId = text("ordr_no"),
            status = status,
            symbol = normalizeCode(text("stnd_is_no").ifBlank { text("stnd_is_cd") }),
            side = sideOf(this),
            orderType = if (text("ordr_ccd").trimStart('0').let { it == "3" }) OrderType.MARKET else OrderType.LIMIT,
            quantity = qty,
            limitPrice = decimalOrNull("ordr_uprc")?.takeIf { it.signum() > 0 },
            filledQuantity = filled,
            avgFillPrice = decimalOrNull("ccls_uprc")?.takeIf { it.signum() > 0 },
            rejectReason = reject.ifBlank { null },
        )
    }

    private fun sideOf(row: JsonNode): OrderSide = if (row.text("trd_dl_ccd_nm").contains("매수")) OrderSide.BUY else OrderSide.SELL

    private fun sameOrderNo(a: String, b: String): Boolean = a.trim().trimStart('0') == b.trim().trimStart('0')

    /** 응답 `dataBody` 를 돌려준다 */
    private fun call(path: String, body: Map<String, String>): JsonNode =
        limiter.execute("KB $path") { tokens.call { token -> callOnce(path, body, token) } }

    private fun callOnce(path: String, body: Map<String, String>, token: String): JsonNode =
        restClient.post()
            .uri(path)
            .contentType(MediaType.APPLICATION_JSON)
            .headers { headers ->
                headers.set("Authorization", "bearer $token") // 명세·예제 모두 소문자 bearer
                headers.set("appKey", properties.appKey)
            }
            .body(objectMapper.writeValueAsString(mapOf("dataHeader" to mapOf("ipAddr" to "", "macAddr" to ""), "dataBody" to body)))
            .exchange { _, res ->
                val bytes = res.body.readAllBytes()
                val node = runCatching { objectMapper.readTree(bytes) }.getOrNull() ?: objectMapper.createObjectNode()
                val header = node.path("dataHeader")
                val data = node.path("dataBody")
                val status = res.statusCode.value()
                val flag = header.text("processFlag")
                val code = header.text("processCode").ifBlank { header.text("resultCode") }
                val message = listOf(header.text("processMessage"), header.text("resultMessage"), data.text("o_msg")).firstOrNull { it.isNotBlank() } ?: ""
                val msg = "KB($path) [$code] $message".trim()
                if (!res.statusCode.is2xxSuccessful || (flag.isNotBlank() && flag != "A")) {
                    val retryAfter = res.headers.getFirst("Retry-After")?.trim()?.toLongOrNull()
                    throw when {
                        status == 429 || message.contains("한도") || message.contains("초과") -> RateLimitError(status, code, msg, retryAfter)
                        status == 401 || status == 403 || message.contains("토큰") || message.contains("인증") -> AuthError(status, code, msg)
                        message.contains("장종료") || message.contains("장운영") || message.contains("장마감") || message.contains("휴장") -> MarketClosedError(status, code, msg)
                        message.contains("부족") -> InsufficientFundsError(status, code, msg)
                        message.contains("호가") || message.contains("수량") || message.contains("단위") -> InvalidOrderError(status, code, msg)
                        message.contains("주문번호") || message.contains("원주문") -> OrderNotFoundError(code, msg)
                        else -> BrokerApiException(status, code, msg)
                    }
                }
                data
            }!!

    /** 토큰 발급 — 429 는 [RateLimitError] */
    private fun issueToken(clock: Clock): Pair<String, Instant> = usage.measure("auth") {
        limiter.throttle()
        val node = restClient.post()
            .uri("/oauth2/token")
            .contentType(MediaType.APPLICATION_JSON)
            .body(
                objectMapper.writeValueAsString(
                    mapOf(
                        "dataHeader" to mapOf("ipAddr" to "", "macAddr" to ""),
                        "dataBody" to mapOf("appKey" to properties.appKey, "appSecret" to properties.appSecret, "grantType" to "client_credentials"),
                    ),
                ),
            )
            .exchange { _, res ->
                val n = runCatching { objectMapper.readTree(res.body.readAllBytes()) }.getOrNull() ?: objectMapper.createObjectNode()
                val data = n.path("dataBody")
                if (!res.statusCode.is2xxSuccessful || !data.hasNonNull("access_token")) {
                    val status = res.statusCode.value()
                    val h = n.path("dataHeader")
                    val code = h.text("processCode").ifBlank { null }
                    val msg = "KB 토큰 발급 실패(${h.text("resultCode")}): ${h.text("processMessage").ifBlank { h.text("resultMessage") }}"
                    throw if (status == 429) RateLimitError(status, code, msg, BrokerTokenManager.FAILURE_COOLDOWN_SECONDS) else AuthError(status, code, msg)
                }
                data
            }

        val expiresIn = node.path("expires_in").asLong(86400)
        logger.info { "KB token issued / expiresIn=${expiresIn}s" }
        node.path("access_token").asText() to clock.instant().plusSeconds(expiresIn)
    }

    private fun BigDecimal.percentToRate(): BigDecimal = divide(BigDecimal(100), 6, RoundingMode.HALF_EVEN)

    /** 고정길이 공백 패딩 문자열 → trim */
    private fun JsonNode.text(field: String): String = path(field).asText("").trim()

    private fun JsonNode.decimal(field: String): BigDecimal = decimalOrNull(field) ?: BigDecimal.ZERO

    /** zero-padded 문자열("000360000", "0000000360050.00") → BigDecimal */
    private fun JsonNode.decimalOrNull(field: String): BigDecimal? =
        path(field).takeIf { !it.isMissingNode && !it.isNull }?.asText("")?.trim()?.replace(",", "")?.takeIf { it.isNotBlank() }?.toBigDecimalOrNull()

    companion object {
        private val KST = KrxCalendar.KST
        private val DATE: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
        /** bdy_cmpr_ccd — KIS 관례(1 상한 2 상승 3 보합 4 하한 5 하락)로 가정 */
        private val FALLING_SIGNS = setOf("4", "5")

        /**
         * 종목코드 정규화: 잔고 `A005930` → `005930`, 체결 조회 표준코드 `KR7005930003`(ISIN) → 4~9번째 자리 `005930`.
         */
        fun normalizeCode(raw: String): String {
            val text = raw.trim()
            return when {
                text.length == 7 && text[0] == 'A' -> text.substring(1)
                text.length == 12 && text.startsWith("KR") -> text.substring(3, 9)
                else -> text
            }
        }
    }
}
