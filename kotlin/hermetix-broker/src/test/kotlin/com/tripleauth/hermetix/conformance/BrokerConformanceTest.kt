package com.tripleauth.hermetix.conformance

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.client.NextApiClient
import com.tripleauth.hermetix.client.NextApiProperties
import com.tripleauth.hermetix.client.TokenManager
import com.tripleauth.hermetix.client.db.DbApiClient
import com.tripleauth.hermetix.client.db.DbApiProperties
import com.tripleauth.hermetix.client.kb.KbApiClient
import com.tripleauth.hermetix.client.kb.KbApiProperties
import com.tripleauth.hermetix.client.ls.LsApiClient
import com.tripleauth.hermetix.client.ls.LsApiProperties
import com.tripleauth.hermetix.client.toss.TossApiClient
import com.tripleauth.hermetix.client.toss.TossApiProperties
import com.tripleauth.hermetix.client.nh.NhApiClient
import com.tripleauth.hermetix.client.nh.NhApiProperties
import com.tripleauth.hermetix.client.kis.KisApiClient
import com.tripleauth.hermetix.client.kis.KisApiProperties
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiClient
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiProperties
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.math.BigDecimal

/**
 * 네 언어가 공유하는 골든 픽스처(`conformance/fixtures/{broker}.json`)를 가짜 HTTP 로 재생해 세 어댑터를 컨포먼스 시나리오에 통과시킨다.
 * 어댑터의 실제 요청 경로(헤더·인증·쓰로틀)를 그대로 지난다.
 */
class BrokerConformanceTest {

    private lateinit var server: MockWebServer

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE, true)

    @BeforeEach
    fun setUp() {
        server = MockWebServer()
        server.start()
    }

    @AfterEach
    fun tearDown() {
        server.shutdown()
    }

    private fun baseUrl() = server.url("/").toString().removeSuffix("/")

    private fun fixture(broker: String): JsonNode {
        val file = File("../../conformance/fixtures/$broker.json")
        check(file.exists()) { "픽스처가 없다: ${file.absolutePath}" }
        return objectMapper.readTree(file)
    }

    /** 픽스처 routes 를 순서대로 매칭 — method / path(정확히) / header(이름·값) 중 지정된 조건만 검사 */
    private class FixtureDispatcher(private val routes: JsonNode, private val objectMapper: ObjectMapper) : Dispatcher() {
        override fun dispatch(request: RecordedRequest): MockResponse {
            val path = request.path?.substringBefore('?') ?: ""
            val route = routes.firstOrNull { r ->
                (!r.has("method") || r["method"].asText() == request.method) &&
                    (!r.has("path") || r["path"].asText() == path) &&
                    (!r.has("header") || request.getHeader(r["header"][0].asText()) == r["header"][1].asText())
            } ?: return MockResponse().setResponseCode(599).setBody("""{"error":"no fixture route for ${request.method} $path tr_id=${request.getHeader("tr_id")} api-id=${request.getHeader("api-id")}"}""")
            return MockResponse()
                .setResponseCode(route.path("status").asInt(200))
                .setHeader("Content-Type", "application/json")
                .setBody(objectMapper.writeValueAsString(route["body"]))
        }
    }

    private fun scenario(fx: JsonNode) = BrokerConformance.Scenario(
        symbol = fx["scenario"]["symbol"].asText(),
        quantity = BigDecimal(fx["scenario"]["quantity"].asText()),
        limitPrice = BigDecimal(fx["scenario"]["limitPrice"].asText()),
    )

    private fun run(broker: String, client: (JsonNode) -> BrokerClient) {
        val fx = fixture(broker)
        server.dispatcher = FixtureDispatcher(fx["routes"], objectMapper)
        val report = BrokerConformance.verify(client(fx), scenario(fx))
        assertThat(report.violations).describedAs(report.toString()).isEmpty()
        assertThat(report.steps).contains("quotes", "candles", "calendar", "account", "holdings", "buyingPower", "createOrder", "getOrder", "getOrders", "cancelOrder", "fills")
    }

    @Test
    fun `next 어댑터는 컨포먼스 시나리오를 통과한다`() = run("next") {
        val props = NextApiProperties(baseUrl = baseUrl(), clientId = "pk_test_conf", clientSecret = "sk_test_conf", accountId = "acc_main")
        NextApiClient(props, TokenManager(props, objectMapper), objectMapper)
    }

    @Test
    fun `kis 어댑터는 컨포먼스 시나리오를 통과한다`() = run("kis") {
        KisApiClient(KisApiProperties(baseUrl = baseUrl(), appkey = "k", appsecret = "s", cano = "50199202", throttleMillis = 1), objectMapper)
    }

    @Test
    fun `kiwoom 어댑터는 컨포먼스 시나리오를 통과한다`() = run("kiwoom") {
        KiwoomApiClient(KiwoomApiProperties(baseUrl = baseUrl(), appkey = "k", secretkey = "s", throttleMillis = 1), objectMapper)
    }

    @Test
    fun `nh 어댑터는 컨포먼스 시나리오를 통과한다 (문서 기반 픽스처)`() = run("nh") {
        // accountNo 를 비워 /n2/acctinfo 로 모의(acct_type=03) 계좌를 고르는 경로까지 검증한다
        NhApiClient(NhApiProperties(baseUrl = baseUrl(), authUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1), objectMapper)
    }

    @Test
    fun `db 어댑터는 컨포먼스 시나리오를 통과한다 (문서 기반 픽스처)`() = run("db") {
        DbApiClient(DbApiProperties(baseUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1), objectMapper)
    }

    @Test
    fun `ls 어댑터는 컨포먼스 시나리오를 통과한다 (문서 기반 픽스처)`() = run("ls") {
        LsApiClient(LsApiProperties(baseUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1, chartThrottleMillis = 1), objectMapper)
    }

    @Test
    fun `toss 어댑터는 컨포먼스 시나리오를 통과한다 (문서 기반 픽스처, 실전 전용)`() = run("toss") {
        // accountSeq 를 비워 /api/v1/accounts 로 BROKERAGE 계좌를 고르는 경로까지 검증
        TossApiClient(TossApiProperties(baseUrl = baseUrl(), clientId = "c_conf", clientSecret = "s_conf", throttleMillis = 1), objectMapper)
    }

    @Test
    fun `toss 미국 보유는 매매기준율로 원화 환산해 계좌 총평가에 더한다`() {
        server.dispatcher = FixtureDispatcher(fixture("toss")["routes"], objectMapper)
        val client = TossApiClient(TossApiProperties(baseUrl = baseUrl(), clientId = "c_conf", clientSecret = "s_conf", throttleMillis = 1), objectMapper)
        val aapl = client.getHoldings().holdings.single { it.symbol == "US:AAPL" }
        assertThat(aapl.currentPrice).isEqualByComparingTo("250") // 단가는 종목 통화 그대로
        assertThat(aapl.marketValue).isEqualByComparingTo("700000")
        assertThat(aapl.unrealizedPnl).isEqualByComparingTo("100") // 손익은 USD 그대로 — 현재 환율로 환산하면 환차손익이 빠진다
        assertThat(aapl.currency).isEqualTo("USD")
        assertThat(client.getAccount().portfolioValue).isEqualByComparingTo("1910000")
    }

    @Test
    fun `kb 어댑터는 컨포먼스 시나리오를 통과한다 (공개 명세 픽스처, 실전 전용)`() = run("kb") {
        KbApiClient(KbApiProperties(baseUrl = baseUrl(), appKey = "k", appSecret = "s", throttleMillis = 1), objectMapper)
    }
}
