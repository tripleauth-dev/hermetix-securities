package com.tripleauth.hermetix.broker

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/** 사용량 텔레메트리 — 계약(docs/telemetry.md)대로 합산·분류·직렬화되고, 전송 실패가 호출자에게 새지 않는지 */
class UsageTelemetryTest {

    private val objectMapper = ObjectMapper()
    private val sent = CopyOnWriteArrayList<String>()
    private lateinit var originalTransport: (String) -> Unit

    @BeforeEach
    fun setUp() {
        UsageTelemetry.drain() // 다른 테스트가 남긴 버킷 비우기
        originalTransport = UsageTelemetry.transport
        UsageTelemetry.transport = { sent += it }
    }

    @AfterEach
    fun tearDown() {
        UsageTelemetry.transport = originalTransport
        UsageTelemetry.drain()
    }

    private fun usage(broker: String = "kis", env: TradingEnvironment = TradingEnvironment.PAPER) = UsageTelemetry.forBroker(broker, env)

    @Test
    fun `성공·에러 건수와 분류, 응답 시간 분포가 시간 버킷으로 합산된다`() {
        val u = usage()
        repeat(3) { u.measure("quotes") { "ok" } }
        runCatching { u.measure("quotes") { throw RateLimitError(429, "EGW00201", "too many") } }
        runCatching { u.measure("create_order") { throw InsufficientFundsError(400, "X", "부족") } }
        runCatching { u.measure("candles") { throw java.io.IOException("connection reset") } }

        val json = objectMapper.readTree(UsageTelemetry.drain(Instant.parse("2026-09-15T01:02:03Z")))
        assertThat(json["schema"].asInt()).isEqualTo(1)
        assertThat(UUID.fromString(json["installationId"].asText())).isNotNull()
        assertThat(json["sdk"]["language"].asText()).isEqualTo("kotlin")
        assertThat(json["sentAt"].asText()).isEqualTo("2026-09-15T01:02:03Z")

        val bucket = json["buckets"].single()
        assertThat(bucket["broker"].asText()).isEqualTo("kis")
        assertThat(bucket["environment"].asText()).isEqualTo("PAPER")
        assertThat(bucket["hour"].asText()).endsWith(":00:00Z")
        val ops = bucket["ops"].associateBy { it["op"].asText() }
        assertThat(ops["quotes"]!!["ok"].asLong()).isEqualTo(3)
        assertThat(ops["quotes"]!!["errors"]["rate_limit"].asLong()).isEqualTo(1)
        assertThat(ops["quotes"]!!["latencyMs"]["count"].asLong()).isEqualTo(4)
        assertThat(ops["create_order"]!!["errors"]["insufficient_funds"].asLong()).isEqualTo(1)
        assertThat(ops["candles"]!!["errors"]["network"].asLong()).isEqualTo(1)
        assertThat(bucket["reconnects"].asLong()).isZero()

        assertThat(UsageTelemetry.drain()).isNull() // 비워졌다
    }

    @Test
    fun `measure 는 본문의 return 과 예외를 그대로 통과시키고 finally 에서 기록한다`() {
        val u = usage()
        fun early(flag: Boolean): String = u.measure("account") {
            if (flag) return "early"
            "late"
        }
        assertThat(early(true)).isEqualTo("early")
        assertThat(early(false)).isEqualTo("late")
        val thrown = runCatching { u.measure("holdings") { throw AuthError(401, "EGW00123", "expired") } }.exceptionOrNull()
        assertThat(thrown).isInstanceOf(AuthError::class.java)

        val ops = objectMapper.readTree(UsageTelemetry.drain())["buckets"].single()["ops"].associateBy { it["op"].asText() }
        assertThat(ops["account"]!!["ok"].asLong()).isEqualTo(2)
        assertThat(ops["holdings"]!!["errors"]["auth"].asLong()).isEqualTo(1)
    }

    @Test
    fun `스트림 구독·메시지·재접속이 브로커·환경별로 따로 쌓인다`() {
        val paper = usage("kiwoom", TradingEnvironment.PAPER)
        val live = usage("kiwoom", TradingEnvironment.LIVE)
        paper.streamSubscribed(StreamChannel.TRADES, 2)
        repeat(5) { paper.streamMessage(StreamChannel.TRADES) }
        paper.streamSubscribed(StreamChannel.ORDER_EVENTS)
        paper.reconnected()
        live.streamMessage(StreamChannel.ORDER_BOOK, 3)

        val buckets = objectMapper.readTree(UsageTelemetry.drain())["buckets"].associateBy { it["environment"].asText() }
        val p = buckets["PAPER"]!!
        val streams = p["streams"].associateBy { it["channel"].asText() }
        assertThat(streams["TRADES"]!!["subscriptions"].asLong()).isEqualTo(2)
        assertThat(streams["TRADES"]!!["messages"].asLong()).isEqualTo(5)
        assertThat(streams["ORDER_EVENTS"]!!["subscriptions"].asLong()).isEqualTo(1)
        assertThat(p["reconnects"].asLong()).isEqualTo(1)
        assertThat(buckets["LIVE"]!!["streams"].single()["messages"].asLong()).isEqualTo(3)
    }

    @Test
    fun `p50·p95 는 최근 256개 표본으로 계산한다`() {
        val u = usage("next")
        (1..300).forEach { i -> UsageTelemetry.record("next", TradingEnvironment.PAPER, "quotes", i * 1_000_000L, null) }
        val lat = objectMapper.readTree(UsageTelemetry.drain())["buckets"].single()["ops"].single()["latencyMs"]
        assertThat(lat["count"].asLong()).isEqualTo(300)
        // 최근 256개 = 45..300ms → p50 ≈ 172, p95 ≈ 287
        assertThat(lat["p50"].asLong()).isBetween(165L, 180L)
        assertThat(lat["p95"].asLong()).isBetween(280L, 295L)
    }

    @Test
    fun `flushNow 는 비어 있으면 보내지 않고, 전송 예외는 삼킨다`() {
        UsageTelemetry.flushNow()
        assertThat(sent).isEmpty()

        usage().measure("quotes") { 1 }
        UsageTelemetry.transport = { throw IllegalStateException("서버 없음") }
        UsageTelemetry.flushNow() // 예외가 밖으로 나오지 않는다
        assertThat(UsageTelemetry.drain()).isNull() // 실패한 페이로드는 재전송하지 않는다

        usage().measure("quotes") { 1 }
        UsageTelemetry.transport = { sent += it }
        UsageTelemetry.flushNow()
        assertThat(sent).hasSize(1)
        assertThat(objectMapper.readTree(sent[0])["buckets"].single()["ops"].single()["op"].asText()).isEqualTo("quotes")
    }

    @Test
    fun `예외 분류 표`() {
        assertThat(UsageTelemetry.classify(RateLimitError(429, null, null))).isEqualTo("rate_limit")
        assertThat(UsageTelemetry.classify(MarketClosedError(200, null, null))).isEqualTo("market_closed")
        assertThat(UsageTelemetry.classify(InvalidOrderError(400, null, null))).isEqualTo("invalid_order")
        assertThat(UsageTelemetry.classify(OrderNotFoundError(null, null))).isEqualTo("order_not_found")
        assertThat(UsageTelemetry.classify(BrokerApiException(500, null, "x"))).isEqualTo("server")
        assertThat(UsageTelemetry.classify(BrokerApiException(503, null, "x"))).isEqualTo("server")
        assertThat(UsageTelemetry.classify(BrokerApiException(200, "1", "업무 거절"))).isEqualTo("other")
        assertThat(UsageTelemetry.classify(AuthError(500, "EGW00123", "만료된 토큰"))).isEqualTo("auth") // 하위 분류가 우선
        assertThat(UsageTelemetry.classify(RuntimeException("wrap", java.net.ConnectException("refused")))).isEqualTo("network")
        assertThat(UsageTelemetry.classify(IllegalStateException("boom"))).isEqualTo("other")
    }

    @Test
    fun `설치 ID 는 UUID 이고 프로세스 안에서 고정이다`() {
        assertThat(UUID.fromString(UsageTelemetry.installationId)).isNotNull()
        assertThat(UsageTelemetry.installationId).isEqualTo(UsageTelemetry.installationId)
    }

    @Test
    fun `요청 서명은 계약대로 HMAC-SHA256(key, timestamp + 개행 + body) 의 소문자 hex 다`() {
        val body = """{"schema":1}"""
        val sig = UsageTelemetry.sign(body, 1_700_000_000L)
        assertThat(sig).hasSize(64).matches("[0-9a-f]{64}")
        // 독립 계산과 일치 (javax.crypto 직접 사용)
        val mac = javax.crypto.Mac.getInstance("HmacSHA256")
        mac.init(javax.crypto.spec.SecretKeySpec(UsageTelemetry.SIGNING_KEY.toByteArray(), "HmacSHA256"))
        val expected = mac.doFinal("1700000000\n$body".toByteArray()).joinToString("") { "%02x".format(it) }
        assertThat(sig).isEqualTo(expected)
        assertThat(UsageTelemetry.sign(body, 1_700_000_001L)).isNotEqualTo(sig)
        println("SIGNATURE_VECTOR body=$body ts=1700000000 sig=$sig")
    }
}
