package com.tripleauth.hermetix.client

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.broker.BrokerClient
import com.tripleauth.hermetix.broker.RateLimitError
import com.tripleauth.hermetix.client.db.DbApiClient
import com.tripleauth.hermetix.client.db.DbApiProperties
import com.tripleauth.hermetix.client.kb.KbApiClient
import com.tripleauth.hermetix.client.kb.KbApiProperties
import com.tripleauth.hermetix.client.kis.KisApiClient
import com.tripleauth.hermetix.client.kis.KisApiProperties
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiClient
import com.tripleauth.hermetix.client.kiwoom.KiwoomApiProperties
import com.tripleauth.hermetix.client.ls.LsApiClient
import com.tripleauth.hermetix.client.ls.LsApiProperties
import com.tripleauth.hermetix.client.nh.NhApiClient
import com.tripleauth.hermetix.client.nh.NhApiProperties
import com.tripleauth.hermetix.client.toss.TossApiClient
import com.tripleauth.hermetix.client.toss.TossApiProperties
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.io.File
import java.nio.file.Files
import java.nio.file.Path
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * 토큰 수명주기([BrokerTokenManager]) — 여덟 어댑터 공통 시나리오 (Python test_tokens.py 와 동일).
 *
 * - 발급 유량 초과(429·EGW00133·IGW00201)는 RateLimitError, 발급 실패 후 60초는 서버에 다시 묻지 않는다
 * - 업무 호출이 토큰을 거부(401)하면 토큰을 버리고 1회 재발급 후 재시도한다
 * - 파일 캐시: 다른 프로세스가 새로 저장한 토큰은 지우지 않고 이어 쓴다, 만료 직전 같은 토큰 재발급은 반복하지 않는다
 *
 * 레이트리미터의 대기는 시계를 돌리지 않는다 — 어댑터의 자동 재시도는 전부 쿨다운 안에서 끝나 서버에 다시 묻지 않는다.
 */
class BrokerTokenManagerTest {

    private val objectMapper: ObjectMapper = ObjectMapper()
        .registerModule(kotlinModule())
        .registerModule(JavaTimeModule())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_USING_DEFAULT_VALUE, true)

    /** 2026-09 — 키움 픽스처 토큰(2026-12-31 만료)이 유효한 시점 */
    private val clock = MutableClock(Instant.ofEpochSecond(1_790_000_000))
    private val servers = mutableListOf<MockWebServer>()

    @AfterEach
    fun tearDown() {
        servers.forEach { it.shutdown() }
        TokenCache.directory = null
    }

    @ParameterizedTest
    @MethodSource("rateLimitCases")
    fun `토큰 발급 유량 초과는 RateLimitError 이고 쿨다운 동안 서버에 다시 묻지 않는다`(broker: String, status: Int, body: String) {
        val http = FakeHttp(broker, tokenOverrides = listOf(status to body))
        val client = client(broker, http)

        assertThatThrownBy { client.getAccount() }
            .isInstanceOfSatisfying(RateLimitError::class.java) { assertThat(it.retryAfterSeconds).isEqualTo(60) }
        clock.advanceSeconds(20)
        assertThatThrownBy { client.getAccount() }
            .isInstanceOfSatisfying(RateLimitError::class.java) { assertThat(it.retryAfterSeconds).isEqualTo(40) }
        assertThat(http.tokenCalls.get()).isEqualTo(1) // 쿨다운 동안 서버에 다시 묻지 않는다

        clock.advanceSeconds(41)
        client.getAccount() // 쿨다운 뒤 재발급
        assertThat(http.tokenCalls.get()).isEqualTo(2)
    }

    @ParameterizedTest
    @MethodSource("brokers")
    fun `토큰 발급 인증 실패도 쿨다운 동안은 다시 묻지 않는다`(broker: String) {
        val http = FakeHttp(broker, tokenOverrides = listOf(401 to """{"error":"invalid_client"}"""))
        val client = client(broker, http)
        repeat(3) { assertThatThrownBy { client.getAccount() }.isInstanceOf(AuthError::class.java) }
        assertThat(http.tokenCalls.get()).isEqualTo(1)

        clock.advanceSeconds(61)
        client.getAccount()
        assertThat(http.tokenCalls.get()).isEqualTo(2)
    }

    @ParameterizedTest
    @MethodSource("brokers")
    fun `거부된 토큰은 버리고 업무 호출을 한 번 다시 시도한다`(broker: String) {
        val http = FakeHttp(broker, apiOverrides = listOf(401 to "{}"))
        val client = client(broker, http)
        client.getAccount()
        assertThat(http.tokenCalls.get()).isEqualTo(2) // 거부된 토큰을 버리고 1회 재발급

        http.apiOverrides.addAll(listOf(401 to "{}", 401 to "{}"))
        // 재발급 토큰도 거부되면 그대로 올린다 (무한 재시도 없음)
        assertThatThrownBy { client.getAccount() }.isInstanceOf(AuthError::class.java)
    }

    @Test
    fun `invalidate 는 다른 프로세스가 저장한 토큰을 지우지 않는다`(@TempDir dir: Path) {
        TokenCache.directory = dir
        val (issueA, callsA) = issuer(Result.success("tok-a" to clock.instant().plusSeconds(86400)))
        val (issueB, callsB) = issuer(Result.success("tok-b" to clock.instant().plusSeconds(86400)))
        val a = BrokerTokenManager("toss", "k", 60, clock, issueA)
        val b = BrokerTokenManager("toss", "k", 60, clock, issueB)
        assertThat(a.get()).isEqualTo("tok-a")
        assertThat(b.get()).isEqualTo("tok-a") // 파일 캐시 공유
        // 토스처럼 재발급이 이전 토큰을 무효로 만드는 서버: b 가 거부당해 재발급하면 a 는 거부당한 뒤 b 의 새 토큰을 쓴다
        b.invalidate("tok-a")
        assertThat(b.get()).isEqualTo("tok-b")
        a.invalidate("tok-a")
        assertThat(a.get()).isEqualTo("tok-b")
        assertThat(callsA.get() to callsB.get()).isEqualTo(1 to 1)
        val files = Files.list(dir).use { it.toList() }
        assertThat(files).hasSize(1)
        assertThat(files[0].fileName.toString()).startsWith("toss-")
    }

    @Test
    fun `만료 직전 같은 토큰 재발급은 만료까지 쓴다`() {
        val expiresAt = clock.instant().plusSeconds(300) // 여유(600초) 안 - DB 는 만료 전 재발급에 남은 시간 그대로의 같은 토큰을 준다
        val (issue, calls) = issuer(Result.success("tok-1" to expiresAt))
        val manager = BrokerTokenManager("db", "k", 600, clock, issue)
        repeat(5) { assertThat(manager.get()).isEqualTo("tok-1") }
        assertThat(calls.get()).isEqualTo(1)

        clock.set(expiresAt)
        manager.get()
        assertThat(calls.get()).isEqualTo(2)
    }

    @Test
    fun `발급 실패는 call 이 재시도하지 않는다`() {
        val (issue, calls) = issuer(Result.failure(AuthError(401, "invalid_client", "rejected")))
        val manager = BrokerTokenManager("kis", "k", 300, clock, issue)
        assertThatThrownBy { manager.call { } }.isInstanceOf(AuthError::class.java)
        assertThat(calls.get()).isEqualTo(1)
    }

    @ParameterizedTest
    @ValueSource(strings = ["db", "kis"])
    fun `파일 캐시 경로는 브로커와 키 해시로 정해진다`(broker: String, @TempDir dir: Path) {
        TokenCache.directory = dir
        val (issue, _) = issuer(Result.success("tok" to clock.instant().plusSeconds(86400)))
        BrokerTokenManager(broker, "k", 300, clock, issue).get()
        // sha256("k") 앞 16자
        assertThat(Files.exists(dir.resolve("$broker-8254c329a92850f6.json"))).isTrue()
    }

    // ------------------------------------------------------------------ fixtures

    /** 매번 같은 결과를 돌려주는 가짜 발급 함수와 호출 횟수 */
    private fun issuer(result: Result<Pair<String, Instant>>): Pair<() -> Pair<String, Instant>, AtomicInteger> {
        val calls = AtomicInteger()
        val issue = {
            calls.incrementAndGet()
            result.getOrThrow()
        }
        return issue to calls
    }

    private fun client(broker: String, http: FakeHttp): BrokerClient {
        val url = http.server.url("/").toString().removeSuffix("/")
        val sleeper: (Long) -> Unit = {} // 대기는 시계를 돌리지 않는다
        return when (broker) {
            "next" -> NextApiProperties(baseUrl = url, clientId = "pk_test_conf", clientSecret = "sk_test_conf", accountId = "acc_main")
                .let { NextApiClient(it, TokenManager(it, objectMapper, clock), objectMapper, sleeper) }
            "kis" -> KisApiClient(KisApiProperties(baseUrl = url, appkey = "k", appsecret = "s", cano = "50199202", throttleMillis = 1), objectMapper, clock, sleeper)
            "kiwoom" -> KiwoomApiClient(KiwoomApiProperties(baseUrl = url, appkey = "k", secretkey = "s", throttleMillis = 1), objectMapper, clock, sleeper)
            "nh" -> NhApiClient(NhApiProperties(baseUrl = url, authUrl = url, appKey = "k", appSecret = "s", throttleMillis = 1), objectMapper, clock, sleeper)
            "db" -> DbApiClient(DbApiProperties(baseUrl = url, appKey = "k", appSecret = "s", throttleMillis = 1), objectMapper, clock, sleeper)
            "ls" -> LsApiClient(LsApiProperties(baseUrl = url, appKey = "k", appSecret = "s", throttleMillis = 1, chartThrottleMillis = 1), objectMapper, clock, sleeper)
            "toss" -> TossApiClient(TossApiProperties(baseUrl = url, clientId = "c_conf", clientSecret = "s_conf", throttleMillis = 1), objectMapper, clock, sleeper)
            "kb" -> KbApiClient(KbApiProperties(baseUrl = url, appKey = "k", appSecret = "s", throttleMillis = 1), objectMapper, clock, sleeper)
            else -> error("unknown broker $broker")
        }
    }

    /** 픽스처 routes 재생 — 토큰 발급 응답과 업무 호출 응답을 앞에서부터 덮어쓸 수 있다 */
    private inner class FakeHttp(
        broker: String,
        tokenOverrides: List<Pair<Int, String>> = emptyList(),
        apiOverrides: List<Pair<Int, String>> = emptyList(),
    ) {
        private val routes: JsonNode = objectMapper.readTree(fixtureFile(broker))["routes"]
        private val tokenPath = routes[0]["path"].asText()
        val tokenOverrides = ConcurrentLinkedQueue(tokenOverrides)
        val apiOverrides = ConcurrentLinkedQueue(apiOverrides)
        val tokenCalls = AtomicInteger()
        val apiCalls = AtomicInteger()

        val server = MockWebServer().apply {
            dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    val path = request.path?.substringBefore('?') ?: ""
                    val override = if (path == tokenPath) {
                        tokenCalls.incrementAndGet()
                        this@FakeHttp.tokenOverrides.poll()
                    } else {
                        apiCalls.incrementAndGet()
                        this@FakeHttp.apiOverrides.poll()
                    }
                    if (override != null) return json(override.first, override.second)
                    val route = routes.firstOrNull { r ->
                        (!r.has("method") || r["method"].asText() == request.method) &&
                            (!r.has("path") || r["path"].asText() == path) &&
                            (!r.has("header") || request.getHeader(r["header"][0].asText()) == r["header"][1].asText())
                    } ?: return json(599, """{"error":"no fixture route for ${request.method} $path"}""")
                    return json(route.path("status").asInt(200), objectMapper.writeValueAsString(route["body"]))
                }
            }
            start()
        }.also { servers += it }

        private fun json(status: Int, body: String) =
            MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(body)
    }

    private fun fixtureFile(broker: String): File =
        File("../../conformance/fixtures/$broker.json").also { check(it.exists()) { "픽스처가 없다: ${it.absolutePath}" } }

    private class MutableClock(@Volatile private var now: Instant) : Clock() {
        override fun instant(): Instant = now
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId?): Clock = this
        fun advanceSeconds(seconds: Long) { now = now.plusSeconds(seconds) }
        fun set(instant: Instant) { now = instant }
    }

    companion object {
        private val BROKERS = listOf("next", "kis", "kiwoom", "nh", "db", "ls", "toss", "kb")

        @JvmStatic
        fun brokers(): List<String> = BROKERS

        @JvmStatic
        fun rateLimitCases(): List<Arguments> = BROKERS.map { Arguments.of(it, 429, "{}") } + listOf(
            Arguments.of("kis", 403, """{"error_code":"EGW00133","error_description":"접근토큰 발급 잠시 후 다시 시도하세요(1분당 1회)"}"""),
            Arguments.of("ls", 403, """{"rsp_cd":"IGW00201","rsp_msg":"초당 거래건수를 초과하였습니다."}"""),
        )
    }
}
