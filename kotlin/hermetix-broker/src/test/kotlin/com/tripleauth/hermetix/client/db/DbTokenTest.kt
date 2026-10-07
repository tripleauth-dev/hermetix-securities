package com.tripleauth.hermetix.client.db

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.broker.RateLimitError
import okhttp3.mockwebserver.Dispatcher
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.RecordedRequest
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.attribute.PosixFilePermissions
import java.time.Clock
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

/**
 * DB증권 토큰 수명주기 — 발급 1분 1건 제한(403 + IGW00201)을 레이트리밋으로 분류하고, 발급 실패 후 60초는 서버에 다시 묻지 않으며,
 * 발급 토큰을 파일로 저장해 같은 키를 쓰는 다른 프로세스(클라이언트)가 재사용하는지 (Python test_db_token.py 와 동일 시나리오).
 */
class DbTokenTest {

    private val objectMapper = ObjectMapper().registerModule(kotlinModule())
    private val servers = mutableListOf<FakeServer>()
    private val clock = MutableClock(Instant.ofEpochSecond(1_800_000_000))

    @AfterEach
    fun tearDown() {
        servers.forEach { it.server.shutdown() }
        DbTokenCache.directory = null
    }

    @Test
    fun `토큰 발급 유량 초과는 RateLimitError 이고 쿨다운 동안 서버에 다시 묻지 않는다`() {
        val server = serve(RATE_LIMITED)
        val sleeps = mutableListOf<Long>()
        val client = client(server, sleeper = { sleeps += it }) // 시계를 돌리지 않는 대기 — 재시도는 전부 쿨다운 안

        assertThatThrownBy { client.getAccount() }
            .isInstanceOfSatisfying(RateLimitError::class.java) { assertThat(it.retryAfterSeconds).isEqualTo(60) }
        assertThat(server.tokenCalls).isEqualTo(1)
        assertThat(sleeps.filter { it > 1 }).containsExactly(30_000L, 30_000L, 30_000L, 30_000L) // Retry-After 상한 30초 (1ms 는 쓰로틀)

        clock.advanceSeconds(20)
        assertThatThrownBy { client.getAccount() }
            .isInstanceOfSatisfying(RateLimitError::class.java) { assertThat(it.retryAfterSeconds).isEqualTo(40) }
        assertThat(server.tokenCalls).isEqualTo(1)
    }

    @Test
    fun `레이트리미터가 쿨다운을 기다린 뒤 다시 발급한다`() {
        val server = serve(RATE_LIMITED, issued("tok-1"))
        client(server).getAccount()
        assertThat(server.tokenCalls).isEqualTo(2)
        assertThat(server.apiTokens).containsExactly("Bearer tok-1")
    }

    @Test
    fun `인증 실패도 쿨다운 동안은 서버에 다시 묻지 않는다`() {
        val server = serve(REJECTED, issued("tok-1"))
        val client = client(server)
        repeat(3) { assertThatThrownBy { client.getAccount() }.isInstanceOf(AuthError::class.java) }
        assertThat(server.tokenCalls).isEqualTo(1)

        clock.advanceSeconds(61)
        client.getAccount()
        assertThat(server.tokenCalls).isEqualTo(2)
    }

    @Test
    fun `발급 토큰은 파일 캐시로 같은 키의 다른 클라이언트와 나눠 쓴다`(@TempDir dir: Path) {
        DbTokenCache.directory = dir
        client(serve(issued("tok-shared"))).getAccount()

        val files = Files.list(dir).use { it.toList() }
        assertThat(files).hasSize(1)
        assertThat(files[0].fileName.toString()).startsWith("db-")
        assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(files[0]))).isEqualTo("rw-------")
        val saved = objectMapper.readTree(files[0].toFile())
        assertThat(saved.fieldNames().asSequence().toList()).containsExactlyInAnyOrder("access_token", "expires_at") // 키·시크릿은 담지 않는다
        assertThat(saved.path("access_token").asText()).isEqualTo("tok-shared")
        assertThat(saved.path("expires_at").asDouble()).isEqualTo((clock.instant().epochSecond + 86400).toDouble())

        val second = serve(issued("tok-other"))
        client(second).getAccount()
        assertThat(second.tokenCalls).isZero()
        assertThat(second.apiTokens).containsExactly("Bearer tok-shared")

        val otherKey = serve(issued("tok-other"))
        client(otherKey, appKey = "k2").getAccount()
        assertThat(otherKey.tokenCalls).isEqualTo(1)
    }

    @Test
    fun `만료가 가까운 캐시 토큰은 쓰지 않는다`(@TempDir dir: Path) {
        DbTokenCache.directory = dir
        client(serve(issued("tok-old"))).getAccount()
        clock.advanceSeconds(86400 - 300)

        val server = serve(issued("tok-new"))
        client(server).getAccount()
        assertThat(server.tokenCalls).isEqualTo(1)
        assertThat(server.apiTokens).containsExactly("Bearer tok-new")
    }

    /** 레이트리미터의 대기가 기본으로 시계를 앞으로 돌린다 */
    private fun client(server: FakeServer, appKey: String = "k", sleeper: (Long) -> Unit = { clock.advanceMillis(it) }) =
        DbApiClient(
            DbApiProperties(baseUrl = server.server.url("/").toString().trimEnd('/'), appKey = appKey, appSecret = "s", throttleMillis = 1),
            objectMapper, clock, sleeper,
        )

    private fun serve(vararg tokenReplies: Pair<Int, String>) = FakeServer(tokenReplies.toMutableList()).also { servers += it }

    /** 토큰 응답을 차례로 돌려주고(마지막 응답 반복), 업무 호출은 잔고 응답으로 받는 가짜 서버 */
    private class FakeServer(private val tokenReplies: MutableList<Pair<Int, String>>) {
        @Volatile var tokenCalls = 0
        val apiTokens = mutableListOf<String>()
        val server = MockWebServer().apply {
            dispatcher = object : Dispatcher() {
                override fun dispatch(request: RecordedRequest): MockResponse {
                    val (status, body) = if (request.path == "/oauth2/token") {
                        tokenCalls++
                        if (tokenReplies.size > 1) tokenReplies.removeAt(0) else tokenReplies[0]
                    } else {
                        apiTokens += request.getHeader("authorization").orEmpty()
                        200 to BALANCE
                    }
                    return MockResponse().setResponseCode(status).setHeader("Content-Type", "application/json").setBody(body)
                }
            }
            start()
        }
    }

    private class MutableClock(private var now: Instant) : Clock() {
        override fun instant(): Instant = now
        override fun getZone(): ZoneId = ZoneOffset.UTC
        override fun withZone(zone: ZoneId?): Clock = this
        fun advanceSeconds(seconds: Long) { now = now.plusSeconds(seconds) }
        fun advanceMillis(millis: Long) { now = now.plusMillis(millis) }
    }

    companion object {
        private const val BALANCE = """{"rsp_cd":"00000","rsp_msg":"정상","Out":{"DpsastAmt":"1000","Dps2":"1000","TotEvalAmt":"0"}}"""
        private val RATE_LIMITED = 403 to """{"rsp_cd":"IGW00201","rsp_msg":"초당 거래건수를 초과하였습니다."}"""
        private val REJECTED = 401 to """{"rsp_cd":"IGW00121","rsp_msg":"유효하지 않은 앱키입니다."}"""
        private fun issued(token: String) = 200 to """{"access_token":"$token","token_type":"Bearer","expires_in":86400}"""
    }
}
