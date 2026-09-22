package com.tripleauth.hermetix.broker

import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * 사용량 텔레메트리 — 어느 증권사가 얼마나 쓰이는지 시간 버킷으로 합산해 `hermetix-service` 로 보낸다.
 * 계약(필드·전송 규칙·보내지 않는 것)은 `docs/telemetry.md` 가 정본이다.
 *
 * - 매매 경로와 분리: 카운터는 메모리, 전송은 데몬 스레드. 실패는 조용히 버리고 큐를 쌓지 않는다
 * - 개인정보·매매 내용 없음: 브로커·환경·호출 종류·건수·에러 분류·응답 시간 분포·스트림 건수·SDK 버전·설치 ID 뿐
 * - 기본 배포본은 항상 켜져 있다 (설정 없음). 테스트는 [transport] 와 [flushNow] 로 전송을 가로챈다
 */
object UsageTelemetry {

    const val ENDPOINT = "https://service-api-prod.hermetix.dev/v1/usage"
    const val SCHEMA = 1
    const val SDK_LANGUAGE = "kotlin"
    /** 요청 서명 키 (docs/telemetry.md "요청 서명") — 공개 SDK 라 비밀이 아니며 스팸·스캐너를 거르는 문턱이다 */
    const val SIGNING_KEY_ID = "v1"
    const val SIGNING_KEY = "d97f20cb942540462ea83648ee30a9786bd658b3dc813f74ef011845da258503"
    private const val FLUSH_INTERVAL_SECONDS = 60L
    private const val LATENCY_SAMPLES = 256

    private val logger = KotlinLogging.logger { }
    private val objectMapper = ObjectMapper()

    val sdkVersion: String by lazy {
        runCatching { UsageTelemetry::class.java.getResourceAsStream("/hermetix-version.txt")?.bufferedReader()?.readText()?.trim() }
            .getOrNull()?.ifBlank { null } ?: "unknown"
    }

    val installationId: String by lazy { loadOrCreateInstallationId() }

    /** 전송 함수 — 기본은 HTTP POST. 테스트에서 교체한다 */
    @Volatile
    var transport: (String) -> Unit = ::post

    private val buckets = ConcurrentHashMap<BucketKey, BucketStats>()

    private val httpClient: HttpClient by lazy { HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build() }

    private val scheduler = Executors.newSingleThreadScheduledExecutor { r -> Thread(r, "hermetix-telemetry").apply { isDaemon = true } }

    init {
        scheduler.scheduleWithFixedDelay({ runCatching { flushNow() } }, FLUSH_INTERVAL_SECONDS, FLUSH_INTERVAL_SECONDS, TimeUnit.SECONDS)
        Runtime.getRuntime().addShutdownHook(Thread({ runCatching { flushNow() } }, "hermetix-telemetry-shutdown"))
    }

    /** 어댑터가 자기 브로커·환경으로 만들어 두고 쓰는 핸들 */
    fun forBroker(brokerId: String, environment: TradingEnvironment): BrokerUsage = BrokerUsage(brokerId, environment)

    /** 지금까지 쌓인 버킷을 페이로드 JSON 으로 만들고 비운다. 비어 있으면 null */
    fun drain(now: Instant = Instant.now()): String? {
        val keys = buckets.keys.toList()
        if (keys.isEmpty()) return null
        val list = keys.mapNotNull { key -> buckets.remove(key)?.let { key to it } }
        if (list.isEmpty()) return null
        val payload = mapOf(
            "schema" to SCHEMA,
            "installationId" to installationId,
            "sdk" to mapOf("language" to SDK_LANGUAGE, "version" to sdkVersion),
            "sentAt" to now.toString(),
            "buckets" to list.map { (key, stats) -> stats.toJson(key) },
        )
        return objectMapper.writeValueAsString(payload)
    }

    /** 즉시 전송 시도 (스케줄러·종료 훅·테스트용). 비어 있으면 아무것도 안 한다 */
    fun flushNow() {
        val body = drain() ?: return
        runCatching { transport(body) }.onFailure { logger.debug { "telemetry send skipped: ${it.message}" } }
    }

    /** 어댑터 계측용 — 보통 [BrokerUsage.measure] 를 통해 호출된다 */
    fun record(brokerId: String, environment: TradingEnvironment, op: String, nanos: Long, failure: Throwable?) {
        val stats = bucket(brokerId, environment)
        val opStats = stats.ops.computeIfAbsent(op) { OpStats() }
        if (failure == null) opStats.ok.incrementAndGet()
        else opStats.errors.computeIfAbsent(classify(failure)) { AtomicLong() }.incrementAndGet()
        opStats.latency.add(nanos / 1_000_000)
    }

    fun recordStream(brokerId: String, environment: TradingEnvironment, channel: StreamChannel, subscriptions: Long, messages: Long) {
        val s = bucket(brokerId, environment).streams.computeIfAbsent(channel.name) { StreamStats() }
        if (subscriptions != 0L) s.subscriptions.addAndGet(subscriptions)
        if (messages != 0L) s.messages.addAndGet(messages)
    }

    fun recordReconnect(brokerId: String, environment: TradingEnvironment) {
        bucket(brokerId, environment).reconnects.incrementAndGet()
    }

    /** 예외를 계약의 에러 분류로 (docs/telemetry.md) */
    fun classify(failure: Throwable): String = when (failure) {
        is RateLimitError -> "rate_limit"
        is AuthError -> "auth"
        is MarketClosedError -> "market_closed"
        is InsufficientFundsError -> "insufficient_funds"
        is InvalidOrderError -> "invalid_order"
        is OrderNotFoundError -> "order_not_found"
        is BrokerApiException -> "other"
        is java.io.IOException, is java.net.http.HttpTimeoutException, is org.springframework.web.client.ResourceAccessException -> "network"
        else -> if (failure.cause != null && failure.cause !== failure) classify(failure.cause!!) else "other"
    }

    private fun bucket(brokerId: String, environment: TradingEnvironment): BucketStats =
        buckets.computeIfAbsent(BucketKey(Instant.now().truncatedTo(ChronoUnit.HOURS), brokerId, environment.name)) { BucketStats() }

    /** `hex(HMAC-SHA256(key, timestamp + "\n" + body))` — 계약의 요청 서명 */
    fun sign(body: String, timestampSeconds: Long): String {
        val mac = javax.crypto.Mac.getInstance("HmacSHA256")
        mac.init(javax.crypto.spec.SecretKeySpec(SIGNING_KEY.toByteArray(Charsets.UTF_8), "HmacSHA256"))
        val digest = mac.doFinal("$timestampSeconds\n$body".toByteArray(Charsets.UTF_8))
        return digest.joinToString("") { "%02x".format(it) }
    }

    private fun post(body: String) {
        val timestamp = Instant.now().epochSecond
        val request = HttpRequest.newBuilder(URI.create(ENDPOINT))
            .timeout(Duration.ofSeconds(3))
            .header("Content-Type", "application/json")
            .header("User-Agent", "hermetix-$SDK_LANGUAGE/$sdkVersion")
            .header("X-Hermetix-Key-Id", SIGNING_KEY_ID)
            .header("X-Hermetix-Timestamp", timestamp.toString())
            .header("X-Hermetix-Signature", sign(body, timestamp))
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build()
        httpClient.send(request, HttpResponse.BodyHandlers.discarding())
    }

    private fun loadOrCreateInstallationId(): String {
        val path = runCatching { Path.of(System.getProperty("user.home"), ".hermetix", "installation-id") }.getOrNull()
            ?: return UUID.randomUUID().toString()
        return runCatching {
            if (Files.exists(path)) {
                Files.readString(path).trim().takeIf { runCatching { UUID.fromString(it) }.isSuccess }
            } else null
        }.getOrNull() ?: UUID.randomUUID().toString().also { id ->
            runCatching {
                Files.createDirectories(path.parent)
                Files.writeString(path, id)
            }
        }
    }

    private data class BucketKey(val hour: Instant, val broker: String, val environment: String)

    private class BucketStats {
        val ops = ConcurrentHashMap<String, OpStats>()
        val streams = ConcurrentHashMap<String, StreamStats>()
        val reconnects = AtomicLong()

        fun toJson(key: BucketKey): Map<String, Any> = mapOf(
            "hour" to key.hour.toString(),
            "broker" to key.broker,
            "environment" to key.environment,
            "ops" to ops.entries.sortedBy { it.key }.map { (op, s) ->
                mapOf(
                    "op" to op,
                    "ok" to s.ok.get(),
                    "errors" to s.errors.entries.associate { it.key to it.value.get() },
                    "latencyMs" to s.latency.summary(),
                )
            },
            "streams" to streams.entries.sortedBy { it.key }.map { (ch, s) ->
                mapOf("channel" to ch, "subscriptions" to s.subscriptions.get(), "messages" to s.messages.get())
            },
            "reconnects" to reconnects.get(),
        )
    }

    private class OpStats {
        val ok = AtomicLong()
        val errors = ConcurrentHashMap<String, AtomicLong>()
        val latency = LatencyReservoir()
    }

    private class StreamStats {
        val subscriptions = AtomicLong()
        val messages = AtomicLong()
    }

    /** op 당 최근 [LATENCY_SAMPLES] 개 표본만 보관 — p50/p95 는 전송 시점에 계산 */
    private class LatencyReservoir {
        private val samples = LongArray(LATENCY_SAMPLES)
        private var count = 0L

        @Synchronized
        fun add(ms: Long) {
            samples[(count % LATENCY_SAMPLES).toInt()] = ms
            count++
        }

        @Synchronized
        fun summary(): Map<String, Long> {
            val n = minOf(count, LATENCY_SAMPLES.toLong()).toInt()
            if (n == 0) return mapOf("count" to 0L, "p50" to 0L, "p95" to 0L)
            val sorted = samples.copyOf(n).sortedArray()
            fun pct(p: Double) = sorted[((n - 1) * p).toInt().coerceIn(0, n - 1)]
            return mapOf("count" to count, "p50" to pct(0.50), "p95" to pct(0.95))
        }
    }
}

/**
 * 브로커 어댑터 하나가 쥐는 계측 핸들. 공개 메서드를 [measure] 로 감싸면 건수·에러 분류·응답 시간이 쌓인다.
 * `inline` 이라 본문 안의 `return` 이 그대로 동작하고, 기록은 `finally` 에서 하므로 어떤 경로로 나가도 세어진다.
 */
class BrokerUsage(val brokerId: String, val environment: TradingEnvironment) {

    inline fun <T> measure(op: String, block: () -> T): T {
        val start = System.nanoTime()
        var failure: Throwable? = null
        try {
            return block()
        } catch (e: Throwable) {
            failure = e
            throw e
        } finally {
            UsageTelemetry.record(brokerId, environment, op, System.nanoTime() - start, failure)
        }
    }

    fun streamSubscribed(channel: StreamChannel, count: Int = 1) = UsageTelemetry.recordStream(brokerId, environment, channel, count.toLong(), 0)

    fun streamMessage(channel: StreamChannel, count: Int = 1) = UsageTelemetry.recordStream(brokerId, environment, channel, 0, count.toLong())

    fun reconnected() = UsageTelemetry.recordReconnect(brokerId, environment)
}
