package com.tripleauth.hermetix.client

import com.fasterxml.jackson.databind.ObjectMapper
import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.broker.BrokerApiException
import com.tripleauth.hermetix.broker.BrokerUsage
import com.tripleauth.hermetix.broker.RateLimitError
import com.tripleauth.hermetix.client.dto.ApiErrorEnvelope
import com.tripleauth.hermetix.client.dto.OAuthErrorResponse
import com.tripleauth.hermetix.client.dto.TokenResponse
import io.github.oshai.kotlinlogging.KotlinLogging
import org.springframework.http.MediaType
import org.springframework.util.LinkedMultiValueMap
import org.springframework.web.client.RestClient
import java.time.Clock
import java.time.Instant

/**
 * 넥스트증권 액세스 토큰 (OAuth 2.0 Client Credentials) — 발급만 여기서 하고, 캐시·갱신·쿨다운·거부 토큰 폐기는
 * 여덟 어댑터 공용 수명주기 [BrokerTokenManager] 에 맡긴다.
 *
 * 공개 스펙 v1.3: 토큰 유효기간 12시간(expires_in=43200), refresh 토큰 없음.
 * 토큰 발급 API 만 에러 형식이 다르다 — 400/401 은 OAuth 표준 `{error, error_description}`,
 * 429/5xx 는 플랫폼 에러 엔벨로프. 429 는 [RateLimitError](서버 `Retry-After`, 없으면 60초), 그 외는 [AuthError] 로 변환한다.
 */
class TokenManager internal constructor(
    private val properties: NextApiProperties,
    private val objectMapper: ObjectMapper,
    clock: Clock,
) {

    constructor(properties: NextApiProperties, objectMapper: ObjectMapper) : this(properties, objectMapper, Clock.systemUTC())

    private val logger = KotlinLogging.logger { }

    private val restClient = RestClient.builder()
        .baseUrl(properties.baseUrl)
        .build()

    /** 사용량 텔레메트리 핸들 — [NextApiClient] 가 붙인다. 실제 토큰 발급이 `auth` op 으로 세어진다 (docs/telemetry.md) */
    @Volatile
    var usage: BrokerUsage? = null

    internal val tokens = BrokerTokenManager("next", properties.clientId, properties.tokenRefreshMarginSeconds, clock) { issue(clock) }

    fun getToken(): String = tokens.get()

    /** 메모리에 든 토큰을 버린다 — 다음 [getToken] 이 파일 캐시 또는 새 발급으로 채운다 */
    fun invalidate() {
        tokens.currentToken?.let { tokens.invalidate(it) }
    }

    private fun issue(clock: Clock): Pair<String, Instant> {
        val u = usage
        return if (u == null) doIssue(clock) else u.measure("auth") { doIssue(clock) }
    }

    private fun doIssue(clock: Clock): Pair<String, Instant> {
        val form = LinkedMultiValueMap<String, String>().apply {
            add("grant_type", "client_credentials")
            add("client_id", properties.clientId)
            add("client_secret", properties.clientSecret)
        }

        val response = restClient.post()
            .uri("/v1/oauth/token")
            .header(NextApiClient.REQUEST_ID_HEADER, NextApiClient.newRequestId())
            .contentType(MediaType.APPLICATION_FORM_URLENCODED)
            .body(form)
            .exchange { _, res ->
                val body = res.body.readAllBytes()
                if (res.statusCode.is2xxSuccessful) {
                    objectMapper.readValue(body, TokenResponse::class.java)
                } else {
                    val retryAfter = res.headers.getFirst("Retry-After")?.trim()?.toLongOrNull()
                    throw parseTokenError(res.statusCode.value(), body, retryAfter)
                }
            }

        logger.info { "token issued / expiresIn=${response.expiresIn}s" }
        return response.accessToken to clock.instant().plusSeconds(response.expiresIn)
    }

    private fun parseTokenError(status: Int, body: ByteArray, retryAfterSeconds: Long?): BrokerApiException {
        // OAuth 표준 형식 (400/401): {"error":"invalid_client","error_description":"..."}
        runCatching { objectMapper.readValue(body, OAuthErrorResponse::class.java) }
            .getOrNull()
            ?.takeIf { it.error != null }
            ?.let { return AuthError(status, it.error, "Next 토큰 발급 실패(${it.error}): ${it.errorDescription ?: ""}") }

        // 플랫폼 엔벨로프 (429/5xx): {"error":{"code":..,"message":..}}
        val error = runCatching { objectMapper.readValue(body, ApiErrorEnvelope::class.java).error }.getOrNull()
        val message = "Next 토큰 발급 실패(${error?.code}): ${error?.message ?: ""}"
        return if (status == 429) {
            RateLimitError(status, error?.code, message, retryAfterSeconds ?: BrokerTokenManager.FAILURE_COOLDOWN_SECONDS)
        } else {
            AuthError(status, error?.code, message)
        }
    }
}
