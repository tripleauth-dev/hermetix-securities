package com.tripleauth.hermetix.client

import com.tripleauth.hermetix.broker.AuthError
import com.tripleauth.hermetix.broker.BrokerApiException
import com.tripleauth.hermetix.broker.RateLimitError
import java.time.Clock
import java.time.Duration
import java.time.Instant

/**
 * 증권사 접근 토큰 수명주기 — 여덟 어댑터 공용 (Python `hermetix.tokens.TokenManager` 와 같은 동작).
 *
 * - 메모리 → 파일 캐시([TokenCache]) → 발급 순으로 찾는다. 파일 캐시는 같은 키를 쓰는 여러 프로세스가 토큰을 나눠 쓰게 한다
 * - 발급에 실패하면 [FAILURE_COOLDOWN_SECONDS] 동안 서버에 다시 묻지 않고 같은 오류(유량 초과면 남은 시간)를 돌려준다
 * - 업무 호출이 토큰을 거부([AuthError])하면 그 토큰을 메모리·파일에서 버리고 한 번 다시 시도한다 ([call])
 *
 * [issue] 는 (토큰, 만료 시각) 을 돌려주거나 [AuthError] / [RateLimitError] 를 던진다 — 텔레메트리 `auth` 측정은 [issue] 가 한다.
 */
internal class BrokerTokenManager(
    private val broker: String,
    private val key: String,
    private val refreshMarginSeconds: Long,
    private val clock: Clock,
    private val issue: () -> Pair<String, Instant>,
) {

    /** (토큰, 갱신 시각) — 한 번에 읽도록 묶어 둔다 */
    @Volatile
    private var current: Pair<String, Instant>? = null

    /** 마지막 발급 실패 (쿨다운 종료 시각, 오류) */
    private var failure: Pair<Instant, BrokerApiException>? = null

    /** 메모리에 들고 있는 토큰 (없으면 null) */
    val currentToken: String? get() = current?.first

    fun get(): String {
        current?.let { (token, refreshAt) -> if (clock.instant().isBefore(refreshAt)) return token }
        return refresh()
    }

    @Synchronized
    private fun refresh(): String {
        val now = clock.instant()
        current?.let { (token, refreshAt) -> if (now.isBefore(refreshAt)) return token }

        val path = TokenCache.path(broker, key)
        TokenCache.load(path)?.let { (token, expiresAt) ->
            val refreshAt = expiresAt.minusSeconds(refreshMarginSeconds)
            if (token != current?.first && now.isBefore(refreshAt)) {
                current = token to refreshAt
                return token
            }
        }
        failure?.let { (until, error) -> if (now.isBefore(until)) throw cooldownError(error, Duration.between(now, until)) }

        val (token, expiresAt) = try {
            issue()
        } catch (e: BrokerApiException) {
            failure = clock.instant().plusSeconds(FAILURE_COOLDOWN_SECONDS) to e
            throw e
        }
        failure = null
        // 만료 전 재발급에 남은 시간 그대로의 같은 토큰을 돌려주는 서버(DB·KIS)는 여유 구간에서 매번 재발급하지 않도록 만료까지 쓴다
        val refreshAt = expiresAt.minusSeconds(refreshMarginSeconds)
        current = token to if (refreshAt.isAfter(clock.instant())) refreshAt else expiresAt
        TokenCache.save(path, token, expiresAt)
        return token
    }

    /** 업무 호출이 거부한 토큰을 버린다 — 다른 프로세스가 새로 저장한 토큰은 지우지 않는다 */
    @Synchronized
    fun invalidate(token: String) {
        if (current?.first == token) current = null
        val path = TokenCache.path(broker, key)
        if (TokenCache.load(path)?.first == token) TokenCache.delete(path)
    }

    /** 토큰을 받아 [block] 을 부르고, 업무 호출이 토큰을 거부하면 버린 뒤 한 번 다시 부른다 */
    fun <T> call(block: (String) -> T): T {
        val token = get()
        return try {
            block(token)
        } catch (e: AuthError) {
            invalidate(token)
            block(get())
        }
    }

    /** 쿨다운 중 재요청 — 서버에 묻지 않고 같은 종류의 오류를 남은 시간(올림 초)과 함께 돌려준다 */
    private fun cooldownError(failure: BrokerApiException, remaining: Duration): BrokerApiException =
        if (failure is RateLimitError) {
            RateLimitError(failure.httpStatus, failure.errorCode, failure.message, (remaining.toMillis() + 999) / 1000)
        } else {
            failure
        }

    companion object {
        /** 발급 실패 후 서버에 다시 묻지 않는 시간 (KIS·DB 발급 1분 1건 제한에 맞춘다) */
        const val FAILURE_COOLDOWN_SECONDS = 60L
    }
}
