package com.tripleauth.hermetix.client.db

import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.time.Instant

/**
 * DB증권 발급 토큰 파일 캐시 — 같은 키를 쓰는 여러 프로세스가 24시간 토큰을 나눠 쓴다 (공식 SDK 의 `.dbsec_token.json` 과 같은 목적).
 * 토큰 발급은 1분 1건이라 프로세스마다 새로 발급하면 두 번째부터 `IGW00201` 이 난다.
 *
 * 위치 `~/.hermetix/tokens/db-<sha256(appKey) 앞 16자>.json`, 내용은 `access_token`·`expires_at`(epoch 초)뿐 — 키·시크릿은 담지 않는다.
 * 디렉터리 700, 파일 600. 읽기·쓰기 실패는 조용히 무시한다 (캐시가 없을 때와 같다).
 */
internal object DbTokenCache {

    /** null 이면 파일 캐시를 쓰지 않는다 (테스트) */
    @Volatile
    var directory: Path? = runCatching { Paths.get(System.getProperty("user.home"), ".hermetix", "tokens") }.getOrNull()

    fun load(objectMapper: ObjectMapper, appKey: String, now: Instant, refreshMarginSeconds: Long): Pair<String, Instant>? {
        val path = pathFor(appKey) ?: return null
        return runCatching {
            if (!Files.exists(path)) return null
            val node = objectMapper.readTree(path.toFile())
            val token = node.path("access_token").asText("")
            val expiresAt = Instant.ofEpochMilli((node.path("expires_at").asDouble(0.0) * 1000).toLong())
            if (token.isNotBlank() && expiresAt.minusSeconds(refreshMarginSeconds).isAfter(now)) token to expiresAt else null
        }.getOrNull()
    }

    fun save(objectMapper: ObjectMapper, appKey: String, token: String, expiresAt: Instant) {
        val path = pathFor(appKey) ?: return
        runCatching {
            val dir = path.parent
            if (!Files.exists(dir)) {
                Files.createDirectories(dir)
                runCatching { Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------")) }
            }
            val tmp = dir.resolve("${path.fileName}.${ProcessHandle.current().pid()}.tmp")
            Files.deleteIfExists(tmp)
            Files.createFile(tmp)
            runCatching { Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("rw-------")) }
            Files.write(tmp, objectMapper.writeValueAsBytes(mapOf("access_token" to token, "expires_at" to expiresAt.toEpochMilli() / 1000.0)))
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        }
    }

    private fun pathFor(appKey: String): Path? {
        val dir = directory ?: return null
        val hash = MessageDigest.getInstance("SHA-256").digest(appKey.toByteArray()).joinToString("") { "%02x".format(it) }
        return dir.resolve("db-${hash.take(16)}.json")
    }
}
