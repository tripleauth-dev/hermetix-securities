package com.tripleauth.hermetix.client

import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.nio.file.attribute.PosixFilePermissions
import java.security.MessageDigest
import java.time.Instant

/**
 * 발급 토큰 파일 캐시 — 같은 키를 쓰는 여러 프로세스가 토큰을 나눠 쓴다
 * (KIS·DB 는 발급 1분 1건, 토스는 재발급 시 이전 토큰 무효라 프로세스마다 발급하면 서로 부딪친다).
 *
 * 위치 `~/.hermetix/tokens/<broker>-<sha256(key) 앞 16자>.json`, 내용은 `access_token`·`expires_at`(epoch 초)뿐 — 키·시크릿은 담지 않는다.
 * 디렉터리 700, 파일 600, 임시 파일에 쓴 뒤 원자적으로 옮긴다. 읽기·쓰기·삭제 실패는 조용히 무시한다 (캐시가 없을 때와 같다).
 */
internal object TokenCache {

    /** null 이면 파일 캐시를 쓰지 않는다 (테스트) */
    @Volatile
    var directory: Path? = runCatching { Paths.get(System.getProperty("user.home"), ".hermetix", "tokens") }.getOrNull()

    private val objectMapper = ObjectMapper()

    fun path(broker: String, key: String): Path? {
        val dir = directory ?: return null
        val hash = MessageDigest.getInstance("SHA-256").digest(key.toByteArray()).joinToString("") { "%02x".format(it) }
        return dir.resolve("$broker-${hash.take(16)}.json")
    }

    /** 저장된 (토큰, 만료 시각) — 만료 여부는 호출하는 쪽이 판단한다 */
    fun load(path: Path?): Pair<String, Instant>? {
        if (path == null) return null
        return runCatching {
            if (!Files.exists(path)) return null
            val node = objectMapper.readTree(path.toFile())
            val token = node.path("access_token").asText("")
            val expiresAt = Instant.ofEpochMilli((node.path("expires_at").asDouble(0.0) * 1000).toLong())
            if (token.isNotBlank()) token to expiresAt else null
        }.getOrNull()
    }

    fun save(path: Path?, token: String, expiresAt: Instant) {
        if (path == null) return
        runCatching {
            val dir = path.parent
            if (!Files.exists(dir)) {
                Files.createDirectories(dir)
                runCatching { Files.setPosixFilePermissions(dir, PosixFilePermissions.fromString("rwx------")) }
            }
            val tmp = dir.resolve("${path.fileName.toString().removeSuffix(".json")}.${ProcessHandle.current().pid()}.tmp")
            Files.deleteIfExists(tmp)
            Files.createFile(tmp)
            runCatching { Files.setPosixFilePermissions(tmp, PosixFilePermissions.fromString("rw-------")) }
            Files.write(tmp, objectMapper.writeValueAsBytes(mapOf("access_token" to token, "expires_at" to expiresAt.toEpochMilli() / 1000.0)))
            Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
        }
    }

    fun delete(path: Path?) {
        if (path == null) return
        runCatching { Files.deleteIfExists(path) }
    }
}
