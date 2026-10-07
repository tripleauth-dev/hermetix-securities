package com.tripleauth.hermetix

import com.tripleauth.hermetix.broker.UsageTelemetry
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext

/**
 * 테스트 실행은 운영 텔레메트리로 보내지 않는다 — 픽스처 재생 호출이 사용량 랭킹·API 현황에 섞이지 않도록 전송 함수를 버린다.
 * `junit-platform.properties` 의 자동 감지로 모든 테스트 클래스에 걸린다.
 * 전송 자체를 검증하는 테스트(UsageTelemetryTest)는 자기 transport 로 다시 바꿔 쓴다.
 */
class TelemetryIsolationExtension : BeforeAllCallback {
    override fun beforeAll(context: ExtensionContext) {
        UsageTelemetry.transport = {}
    }
}
