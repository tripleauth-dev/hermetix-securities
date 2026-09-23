# Hermetix 로드맵

목표는 **국내외 증권사 모의투자를 하나의 인터페이스로 통일한 연결 계층**이 되어, 그 위의 모든 봇/도구의 기반이 되는 것입니다.

지향하는 설계 원칙:

1. 어댑터가 하나 늘 때마다 모든 사용자가 이득을 보는 **네트워크 효과 구조**
2. 브로커마다 되는 것/안 되는 것을 코드로 선언하는 **capability 정직성**
3. 어느 브로커든 같은 방식으로 처리하는 **통일된 에러 체계**
4. "브로커 추가 = 레시피 + 표준 테스트 통과"인 **기여 플레이북**
5. 전략 엔진에 종속되지 않는 **독립적인 연결 계층** — 단, 연결 계층만 제공하는 프로젝트들과 달리 전략 엔진과 공식 전략을 함께 제공합니다 ("바로 돌아가는 봇"이 진입 장벽을 낮추는 무기)

---

## Phase A — 코어 재편 (0.5.x) ✅ 완료

- [x] `hermetix-broker` / `hermetix-engine` 모듈 분리 — 봇 없이 연결 계층만 쓰는 사용자(시세 수집, 대시보드, 알림) 지원
- [x] `BrokerCapabilities` 선언 — 캔들 주기, clientOrderId, 네이티브 브라켓 등을 코드로 선언하고 엔진이 적응. 기동 시 전략-브로커 호환성 검증 (fail-fast)
- [x] 에러 체계 통일 — `RateLimitError`(자동 백오프) / `MarketClosedError`(조용히 스킵, 비상정지 카운트 제외) / `InsufficientFundsError` / `InvalidOrderError` / `AuthError` / `OrderNotFoundError`
- [x] (0.6.0) 거래 환경 `paper | live` — 어댑터가 호스트·TR ID 를 고르고, LIVE 는 `hermetix.live.enabled` 명시 동의 없이는 기동 거부
- [x] (0.6.0) 주문 금액 상한 `RiskGuard` — 1건 / 일일 누적
- [x] (0.6.0) `MARKET:CODE` 심볼 접두 — 다중 시장 브로커 대비, 기존 전략 무변경

## Phase B — 커뮤니티 성장 엔진

- [x] 어댑터 컨포먼스 테스트 킷 — `conformance/` 골든 픽스처 + 네 언어 공통 시나리오 (`BrokerConformance` 는 test-fixtures 아티팩트로 배포). 새 어댑터는 통과가 기여 조건
- [x] 레이트리밋 공용 부품 `RateLimiter` — 쓰로틀 + 백오프 + `Retry-After`
- [x] 브로커 지원 매트릭스 (README 최상단)
- [x] "새 브로커 요청" 이슈 템플릿 (`.github/ISSUE_TEMPLATE/broker-request.md`)
- [x] 어댑터 추가 가이드 문서화 — `conformance/README.md` (실측 → 픽스처 → 구현 → 컨포먼스 → 등록)

## Phase C — 브로커 커버리지 확장

- [x] 국내 증권사 REST 오픈API 실태 조사 — [claudedocs/korean-broker-openapi-survey-2026-09.md](claudedocs/korean-broker-openapi-survey-2026-09.md)
- [x] 조사 결과 1·2순위 어댑터 추가 — `nh`(NH PLUG), `db`(DB증권): 공식 SDK 기반 문서 구현, 네 언어 컨포먼스 통과
- [x] 3순위 어댑터 추가 — `ls`(LS증권, TR 카탈로그 기반) · `toss`(토스증권, 실전 전용) · `kb`(KB증권 오픈베타, 실전 전용): 문서 기반 구현, 네 언어 컨포먼스 통과
- 위 다섯 어댑터의 실측 검증은 **해당 증권사 계좌를 가진 사용자의 제보로 진행**한다 — 메인테이너가 계좌를 새로 개설하지 않는다. 제보(이슈 템플릿)가 오면 실측 응답으로 골든 픽스처를 교체하고 README 상태를 ⚠️ 미검증 → ✅ 검증으로 승격한다
- [x] (0.11.3) 실측 제보 도구 `python -m hermetix.verify <broker>` — 실서버 검증 흐름 + 마스킹된 요청·응답·프레임 파일, 실측 제보 이슈 템플릿, 검증 상태 3단계(조회·주문·실시간)와 `verifiedAt` 을 README·API 현황에 표시
- [ ] 미검증 5개 중 3개 이상 검증 (6주 목표) — 증권사별 커뮤니티 공지 + 증권사 오픈API 팀에 테스트 계정 요청
- [ ] next·kis·kiwoom 실전 계좌 스모크(원거리 지정가 → 취소) → README 에 실전 검증 구분
- [ ] 평일 장중 모의 스모크 자동 실행(로컬 launchd) + 실패 알림, API 현황에 마지막 검증일
- [ ] 토스·KB 에 모의투자 샌드박스가 출시되면 `environments` 에 PAPER 추가
- [ ] 넥스트증권 `/v2` 연동: `POST /v2/orders/advanced`(BRACKET) 네이티브 브라켓 전환, `POST /v2/kill-switch` 를 `TradingGuard.halt()` 에 연결 (공개 스펙 v1.3 시점 서버 제공 확인)

## Phase D — 실시간 계층 (Hermetix Pro)

- [x] (0.8.0) 웹소켓 스트리밍 추상화 — `MarketStream` / `StreamingBrokerClient` SPI, `BrokerCapabilities.streams` 선언, JDK 내장 웹소켓 기반 재연결 공용 부품. **넥스트증권은 공개 스펙 v1.3 에 웹소켓이 없어 대상에서 제외** — KIS(`H0STCNT0`)·키움(`0B`) 체결가 채널 구현, 2026-09-14 모의 장중 실측 통과
- [x] (0.8.0) 이벤트 기반 틱 — `StrategySpec.trigger = ON_TRADE`: 체결가 틱마다 전략 호출, 틱 합치기 + `minTickInterval`, 스트림 단절 시 폴링 안전망
- [x] KIS·키움 모의 웹소켓 실측 → 픽스처 `stream` 섹션을 실측 프레임으로 교체 → README 실시간 열 ✅
- [x] Python / JS / Go 포팅 — 같은 SPI·엔진 트리거·파서, 픽스처 `stream` 섹션(실측 프레임)으로 네 언어 파서 일치 검증. Python 은 선택 의존성 `websockets`(`pip install hermetix[stream]`), JS 는 Node 22 내장 WebSocket, Go 는 `github.com/coder/websocket`
- [x] (0.8.x) 2차 채널 — 호가(KIS `H0STASP0`/키움 `0D`, `StrategySpec.orderBook` → `context.orderBook()`) 2026-09-14 모의 실측 통과. 주문 통보(KIS `H0STCNI9`/키움 `00`, 엔진 자동 구독 → 브라켓·KIS 메모리 추적 즉시 반영) 는 구현·등록 확인까지 — **통보 프레임 실측 전** (KIS 는 HTS ID 필요, 이 키움 모의 계좌는 공매도 이수 전용이라 주문 불가)
- [x] 2차 채널 Python / JS / Go 포팅 — 같은 SPI·파서·엔진 연결, 픽스처 `stream.orderBook`(실측)·`stream.orderEvents`(문서 기반) 로 네 언어 일치 검증. KIS 통보 복호화는 Python `cryptography`(stream extra), JS·Go 는 표준 라이브러리
- [ ] 주문 통보 실측 → 픽스처 `orderEvents.measured=true`, README ⚠️ 주문통보 → ✅ (KIS HTS ID + 일반 키움 모의 계좌 필요)
- [x] (0.10.0) nh·db·ls·toss 실시간 스트림 — 공식 문서·SDK·AsyncAPI 로 조사(2026-09-14)해 체결·호가·주문통보를 문서 기반으로 구현, 네 언어, 픽스처 `stream.measured=false`. **KB 는 웹소켓이 없어 제외**(개인 오픈베타 명세 95개 전부 REST). 실측은 해당 증권사 계좌 사용자 제보로

## Phase F — 증권사 사용량 랭킹 (진행 중)

OpenRouter 의 모델 랭킹처럼, SDK 가 집계한 실사용량으로 **국내외 증권사 오픈 API 사용량 랭킹**을 공개한다. 계약은 [docs/telemetry.md](docs/telemetry.md).

- [x] (0.11.0) 사용량 텔레메트리 — 브로커·환경·호출 종류별 건수·에러 분류·응답 시간 분포·스트림 건수·SDK 버전·설치 ID 를 시간 버킷으로 합산해 60초마다 전송. 종목·수량·가격·계좌·주문번호·키·IP 는 보내지 않음. 기본 배포본 항상 켜짐(설정 없음), 매매 경로와 분리, 새 의존성 없음. Kotlin 코어 + 네 언어 계측
- [x] (0.11.0) `hermetix-service` (별도 레포, AWS ECS) — `POST /v1/usage` 수신·집계, `GET /v1/rankings`·`/v1/status`, 랭킹·API 현황 페이지. 수신 도메인 `https://service-api-prod.hermetix.dev` (2026-09-22 변경, 이전 `hermetix-api-prod.tripleauth.com`), SDK 네 언어 엔드포인트 상수 갱신
- [ ] 랭킹 페이지 공개 — 브로커별 호출량·실전 비율·에러율·p50/p95·설치 수·언어 비중·실시간 채널 사용량, 7일/30일

## Phase E — 다언어 도달 (진행 중)

- [x] (2026-09-14) 한 줄 설치 배포 — Kotlin JitPack, Go `go/v0.10.0` 태그, npm `hermetix`, PyPI `hermetix`. 릴리즈 절차는 수동(CI 없음): 태그 → `npm publish`(2FA 브라우저) → `twine upload`
- [x] Python / JavaScript·TypeScript / Go 네이티브 포팅 — Kotlin 레퍼런스를 언어별로 손 포팅하고 같은 골든 픽스처로 동작 일치를 보증한다
- 원칙: **증권사 키는 항상 사용자 기기에서만 쓰인다.** Hermetix 가 운영하는 서버로 키를 받아 대신 호출하는 구조(게이트웨이/데몬 호스팅)는 한국 금융 라이선스 문제로 채택하지 않는다. 다언어 지원은 ccxt 처럼 각 언어의 로컬 라이브러리로만 제공한다

---

버전 정책: SPI/BrokerClient 시그니처 변경 = minor 상승 (0.N.0). 태그 = 릴리즈 (JitPack). 상세 설계는 [docs/architecture.md](docs/architecture.md).
