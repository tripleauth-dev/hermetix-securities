# 증권사별 설정과 제약

루트 README 의 [지원 증권사](../README.md#지원-증권사) 표를 보충하는 상세입니다. 증권사 ID(`next`, `kis` …)는 [브로커 팩토리 규약](broker-factory.md)·`BrokerCapabilities.brokerId`·엔진 설정 `hermetix.broker` 에서 같은 값을 씁니다.

## 검증 상태의 의미

- ✅ **검증** — 실서버 스모크 테스트(시세→캔들→계좌→주문 전 구간)를 통과한 환경. KIS·키움의 실전은 호스트·TR ID 전환만 구현돼 있고 실계좌 스모크는 아직입니다
- ⚠️ **미검증** — 공식 SDK·문서에서 엔드포인트와 필드명을 역추적해 만든 어댑터. 네 언어 컨포먼스 시나리오는 통과했지만 픽스처가 실측이 아니라 문서 재구성값이라, 모의계좌 실측으로 확인되기 전까지는 스펙 해석 오류가 있을 수 있습니다. 실측을 도와주실 분은 [새 증권사 요청 이슈](../../../issues/new?template=broker-request.md)로 알려주세요
- ⚠️ **실전 전용** — 모의투자 환경이 없어 실계좌로만 검증할 수 있는 증권사(`toss`, `kb`). 반드시 `hermetix.live.enabled: true` 와 주문 금액 상한(`hermetix.risk.*`)을 함께 설정하고 소액으로 시작하세요
- 증권사별 지원 기능은 [`BrokerCapabilities`](../kotlin/hermetix-broker/src/main/kotlin/com/tripleauth/hermetix/broker/BrokerCapabilities.kt) 로 코드에 선언되며(캔들 주기·지원 환경·시장·멱등키·실시간 채널 등), 엔진이 기동 시 전략-증권사 호환성을 검증합니다

2026-09 조사 시점에 REST 오픈 API 를 제공하는 국내 증권사는 모두 붙였습니다 ([조사 보고서](../claudedocs/korean-broker-openapi-survey-2026-09.md) — 미래에셋·삼성·대신·신한은 REST 미제공). 새 증권사 어댑터 기여는 [컨포먼스 킷](../conformance/README.md) 절차(실측 픽스처 → 구현 → 네 언어 공통 시나리오 통과)를 따릅니다.

## 지원 기능 요약

| ID | 시장 | 캔들 | 모의 | 실전 | 실시간 채널 |
|---|---|---|---|---|---|
| `next` | 미국주식 | 1m · 1d | ✅ | ✅ 키 프리픽스(`pk_test_`/`pk_live_`)로 구분 | 없음 (공개 스펙 v1.3) |
| `kis` | KRX 국내주식 | 1d | ✅ | ✅ 호스트·TR ID 자동 전환 | 체결가 ✅ · 호가 ✅ · 주문통보 ⚠️ |
| `kiwoom` | KRX 국내주식 | 1d | ✅ | ✅ 호스트 자동 전환 | 체결가 ✅ · 호가 ✅ · 주문통보 ⚠️ |
| `nh` | KRX 국내주식 | 1d | ✅ 호스트 분리 | ✅ | 체결가·호가·주문통보 ⚠️ |
| `ls` | KRX 국내주식 | 1d | ✅ 키로 구분 | ✅ | 체결가·호가·주문통보 ⚠️ |
| `db` | KRX 국내주식 | 1d | ✅ 키로 구분 | ✅ | 체결가·호가·주문통보 ⚠️ |
| `toss` | KRX · 미국주식 | 1m · 1d | ❌ 샌드박스 없음 | ✅ | 체결가·호가·주문통보 ⚠️ |
| `kb` | KRX 국내주식 | 1d | ❌ "추후 제공" | ✅ 오픈베타 | 없음 (REST 전용) |

KIS·키움의 실전은 호스트·TR ID 전환만 구현돼 있고 실계좌 스모크는 아직입니다. 주문통보 ⚠️ 는 KIS 는 HTS ID, 키움 모의는 일반 주문이 가능한 계좌가 필요해 실측하지 못한 것입니다.

## 엔진 설정 (Kotlin, `application.yml`)

증권사를 바꿀 땐 `hermetix.broker` 한 줄과 그 증권사의 키 블록만 바뀝니다. 연결 계층만 쓸 때의 자격 증명 키는 [브로커 팩토리 규약](broker-factory.md)을 보세요.

```yaml
hermetix:
  broker: next                   # next | kis | kiwoom | nh | db | ls | toss | kb
  next:
    client-id: pk_test_...       # pk_test_ = 모의, pk_live_ = 실전 (키 프리픽스가 환경을 결정)
    client-secret: sk_test_...
    account-id: acc_main

# 한국투자증권 (✅ 검증)
#  broker: kis
#  kis:
#    appkey: ${KIS_APPKEY:}
#    appsecret: ${KIS_APPSECRET:}
#    cano: ${KIS_CANO:}           # 모의계좌번호 8자리
#    hts-id: ${KIS_HTS_ID:}       # 주문통보 구독 키 (없으면 통보만 생략)

# 키움증권 (✅ 검증)
#  broker: kiwoom
#  kiwoom:
#    appkey: ${KIWOOM_APPKEY:}
#    secretkey: ${KIWOOM_SECRETKEY:}

# NH투자증권 NH PLUG (⚠️ 미검증) — 계좌번호를 비우면 모의(acct_type=03) 계좌를 자동 선택
#  broker: nh
#  nh:
#    app-key: ${NH_APP_KEY:}
#    app-secret: ${NH_APP_SECRET:}
#    account-no: ${NH_ACCOUNT_NO:}
#    market-cd: KRX               # 시장 설정이 실시간 채널을 정합니다 (KRX oc/ob · NXT nc/nb · UNT mc/mb)

# DB증권 (⚠️ 미검증) — 모의투자용 키를 넣으면 모의, 실전 키면 실전 (호스트 동일)
#  broker: db
#  db:
#    app-key: ${DB_APP_KEY:}
#    app-secret: ${DB_APP_SECRET:}

# LS증권 (⚠️ 미검증) — 모의투자용 appkey 로 모의, 실전 키면 실전 (호스트 동일)
#  broker: ls
#  ls:
#    app-key: ${LS_APP_KEY:}
#    app-secret: ${LS_APP_SECRET:}

# 토스증권 (⚠️ 실전 전용, 미검증) — 샌드박스 없음. live.enabled 와 risk 상한 필수. account-seq 비우면 첫 위탁계좌
#  broker: toss
#  live.enabled: true
#  toss:
#    client-id: ${TOSS_CLIENT_ID:}
#    client-secret: ${TOSS_CLIENT_SECRET:}
#    account-seq: ${TOSS_ACCOUNT_SEQ:}   # 주문통보(personal:order) 키

# KB증권 (⚠️ 실전 전용 오픈베타, 미검증) — chart-market-clsf 는 차트 종목의 시장 (0 KOSPI / 1 KOSDAQ)
#  broker: kb
#  live.enabled: true
#  kb:
#    app-key: ${KB_APP_KEY:}
#    app-secret: ${KB_APP_SECRET:}
#    chart-market-clsf: "0"
```

모든 증권사에 `ws-url` 을 두면 환경에 맞는 기본 웹소켓 주소를 덮어씁니다. 실전 전환(`environment: live`, `live.enabled`, `risk.*`)은 [README 의 실전투자로 전환](../README.md#실전투자로-전환)을 보세요.

## 실시간 스트림 — 증권사별 상태와 프로토콜

✅ 모의 웹소켓 장중 실측(2026-09-14) · ⚠️ 공식 문서/SDK 기반 구현, 실측 전 · ❌ 증권사 스펙에 웹소켓 없음

| 증권사 | 체결가 | 호가 | 주문통보 | 프로토콜·제약 |
|---|:---:|:---:|:---:|---|
| `next` | ❌ | ❌ | ❌ | 공개 스펙 v1.3 에 웹소켓 없음 — 폴링 |
| `kis` | ✅ | ✅ | ⚠️ | 접속키 `/oauth2/Approval`, 모의 `ws://ops…:31000`. 주문통보는 HTS ID 로 구독하고 AES-256-CBC 프레임을 구독 응답 key/iv 로 복호화 |
| `kiwoom` | ✅ | ✅ | ⚠️ | REST 토큰으로 LOGIN → REG. 주문체결(00)은 계좌 단위 등록. 모의계좌는 일반 주문이 가능한 계좌여야 통보 확인 가능 |
| `nh` | ⚠️ | ⚠️ | ⚠️ | 채널이 `market-cd` 로 갈림. 모의 서버는 시세 채널 "미제공" 표기(통보만 올 수 있음). 세션당 등록 10~30건, 앱키당 세션 2개 |
| `ls` | ⚠️ | ⚠️ | ⚠️ | 서버가 시장을 판별하지 않아 KOSPI·KOSDAQ TR 을 종목마다 둘 다 구독. 토큰 익일 07:00 만료 |
| `db` | ⚠️ | ⚠️ | ⚠️ | 토큰은 메시지 헤더, `tr_key` "J 005930". 접속 후 10초 내 첫 전송 필수, 세션 2개·종목 50개 |
| `toss` | ⚠️ | ⚠️ | ⚠️ | 실전 전용. 구독 집합 전체를 배열 하나로 선언, Bearer 핸드셰이크, 60초 `PING`. 계정당 연결 2개·구독 100개·선언 5회/초 |
| `kb` | ❌ | ❌ | ❌ | 개인 오픈베타 명세(2026-09) 전부 REST — 폴링 |

### 채널 코드·접속 주소·한도

| 증권사 | 체결가 | 호가 | 주문통보 | 접속 주소 (모의 / 실전) | 세션·구독 한도 |
|---|---|---|---|---|---|
| `kis` | `H0STCNT0` | `H0STASP0` | `H0STCNI9` 모의 / `H0STCNI0` 실전 | `ws://ops.koreainvestment.com:31000` / `:21000` (TLS 없음) | 접속키는 접속마다 `/oauth2/Approval` 로 발급. 통보 프레임은 AES-256-CBC, key·iv 는 구독 응답에 실림 |
| `kiwoom` | `0B` | `0D` | `00` | `wss://mockapi.kiwoom.com:10000/api/dostk/websocket` / `wss://api.kiwoom.com:10000/…` | REST 토큰으로 `LOGIN` 후 `REG`. 통보는 계좌 단위 등록 |
| `nh` | `oc` KRX / `nc` NXT / `mc` 통합 | `ob` / `nb` / `mb` | `d2` `d3` | `wss://moapi.nhplug.com:17070/websocket` / `wss://api.nhplug.com:7070/websocket` | 세션당 등록 10건(SDK 실측) ~ 30건(공식), 앱키당 세션 2개. 운영 서버가 중간 CA 를 보내지 않아 JVM·Go 트러스트 문제가 있을 수 있음 |
| `db` | `S00` | `S01` | `IS0` 접수 / `IS1` 체결 | `wss://openapi.dbsec.co.kr:17070/websocket` / `:7070/websocket` | 접속 후 10초 안에 첫 전송 필수(어댑터가 즉시 구독). 계좌당 세션 2개, 종목 50개, 접속 6회/분. 통보는 해제 메시지가 없어 세션 종료가 곧 해제. `tr_key` 는 `"J 005930"` 형식 |
| `ls` | `S3_` KOSPI + `K3_` KOSDAQ | `H1_` + `HA_` | `SC0` 접수 · `SC1` 체결 · `SC2` 정정 · `SC3` 취소 · `SC4` 거부 | `wss://openapi.ls-sec.co.kr:29443/websocket` / `:9443/websocket` | 서버가 시장을 판별하지 않아 종목마다 KOSPI·KOSDAQ TR 을 둘 다 등록(등록 수 2배). 토큰은 익일 07:00 만료라 재접속 시 새 토큰. 세션·등록 한도는 문서에 없음 |
| `toss` | `trade:kr` / `trade:us` | `orderbook:kr` / `orderbook:us` | `personal:order` | `wss://openapi-ws.tossinvest.com/ws/v1` (실전 전용, Bearer 핸드셰이크) | 구독 집합 전체를 배열 하나로 선언하며 변경은 200ms 동안 합침. 계정당 연결 2개(3번째가 오면 가장 오래된 것 종료), 구독 100개, 선언 5회/초. 180초 무송신 시 끊겨 60초마다 `PING`. 체결량은 누적 스냅샷의 차이 |
| `next`, `kb` | 없음 | 없음 | 없음 | 웹소켓 스펙 없음 | 폴링만 |

프레임 파서와 골든 픽스처는 `conformance/fixtures/*.json` 의 `stream` 섹션에 있고, `measured: false` 인 픽스처는 문서 예시에서 재구성한 값입니다.

⚠️ 항목은 공식 문서·SDK·AsyncAPI 예시로 만든 파서라 필드 해석이 틀릴 수 있습니다. 해당 증권사 계좌가 있다면 각 언어의 스모크 테스트(`HERMETIX_RAW_DUMP` 로 원시 프레임 덤프)를 돌려 [새 증권사 요청 이슈](../../../issues/new?template=broker-request.md)로 프레임을 보내 주세요. 실측 프레임으로 픽스처를 교체하면 ✅ 로 올라갑니다. 프레임 샘플과 기대값은 [컨포먼스 픽스처](../conformance/README.md)의 `stream` 섹션에 있습니다.
