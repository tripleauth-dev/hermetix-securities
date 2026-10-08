# hermetix-mcp 설계

AI 클라이언트(Claude Desktop·Claude Code·Cursor 등)가 hermetix SDK 로 증권사 시세·계좌를 조회하게 하는 MCP 서버.
상태: **v0.1.0 구현** (`mcp/src`). 참고한 구현: `~/nextAPI/next-openapi-mcp` (넥스트증권 전용 원격 MCP).

## 원칙

1. **로컬 전용** — stdio 로만 동작한다. 원격(Streamable HTTP) 모드와 OAuth 는 만들지 않는다. 증권사 키는 사용자 기기에서 증권사로만 간다 (hermetix 서버로 가지 않는다 — SDK 와 같은 원칙)
2. **키는 대화에 나오지 않는다** — tool 입력·출력·에러 메시지 어디에도 키·시크릿·토큰을 싣지 않는다. 계좌번호는 tool 출력에 마스킹(`5019****`)
3. **v1 은 조회만** — 주문 생성·취소 tool 은 없다. AI 가 실제 주문을 넣는 경로가 코드에 존재하지 않게 한다 (§주문은 v2)
4. **SDK 위의 얇은 층** — 증권사 로직은 전부 JS SDK(`hermetix`) 가 한다. MCP 는 인자 검증·결과 직렬화·설정 로딩만. 새 증권사는 SDK 에 들어오면 MCP 코드 변경 없이 쓸 수 있어야 한다
5. **결과는 정규화 모델** — 증권사 원본 JSON 이 아니라 SDK 공통 모델(Quote·Holding·Order …)을 돌려준다. 어느 증권사든 같은 모양

## 위치·배포

```
mcp/
  DESIGN.md         <- 이 문서
  package.json      <- npm 패키지 "hermetix-mcp", bin: hermetix-mcp
  src/
    index.ts        <- stdio 기동, console.log → stderr 우회
    config.ts       <- 설정 파일 로딩·검증
    tools.ts        <- tool 등록
    format.ts       <- 모델 → JSON (Decimal → 문자열, 계좌 마스킹)
    resources.ts    <- 문서 resource
    version.ts      <- package.json 버전
  scripts/copy-docs.mjs <- 빌드 시 저장소 문서를 docs/ 로 복사
  tests/
```

- 실행: `npx -y hermetix-mcp` (Node 22 이상 — SDK 와 같음)
- 의존성: `hermetix`(SDK, npm `^0.11.6`), `@modelcontextprotocol/sdk`, `zod`. 그 외 없음
- 버전은 SDK 와 따로 간다. 0.1.0 부터. 태그는 `mcp/v0.1.0` (SDK 릴리스 절차에 영향 없음)
- SDK 는 저장소의 `js/` 가 아니라 npm 배포본을 쓴다 — MCP 는 SDK 공개 API 만 쓰므로, 새 SDK 기능이 필요하면 SDK 를 먼저 릴리스하고 범위를 올린다

## 설정

키는 MCP 클라이언트 설정(JSON)이 아니라 **별도 파일**에 둔다. 클라이언트 설정 파일은 동기화·공유되는 경우가 많기 때문이다.

`~/.hermetix/mcp.json` (권한 0600 이 아니면 기동 시 경고):

```json
{
  "accounts": {
    "kis-paper": { "broker": "kis", "apiKey": "…", "apiSecret": "…", "account": "50199202" },
    "toss":      { "broker": "toss", "apiKey": "…", "apiSecret": "…", "environment": "LIVE" }
  },
  "default": "kis-paper"
}
```

- 항목 모양은 SDK 팩토리 자격 증명 그대로 (`docs/broker-factory.md`) — `broker` 만 추가. 검증도 팩토리(`hermetix.client`)에 맡긴다 (모르는 키·빈 시크릿은 기동 시 에러, 에러 메시지에 값은 싣지 않는다)
- 값 대신 `"env:KIS_APPKEY"` 를 쓰면 환경변수에서 읽는다 (파일에 키를 두기 싫은 사용자용)
- 파일 위치는 `HERMETIX_MCP_CONFIG` 환경변수로 바꿀 수 있다
- 설정이 없거나 틀려도 서버는 뜬다 — tool 이 이유와 예시를 돌려준다. 틀린 별칭만 빠지고 나머지 별칭은 동작한다. 별칭이 하나뿐이면 `default` 생략 가능
- `environment` 생략 시 PAPER (SDK 기본값과 같음). LIVE 계정도 조회는 허용한다 — v1 에 주문이 없으므로

클라이언트 등록 예 (Claude Desktop / Claude Code):

```json
{ "mcpServers": { "hermetix": { "command": "npx", "args": ["-y", "hermetix-mcp"] } } }
```

## tool (v1)

모든 tool 은 `account` 인자(설정의 별칭, 생략 시 `default`)를 받는다. 증권사별 tool 을 따로 만들지 않는다 — tool 수가 늘면 AI 가 tool 을 잘못 고른다.

| tool | SDK 호출 | 입력 | 비고 |
|---|---|---|---|
| `list_accounts` | — | 없음 | 별칭·증권사·환경·지원 기능(capabilities) 목록. 키는 싣지 않는다 |
| `get_quotes` | `getQuotes` | `symbols: string[]` (최대 20) | `KRX:005930`·`US:AAPL` 접두 허용 (capabilities.markets) |
| `get_candles` | `getCandles` | `symbol`, `interval`, `limit?` (기본 30, 최대 200) | interval 은 해당 증권사 capabilities.candleIntervals 로 검증 |
| `get_calendar` | `getCalendar` | 없음 | |
| `get_account` | `getAccount` | 없음 | 계좌번호 마스킹 |
| `get_holdings` | `getHoldings` | 없음 | |
| `get_buying_power` | `getBuyingPower` | 없음 | |
| `list_orders` | `getOrders` | 없음 | |
| `get_order` | `getOrder` | `orderId` | |
| `get_fills` | `getFills` | 없음 | |

- tool 설명은 한국어 + 짧게. 응답 형식(통화·소수 문자열)과 "조회 전용"을 명시
- 결과는 JSON text 한 벌 (`structuredContent` 는 쓰지 않는다 — 결과가 배열인 tool 이 많고, text 는 모든 클라이언트가 읽는다)
- 모든 tool 에 `readOnlyHint: true` 를 단다
- Decimal 은 문자열, 시각은 ISO-8601. 목록은 기본 50건으로 자르고 `truncated: true` 를 붙인다

### 에러

SDK 예외를 tool 에러(`isError: true`)로 돌려준다. 텍스트는 `분류 + 증권사 메시지` 한 줄:

| SDK 예외 | 표시 |
|---|---|
| `AuthError` | 인증 실패 — 키·환경(PAPER/LIVE) 확인 |
| `RateLimitError` | 호출 한도 초과 — N초 뒤 재시도 (SDK 가 이미 재시도한 뒤) |
| `MarketClosedError` | 장 운영 시간 아님 |
| `OrderNotFoundError` | 주문 없음 |
| 그 외 `BrokerApiError` | HTTP 상태 + 증권사 코드·메시지 |
| fetch 연결 실패 | 증권사 서버에 연결하지 못함 |
| 설정 에러 | 어느 별칭의 어느 항목이 문제인지 (값은 싣지 않음) |

## resources

AI 가 hermetix 로 **코드를 짤 때** 쓰는 문서. 패키지에 함께 넣는다 (빌드 시 저장소 `docs/`·`js/README.md` 에서 복사).

| URI | 내용 |
|---|---|
| `hermetix://docs/brokers` | `docs/brokers.md` — 증권사별 지원 기능·제약 |
| `hermetix://docs/broker-factory` | `docs/broker-factory.md` — 자격 증명 규약 |
| `hermetix://docs/strategy-guide` | `docs/strategy-guide.md` — 전략 작성 |
| `hermetix://docs/js` | `js/README.md` — JS SDK 사용법 |

넥스트 MCP 의 `list_endpoints`·`describe_endpoint`(OpenAPI 탐색)는 두지 않는다 — hermetix 사용자는 증권사 원본 API 가 아니라 SDK 인터페이스로 코드를 짠다.

## 구현 시 주의

- **stdout 오염** — stdio MCP 는 stdout 이 프로토콜이다. SDK 스트림·KIS 주문 통보 경로에 `console.log` 가 있다 (`js/src/stream.ts`, `js/src/brokers/*Stream.ts`, `kis.ts`). v1 은 스트림을 안 쓰지만 기동 시 `console.log`/`console.info` 를 stderr 로 돌려 막는다
- **클라이언트 재사용** — 별칭당 SDK 클라이언트 하나를 프로세스 수명 동안 쓴다. 토큰은 SDK 의 파일 캐시(`~/.hermetix/tokens`)가 관리하므로 다른 hermetix 프로세스와 토큰을 나눠 쓴다 (KIS·DB 발급 1분 1건 제한 대응)
- **텔레메트리** — SDK 에 내장돼 있어 MCP 호출도 그대로 집계된다 (`sdk.language = "js"`). MCP 경유를 따로 구분하려면 페이로드 스키마 변경이 필요하다 → 열린 질문
- **종료** — 클라이언트가 stdin 을 닫으면 이벤트 루프가 비고, SDK 의 `beforeExit` 훅이 텔레메트리 마지막 전송(최대 3초) 후 종료

## 테스트

- `conformance/fixtures/*.json` 재생 HTTP 로 증권사별 tool 호출 → 정규화 결과 스냅샷 (SDK 테스트와 같은 픽스처)
- MCP SDK 의 in-memory transport 로 `tools/list`·`tools/call` 왕복
- 출력에 키·시크릿·전체 계좌번호가 없는지 검사하는 테스트 (고유 키 문자열로 검사)
- 테스트는 HOME 을 임시 디렉터리로 바꾸고 텔레메트리 전송을 끈다 (`tests/setup.ts`)
- 설정 로딩: 모르는 키, 빈 시크릿, `env:` 참조 누락, 파일 권한 경고

## 주문 (v2, 범위 밖)

v1 이 쓰이는 걸 확인한 뒤 결정한다. 증권사 API 에는 넥스트 백엔드의 order-intent(서버 쪽 확인 대기 주문) 같은 장치가 없어서, 사람 확인을 MCP 안에서 만들어야 한다. 후보:

1. **MCP elicitation** — 서버가 클라이언트에 확인 창을 요청. 구현 단순, 지원하지 않는 클라이언트에서는 주문 불가
2. **로컬 확인 페이지** — `127.0.0.1` 에 일회용 코드가 든 URL 을 띄우고 사람이 브라우저에서 확정. 클라이언트와 무관하지만 로컬 HTTP 서버가 생긴다

어느 쪽이든 PAPER 계정만 기본 허용, LIVE 는 설정 파일의 명시 동의(`allowLiveOrders: true`)를 요구한다 (엔진의 `liveTradingEnabled` 와 같은 원칙).

## 열린 질문

1. 텔레메트리에서 MCP 경유 호출을 구분할지 (스키마에 `sdk.surface` 같은 필드 추가 → 서버·프론트 변경 필요)
2. 설정 파일을 SDK 예제·다른 언어와 공유할 공통 형식으로 키울지, MCP 전용으로 둘지
3. 실시간 시세(스트림)를 tool 로 노출할지 — MCP 는 요청/응답이라 "최근 N초 체결 요약" 같은 형태가 필요
