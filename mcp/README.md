# hermetix-mcp

Claude Desktop·Claude Code·Cursor 같은 AI 클라이언트에서 **내 증권 계좌와 시세를 대화로 조회**하게 해 주는 MCP 서버입니다. [hermetix](https://github.com/tripleauth-dev/hermetix-securities) SDK 위에서 동작합니다.

> "토스 계좌 보유 종목 보여줘" · "삼성전자 최근 20일 일봉으로 추세 요약해줘" · "오늘 체결된 주문 있어?"

- **로컬 전용** — 내 PC 에서 실행됩니다. 증권사 키는 내 기기에서 증권사로만 가고, hermetix 서버나 AI 대화에는 실리지 않습니다
- **조회 전용** — 주문을 넣거나 취소하는 기능이 없습니다. AI 가 실수로 주문할 수 없습니다
- **8개 증권사, 같은 사용법** — 넥스트·한국투자·키움·NH·LS·DB·토스·KB

## 시작하기 (5분)

준비물: **Node.js 22 이상** (`node -v` 로 확인), 쓰려는 증권사의 Open API 키.

### 1단계 — 키 파일 만들기

`~/.hermetix/mcp.json` 을 만들고 계좌를 적습니다.

```json
{
  "accounts": {
    "kis-paper": { "broker": "kis", "apiKey": "앱키", "apiSecret": "앱시크릿", "account": "계좌번호 앞 8자리" }
  },
  "default": "kis-paper"
}
```

다른 사용자가 읽지 못하게 권한을 바꿉니다.

```bash
chmod 600 ~/.hermetix/mcp.json
```

- `accounts` 의 키(`kis-paper`)는 내가 정하는 **별칭**입니다. 여러 계좌를 등록하면 대화에서 별칭으로 고릅니다
- `default` 는 계좌를 말하지 않았을 때 쓸 별칭입니다. 계좌가 하나면 생략해도 됩니다

### 2단계 — AI 클라이언트에 등록하기

**Claude Code** — 터미널에서 한 줄:

```bash
claude mcp add hermetix -- npx -y hermetix-mcp
```

**Claude Desktop** — 설정 → 개발자 → **설정 편집** 으로 `claude_desktop_config.json` 을 열고 `mcpServers` 에 추가한 뒤, 앱을 **⌘Q 로 완전히 종료했다가** 다시 엽니다.

```json
{
  "mcpServers": {
    "hermetix": { "command": "npx", "args": ["-y", "hermetix-mcp"] }
  }
}
```

**Cursor** — 프로젝트의 `.cursor/mcp.json` (또는 전역 `~/.cursor/mcp.json`) 에 위와 같은 JSON.

> Claude Desktop 의 "커넥터 → 커스텀 커넥터 추가" 는 원격 서버(URL) 용입니다. hermetix-mcp 는 내 PC 에서 도는 로컬 서버라 위처럼 설정 파일에 등록합니다.

### 3단계 — 확인하기

새 대화에서 이렇게 물어봅니다.

> 내 hermetix 계좌 목록 보여줘

등록한 별칭·증권사·모의/실전이 나오면 끝입니다. 이어서 "보유 종목 보여줘", "삼성전자 현재가" 처럼 물어보세요.

## 계좌 설정 자세히

### 증권사별 항목

| 증권사 | `broker` | `apiKey` / `apiSecret` | `account` | 기본 환경 |
|---|---|---|---|---|
| 넥스트증권 | `next` | client_id / client_secret | 계좌 ID (생략 시 `acc_main`) | 모의 |
| 한국투자증권 | `kis` | 앱키 / 앱시크릿 | **필수** — 계좌번호 앞 8자리 | 모의 |
| 키움증권 | `kiwoom` | 앱키 / 시크릿키 | 쓰지 않음 | 모의 |
| NH투자증권 | `nh` | app_key / app_secret | 계좌번호 (생략 시 첫 계좌) | 모의 |
| LS증권 | `ls` | app_key / app_secret | 쓰지 않음 | 모의 |
| DB증권 | `db` | app_key / app_secret | 쓰지 않음 | 모의 |
| 토스증권 | `toss` | client_id / client_secret | 계좌 순번 (생략 시 첫 위탁계좌) | **실전** (모의 없음) |
| KB증권 | `kb` | app_key / app_secret | 쓰지 않음 | **실전** (모의 없음) |

- 실전 계좌를 쓰려면 `"environment": "LIVE"` 를 넣습니다. 모의/실전 키를 섞으면 인증 실패가 납니다
- 증권사별 추가 항목(`hts_id`, `mac_address` 등)은 [브로커 팩토리 문서](https://github.com/tripleauth-dev/hermetix-securities/blob/main/docs/broker-factory.md)의 이름 그대로 넣습니다. 모르는 항목은 오타로 보고 거부합니다

### 여러 계좌

```json
{
  "accounts": {
    "kis-paper": { "broker": "kis", "apiKey": "…", "apiSecret": "…", "account": "50123456" },
    "kiwoom":    { "broker": "kiwoom", "apiKey": "…", "apiSecret": "…" },
    "toss":      { "broker": "toss", "apiKey": "…", "apiSecret": "…" }
  },
  "default": "kis-paper"
}
```

대화에서 "토스 계좌 보유 종목" 처럼 말하면 AI 가 `toss` 별칭을 고릅니다.

### 키를 파일에 두기 싫을 때 — 환경변수

값 자리에 `"env:변수이름"` 을 쓰면 환경변수에서 읽습니다.

```json
"toss": { "broker": "toss", "apiKey": "env:TOSS_CLIENT_ID", "apiSecret": "env:TOSS_CLIENT_SECRET" }
```

단, **Claude Desktop·Cursor 는 터미널 밖에서 실행돼 `~/.zshrc` 의 환경변수를 보지 못합니다.** 키를 담은 env 파일을 따로 두고 실행할 때 불러오게 등록하세요.

```json
{
  "mcpServers": {
    "hermetix": {
      "command": "/bin/sh",
      "args": ["-c", "set -a; . ~/.hermetix.env; set +a; exec npx -y hermetix-mcp"]
    }
  }
}
```

`~/.hermetix.env` 는 `TOSS_CLIENT_ID=…` 줄로 된 파일이고, 이것도 `chmod 600` 으로 둡니다.

### 설정 파일 위치 바꾸기

환경변수 `HERMETIX_MCP_CONFIG` 에 경로를 넣으면 그 파일을 씁니다.

## 할 수 있는 것

| 기능 (tool) | 이렇게 물어보세요 |
|---|---|
| `list_accounts` | "등록된 계좌 보여줘" |
| `get_quotes` | "삼성전자, SK하이닉스 현재가" (한 번에 최대 20종목) |
| `get_candles` | "삼성전자 최근 30일 일봉" (분봉은 지원하는 증권사만) |
| `get_calendar` | "다음 개장일 언제야?" |
| `get_account` | "계좌 평가금액이랑 예수금" |
| `get_holdings` | "보유 종목이랑 수익률" |
| `get_buying_power` | "지금 매수 가능 금액" |
| `list_orders` · `get_order` | "최근 주문 상태" |
| `get_fills` | "오늘 체결 내역" |

- 금액은 정확도를 위해 소수 문자열로 받습니다. 계좌번호는 앞 4자리만 보입니다
- 증권사마다 지원 범위가 다릅니다 (예: 분봉, 미국주식). "계좌 목록" 에서 증권사별 지원 기능을 볼 수 있습니다
- **주문은 할 수 없습니다.** 주문은 증권사 앱이나 [hermetix SDK](https://github.com/tripleauth-dev/hermetix-securities) 로 하세요

### 해외 주식 손익 (토스)

미국 종목의 평균단가·현재가·**평가손익은 달러**(`currency: "USD"`)이고, **평가금액만 원화**로 환산해 계좌 총평가에 더합니다. 토스 앱의 원화 손익은 매수 당시 환율로 계산한 환차손익이 들어가 있어 숫자가 다릅니다 — 토스 API 가 종목별 원화 원금을 주지 않아 같은 값을 만들 수 없습니다.

### 전략 코드 작성 도우미

hermetix 문서(증권사별 지원 기능, 자격 증명 규약, 전략 작성 가이드, JS SDK 사용법)가 함께 들어 있습니다. "hermetix 로 변동성 돌파 전략 짜줘" 처럼 요청하면 AI 가 참고합니다.

## 문제 해결

먼저 **"계좌 목록 보여줘"** 로 상태를 확인하세요. 설정 오류가 있으면 이유가 그대로 나옵니다.

| 증상 | 원인과 해결 |
|---|---|
| "설정 파일이 없습니다" | `~/.hermetix/mcp.json` 이 없거나 경로가 다릅니다 |
| "환경변수 … 가 비어 있습니다" | `env:` 로 가리킨 변수가 실행 환경에 없습니다. 위 env 파일 방식으로 등록하거나 값을 직접 넣으세요 |
| "모르는 항목 '…'" | 항목 이름 오타입니다. 위 표와 브로커 팩토리 문서의 이름을 쓰세요 |
| "인증 실패" | 키가 틀렸거나 모의/실전(`environment`)이 키와 다릅니다 |
| "호출 한도 초과" | 증권사 초당 호출 제한입니다. 잠시 뒤 다시 물어보세요 |
| "장 운영 시간이 아닙니다" | 증권사가 장외 시간에 일부 조회를 막습니다 |
| Claude Desktop 에 hermetix 가 안 보임 | 앱을 ⌘Q 로 완전히 종료했다가 다시 여세요. 그래도 안 되면 아래 로그를 보세요 |
| 로그에 `npx: command not found` | Desktop 이 Node 경로를 모릅니다 (nvm 사용 시 흔함). `command` 를 `npx` 의 절대경로(`which npx` 결과)로 바꾸세요 |
| 로그에 `Permission denied` 또는 `ENOTEMPTY` | 새 버전을 받는 중 설치가 끊겨 npx 캐시가 깨졌습니다. 터미널에서 `rm -rf ~/.npm/_npx` 후 Desktop 을 다시 여세요 |

Claude Desktop 로그 위치 (macOS): `~/Library/Logs/Claude/mcp-server-hermetix.log`

## 보안

- 키는 `~/.hermetix/mcp.json`(또는 env 파일)에만 있고, AI 와의 대화·hermetix 서버로 가지 않습니다
- 접근 토큰은 SDK 와 같은 `~/.hermetix/tokens/` 에 캐시됩니다 (토큰과 만료 시각만, 키 없음)
- 파일 권한이 600 이 아니면 실행 시 경고를 남깁니다

## 사용량 데이터

SDK 에 들어 있는 익명 사용량 집계가 그대로 동작합니다(증권사·기능별 성공/에러 건수, 응답 시간). 키·계좌·종목·금액은 보내지 않습니다. 자세한 내용은 [텔레메트리 문서](https://github.com/tripleauth-dev/hermetix-securities/blob/main/docs/telemetry.md)에 있습니다.

## 개발

```bash
cd mcp
npm install
npm test       # 8개 증권사 골든 픽스처로 tool 호출 재생
```

설계: [DESIGN.md](https://github.com/tripleauth-dev/hermetix-securities/blob/main/mcp/DESIGN.md) · 문의: [Discord](https://discord.gg/wRgFGA7rpG)
