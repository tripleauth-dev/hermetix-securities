# hermetix-mcp

AI 클라이언트(Claude Desktop·Claude Code·Cursor 등)가 [hermetix](../README.md) SDK 로 국내 증권사 시세·계좌를 조회하게 하는 MCP 서버입니다.

- **로컬 전용**: 내 PC 에서 stdio 로 실행됩니다. 증권사 키는 내 기기에서 증권사로만 갑니다
- **조회 전용**: 주문을 넣거나 취소하는 tool 이 없습니다
- **8개 증권사, 같은 tool**: 넥스트·한국투자·키움·NH·LS·DB·토스·KB. 결과는 증권사와 무관하게 같은 모양입니다

Node 22 이상이 필요합니다.

## 1. 키 설정

`~/.hermetix/mcp.json` 을 만들고 권한을 600 으로 둡니다 (`chmod 600 ~/.hermetix/mcp.json`).

```json
{
  "accounts": {
    "kis-paper": { "broker": "kis", "apiKey": "env:KIS_APPKEY", "apiSecret": "env:KIS_APPSECRET", "account": "env:KIS_CANO" },
    "kiwoom":    { "broker": "kiwoom", "apiKey": "…", "apiSecret": "…" },
    "toss":      { "broker": "toss", "apiKey": "…", "apiSecret": "…", "environment": "LIVE" }
  },
  "default": "kis-paper"
}
```

- `accounts` 의 키(`kis-paper` 등)는 내가 정하는 별칭입니다. tool 의 `account` 인자에 씁니다
- 항목은 SDK [브로커 팩토리](../docs/broker-factory.md) 자격 증명과 같고 `broker` 만 더합니다 (`hts_id` 같은 증권사별 항목도 그대로)
- `"env:이름"` 은 환경변수에서 읽습니다. 파일에 키를 두기 싫으면 이렇게 쓰세요
- `environment` 를 생략하면 모의투자(PAPER) 입니다
- 다른 위치를 쓰려면 환경변수 `HERMETIX_MCP_CONFIG` 에 경로를 넣습니다

키를 MCP 클라이언트 설정에 넣지 않는 이유: 클라이언트 설정 파일은 동기화·공유되는 일이 많습니다.

## 2. 클라이언트 등록

Claude Code:

```bash
claude mcp add hermetix -- npx -y hermetix-mcp
```

Claude Desktop (`claude_desktop_config.json`) · Cursor (`.cursor/mcp.json`):

```json
{ "mcpServers": { "hermetix": { "command": "npx", "args": ["-y", "hermetix-mcp"] } } }
```

`env:` 참조를 쓰면 그 환경변수가 클라이언트 프로세스에 있어야 합니다. 없으면 위 JSON 에 `"env": { "KIS_APPKEY": "…" }` 를 더하는 대신 설정 파일에 값을 직접 넣는 쪽을 권합니다.

## tool

| tool | 내용 |
|---|---|
| `list_accounts` | 별칭·증권사·모의/실전·지원 기능 |
| `get_quotes` | 현재가·호가·등락 (최대 20종목) |
| `get_candles` | OHLCV 캔들 (증권사가 지원하는 간격만) |
| `get_calendar` | 개장일·정규장 시간 |
| `get_account` | 예수금·평가금액 (계좌번호는 앞 4자리만) |
| `get_holdings` | 보유 종목 |
| `get_buying_power` | 매수 가능 금액 |
| `list_orders` · `get_order` | 주문 목록·상세 |
| `get_fills` | 체결 내역 |

모든 tool 은 `account`(별칭)를 받고, 생략하면 `default` 별칭을 씁니다. 금액·수량은 소수 문자열, 시각은 ISO-8601 입니다.

문서 resource(`hermetix://docs/brokers`, `broker-factory`, `strategy-guide`, `js`)도 제공합니다. AI 에게 hermetix 로 전략 코드를 짜 달라고 할 때 참고 자료가 됩니다.

## 사용량 데이터

SDK 에 들어 있는 익명 사용량 집계가 그대로 동작합니다. 무엇을 보내는지는 [텔레메트리 문서](../docs/telemetry.md)에 있습니다. 키·계좌·종목·금액은 보내지 않습니다.

## 개발

```bash
cd mcp
npm install
npm test       # 8개 증권사 골든 픽스처로 tool 호출 재생
```

설계: [DESIGN.md](DESIGN.md)
