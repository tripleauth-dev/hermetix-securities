---
name: 실측 제보 (검증 상태 올리기)
about: 계좌가 있는 증권사 어댑터를 실서버에 돌린 결과 파일을 보내 ⚠️ 미검증 → ✅ 검증으로 올립니다
title: "Verify: <증권사 id> (<모의|실전>)"
labels: broker-verification
---

## 돌린 명령

```bash
pip install 'hermetix[stream]'
HERMETIX_API_KEY=... HERMETIX_API_SECRET=... HERMETIX_ACCOUNT=... python -m hermetix.verify <broker>
```

<!-- 실전 전용 증권사(toss·kb)는 `--live --read-only`(조회만) 또는 `--live --live-orders`(원거리 지정가 1주 → 즉시 취소) -->

- 증권사 / 환경: <!-- 예: nh / 모의 -->
- SDK 버전 / Python 버전: <!-- 출력 파일 sdk 항목 -->
- 실행 시각(장중 여부): <!-- 장 마감 시간에 돌리면 주문 단계가 skip 됩니다. 가능하면 정규장 중에 -->

## 결과

<!-- 명령 마지막 줄 "OK — N/N 단계 통과" 또는 "FAILED" 를 그대로 -->

```
```

## 첨부

- [ ] `hermetix-verify-<broker>.json` 을 첨부했습니다 (드래그 앤 드롭)
- [ ] 첨부 전에 파일을 열어 남기고 싶지 않은 값이 없는지 확인했습니다 — 키·토큰·계좌번호·고객명은 자동으로 `***` 처리됩니다

## 기여자 표기

README 의 검증 기여자 표에 올릴 이름(GitHub 아이디 그대로면 비워 두세요):

<!-- 예: @handle 또는 익명 -->
