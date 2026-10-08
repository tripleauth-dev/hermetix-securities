// 빌드 후처리
// 1) 저장소 문서를 패키지 docs/ 로 복사한다 (npm 패키지에는 저장소 밖 파일이 안 들어가므로)
// 2) bin 파일에 실행 권한을 준다 — tarball 에 755 로 들어가야, npx 설치가 중간에 끊겨도
//    (Claude Desktop 이 서버를 동시에 두 개 띄우며 캐시를 갱신하는 경우) Permission denied 가 나지 않는다
import { chmodSync, copyFileSync, mkdirSync } from "node:fs";

const root = new URL("../../", import.meta.url);
const out = new URL("../docs/", import.meta.url);
mkdirSync(out, { recursive: true });
for (const name of ["brokers.md", "broker-factory.md", "strategy-guide.md"]) copyFileSync(new URL(`docs/${name}`, root), new URL(name, out));
copyFileSync(new URL("js/README.md", root), new URL("js-README.md", out));

chmodSync(new URL("../dist/src/index.js", import.meta.url), 0o755);
