// 빌드 단계: 저장소 문서를 패키지 docs/ 로 복사한다 (npm 패키지에는 저장소 밖 파일이 안 들어가므로)
import { copyFileSync, mkdirSync } from "node:fs";

const root = new URL("../../", import.meta.url);
const out = new URL("../docs/", import.meta.url);
mkdirSync(out, { recursive: true });
for (const name of ["brokers.md", "broker-factory.md", "strategy-guide.md"]) copyFileSync(new URL(`docs/${name}`, root), new URL(name, out));
copyFileSync(new URL("js/README.md", root), new URL("js-README.md", out));
