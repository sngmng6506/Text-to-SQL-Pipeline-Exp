# services/

파이프라인에 종속되지 않는 **재사용 가능한 AI/DB 서비스 및 유틸리티**.

simple, ir-use 등 어떤 파이프라인에서든 독립적으로 호출할 수 있다.

## 모듈

| 파일 | 역할 | 의존성 |
|------|------|--------|
| `sql_ast.py` | SQL AST 정규화·비교·다수결 (sqlglot) | sqlglot |
| `few_shot.py` | MQS 유사도 기반 Few-shot 예시 검색 (한국어/영어 풀) | TEI 임베딩 서버 |
| `translate.py` | 한국어 → 영어 질의 번역 (LLM) | vLLM 서버 |
| `sql_judge.py` | LLM Judge를 이용한 SQL 후보 재정렬 | vLLM 서버 |
| `exec_preview.py` | Judge용 SQL LIMIT 미리 실행 및 결과 포맷 | PostgreSQL DB |

## 원칙

- 특정 파이프라인에 종속되는 순차 단계는 `steps/`에 둔다.
- DB 스키마 자체를 다루는 로직은 `schema/`에 둔다.
- 여기에는 **어디서든 import해서 쓸 수 있는** 서비스만 배치한다.
