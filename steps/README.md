# steps/

ir-use 파이프라인의 **순차 실행 단계**. 각 모듈은 Jinja2 프롬프트를 렌더링하여 LLM에 전달하는 역할을 한다.

## 모듈 (실행 순서)

| 순서 | 파일 | 역할 |
|:----:|------|------|
| 1 | `extract_entity.py` | 질의에서 엔티티(테이블·컬럼·값) 추출 |
| 2 | `schema_linking.py` | 엔티티 기반 스키마 링킹 |
| 3 | `rewrite_query.py` | 스키마 정보를 반영한 질의 재작성 |
| 4 | `generate_sql.py` | 최종 SQL 생성 |

## 원칙

- **ir-use 파이프라인 전용** 단계만 이 폴더에 둔다.
- 파이프라인에 종속되지 않는 재사용 가능한 서비스(번역, Judge, Few-shot 등)는 `services/`에 배치한다.
