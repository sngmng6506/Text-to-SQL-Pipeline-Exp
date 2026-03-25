# v9 프롬프트

v8의 태그 기반 자연어 재작성 방식을 **IR(Intermediate Representation) 기반 3단계 분리 파이프라인**으로 전면 재설계했습니다.

---

## v8 대비 구조 변화

| 단계 | v8 | v9 |
|------|----|----|
| schema_linking | filter 조건값 포함, 집계 전략 결정 | **grounding만** (컬럼 식별, 값·집계 제외) |
| rewrite_query | 자연어 태그 한 줄 (`<table.col = '표현'>`) | **SQL AST 구조의 JSON IR** 출력 |
| generate_sql | 태그 파싱 + schema_linking_json 양쪽 참조 | **IR JSON을 SQL로 직접 변환** |

---

## 파이프라인 흐름

```
자연어 질문
    │
    ▼
[schema_linking]  ─── 컬럼 grounding만 수행
    │  linked_tables, linked_columns, column_mappings
    │  filter_columns (값 없음), temporal_columns (값 없음)
    │
    ▼
[rewrite_query]  ─── schema_linking 결과를 받아 JSON IR 생성
    │  { FROM, JOIN, FILTER, GROUP_BY, AGGREGATE,
    │    HAVING, ORDER_BY, SELECT }
    │  날짜 표현 → SARGable PostgreSQL 조건으로 직접 변환
    │
    ▼
[generate_sql]  ─── IR + 원본 질의 + few-shot → PostgreSQL SQL
```

---

## 각 프롬프트 상세

### `schema_linking.j2`

**역할**: 자연어 표현을 스키마 심볼에 grounding. SQL 설계는 하지 않는다.

**출력 필드**

| 필드 | 설명 |
|------|------|
| `linked_tables` | 필요한 최소 테이블 목록 |
| `linked_columns` | 의미적 대응 컬럼 (FK 포함) |
| `column_mappings` | 질문 표현 → `테이블.컬럼` 매핑 |
| `filter_columns` | WHERE 조건 후보 컬럼 (값 없음) |
| `temporal_columns` | 날짜/시간 컬럼 후보 (값 없음) |

**v8 대비 변경점**
- `filter_conditions` (값 포함) 제거 → `filter_columns` (컬럼명만)으로 교체
- `aggregation`, `group_by_columns`, `sort_order` 제거
- 집계·정렬·그룹 전략 결정을 rewrite_query 단계로 위임

---

### `rewrite_query.j2`

**역할**: schema_linking 결과를 입력받아 SQL AST 구조의 JSON IR을 출력한다.

**입력 변수**: `question`, `schema_linking_json`

**출력 IR JSON**

```json
{
  "FROM": ["테이블"],
  "JOIN": ["left.fk = right.pk"],
  "FILTER": ["SARGable 조건식"],
  "GROUP_BY": ["테이블.컬럼"],
  "AGGREGATE": ["COUNT(테이블.컬럼)"],
  "HAVING": ["조건식"],
  "ORDER_BY": ["테이블.컬럼 DESC"],
  "SELECT": ["테이블.컬럼"]
}
```

**핵심 규칙**
- 날짜 표현은 SARGable PostgreSQL 형태로 직접 변환 (Rule 4)
  - "어제" → `date_trunc('day', now()) - interval '1 day'` 범위 조건 2개
  - "오늘/지난주/이번달/최근 N일" 등 동일 방식
- JOIN ON 조건은 `linked_columns`의 FK 컬럼을 근거로 결정 (Rule 8)
- 문자열 필터는 ILIKE 형태로 명시 (Rule 10)
- `linked_columns`에 없는 컬럼은 절대 사용 금지

**v8 대비 변경점**
- 자연어 태그 한 줄 출력 → JSON IR 출력으로 전환
- 날짜 SARGable 변환을 재작성 단계에서 처리
- JOIN 조건 추론 규칙 명시

---

### `generate_sql.j2`

**역할**: IR JSON을 PostgreSQL SQL로 변환한다.

**입력 변수**: `question` (원본), `rewritten_question` (IR JSON), `schema_candidates`, `few_shot_examples`

**입력 구조**

```
[few-shot 참고 예시]   ← 자연어 Q → SQL 패턴 참고용
---
원본 질의: {{ question }}
IR JSON:   {{ rewritten_question }}
Schema Candidates: {{ schema_candidates }}
```

**핵심 규칙**
- IR 필드를 1:1로 SQL 절로 변환 (FROM/JOIN/WHERE/GROUP BY/HAVING/ORDER BY/SELECT)
- FILTER 조건은 그대로 사용 (SARGable 변환 금지)
- 문자열 ILIKE 조건은 그대로 유지
- SQLite 문법 금지, PostgreSQL 전용 (ROW_NUMBER() 등)

**v8 대비 변경점**
- 자연어 태그 파싱 규칙 제거 → IR JSON 직접 변환
- 원본 질의(`question`) 추가 — LLM이 맥락 유지를 위해 참조
- few-shot 예시: 자연어 Q → SQL 구조 참고용 (도메인 무관), IR과 포맷 불일치를 명시적으로 안내
- 충돌 우선순위 규칙 불필요 (정보 소스가 IR로 단일화)

---

## main.py / steps 연동

| 변수 | 설명 |
|------|------|
| `schema_linking_json` | schema_linking 출력 (rewrite_query 입력) |
| `rewritten_question` | IR JSON 문자열 (generate_sql 입력) |
| `question` | 원본 자연어 질문 (generate_sql 추가 입력) |
| `few_shot_examples` | MQS 검색 결과 (`question_ko`, `sql` 필드) |

v9에서 rewrite 단계는 JSON IR을 출력하므로 `main.py`에서 `_REWRITE_IR_MODE = True`로 처리:
- system prompt: "Output ONLY a valid JSON IR object"
- `stop: ["\n"]` 파라미터 제거 (멀티라인 JSON)
- `REWRITE_MAX_TOKENS`: 128 → 512

---

## 설계 의도

**역할 분리 원칙**

| 단계 | 책임 |
|------|------|
| schema_linking | 무엇이 어떤 컬럼인가 (grounding) |
| rewrite_query | SQL을 어떻게 구성할 것인가 (구조 설계) |
| generate_sql | IR을 SQL 문법으로 변환 (렌더링) |

각 단계가 명확히 분리되어 있어 LLM이 역할 혼동 없이 좁은 범위의 작업에 집중할 수 있습니다.
특히 날짜 SARGable 변환과 JOIN 조건 추론을 rewrite 단계에서 수행함으로써,
generate_sql 단계는 IR을 기계적으로 변환하는 역할에 집중합니다.
