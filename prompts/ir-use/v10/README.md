# v10 프롬프트

v9의 IR 기반 파이프라인 구조를 유지하면서, **IR 생성 단계의 판단 자율성 확대**와 **다수결 기반 IR 선택(Majority Voting)** 을 도입했습니다.

---

## v9 대비 구조 변화

| 항목 | v9 | v10 |
|------|----|----|
| IR 생성 방식 | 단일 생성 (temperature=0.0) | **N개 배치 생성 후 다수결 선택** |
| rewrite_query 규칙 | 엄격한 규칙 기반 | **질문 의도 우선 판단 기반** |
| 문자열 필터 | 무조건 ILIKE | **코드값 → `=`, 자연어 → `ILIKE` 판단** |
| 날짜 처리 | 무조건 SARGable 변환 | **날짜 범위 vs 최신 N건 구분 후 처리** |
| SELECT 범위 | linked_columns 전부 | **질문에서 요청한 컬럼만** |

---

## 파이프라인 흐름

```
자연어 질문
    │
    ▼
[schema_linking]  ─── 컬럼 grounding만 수행 (v9와 동일)
    │  linked_tables, linked_columns, column_mappings
    │  filter_columns (값 없음), temporal_columns (값 없음)
    │
    ▼
[rewrite_query × N]  ─── IR 후보 N개 배치 생성 (temperature > 0)
    │  각 후보: { FROM, JOIN, FILTER, GROUP_BY, AGGREGATE,
    │             HAVING, ORDER_BY, SELECT }
    │
    ▼
[ir_selector]  ─── 다수결(Majority Voting)로 최적 IR 선택
    │  fingerprint 기반 집계 → 득표 최다 그룹 → completeness 최고 선택
    │
    ▼
[generate_sql]  ─── IR + 원본 질의 + few-shot → PostgreSQL SQL (v9와 동일)
```

---

## 각 프롬프트 상세

### `schema_linking.j2`

v9와 동일. 변경 없음.

---

### `rewrite_query.j2`

**역할**: 자연어 질문과 schema_linking 결과를 함께 읽고, 질문 의도를 우선으로 JSON IR을 출력한다.

**입력 변수**: `question`, `schema_linking_json`

**v9 대비 핵심 변경점**

| 항목 | v9 | v10 |
|------|----|----|
| 판단 주체 | 규칙 | **질문 의도 우선, 스키마 링킹은 grounding 참고** |
| 문자열 필터 | `ILIKE` 강제 | 코드값 `=` / 자연어 `ILIKE` 판단 |
| 날짜 조건 | 항상 SARGable 변환 | 날짜 범위인지, 최신 N건인지 구분 후 처리 |
| SELECT | linked_columns 전부 나열 | 질문에서 실제 요청한 컬럼만 선택 |
| LIMIT | 없음 | 단건/N건 의도 → ORDER_BY + LIMIT으로 IR에 표현 |
| filter_columns | 전부 FILTER 추가 | **질문에서 필터 의도 있는 컬럼만** 추가 |

**출력 IR JSON 구조** (v9와 동일)

```json
{
  "FROM": ["테이블"],
  "JOIN": ["left.fk = right.pk"],
  "FILTER": ["SARGable 조건식 또는 = '코드값'"],
  "GROUP_BY": ["테이블.컬럼"],
  "AGGREGATE": ["COUNT(테이블.컬럼)"],
  "HAVING": ["조건식"],
  "ORDER_BY": ["테이블.컬럼 DESC", "LIMIT N"],
  "SELECT": ["테이블.컬럼"]
}
```

**추가된 예시**

- 예시 3: 최신 N건 → FILTER 없이 `ORDER_BY + LIMIT` 처리

---

### `generate_sql.j2`

v9와 동일. 변경 없음.

---

## IR 배치 생성 & 다수결 선택 (신규)

### 설정 (`config.py`)

```python
IR_CANDIDATES_N    = 6    # 한 번에 생성할 IR 후보 수 (1이면 단일 생성, 기존 동작)
IR_CANDIDATES_TEMP = 0.4  # 다양성을 위한 temperature
```

### 다수결 로직 (`schema/ir_selector.py`)

1. 각 IR을 **fingerprint** 로 변환:
   - `(FROM 테이블 집합, JOIN 테이블 집합, FILTER 수, GROUP_BY 집합, ORDER_BY 정보, SELECT 수, 집계 함수)`
2. 가장 많이 등장한 fingerprint 그룹 선택 (다수결)
3. 동점 시 **completeness 점수** (채워진 필드 항목 수) 최고인 후보 선택

### 로그 예시

```
==================================================
  STEP 4 · IR 후보 6개 (다수결)  (12.30s)
==================================================
[후보 1] ★ 선택  (완성도=9)
{ "FROM": [...], "FILTER": ["status = 'FAULT'"], ... }

[후보 2]   득표 3  (완성도=7)
{ "FROM": [...], "FILTER": ["status ILIKE '%장애%'"], ... }
...

==================================================
  STEP 4-선택 · IR JSON (선택됨)  (0.00s)
==================================================
{ "FROM": [...], ... }
```

---

## main.py / steps 연동

| 변수 | 설명 |
|------|------|
| `schema_linking_json` | schema_linking 출력 (rewrite_query 입력) |
| `rewritten_question` | 다수결로 선택된 IR JSON 문자열 (generate_sql 입력) |
| `question` | 원본 자연어 질문 (generate_sql 추가 입력) |
| `few_shot_examples` | MQS 검색 결과 (`question_ko`, `sql` 필드) |

`IR_CANDIDATES_N > 1` 이고 `REWRITE_IR_MODE = True` 일 때 배치 생성 활성화:
- `client.chat_completions_n()` 호출 (단일 HTTP 요청으로 N개 반환)
- `ir_selector.majority_vote()` 로 최적 IR 선택
- `IR_CANDIDATES_N = 1` 이면 기존 단일 생성 모드로 폴백

---

## 설계 의도

### v10에서 다수결을 도입한 이유

v9에서 IR 생성 규칙이 지나치게 엄격하여 후보 다양성이 없었고, 이는 다음 문제를 유발했습니다:
- 문자열 필터를 무조건 `ILIKE`로 처리 → 코드값 정확 매칭 누락
- 날짜 조건과 최신 N건 조건을 구분하지 않음 → 의미 오류
- `linked_columns`를 전부 `SELECT`에 나열 → 불필요한 컬럼 포함

v10에서는 **질문 의도 우선 판단**으로 규칙을 완화하고, 다수결로 가장 신뢰도 높은 IR을 선택합니다.

### 역할 분리 원칙 (v9와 동일)

| 단계 | 책임 |
|------|------|
| schema_linking | 무엇이 어떤 컬럼인가 (grounding) |
| rewrite_query | SQL을 어떻게 구성할 것인가 (구조 설계, 질문 의도 기반) |
| ir_selector | 여러 해석 중 가장 신뢰도 높은 IR 선택 |
| generate_sql | IR을 SQL 문법으로 변환 (렌더링) |
