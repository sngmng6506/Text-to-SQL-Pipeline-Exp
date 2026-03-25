# Prompt v5 변경 내역

## 개요

v5는 v4에서 발생한 **SQL 생성 오류(한글 설명을 컬럼명으로 오인)** 를 수정한 버전입니다.
파이프라인 구조는 v4와 동일하며, `generate_sql.j2` 프롬프트만 수정되었습니다.

---

## 파이프라인 비교

```
v4 (5단계):
  Step1 IR → Step2 스키마링킹(LLM) → Step3 Rewrite(LLM) → Step4 SQL(LLM) → Step5 DB실행

v5 (5단계):
  Step1 IR → Step2 스키마링킹(LLM) → Step3 Rewrite(LLM) → Step4 SQL(LLM) → Step5 DB실행
```

파이프라인 단계 수는 동일합니다.

---

## 각 프롬프트 역할

| 파일 | 역할 | v4 대비 |
|---|---|---|
| `schema_linking.j2` | 원본 질문 → 테이블·컬럼 매핑 | **변경 없음** |
| `rewrite_query.j2` | 스키마 링킹 결과 기반 질문 재작성 | **변경 없음** |
| `generate_sql.j2` | SQL 생성 | **수정** (아래 참고) |

---

## 핵심 변경: generate_sql.j2

### 문제 (v4)

재작성 질의의 `<table.column = '한글설명'>` 태그에서 SQL 생성기가
실제 컬럼명(`column`) 대신 한글 설명(`'한글설명'`)을 컬럼명으로 사용하는 오류가 발생했습니다.

```
재작성 질의: <crane_cell.equipment_id = '크레인 장비'>의 평균 ...

잘못 생성된 SQL:
  SELECT DISTINCT "크레인 장비"   ← "크레인 장비"를 컬럼명으로 오인
  FROM crane_cell
  WHERE "담당 구역 수" > ...       ← 오류: 해당 컬럼 없음
```

### 원인 분석

v4의 `generate_sql.j2`에서 `[2순위] 스키마 후보` 섹션으로 `schema_candidates`를 함께 전달했습니다.
스키마 후보 텍스트(`equipment_id (varchar) — 크레인 장비`)와 재작성 질의 태그(`= '크레인 장비'`)가
동시에 노출되면서 LLM이 한글 설명을 컬럼 alias로 오인했습니다.

### 수정 내용 (v5)

**1. `[2순위] 스키마 후보` 섹션 제거**

스키마 링킹 결과(`schema_linking_json`)에 이미 `linked_tables`, `column_mappings`,
`filter_conditions` 등이 구조화되어 있으므로, 원시 스키마 텍스트를 SQL 생성 단계에
추가로 전달할 필요가 없습니다.

```diff
- ## [2순위] 스키마 후보 — 링킹 결과에 없는 컬럼을 보완할 때만 참고
- ```
- {{ schema_candidates }}
- ```
```

**2. 태그 파싱 규칙 섹션 추가**

`<table.column = '설명'>` 형식의 해석 방법을 명시적으로 정의하고,
오용 패턴과 올바른 사용 예시를 함께 제공합니다.

```
## 태그 파싱 규칙 — 반드시 준수
- table  : 테이블명 → FROM/JOIN에 사용
- column : 실제 컬럼명 → SELECT/WHERE/GROUP BY에 반드시 이 값을 사용
- '설명' : 원본 한국어 표현 (무시) → SQL 컬럼명으로 절대 사용 금지

예시:
  <crane_cell.equipment_id = '크레인 장비'>
    → 컬럼: equipment_id  (O)
    → "크레인 장비"를 컬럼명으로 쓰면 안 됨  (X)
```

**3. 태그 파싱 예시(예시 5) 추가**

실제 오류 케이스(`crane_cell`)를 출력 예시에 추가하여 LLM이 올바른 패턴을 학습하도록 유도합니다.

---

## v4 대비 장단점

| | v4 | v5 |
|--|----|----|
| SQL 컬럼명 오인 오류 | 발생 가능 | 수정됨 |
| 스키마 후보 fallback | 있음 (schema_candidates 전달) | 없음 (schema_linking_json만 사용) |
| generate_sql 프롬프트 길이 | 상대적으로 김 | 단축됨 |
| 누락 컬럼 보완 수단 | schema_candidates | schema_linking validator 재링킹 |

---

## 전환 방법

```python
# config.py
PROMPT_VERSION = "v5"
```
