# Prompt v4 변경 내역

## 개요

v4는 v3에서 **엔티티 추출 단계(LLM 호출)를 제거**하고, 원본 질문을 스키마 링킹에 직접 입력하는 버전입니다.
LLM 호출 횟수를 1회 줄이면서 스키마 링킹 모델이 질문 분석과 컬럼 매핑을 한 번에 처리합니다.

---

## 파이프라인 비교

```
v3 (6단계):
  Step1 엔티티추출(LLM) → Step2 IR → Step3 스키마링킹(LLM) → Step4 Rewrite(LLM) → Step5 SQL(LLM) → Step6 DB실행

v4 (5단계):
  Step1 IR → Step2 스키마링킹(LLM) → Step3 Rewrite(LLM) → Step4 SQL(LLM) → Step5 DB실행
```

---

## 각 프롬프트 역할

| 파일 | 역할 | v3 대비 |
|---|---|---|
| `extract_entity.j2` | — | **제거** (파일 없음) |
| `schema_linking.j2` | 원본 질문 → 테이블·컬럼 매핑 | 입력이 entity_json → question으로 변경 |
| `rewrite_query.j2` | 스키마 링킹 결과 기반 질문 재작성 | entity_json 섹션 제거 |
| `generate_sql.j2` | SQL 생성 | entity_json 섹션 제거 |

---

## 핵심 변경: schema_linking 입력

### v3
```
입력: schema_candidates (IR 결과) + entity_json (엔티티 추출 결과)
```

### v4
```
입력: schema_candidates (IR 결과) + question (원본 질문 그대로)
```

스키마 링킹 모델이 질문을 직접 읽고 필요한 테이블·컬럼을 스스로 파악합니다.

---

## schema_linking 출력 포맷 (v3 대비 동일)

```json
{
  "linked_tables": ["orders", "customers"],
  "column_mappings": {
    "주문 건수": "orders.id",
    "지역": "customers.region"
  },
  "filter_conditions": {
    "products.stock_quantity": "10 미만"
  },
  "time_column": "orders.order_date",
  "aggregation": "COUNT",
  "group_by_columns": ["customers.region"],
  "sort_order": {"column": "orders.id", "direction": "DESC"}
}
```

---

## IR 쿼리 변화

| 버전 | IR 검색 쿼리 |
|------|-------------|
| v3 | entity_phrases 추출 결과 ("알람 이력 장비") |
| v4 | 원본 질문 그대로 ("어제 발생한 알람 이력에서 장비별 건수 보여줘") |

---

## 전환 방법

```python
# config.py
PROMPT_VERSION = "v4"
```

`extract_entity.j2`가 없는 것을 자동 감지하여 엔티티 추출 단계를 건너뜁니다.
스키마 링킹 프롬프트에 `question` 변수가 자동으로 전달됩니다.

---

## v3 대비 장단점

| | v3 | v4 |
|--|----|----|
| LLM 호출 수 | 4회 | 3회 |
| 지연 시간 | 상대적으로 느림 | 빠름 |
| IR 쿼리 품질 | entity_phrases로 노이즈 제거 | 원본 질문 (노이즈 포함 가능) |
| 스키마 링킹 부담 | 가벼움 (엔티티 이미 구조화) | 무거움 (질문 직접 파싱) |
| 추천 상황 | SLM 사용, 복잡한 질문 | 빠른 응답이 필요할 때 |
