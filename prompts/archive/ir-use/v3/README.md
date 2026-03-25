# Prompt v3 변경 내역

## 개요

v3는 v2에서 결합되어 있던 **엔티티 추출 + 스키마 링킹**을 명확히 분리하고, 스키마 링킹을 독립 단계로 추가한 버전입니다.

---

## 파이프라인 비교

```
v1/v2 (5단계):
  Step1 엔티티추출 → Step2 IR → Step3 Rewrite → Step4 SQL → Step5 DB실행

v3 (6단계):
  Step1 엔티티추출 → Step2 IR → Step3 스키마링킹 → Step4 Rewrite → Step5 SQL → Step6 DB실행
```

---

## 각 프롬프트 역할

| 파일 | 역할 | v2 대비 |
|---|---|---|
| `extract_entity.j2` | 순수 NL 의도 추출 (스키마 모름) | 5개 필드로 확장 |
| `schema_linking.j2` | 엔티티 → 실제 테이블·컬럼 매핑 | **신규** |
| `rewrite_query.j2` | 스키마 링킹 결과 기반 질문 재작성 | schema_linking_json 활용 |
| `generate_sql.j2` | SQL 생성 | schema_linking_json 힌트 추가 |

---

## extract_entity 출력 포맷 (v1 대비 확장)

```json
{
  "entity_phrases": ["알람 이력", "장비"],
  "time_phrases": ["어제"],
  "aggregation_intent": ["건수"],
  "filter_conditions": [],
  "group_by_intent": ["장비별"]
}
```

v1은 `entity_phrases`, `time_phrases` 2개 필드뿐이었으나 v3는 집계/필터/그룹핑 의도를 자연어 그대로 분리 추출합니다.

## schema_linking 출력 포맷 (신규)

```json
{
  "linked_tables": ["orders", "customers"],
  "column_mappings": {"주문": "orders", "지역": "customers.region"},
  "time_column": "orders.order_date",
  "aggregation": "COUNT",
  "group_by_columns": ["customers.region"]
}
```

---

## 전환 방법

```python
# config.py
PROMPT_VERSION = "v3"
```

v1/v2에는 `schema_linking.j2`가 없으므로 Step 3이 자동 스킵됩니다.
