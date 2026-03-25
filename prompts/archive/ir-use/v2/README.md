# Prompt v2 변경 내역

## 개요

v2는 엔티티 추출(Step 1) 방식을 전면 개편한 버전입니다.
v1의 키워드 기반 추출에서 벗어나, **DB 스키마를 직접 참조한 구조화된 JSON 출력**으로 전환합니다.

---

## 변경 파일

### `extract_entity.j2` — 전면 재작성

#### v1 출력 포맷
```json
{
  "entity_phrases": ["알람 이력", "장비", "건수"],
  "time_phrases": ["어제"]
}
```
- 자연어 키워드 단순 추출
- 스키마와 무관한 일반 명사 수준

#### v2 출력 포맷
```json
{
  "tables": ["alarm_history", "equipment"],
  "columns": {
    "alarm_history": ["id", "create_date"],
    "equipment": ["name"]
  },
  "filters": [
    {"column": "alarm_history.create_date", "value": "어제"}
  ],
  "aggregations": ["COUNT"]
}
```
- **실제 테이블명·컬럼명** 으로 직접 매핑
- WHERE 조건(`filters`)과 집계 함수(`aggregations`) 명시적 분리

#### 주요 변경 사항

| 항목 | v1 | v2 |
|------|----|----|
| 스키마 컨텍스트 | 없음 | 전체 테이블·컬럼 목록 주입 (`{{ schema_info }}`) |
| 출력 키 | `entity_phrases`, `time_phrases` | `tables`, `columns`, `filters`, `aggregations` |
| 테이블 특정 여부 | 키워드 수준 (모호) | 실제 테이블명 특정 |
| 필터 조건 | `time_phrases`에 혼재 | `filters[].column / value` 로 명시 |
| 집계 함수 | 없음 | `aggregations` 배열로 명시 |
| 할루시네이션 방지 | 없음 | 스키마 목록 외 추측 금지 규칙 |

### `rewrite_query.j2`, `generate_sql.j2`

v2에서는 변경 없이 v1과 동일합니다.

---

## 코드 변경 사항 (자동 적용됨)

### `steps/extract_entity.py`
- `SCHEMA_JSON_PATH`에서 스키마를 임포트 시점에 로드 (`_SCHEMA_INFO`)
- `render_prompt()` 호출 시 `schema_info=_SCHEMA_INFO` 를 템플릿에 추가 전달
- v1 템플릿은 `schema_info` 변수를 사용하지 않으므로 **v1 호환 유지**

### `main.py` — `_build_schema_query()`
v2 출력의 `tables` 키를 인식하도록 분기 추가:
```python
# v2: 실제 테이블명 기반으로 정밀 스키마 검색
if "tables" in entities:
    parts = list(entities.get("tables") or [])
    ...

# v1: 기존 entity_phrases 방식 유지
```

---

## 버전 전환 방법

`config.py` 한 줄만 변경하면 전체 파이프라인(main.py, eval/main_evaluate.py 포함)이 전환됩니다.

```python
# config.py
PROMPT_VERSION = "v2"   # "v1" ↔ "v2" 전환
```

---


## 의견. 
1. 스키마 링킹과 재작성이 동시에 이루어지고 있음 
-> v3에서는 분리 
2. 엔티티 추출이 적음. 