# Prompt v8 변경 내역

## 개요

v8은 v7의 프롬프트 구조를 유지하면서 **DAIL-SQL의 MQS(Masked Question Similarity) 기법을 활용한 동적 few-shot 예시**를 `generate_sql.j2`에 추가한 버전입니다.
고정된 예시 대신, 입력 질문과 의미적으로 유사한 질문-SQL 쌍을 런타임에 선택하여 프롬프트에 삽입합니다.

---

## 파이프라인 비교

```
v7 (5단계):
  Step1 IR → Step2 스키마링킹(LLM) → Step3 Rewrite(LLM) → Step4 SQL(LLM) → Step5 DB실행

v8 (5단계):
  Step1 IR → Step2 스키마링킹(LLM) → Step3 Rewrite(LLM) → Step4 SQL(LLM, +MQS few-shot) → Step5 DB실행
```

파이프라인 단계 수는 동일하며, SQL 생성 단계에 동적 few-shot이 추가됩니다.

---

## 각 프롬프트 역할

| 파일 | 역할 | v7 대비 |
|---|---|---|
| `schema_linking.j2` | 원본 질문 → 테이블·컬럼 매핑 | **변경 없음** |
| `rewrite_query.j2` | 스키마 링킹 결과 기반 질문 재작성 | **변경 없음** |
| `generate_sql.j2` | SQL 생성 | **수정** — MQS few-shot 섹션 추가 |

---

## 핵심 변경: MQS 기반 동적 few-shot

### DAIL-SQL MQS란?

DAIL-SQL (Gao et al., 2023)에서 제안한 **Masked Question Similarity** 방식으로,
입력 질문과 예시 후보들을 비교할 때 DB 고유 토큰(테이블명, 컬럼명, 값 등)을 마스킹한 뒤
순수 의미 유사도를 기준으로 가장 적합한 few-shot 예시를 선택합니다.

```
원본 질문: "장비별 어제 알람 건수를 내림차순으로 보여줘"
마스킹 후: "[ENTITY]별 어제 [ENTITY] 건수를 내림차순으로 보여줘"
                        ↓ 유사도 계산
후보 예시: "[ENTITY]별 [DATE] [ENTITY] 건수를 내림차순으로 보여줘" → 선택
```

### 기존 고정 예시와의 차이

| | v7 (고정 예시) | v8 (MQS 동적 선택) |
|--|--------------|-------------------|
| 예시 선택 방식 | 작성자가 하드코딩 | 질문 유사도 기반 자동 선택 |
| 예시 관련성 | 일반적 케이스만 커버 | 입력 질문에 특화 |
| 예시 개수 | 고정 | 설정값으로 조절 (기본 3개) |
| 예시 출처 | 프롬프트 내 정적 텍스트 | 별도 예시 풀(pool) JSON |

### 동작 흐름

```
1. 입력 질문에서 DB 고유 토큰 마스킹 (스키마 후보 기반)
2. 예시 풀(pool)의 모든 질문도 동일하게 마스킹
3. 마스킹된 질문 간 임베딩 유사도 계산 (TEI 서버 활용)
4. 상위 N개 예시 선택
5. 선택된 예시(원본 질문 + SQL)를 generate_sql.j2에 삽입
```

---

## 구현 구성 요소

### 신규: 예시 풀 (few-shot pool)

평가 데이터 중 정답이 확인된 질문-SQL 쌍을 별도 JSON으로 관리합니다.

```
data/few_shot/pool.json
[
  {
    "question": "어제 장비별 알람 건수를 내림차순으로 보여줘",
    "sql": "SELECT equipment_id, COUNT(*) AS cnt\nFROM alarm_history\nWHERE ...\nGROUP BY equipment_id\nORDER BY cnt DESC\nLIMIT 200;"
  },
  ...
]
```

### 신규: MQS 선택 로직

`steps/few_shot.py` 에 구현 예정.

```python
def select_few_shots(question: str, pool: list[dict], top_k: int = 3) -> list[dict]:
    """MQS 방식으로 few-shot 예시를 선택한다."""
    masked_input = mask_schema_tokens(question)
    masked_pool  = [mask_schema_tokens(e["question"]) for e in pool]
    # TEI 임베딩 유사도 계산 후 top_k 반환
    ...
```

### 수정: generate_sql.j2

프롬프트에 `{{ few_shot_examples }}` 변수 섹션이 추가됩니다.

```
## 유사 질문 예시 (Few-shot)
아래는 현재 질문과 유사한 질문-SQL 쌍이다. SQL 구조 참고용으로만 사용하고,
테이블명·컬럼명은 반드시 스키마 링킹 결과를 따를 것.

{{ few_shot_examples }}
```

---

## 설정 (config.py 추가 예정)

```python
FEW_SHOT_POOL_PATH = DATA_DIR / "few_shot" / "pool.json"
FEW_SHOT_TOP_K     = 3   # 선택할 예시 수
```

---

## 기대 효과

| 오류 타입 | 기대 개선 이유 |
|-----------|--------------|
| WRONG_AGGREGATION | 유사한 집계 패턴 예시로 올바른 집계 함수 유도 |
| WRONG_CONDITION | 유사한 조건 구조 예시 참고 |
| WRONG_COLUMN (일부) | SQL 구조 패턴을 참고해 링킹 결과 활용 개선 |

---

## 유의사항

- 예시 풀의 SQL은 **실제 DB에서 검증된 정답 SQL만** 포함해야 합니다.
- 마스킹 품질이 MQS 성능에 직접적인 영향을 미칩니다.
- 예시가 추가되면 프롬프트 길이가 늘어나 **스키마 링킹 이상으로 토큰 소모가 증가**할 수 있습니다. `SQL_MAX_TOKENS` 조정이 필요할 수 있습니다.
- few-shot 예시가 오히려 LLM을 잘못된 패턴으로 유도할 가능성도 있으므로, 예시 풀의 질 관리가 중요합니다.

---

## 전환 방법

```python
# config.py
PROMPT_VERSION = "v8"
```
