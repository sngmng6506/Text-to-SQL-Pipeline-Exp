# MQS Pool Builder

DAIL-SQL의 **MQS(Masked Question Similarity)** 방식을 활용한 Few-Shot 예시 풀(pool)을 구축하는 스크립트입니다.

Spider 데이터셋의 질문/SQL을 파싱하고, 한국어로 번역한 뒤 임베딩하여 파이프라인의 SQL 생성 단계에서 유사 예시를 동적으로 검색할 수 있게 합니다.

---

## 폴더 구조

```
MQS-pool/
├── main_build_pool.py       ← 빌드 스크립트
├── README.md
└── spider/
    ├── dev.sql              ← Spider dev 원본 (Question + SQL 형식)
    ├── train_spider.json    ← Spider train 원본 (JSON 형식)
    ├── dev/                 ← dev 풀 산출물
    │   ├── questions_en.txt
    │   ├── sqls.txt
    │   ├── pool.json
    │   └── pool_embeddings.npz
    └── train/               ← train 풀 산출물
        ├── questions_en.txt
        ├── sqls.txt
        ├── pool.json
        └── pool_embeddings.npz
```

---

## 처리 흐름

```
원본 데이터
    │
    ▼ 1. 파싱 (parse_dev_sql / parse_train_json)
질문(EN) + SQL + 마스킹 정보
    │
    ▼ 2. 마스킹
    ├── mask_question_en(): query_toks 기반으로 질문 내 DB 특화 토큰 → [COL]
    └── mask_sql():         query_toks_no_value 기반으로 SQL 내 테이블·컬럼 → [COL], 값 → [VAL]
    │
    ▼ 3. 번역 (OpenAI API, 병렬)
    ├── question_en → question_ko
    └── masked_question_en → masked_question_ko
    │
    ▼ 4. 저장 → pool.json
    │
    ▼ 5. 임베딩 (TEI, 병렬)
    └── masked_question_ko (없으면 question_ko) → 벡터화
    │
    ▼ pool_embeddings.npz
```

---

## pool.json 스키마

각 항목은 다음 필드를 포함합니다.

| 필드 | 설명 |
|---|---|
| `id` | 데이터셋 내 순번 |
| `question_en` | 원본 영어 질문 |
| `masked_question_en` | DB 특화 토큰을 `[COL]`로 치환한 영어 질문 (train만 해당) |
| `question_ko` | 번역된 한국어 질문 (LLM 표시용) |
| `masked_question_ko` | 마스킹된 한국어 질문 (임베딩 검색용) |
| `translated` | `question_ko` 번역 성공 여부 |
| `masked_translated` | `masked_question_ko` 번역 성공 여부 |
| `sql` | 원본 SQL (LLM 표시용) |
| `masked_sql` | 테이블·컬럼을 `[COL]`, 값을 `[VAL]`로 치환한 SQL (train만 해당) |

---

## pool_embeddings.npz 스키마

| 키 | dtype | 내용 |
|---|---|---|
| `embeddings` | `float32 [N, D]` | L2 정규화된 임베딩 벡터 |
| `ids` | `int32 [N]` | pool.json의 id와 대응 |
| `questions_ko` | `str [N]` | 검색용 텍스트 (`masked_question_ko` 우선) |
| `original_questions_ko` | `str [N]` | 원본 한국어 질문 (LLM 표시용) |
| `sqls` | `str [N]` | 원본 SQL |
| `masked_sqls` | `str [N]` | 마스킹된 SQL |

> 임베딩은 `masked_question_ko`(없으면 `question_ko`)로 생성되며 L2 정규화됩니다.  
> 코사인 유사도 계산 시 내적만으로 처리할 수 있습니다.

---

## 마스킹 방식

### 질문 마스킹 (`mask_question_en`)

`query_toks`에서 SQL 키워드/연산자가 아닌 토큰(테이블명·컬럼명·값)을 추출하여 `question_toks`의 대응 단어를 `[COL]`로 치환합니다.

```
질문: How many heads of the departments are older than 56 ?
SQL:  SELECT count(*) FROM head WHERE age > 56
→ 마스킹: How many [COL] of the [COL] are older than [COL] ?
```

### SQL 마스킹 (`mask_sql`)

`query_toks_no_value`를 활용해 SQL 구조 키워드와 연산자를 유지하고, 테이블명·컬럼명은 `[COL]`, 값 placeholder는 `[VAL]`로 치환합니다.

```
SQL:         SELECT count(*) FROM head WHERE age > 56
→ masked_sql: SELECT count ( * ) FROM [COL] WHERE [COL] > [VAL]
```

> 마스킹은 **유사 예시 검색(임베딩)**에만 사용됩니다.  
> LLM에 제공되는 few-shot 예시에는 원본 `question_ko`와 원본 `sql`이 사용됩니다.

---

## 실행 방법

TAG-test 루트 디렉토리에서 실행합니다.

```bash
# dev 풀 처음부터 빌드
python MQS-pool/main_build_pool.py

# train 풀 처음부터 빌드
python MQS-pool/main_build_pool.py --source train

# 이어서 실행 (번역/임베딩 완료 건 건너뜀)
python MQS-pool/main_build_pool.py --source train --resume

# 번역 완료 후 임베딩만 실행
python MQS-pool/main_build_pool.py --source train --embed-only
```

---

## 환경 변수

`eval-analysis/.env` 또는 `.env`에 설정합니다.

| 변수 | 기본값 | 설명 |
|---|---|---|
| `OPENAI_API_KEY` | - | OpenAI API 키 (번역용) |
| `OPENAI_MODEL` | `gpt-4o-mini` | 번역에 사용할 모델 |
| `OPENAI_TIMEOUT_SEC` | `60` | OpenAI 요청 타임아웃 (초) |
| `TEI_BASE_URL` | `http://172.22.51.221:8080` | TEI 임베딩 서버 URL |
| `TEI_TIMEOUT_SEC` | `60` | TEI 요청 타임아웃 (초) |

---

## 성능 설정

`main_build_pool.py` 상단의 상수로 조정합니다.

| 상수 | 기본값 | 설명 |
|---|---|---|
| `TRANSLATE_BATCH` | `25` | OpenAI 번역 배치 크기 |
| `TRANSLATE_WORKERS` | `4` | 번역 병렬 스레드 수 |
| `EMBED_BATCH` | `8` | TEI 배치 크기 (서버 최대값) |
| `EMBED_WORKERS` | `4` | 임베딩 병렬 요청 수 |
| `EMBED_SAVE_EVERY` | `500` | 임베딩 중간 저장 간격 (건) |

---

## Resume 동작

중단 후 `--resume` 또는 `--embed-only`로 재실행 시 이어서 처리합니다.

| 단계 | Resume 조건 |
|---|---|
| 번역 (`question_ko`) | `pool.json`에 `question_ko` 값이 있으면 건너뜀 |
| 번역 (`masked_question_ko`) | `pool.json`에 `masked_question_ko` 값이 있으면 건너뜀 |
| 임베딩 | npz에 `masked_sqls`, `original_questions_ko` 키가 모두 있으면 기존 ID 건너뜀 |
| 임베딩 (구버전 npz) | 위 키 중 하나라도 없으면 전체 재임베딩 |

임베딩은 `EMBED_SAVE_EVERY`건마다 `.tmp.npz` → rename 방식으로 중간 저장합니다.
