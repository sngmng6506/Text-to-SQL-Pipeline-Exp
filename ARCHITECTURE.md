# TAG-test 아키텍처

> AI 어시스턴트가 대화를 재개할 때 프로젝트 전체를 빠르게 파악하기 위한 문서.
> 코드를 수정할 때마다 이 문서도 함께 갱신한다.

---

## 1. 프로젝트 개요

자연어 질문을 PostgreSQL 쿼리로 변환하는 **Text-to-SQL 파이프라인**.
두 가지 실행 모드를 지원한다.

| 파이프라인 | 모듈 | 모델 | 설명 |
|---|---|---|---|
| **ir-use** | `pipelines/ir_use.py` | vLLM (원격) | 엔티티 추출 → 스키마 링킹 → 질의 재작성(IR) → SQL 생성 → DB 실행 |
| **simple** | `pipelines/simple.py` | transformers (로컬) | 스키마 검색 → SQL 후보 N개 병렬 생성 → 검증 → DB 실행 |

---

## 2. 디렉토리 구조

```
TAG-test/
├── run.py                    # CLI 진입점 — argparse 서브커맨드로 파이프라인 라우팅
│
├── config/                   # 설정 패키지 (__init__.py 가 전부 re-export)
│   ├── __init__.py           #   from .common/ir_use/simple import *
│   ├── common.py             #   공통 경로(BASE_DIR 등), SCHEMA_TOP_K, SQL_MAX_TOKENS
│   ├── ir_use.py             #   IR-use 전용: 프롬프트 버전, SL/IR 파라미터, Judge, MQS, Value Hints
│   └── simple.py             #   Simple 전용: SIMPLE_PROMPT_VERSION, 프롬프트 경로
│
├── pipelines/
│   ├── ir_use.py             # IR-use 파이프라인 본체 (~1 082줄)
│   └── simple.py             # Simple 파이프라인 본체 (~298줄)
│
├── clients/
│   ├── chat.py               # VllmChatClient — vLLM OpenAI-compatible 호출
│   ├── embed.py              # EmbedClient — TEI 임베딩 서버 호출
│   └── local.py              # LocalChatClient — transformers 로컬 추론 (simple용)
│
├── steps/                    # IR-use 파이프라인 단계별 프롬프트 렌더러
│   ├── extract_entity.py     #   엔티티 추출
│   ├── schema_linking.py     #   스키마 링킹
│   ├── rewrite_query.py      #   질의 재작성 / IR 생성
│   ├── generate_sql.py       #   SQL 생성
│   └── few_shot.py           #   MQS Few-Shot 검색
│
├── schema/
│   ├── extract.py            # DB → table_schema JSON 추출
│   ├── build.py              # TEI로 스키마 임베딩 빌드 (.npz)
│   ├── search.py             # 임베딩 기반 스키마 검색 (양쪽 파이프라인 공유)
│   ├── validator.py          # 스키마 검증 (information_schema 기반)
│   └── value_hints.py        # DB 컬럼 허용값/범위 힌트 조회
│
├── ir/
│   ├── llm_judge.py          # LLM Judge — IR 후보 점수 매기기
│   ├── reranker.py           # Cross Encoder 기반 IR 재순위
│   ├── selector.py           # 다수결(majority_vote) IR 선택
│   └── schema.py             # guided_json용 JSON Schema 정의
│
├── db/
│   └── client.py             # PostgreSQL 실행 유틸 (SELECT/WITH만 허용)
│
├── prompts/
│   ├── ir-use/v10/           # IR-use 현행 프롬프트 (Jinja2 .j2)
│   │   ├── extract_entity.j2
│   │   ├── schema_linking.j2
│   │   ├── rewrite_query.j2
│   │   └── generate_sql.j2
│   ├── simple/v1/            # Simple 현행 프롬프트
│   │   └── generate_sql.j2
│   └── archive/ir-use/v1~v9/ # 과거 버전 보관
│
├── eval/
│   └── run.py                # 평가 스크립트 — 데이터셋 일괄 실행 → xlsx 저장
│
├── eval-analysis/
│   ├── run.py                # 평가 분석 — OpenAI API로 정답 여부 판정 → xlsx + json 저장
│   ├── prompts/analyze.j2    # 분석용 프롬프트
│   └── .env                  # 분석 전용 환경변수 (OpenAI API key 등)
│
├── MQS-pool/                 # Few-Shot용 질문-SQL 풀 + 임베딩
│
├── data/
│   ├── schema/               # DB 스키마 JSON (table_schema_*.json)
│   └── eval_data/            # 평가 데이터셋 + 결과 xlsx
│
├── artifacts/embeddings/     # 빌드된 스키마 임베딩 (.npz)
├── .env                      # 메인 환경변수 (VLLM_*, DB_*, TEI_*)
├── requirements.txt
└── README.md
```

---

## 3. 실행 방법

```bash
# IR-use 파이프라인
python run.py ir-use --question "어제 발생한 알람 건수 보여줘"

# Simple 파이프라인
python run.py simple --question "어제 발생한 알람 건수 보여줘"

# 평가
python eval/run.py ir-use
python eval/run.py ir-use --resume            # 최신 파일에서 이어서
python eval/run.py ir-use --resume result.xlsx

python eval/run.py simple                     # 모델 1회 로드 후 전체 루프
python eval/run.py simple --resume

# 평가 분석
cd eval-analysis && python run.py --input ../data/eval_data/results/v10/result_XXX.xlsx
```

---

## 4. 핵심 데이터 흐름

### 4-1. IR-use 파이프라인

```
질문
 ↓ steps/extract_entity.py
엔티티 JSON
 ↓ schema/search.py (임베딩 검색)
스키마 후보 top-K
 ↓ steps/schema_linking.py (SL_CANDIDATES_N개 생성, guided_json)
스키마 링킹 JSON × N
 ↓ steps/rewrite_query.py (각 SL당 IR_CANDIDATES_N개, temperature 다양성)
IR JSON 후보 × SL×IR
 ↓ ir/llm_judge.py | ir/reranker.py | ir/selector.py (IR_SELECT_METHOD로 선택)
최적 IR 1개
 ↓ steps/generate_sql.py
SQL
 ↓ db/client.py
실행 결과
```

### 4-2. Simple 파이프라인

```
질문
 ↓ schema/search.py (임베딩 검색)
스키마 후보 top-K
 ↓ clients/local.py (num_return_sequences 병렬)
SQL 후보 N개
 ↓ schema/validator.py (구문 검증)
유효 SQL 필터링
 ↓ db/client.py (실행 + 다수결 선택)
최종 SQL + 결과
```

---

## 5. 설정 구조 (`config/`)

`from config import X`로 어디서든 import 가능 (`__init__.py`가 re-export).

| 파일 | 내용 |
|---|---|
| `common.py` | `BASE_DIR`, 데이터/스키마/아티팩트 경로, `SCHEMA_TOP_K`, `SQL_MAX_TOKENS` |
| `ir_use.py` | `IR_PROMPT_VERSION(v10)`, 템플릿 경로, SL/IR 생성 수, Judge 설정, temperature, MQS, Value Hints |
| `simple.py` | `SIMPLE_PROMPT_VERSION(v1)`, 프롬프트 경로, CLI 기본값(`MODEL_PATH`, `DEVICE`, `N`) |

버전 실험 시 `ir_use.py`의 `IR_PROMPT_VERSION` 또는 `simple.py`의 `SIMPLE_PROMPT_VERSION` 한 줄만 변경.

---

## 6. 외부 의존 서비스

| 서비스 | 환경변수 | 용도 |
|---|---|---|
| vLLM | `VLLM_BASE_URL`, `VLLM_MODEL` | IR-use LLM 추론 |
| TEI | `TEI_BASE_URL` | 임베딩 생성/검색 |
| PostgreSQL | `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | SQL 실행 |
| OpenAI API | eval-analysis `.env`의 키 | 평가 분석 (정답 여부 판정) |

---

## 7. 프롬프트 버전 관리

```
prompts/
├── ir-use/v10/      ← config/ir_use.py  IR_PROMPT_VERSION = "v10"
├── simple/v1/       ← config/simple.py  SIMPLE_PROMPT_VERSION = "v1"
└── archive/ir-use/  ← v1~v9 보관
```

새 버전을 만들 때: 폴더 복사 → `.j2` 수정 → config에서 버전 문자열만 변경.

---

## 8. 주의사항

- `db/client.py`는 **SELECT/WITH만 허용** (DML 차단)
- `schema/validator.py`는 `information_schema`를 1회 조회 후 `lru_cache` 영구 캐싱
- `clients/local.py`는 모델을 **지연 초기화** (최초 호출 시 1회 로드)
- `ir/schema.py`에 `guided_json`용 JSON Schema가 정의되어 있음 — 스키마 링킹·IR 구조 변경 시 반드시 함께 수정
- `eval/run.py simple`은 루프 시작 전 모델을 **1회만 로드**하므로 매 질문마다 재로딩 없음
- Simple 평가 결과는 `data/eval_data/results-simple/<SIMPLE_PROMPT_VERSION>/` 에 저장됨
