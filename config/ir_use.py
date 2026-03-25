"""
IR-use 파이프라인 설정 (pipelines/ir_use.py)

프롬프트 경로, SL/IR 생성, Judge, temperature, MQS, Value Hints 등.
"""

from __future__ import annotations

import os

from .common import BASE_DIR, DATA_DIR, PROMPTS_DIR

# ── 프롬프트 버전 ────────────────────────────────────────────────────────────
# ※ 버전 실험 시 IR_PROMPT_VERSION 한 줄만 변경 (예: "v9", "v10")
IR_PROMPT_VERSION = "v10"
IR_PROMPTS_DIR    = PROMPTS_DIR / "ir-use" / IR_PROMPT_VERSION

EXTRACT_ENTITY_TEMPLATE_PATH = IR_PROMPTS_DIR / "extract_entity.j2"
SCHEMA_LINKING_TEMPLATE_PATH = IR_PROMPTS_DIR / "schema_linking.j2"
REWRITE_QUERY_TEMPLATE_PATH  = IR_PROMPTS_DIR / "rewrite_query.j2"
GENERATE_SQL_TEMPLATE_PATH   = IR_PROMPTS_DIR / "generate_sql.j2"

# 평가 결과 저장 경로 (버전별 분리)
EVAL_OUTPUT_DIR     = DATA_DIR / "eval_data" / "results" / IR_PROMPT_VERSION
ANALYSIS_OUTPUT_DIR = BASE_DIR / "eval-analysis" / "results" / IR_PROMPT_VERSION

# ── 파이프라인 파라미터 ──────────────────────────────────────────────────────
ENTITY_MAX_TOKENS         = 512   # reasoning 모델: 내부 추론 토큰 포함이라 여유 필요
SCHEMA_LINKING_MAX_TOKENS = 1024  # reasoning 모델: 추론 후 JSON 출력까지 여유 필요
REWRITE_IR_MODE           = True
REWRITE_MAX_TOKENS        = 768 if REWRITE_IR_MODE else 256
SCHEMA_LINKING_MAX_RETRIES = 2
SQL_DB_MAX_RETRIES         = 2

# ── 스키마 링킹 다중 생성 ────────────────────────────────────────────────────
SL_CANDIDATES_N = 3

# ── IR 배치 생성 & 선택 ──────────────────────────────────────────────────────
IR_CANDIDATES_N        = 3
IR_CANDIDATES_TEMP     = 0.5
IR_GUIDED_JSON              = False
SCHEMA_LINKING_GUIDED_JSON  = False
GUIDED_DECODING_BACKEND     = "outlines"

# IR 후보 선택 방법: "llm_judge" | "cross_encoder" | "majority_vote"
IR_SELECT_METHOD    = "llm_judge"
IR_JUDGE_COT        = False
IR_JUDGE_TEMPERATURE = 0.0
IR_JUDGE_SCORE_MAX_TOKENS = 128
IR_JUDGE_REASONING_MAX_TOKENS = 1024
PAIR_JUDGE_TEMPERATURE = 0.0
PAIR_JUDGE_SCORE_MAX_TOKENS = 128
PAIR_JUDGE_REASONING_MAX_TOKENS = 1024
RERANKER_MODEL_PATH = os.getenv("RERANKER_MODEL_PATH", "")

# ── Deterministic 단계 temperature ──────────────────────────────────────────
ENTITY_EXTRACTION_TEMPERATURE = 0.0
SCHEMA_LINKING_TEMPERATURE = 0.0
SCHEMA_CORRECTION_TEMPERATURE = 0.0
REWRITE_SINGLE_TEMPERATURE = 0.0
SQL_GENERATION_TEMPERATURE = 0.0

# ── MQS Few-Shot ─────────────────────────────────────────────────────────────
MQS_POOL_SOURCE   = "train"
MQS_POOL_NPZ_PATH = BASE_DIR / "MQS-pool" / "spider" / MQS_POOL_SOURCE / "pool_embeddings.npz"
FEW_SHOT_TOP_K    = 3

# ── Value Hints ──────────────────────────────────────────────────────────────
VALUE_HINT_DISTINCT_LIMIT     = 20
VALUE_HINT_ALLOWED_MAX        = 20
VALUE_HINT_SAMPLE_MAX         = 5
VALUE_HINT_TIMEOUT_MS         = 1500
VALUE_HINT_INCLUDE_FILTER_COLUMNS   = True
VALUE_HINT_INCLUDE_TEMPORAL_COLUMNS = True
VALUE_HINT_INCLUDE_MAPPED_COLUMNS   = True
VALUE_HINT_EXCLUDE_PK_FK            = False
VALUE_HINT_EXCLUDE_BOOLEAN_COLUMNS  = True
VALUE_HINT_EXCLUDE_ID_LIKE_COLUMNS  = True
