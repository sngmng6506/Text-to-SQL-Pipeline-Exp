"""
Simple 파이프라인 설정 (pipelines/simple.py)

IR 단계를 생략하고, 로컬 모델로 SQL 후보를 직접 생성하는 경량 파이프라인.

분리 원칙:
  .env       → 서버 주소(URL), 모델명, DB 접속 정보 등 환경별로 달라지는 값
  이 파일    → 토큰 수, top_k, timeout 등 파라미터 튜닝값 (하드코딩)
"""

from __future__ import annotations

import os

from .common import BASE_DIR, PROMPTS_DIR

# ── 프롬프트 버전 ────────────────────────────────────────────────────────────
# ※ 버전 실험 시 SIMPLE_PROMPT_VERSION 한 줄만 변경 (예: "v1", "v2")
SIMPLE_PROMPT_VERSION = "v1"
SIMPLE_PROMPTS_DIR    = PROMPTS_DIR / "simple" / SIMPLE_PROMPT_VERSION

SIMPLE_GENERATE_SQL_TEMPLATE_PATH = SIMPLE_PROMPTS_DIR / "generate_sql.j2"
SIMPLE_JUDGE_SQL_TEMPLATE_PATH    = SIMPLE_PROMPTS_DIR / "judge_sql.j2"

# 평가 결과 저장 경로 (버전별 분리)
SIMPLE_EVAL_OUTPUT_DIR      = BASE_DIR / "data" / "eval_data" / "results-simple" / SIMPLE_PROMPT_VERSION
SIMPLE_ANALYSIS_OUTPUT_DIR  = BASE_DIR / "eval-analysis" / "results" / "simple" / SIMPLE_PROMPT_VERSION

# ── 생성 서버 (.env 필수) ────────────────────────────────────────────────────
SIMPLE_VLLM_BASE_URL    = os.getenv("SIMPLE_VLLM_BASE_URL")   # 필수
SIMPLE_VLLM_MODEL       = os.getenv("SIMPLE_VLLM_MODEL")      # 필수
SIMPLE_VLLM_TIMEOUT_SEC = 180

# ── 생성 파라미터 ────────────────────────────────────────────────────────────
SIMPLE_N                  = 16
SIMPLE_SQL_DB_MAX_RETRIES = 2   # 구문 오류 / DB 실행 실패 시 최대 재시도 횟수

# ── SQL 선택 방식 ─────────────────────────────────────────────────────────────
# "llm_judge"   : AST 그룹 대표 SQL을 LLM Judge로 재정렬 후 선택
# "ast_majority": AST 득표 순서 그대로 선택
SIMPLE_SQL_SELECT_METHOD = "llm_judge"

# ── LLM Judge 서버 (.env 필수) ───────────────────────────────────────────────
SIMPLE_SQL_JUDGE_BASE_URL = os.getenv("SIMPLE_SQL_JUDGE_BASE_URL", "")  # 필수
SIMPLE_SQL_JUDGE_MODEL    = os.getenv("SIMPLE_SQL_JUDGE_MODEL", "")     # 필수

# ── LLM Judge 파라미터 ───────────────────────────────────────────────────────
SIMPLE_SQL_JUDGE_TOP_K            = 5      # Judge에 넣을 AST 그룹 대표 최대 수
SIMPLE_SQL_JUDGE_TIMEOUT_SEC      = 180
SIMPLE_SQL_JUDGE_TEMPERATURE      = 0.0
SIMPLE_SQL_JUDGE_MAX_TOKENS       = 2048
SIMPLE_SQL_JUDGE_EXEC_PREVIEW     = True   # Judge 전 각 후보를 LIMIT으로 실행해 결과 첨부
SIMPLE_SQL_JUDGE_EXEC_PREVIEW_ROWS = 3     # 첨부할 최대 행 수

# ── 번역 (KO → EN) ───────────────────────────────────────────────────────────
# 번역에는 Judge와 동일한 gpt-oss 서버를 재사용한다 (.env의 SIMPLE_SQL_JUDGE_* 참조)
SIMPLE_TRANSLATE_ENABLED    = True   # False로 설정하면 번역 단계 건너뜀
SIMPLE_TRANSLATE_MAX_TOKENS = 1024   # 추론 모델(gpt-oss)의 thinking 토큰 소비 감안

# ── Few-Shot (MQS Pool) ──────────────────────────────────────────────────────
SIMPLE_FEW_SHOT_TOP_K        = 3
SIMPLE_MQS_POOL_NPZ_PATH     = BASE_DIR / "MQS-pool" / "spider" / "train" / "pool_embeddings.npz"
SIMPLE_MQS_POOL_EN_NPZ_PATH  = BASE_DIR / "MQS-pool" / "spider" / "train" / "pool_embeddings_en.npz"

# ── Value Hints ──────────────────────────────────────────────────────────────
SIMPLE_VALUE_HINT_TIMEOUT_MS     = 1500
SIMPLE_VALUE_HINT_DISTINCT_LIMIT = 20


