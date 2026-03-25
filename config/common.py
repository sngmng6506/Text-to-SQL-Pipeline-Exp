"""
공통 경로 및 파라미터

두 파이프라인(ir-use / simple)이 공유하는 설정.
"""

from __future__ import annotations

from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

# 입력 데이터(스키마 등)
DATA_DIR = BASE_DIR / "data"
SCHEMA_DIR = DATA_DIR / "schema"
SCHEMA_JSON_PATH = SCHEMA_DIR / "table_schema_column_enum_description.json"

# 평가 데이터셋
EVAL_DATA_DIR     = DATA_DIR / "eval_data"
EVAL_DATASET_PATH = EVAL_DATA_DIR / "SQL-dataset-multi.xlsx"

# 산출물(임베딩 등)
ARTIFACTS_DIR = BASE_DIR / "artifacts"
EMBEDDINGS_DIR = ARTIFACTS_DIR / "embeddings"
SCHEMA_EMBEDDINGS_NPZ_PATH = EMBEDDINGS_DIR / (SCHEMA_JSON_PATH.stem + ".npz")

# 프롬프트 루트
PROMPTS_DIR = BASE_DIR / "prompts"

# 공통 파이프라인 파라미터
SCHEMA_TOP_K      = 5
SCHEMA_CANDIDATES = 5
SQL_MAX_TOKENS    = 384
