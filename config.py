"""
TAG-test 경로/파일명 통합 관리

여기만 바꾸면 스키마/산출물/프롬프트 위치를 일괄 변경할 수 있습니다.
"""

from __future__ import annotations

from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

# 입력 데이터(스키마 등)
DATA_DIR = BASE_DIR / "data"
SCHEMA_DIR = DATA_DIR / "schema"
SCHEMA_JSON_PATH = SCHEMA_DIR / "table_schema.json"

# 산출물(임베딩 등)
ARTIFACTS_DIR = BASE_DIR / "artifacts"
EMBEDDINGS_DIR = ARTIFACTS_DIR / "embeddings"
SCHEMA_EMBEDDINGS_NPZ_PATH = EMBEDDINGS_DIR / "table_schema_embeddings.npz"

# 프롬프트
PROMPTS_DIR = BASE_DIR / "prompts"
EXTRACT_ENTITY_TEMPLATE_PATH = PROMPTS_DIR / "extract_entity.j2"
REWRITE_QUERY_TEMPLATE_PATH = PROMPTS_DIR / "rewrite_query.j2"
GENERATE_SQL_TEMPLATE_PATH = PROMPTS_DIR / "generate_sql.j2"
