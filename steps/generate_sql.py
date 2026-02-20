"""
SQL 생성 단계 — 프롬프트 렌더러
"""

from __future__ import annotations

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from config import PROMPTS_DIR, GENERATE_SQL_TEMPLATE_PATH

_env = Environment(
    loader=FileSystemLoader(str(PROMPTS_DIR)),
    undefined=StrictUndefined,
    autoescape=False,
)


def render_prompt(*, rewritten_question: str, schema_candidates: str) -> str:
    """재작성된 질문과 스키마 후보를 받아 SQL 생성 프롬프트를 렌더링"""
    return _env.get_template(GENERATE_SQL_TEMPLATE_PATH.name).render(
        rewritten_question=rewritten_question,
        schema_candidates=schema_candidates,
    )
