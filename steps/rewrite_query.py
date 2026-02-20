"""
질의 재작성 단계 — 프롬프트 렌더러
"""

from __future__ import annotations

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from config import PROMPTS_DIR, REWRITE_QUERY_TEMPLATE_PATH

_env = Environment(
    loader=FileSystemLoader(str(PROMPTS_DIR)),
    undefined=StrictUndefined,
    autoescape=False,
)


def render_prompt(*, question: str, standard_columns_names: str) -> str:
    """질문과 스키마 후보를 받아 질의 재작성 프롬프트를 렌더링"""
    return _env.get_template(REWRITE_QUERY_TEMPLATE_PATH.name).render(
        question=question,
        standard_columns_names=standard_columns_names,
    )
