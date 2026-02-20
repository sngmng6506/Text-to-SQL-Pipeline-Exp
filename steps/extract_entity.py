"""
엔티티 추출 단계 — 프롬프트 렌더러
"""

from __future__ import annotations

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from config import PROMPTS_DIR, EXTRACT_ENTITY_TEMPLATE_PATH

_env = Environment(
    loader=FileSystemLoader(str(PROMPTS_DIR)),
    undefined=StrictUndefined,
    autoescape=False,
)


def render_prompt(question: str) -> str:
    """질문을 받아 엔티티 추출 프롬프트를 렌더링"""
    return _env.get_template(EXTRACT_ENTITY_TEMPLATE_PATH.name).render(question=question)
