"""
질의 재작성 단계 — 프롬프트 렌더러
"""

from __future__ import annotations

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from config import IR_PROMPTS_DIR, REWRITE_QUERY_TEMPLATE_PATH

_env = Environment(
    loader=FileSystemLoader(str(IR_PROMPTS_DIR)),
    undefined=StrictUndefined,
    autoescape=False,
)


def render_prompt(
    *,
    question: str,
    schema_candidates: str,
    entity_json: str = "{}",
    schema_linking_json: str = "{}",
    value_hints_json: str = "{}",
) -> str:
    """질문, 스키마 후보, 엔티티/스키마링킹/value hints 결과를 받아 질의 재작성 프롬프트를 렌더링"""
    return _env.get_template(REWRITE_QUERY_TEMPLATE_PATH.name).render(
        question=question,
        schema_candidates=schema_candidates,
        entity_json=entity_json,
        schema_linking_json=schema_linking_json,
        value_hints_json=value_hints_json,
    )
