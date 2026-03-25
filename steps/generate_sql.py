"""
SQL 생성 단계 — 프롬프트 렌더러
"""

from __future__ import annotations

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from config import IR_PROMPTS_DIR, GENERATE_SQL_TEMPLATE_PATH

_env = Environment(
    loader=FileSystemLoader(str(IR_PROMPTS_DIR)),
    undefined=StrictUndefined,
    autoescape=False,
)


def render_prompt(
    *,
    rewritten_question: str,
    schema_candidates: str,
    question: str = "",
    value_hints_json: str = "{}",
    entity_json: str = "{}",
    schema_linking_json: str = "{}",
    few_shot_examples: list[dict] | None = None,
) -> str:
    """재작성된 질문(또는 IR JSON), 원본 질문, 스키마 후보, 엔티티/스키마링킹 결과를 받아 SQL 생성 프롬프트를 렌더링"""
    return _env.get_template(GENERATE_SQL_TEMPLATE_PATH.name).render(
        question=question,
        rewritten_question=rewritten_question,
        schema_candidates=schema_candidates,
        value_hints_json=value_hints_json,
        entity_json=entity_json,
        schema_linking_json=schema_linking_json,
        few_shot_examples=few_shot_examples or [],
    )
