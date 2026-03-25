"""
스키마 링킹 단계 — 프롬프트 렌더러

v3+: schema_linking.j2가 있을 때만 실행. v1/v2는 자동 스킵.
v4 : extract_entity.j2가 없으면 엔티티 추출 없이 원본 질문을 직접 받음.
"""

from __future__ import annotations

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from config import (
    EXTRACT_ENTITY_TEMPLATE_PATH,
    IR_PROMPTS_DIR,
    SCHEMA_LINKING_TEMPLATE_PATH,
)

_env = Environment(
    loader=FileSystemLoader(str(IR_PROMPTS_DIR)),
    undefined=StrictUndefined,
    autoescape=False,
)


def is_enabled() -> bool:
    """현재 프롬프트 버전에 schema_linking.j2가 존재하는지 확인."""
    return SCHEMA_LINKING_TEMPLATE_PATH.exists()


def entity_extraction_enabled() -> bool:
    """현재 프롬프트 버전에 extract_entity.j2가 존재하는지 확인.
    v4처럼 엔티티 추출 없이 바로 스키마 링킹하는 버전에서는 False."""
    return EXTRACT_ENTITY_TEMPLATE_PATH.exists()


def render_prompt(
    *,
    schema_candidates: str,
    entity_json: str = "{}",
    question: str = "",
) -> str:
    """스키마 링킹 프롬프트 렌더링.

    - v3: entity_json 사용 (엔티티 추출 결과 기반)
    - v4: question 사용 (원본 질문 직접 입력, entity_json 없음)
    """
    template = _env.get_template(SCHEMA_LINKING_TEMPLATE_PATH.name)
    if entity_extraction_enabled():
        # v3: entity_json 전달
        return template.render(
            schema_candidates=schema_candidates,
            entity_json=entity_json,
        )
    else:
        # v4: 원본 질문 직접 전달
        return template.render(
            schema_candidates=schema_candidates,
            question=question,
        )


