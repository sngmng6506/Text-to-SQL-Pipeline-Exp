"""
엔티티 추출 단계 — 프롬프트 렌더러
"""

from __future__ import annotations

import json

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from config import IR_PROMPTS_DIR, EXTRACT_ENTITY_TEMPLATE_PATH, SCHEMA_JSON_PATH

_env = Environment(
    loader=FileSystemLoader(str(IR_PROMPTS_DIR)),
    undefined=StrictUndefined,
    autoescape=False,
)


def _load_schema_info() -> str:
    """
    스키마 JSON에서 테이블명·컬럼명만 compact 문자열로 반환.
    v1 템플릿은 schema_info 변수를 사용하지 않으므로 무시됨.
    """
    try:
        with open(SCHEMA_JSON_PATH, encoding="utf-8") as f:
            schemas = json.load(f)
        lines = []
        for s in schemas:
            schema_text = s.get("schema_text", "")
            if "Columns:" in schema_text:
                col_part = schema_text.split("Columns:", 1)[1].strip()
                cols = [c.strip().split()[0] for c in col_part.split(",") if c.strip()]
                lines.append(f"- {s['table']}: {', '.join(cols)}")
            else:
                lines.append(f"- {s['table']}")
        return "\n".join(lines)
    except Exception:
        return ""


_SCHEMA_INFO = _load_schema_info()


def render_prompt(question: str) -> str:
    """질문을 받아 엔티티 추출 프롬프트를 렌더링"""
    return _env.get_template(EXTRACT_ENTITY_TEMPLATE_PATH.name).render(
        question=question,
        schema_info=_SCHEMA_INFO,
    )
