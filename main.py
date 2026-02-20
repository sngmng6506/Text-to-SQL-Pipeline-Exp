"""
TAG 파이프라인 실행 진입점

흐름:
  1. 엔티티 추출 (LLM)         — steps/extract_entity.py
  2. 스키마 후보 검색 (TEI 임베딩) — schema/index.py
  3. 질의 재작성 (LLM)          — steps/rewrite_query.py
  4. SQL 생성 (LLM)            — steps/generate_sql.py

실행 예:
  python main.py --question "알람 이력에서 어제 발생한 건수 보여줘" --top-k 10
"""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Any

from dotenv import load_dotenv

from clients.chat import default_vllm_client
from schema.index import query_schema
from steps.extract_entity import render_prompt as render_entity_prompt
from steps.rewrite_query import render_prompt as render_rewrite_prompt
from steps.generate_sql import render_prompt as render_sql_prompt


_JSON_OBJ_RE = re.compile(r"\{[\s\S]*\}")


def _parse_json_object(text: str) -> dict[str, Any]:
    """
    LLM이 JSON만 준다는 가정이지만, 혹시 앞뒤 텍스트가 섞여도 첫 JSON 객체를 파싱.
    """
    text = (text or "").strip()
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        m = _JSON_OBJ_RE.search(text)
        if not m:
            return {}
        try:
            obj = json.loads(m.group(0))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}


def _build_schema_query(entities: dict[str, Any]) -> str:
    """entity_phrases 중심으로 schema/index 검색 문자열을 구성."""
    entity_phrases = entities.get("entity_phrases") if isinstance(entities, dict) else None
    time_phrases = entities.get("time_phrases") if isinstance(entities, dict) else None

    parts: list[str] = []
    if isinstance(entity_phrases, list):
        parts.extend([str(x) for x in entity_phrases if x])
    if isinstance(time_phrases, list):
        parts.extend([str(x) for x in time_phrases[:1] if x])

    return " ".join(parts).strip()


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--question", required=True, help="사용자 질문(자연어)")
    parser.add_argument("--top-k", type=int, default=5, help="스키마 후보 상위 개수")
    parser.add_argument("--max-tokens", type=int, default=512)
    args = parser.parse_args()

    client = default_vllm_client()
    model_id = os.getenv("VLLM_MODEL")

    # ── 1. 엔티티 추출 ──────────────────────────────────────────────
    content = client.chat_completions(
        messages=[{"role": "user", "content": render_entity_prompt(args.question)}],
        model=model_id,
        temperature=0.0,
        max_tokens=args.max_tokens,
    )
    entities = _parse_json_object(content)
    schema_query = _build_schema_query(entities) or args.question

    print("\n=== ENTITY(JSON) ===")
    print(json.dumps(entities, ensure_ascii=False, indent=2))
    print("\n=== SCHEMA QUERY ===")
    print(schema_query)

    # ── 2. 스키마 후보 검색 ─────────────────────────────────────────
    results = query_schema(schema_query, top_k=args.top_k)
    print(f"\n=== TOP {args.top_k} SCHEMAS ===")
    for i, r in enumerate(results, 1):
        print(f"{i}. {r['table']} (score: {r['score']:.4f})")
        print(f"   {r['schema_text']}")

    # ── 3. 질의 재작성 ──────────────────────────────────────────────
    candidates = results[: min(5, len(results))]
    standard_columns = "\n".join([r["schema_text"][:400] for r in candidates])

    rewritten = client.chat_completions(
        messages=[
            {
                "role": "system",
                "content": "You are a rewriting function. Do NOT output reasoning. Output ONLY the rewritten question as a single line in Korean. No explanations.",
            },
            {"role": "user", "content": render_rewrite_prompt(question=args.question, standard_columns_names=standard_columns)},
        ],
        model=model_id,
        temperature=0.0,
        max_tokens=min(args.max_tokens, 256),
        extra={"stop": ["\n"]},
    ).strip()

    if "Rewritten Question:" in rewritten:
        rewritten = rewritten.split("Rewritten Question:", 1)[1].strip()

    print("\n=== REWRITTEN QUESTION ===")
    print(rewritten)

    # ── 4. SQL 생성 ─────────────────────────────────────────────────
    schema_candidates = "\n".join([r["schema_text"] for r in candidates])

    sql_text = client.chat_completions(
        messages=[
            {
                "role": "system",
                "content": "You are a SQL generator. Output ONLY SQL starting with SELECT or WITH. No explanations.",
            },
            {"role": "user", "content": render_sql_prompt(rewritten_question=rewritten, schema_candidates=schema_candidates)},
        ],
        model=model_id,
        temperature=0.0,
        max_tokens=min(args.max_tokens, 256),
        extra={"stop": ["\n\n"]},
    ).strip()

    print("\n=== SQL ===")
    print(sql_text)


if __name__ == "__main__":
    main()
