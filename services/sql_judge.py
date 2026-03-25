from __future__ import annotations

import re
from typing import Any

from jinja2 import Environment, FileSystemLoader

from config import SIMPLE_JUDGE_SQL_TEMPLATE_PATH

_SCORE_LINE_RE   = re.compile(r"SCORES\s*:\s*\[([^\]]+)\]", re.IGNORECASE)
_COMMENT_LINE_RE = re.compile(r"^\d+\.\s+(.+)", re.MULTILINE)


def _render_prompt(
    *,
    question: str,
    schema_text: str,
    candidates_text: str,
) -> str:
    tpl_path = SIMPLE_JUDGE_SQL_TEMPLATE_PATH
    env = Environment(
        loader=FileSystemLoader(str(tpl_path.parent)),
        keep_trailing_newline=True,
    )
    tpl = env.get_template(tpl_path.name)
    return tpl.render(
        question=question,
        schema_candidates=schema_text,
        candidates_text=candidates_text,
    )


def _parse_scores(raw: str, n: int) -> list[float] | None:
    m = _SCORE_LINE_RE.search(raw or "")
    if not m:
        return None
    parts = [p.strip() for p in m.group(1).split(",")]
    try:
        scores = [float(p) for p in parts if p]
    except ValueError:
        return None
    if len(scores) != n:
        return None
    return scores


def rerank_sql_groups(
    *,
    question: str,
    schema_text: str,
    groups: list[tuple[str, int]],  # [(sql, votes), ...]
    client: Any,
    model_id: str | None,
    temperature: float = 0.0,
    max_tokens: int = 256,
    exec_previews: list[str | None] | None = None,  # 후보별 LIMIT 실행 결과
) -> tuple[list[tuple[str, int]], list[dict[str, Any]], str]:
    """
    AST 그룹 대표 SQL들을 LLM Judge로 재정렬한다.

    Returns
    -------
    (reranked_groups, meta, raw_response)
      - reranked_groups: 점수 높은 순으로 재정렬된 (sql, votes)
      - meta: [{idx, votes, score, selected, sql}, ...]
      - raw_response: judge 원문 응답 (디버깅용)
    """
    if len(groups) <= 1:
        meta = [
            {
                "idx": 0,
                "votes": groups[0][1],
                "score": 10.0,
                "selected": True,
                "sql": groups[0][0],
                "score_source": "single",
            }
        ] if groups else []
        return groups, meta, ""

    # votes는 judge 입력에 노출하지 않고, 내부 동점 타이브레이크에만 사용한다.
    _parts = []
    for i, (sql, _votes) in enumerate(groups):
        block = f"[후보 {i+1}]\n{sql}"
        if exec_previews and i < len(exec_previews) and exec_previews[i] is not None:
            block += f"\n\n[실행 결과 (샘플)]\n{exec_previews[i]}"
        _parts.append(block)
    candidates_text = "\n\n".join(_parts)
    prompt = _render_prompt(
        question=question,
        schema_text=schema_text,
        candidates_text=candidates_text,
    )

    # reasoning 모델(gpt-oss 등)에서 content 대신 reasoning만 반환되는 경우를 줄이기 위해
    # 1차: thinking 비활성화 요청, 2차: 기본 호출로 재시도한다.
    raw = ""
    _last_err: Exception | None = None
    for _extra in (
        {"chat_template_kwargs": {"enable_thinking": False}},
        None,
    ):
        try:
            raw = client.chat_completions(
                messages=[{"role": "user", "content": prompt}],
                model=model_id,
                temperature=temperature,
                max_tokens=max_tokens,
                extra=_extra,
            )
            break
        except Exception as e:
            _last_err = e
            continue

    if not raw:
        # 호출 자체 실패 시 기존 순서 유지 + 득표수를 점수로 fallback
        fallback = [
            {
                "idx": i,
                "votes": v,
                "score": float(v),
                "selected": i == 0,
                "sql": s,
                "score_source": "votes_fallback",
            }
            for i, (s, v) in enumerate(groups)
        ]
        err_msg = f"JUDGE_CALL_FAILED: {_last_err}" if _last_err else "JUDGE_CALL_FAILED"
        return groups, fallback, err_msg

    parsed = _parse_scores(raw, len(groups))
    if parsed is None:
        # 파싱 실패 시 기존 순서 유지 + 득표수를 점수로 fallback
        fallback = [
            {
                "idx": i,
                "votes": v,
                "score": float(v),
                "selected": i == 0,
                "sql": s,
                "score_source": "votes_fallback",
            }
            for i, (s, v) in enumerate(groups)
        ]
        return groups, fallback, raw

    scored = [
        {
            "idx": i,
            "votes": votes,
            "score": parsed[i],
            "selected": False,
            "sql": sql,
            "score_source": "judge",
        }
        for i, (sql, votes) in enumerate(groups)
    ]
    # 코멘트 파싱 후 각 후보에 저장 (정렬 전 원래 idx 기준)
    _comments = [m.group(1).strip() for m in _COMMENT_LINE_RE.finditer(raw or "")]
    for i, s in enumerate(scored):
        if i < len(_comments):
            s["comment"] = _comments[i]

    scored.sort(key=lambda x: (x["score"], x["votes"]), reverse=True)
    scored[0]["selected"] = True

    reranked = [(m["sql"], m["votes"]) for m in scored]
    return reranked, scored, raw

