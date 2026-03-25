"""
IR 후보 선택기 (다수결 fallback)

majority_vote(): 구조적 fingerprint 기반 다수결로 IR 후보 중 하나를 선택한다.
Cross Encoder 기반 선택은 ir/reranker.py (IRRerankerService) 참고.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any


_JSON_OBJ_RE = re.compile(r"\{[\s\S]*\}")

# IR 필드 고정 순서 (ir/reranker.py에서도 사용)
_FIELD_ORDER = ["FROM", "JOIN", "FILTER", "GROUP_BY", "AGGREGATE", "HAVING", "ORDER_BY", "SELECT", "COMPUTED"]


def _parse_ir(raw: str) -> dict[str, Any]:
    """JSON 펜스 제거 후 파싱. 실패하면 빈 dict 반환."""
    text = raw.strip()
    text = re.sub(r"^```[a-zA-Z]*\n?", "", text)
    text = re.sub(r"\n?```$", "", text.strip())
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


# ── Majority Vote (fallback) ─────────────────────────────────────────────────

def _to_set(val: Any) -> frozenset:
    if isinstance(val, list):
        return frozenset(str(v).strip().lower() for v in val if v)
    if val:
        return frozenset([str(val).strip().lower()])
    return frozenset()


def _fingerprint(ir: dict[str, Any]) -> tuple:
    from_tables  = _to_set(ir.get("FROM"))
    join_tables  = _to_set(
        [j.get("table") for j in ir.get("JOIN", []) if isinstance(j, dict)]
    )
    filter_count = len(ir.get("FILTER", []) or [])
    group_cols   = _to_set(ir.get("GROUP_BY"))
    order_info   = (
        len(ir.get("ORDER_BY", []) or []),
        frozenset(
            str(o.get("direction", "")).upper()
            for o in (ir.get("ORDER_BY") or [])
            if isinstance(o, dict)
        ),
    )
    select_count = len(ir.get("SELECT", []) or [])
    agg_funcs    = _to_set(
        [a.get("func") for a in (ir.get("AGGREGATE") or []) if isinstance(a, dict)]
    )
    return (from_tables, join_tables, filter_count, group_cols, order_info, select_count, agg_funcs)


def _completeness(ir: dict[str, Any]) -> int:
    score = 0
    for key in ("FROM", "JOIN", "FILTER", "GROUP_BY", "AGGREGATE", "ORDER_BY", "SELECT", "HAVING"):
        val = ir.get(key)
        if val:
            score += len(val) if isinstance(val, list) else 1
    return score


def majority_vote(raw_candidates: list[str]) -> tuple[str, list[dict[str, Any]]]:
    """fingerprint 기반 다수결로 IR 선택."""
    parsed: list[dict[str, Any]] = []
    for i, raw in enumerate(raw_candidates):
        ir = _parse_ir(raw)
        fp = _fingerprint(ir) if ir else None
        parsed.append({
            "idx": i,
            "raw": raw,
            "ir": ir,
            "fingerprint": fp,
            "completeness": _completeness(ir) if ir else 0,
        })

    valid = [p for p in parsed if p["ir"]]
    if not valid:
        parsed[0]["selected"] = True
        for p in parsed[1:]:
            p["selected"] = False
        return raw_candidates[0], parsed

    fp_counter: Counter = Counter(p["fingerprint"] for p in valid)
    most_common_fp, _ = fp_counter.most_common(1)[0]
    group = [p for p in valid if p["fingerprint"] == most_common_fp]
    winner = max(group, key=lambda p: p["completeness"])

    for p in parsed:
        p["vote_count"] = fp_counter.get(p["fingerprint"], 0)
        p["selected"] = p["idx"] == winner["idx"]

    selected_str = json.dumps(winner["ir"], ensure_ascii=False)
    return selected_str, parsed
