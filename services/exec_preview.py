"""
exec_preview.py

Judge용 SQL 실행 미리보기 유틸리티.

- LIMIT을 삽입해 소량의 샘플 행만 가져온다.
- 결과를 Judge 프롬프트에 삽입할 짧은 텍스트로 변환한다.
"""

from __future__ import annotations

from typing import Any

from db.client import execute_sql

# 셀 값 최대 표시 길이 (초과 시 말줄임표)
_MAX_CELL_LEN = 80


def _add_preview_limit(sql: str, limit: int = 3) -> str:
    """
    SQL에 LIMIT을 추가한다.

    - 이미 LIMIT이 있으면 *더 작은* 값으로 교체한다.
    - sqlglot 파싱 실패 시 서브쿼리 wrap으로 fallback한다.
    """
    try:
        import sqlglot
        import sqlglot.expressions as exp

        parsed = sqlglot.parse_one(sql.rstrip(";"), read="postgres")

        existing_limit = parsed.find(exp.Limit)
        if existing_limit is not None:
            try:
                existing_val = int(existing_limit.expression.this)
                if existing_val <= limit:
                    # 이미 충분히 작음 — 그대로 반환
                    return parsed.sql(dialect="postgres")
            except Exception:
                pass

        return parsed.limit(limit).sql(dialect="postgres")

    except Exception:
        stripped = sql.rstrip(";").strip()
        return f"SELECT * FROM ({stripped}) _preview LIMIT {limit}"


def _truncate_cell(val: Any) -> str:
    """셀 값을 문자열로 변환하고 너무 길면 자른다."""
    s = str(val)
    if len(s) > _MAX_CELL_LEN:
        return s[:_MAX_CELL_LEN] + "…"
    return s


def _format_preview_rows(rows: list[dict], max_rows: int = 3) -> str:
    """DB 실행 결과를 Judge 프롬프트에 삽입할 텍스트로 변환한다."""
    if not rows:
        return "(결과 없음)"

    shown = rows[:max_rows]
    lines = [
        "{" + ", ".join(f"{k}: {_truncate_cell(v)}" for k, v in r.items()) + "}"
        for r in shown
    ]
    suffix = f"\n... ({len(rows) - max_rows}행 더)" if len(rows) > max_rows else ""
    return "\n".join(lines) + suffix


def fetch_exec_preview(sql: str, limit: int = 3) -> str:
    """
    SQL을 LIMIT을 붙여 실행하고, 결과를 포맷된 문자열로 반환한다.

    실행 오류 발생 시 오류 메시지를 문자열로 반환한다 (예외를 raise하지 않음).
    """
    try:
        limited_sql = _add_preview_limit(sql, limit)
        rows = execute_sql(limited_sql)
        return _format_preview_rows(rows, max_rows=limit)
    except Exception as e:
        return f"실행 오류: {e}"
