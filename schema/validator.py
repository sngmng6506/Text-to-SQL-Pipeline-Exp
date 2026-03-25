"""
스키마 검증 레이어

DB의 information_schema를 1회 조회 후 lru_cache로 영구 캐싱.
schema_linking 결과에 존재하지 않는 테이블·컬럼이 있으면 오류 목록과
재링킹용 correction suffix를 반환한다.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from typing import Any

from db.client import get_conn


# ---------------------------------------------------------------------------
# DB 조회 + 캐싱
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def get_schema_lookup() -> dict[str, frozenset[str]]:
    """public 스키마 내 모든 테이블·컬럼 목록을 조회하여 캐싱.

    Returns:
        {table_name: frozenset(column_names)}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, column_name
                """
            )
            rows = cur.fetchall()

    lookup: dict[str, set[str]] = {}
    for table_name, column_name in rows:
        lookup.setdefault(table_name, set()).add(column_name)

    return {t: frozenset(cols) for t, cols in lookup.items()}


@lru_cache(maxsize=1)
def _get_column_types() -> dict[str, str]:
    """public 스키마 내 모든 컬럼의 data_type을 조회하여 캐싱.

    Returns:
        {"table.column": "data_type"}
        예: {"alarm_queue.process_status": "integer",
             "crane_cell.enable": "boolean",
             "crane_cell.cell_status": "USER-DEFINED"}
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                """
            )
            rows = cur.fetchall()
    return {f"{t}.{c}": dt for t, c, dt in rows}


# ---------------------------------------------------------------------------
# 내부 유틸
# ---------------------------------------------------------------------------

def _valid_col(col_ref: str, lookup: dict[str, frozenset[str]]) -> bool:
    """'table.column' 형식이고 DB에 실제로 존재하는지 검사."""
    if not col_ref or "." not in col_ref:
        return False
    table, _, col = col_ref.partition(".")
    return table in lookup and col in lookup[table]


def _col_hint(col_ref: str, lookup: dict[str, frozenset[str]]) -> str:
    """잘못된 col_ref에 대해 실제 컬럼 목록 힌트 문자열 반환."""
    table = col_ref.split(".")[0] if "." in col_ref else col_ref
    if table not in lookup:
        return f'"{table}" 테이블이 DB에 없음'
    cols = ", ".join(sorted(lookup[table]))
    return f'"{table}" 테이블의 실제 컬럼: {cols}'


_TABLE_COL_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\b")

# ── 타입 불일치 검사용 ──────────────────────────────────────────────────────
# FILTER/HAVING: table.col OP 'string literal'
_STR_LITERAL_CMP_RE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\s*"
    r"(?:=|!=|<>|LIKE|ILIKE|NOT\s+LIKE|NOT\s+ILIKE)\s*'[^']*'"
    r"|'[^']*'\s*(?:=|!=|<>)\s*"
    r"([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)
# FILTER/HAVING: boolean col = 0|1
_BOOL_INT_CMP_RE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\s*(?:=|!=|<>)\s*\b([01])\b"
    r"|\b([01])\b\s*(?:=|!=|<>)\s*([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)",
    re.IGNORECASE,
)
# JOIN: table.col = table.col (타입 호환성)
_COL_COL_EQ_RE = re.compile(
    r"\b([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\s*=\s*"
    r"([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\b",
    re.IGNORECASE,
)
# 문자열 리터럴과 직접 비교하면 안 되는 타입
_NON_STRING_TYPES = frozenset({
    "integer", "bigint", "smallint", "int4", "int8", "int2",
    "numeric", "decimal", "float4", "float8", "real", "double precision",
    "boolean",
})
# JOIN에서 직접 비교가 불가한 타입 조합
_INCOMPATIBLE_JOIN_PAIRS: list[tuple[frozenset[str], frozenset[str]]] = [
    (frozenset({"USER-DEFINED"}), frozenset({"character varying", "text", "character"})),
]


def _check_filter_type_errors(field: str, expr: str, col_types: dict[str, str]) -> list[str]:
    """FILTER/HAVING 표현식에서 타입 불일치 패턴을 검사."""
    errors: list[str] = []
    for m in _STR_LITERAL_CMP_RE.finditer(expr):
        col_ref = m.group(1) or m.group(2)
        if col_ref:
            dtype = col_types.get(col_ref)
            if dtype and dtype in _NON_STRING_TYPES:
                errors.append(
                    f'{field}: "{col_ref}" (타입: {dtype})에 문자열 리터럴 비교 → 타입 불일치'
                )
    for m in _BOOL_INT_CMP_RE.finditer(expr):
        col_ref = m.group(1) or m.group(4)
        if col_ref and col_types.get(col_ref) == "boolean":
            errors.append(
                f'{field}: "{col_ref}" (타입: boolean)에 정수(0/1) 비교 → TRUE/FALSE 사용 필요'
            )
    return errors


def _check_join_type_errors(expr: str, col_types: dict[str, str]) -> list[str]:
    """JOIN 표현식에서 타입 불일치 컬럼 간 직접 비교를 검사."""
    errors: list[str] = []
    for m in _COL_COL_EQ_RE.finditer(expr):
        col1, col2 = m.group(1), m.group(2)
        if col1.lower() == col2.lower():
            continue
        t1 = col_types.get(col1)
        t2 = col_types.get(col2)
        if t1 and t2:
            for grp1, grp2 in _INCOMPATIBLE_JOIN_PAIRS:
                if (t1 in grp1 and t2 in grp2) or (t1 in grp2 and t2 in grp1):
                    errors.append(
                        f'JOIN: "{col1}" ({t1}) = "{col2}" ({t2}) → 타입 불일치 (::text CAST 필요)'
                    )
    return errors


def _extract_col_refs(text: str) -> set[str]:
    """표현식 문자열에서 table.column 참조를 추출."""
    if not isinstance(text, str) or not text.strip():
        return set()
    return {f"{m.group(1)}.{m.group(2)}" for m in _TABLE_COL_RE.finditer(text)}


def _allowed_sl_columns(sl_result: dict[str, Any] | None) -> set[str]:
    """SL 결과가 허용한 table.column 집합을 반환."""
    if not isinstance(sl_result, dict) or not sl_result:
        return set()

    allowed: set[str] = set()

    for col_ref in sl_result.get("linked_columns", []):
        if isinstance(col_ref, str) and "." in col_ref:
            allowed.add(col_ref)

    for col_ref in (sl_result.get("column_mappings", {}) or {}).values():
        if isinstance(col_ref, str) and "." in col_ref:
            allowed.add(col_ref)

    for field in ("filter_columns", "temporal_columns"):
        for col_ref in sl_result.get(field, []):
            if isinstance(col_ref, str) and "." in col_ref:
                allowed.add(col_ref)

    return allowed


def _allowed_sl_tables(sl_result: dict[str, Any] | None) -> set[str]:
    """SL 결과가 허용한 테이블 집합을 반환."""
    if not isinstance(sl_result, dict) or not sl_result:
        return set()

    allowed: set[str] = set()
    for table in sl_result.get("linked_tables", []):
        if isinstance(table, str) and table.strip():
            allowed.add(table.strip())
    return allowed


# ---------------------------------------------------------------------------
# 공개 API
# ---------------------------------------------------------------------------

def find_errors(
    sl_result: dict[str, Any],
    lookup: dict[str, frozenset[str]],
) -> list[str]:
    """schema_linking JSON 결과를 검증하고 오류 메시지 목록을 반환.

    검사 대상:
      - linked_tables        : 테이블 존재 여부
      - linked_columns       : table.column 형식 + 존재 여부 (v9+)
      - column_mappings      : table.column 형식 + 존재 여부
      - filter_columns       : table.column 형식 + 존재 여부 (v9+)
      - temporal_columns     : table.column 형식 + 존재 여부 (v9+)
      - filter_conditions    : 키가 table.column 형식 + 존재 여부
      - time_column          : table.column 형식 + 존재 여부
      - group_by_columns     : table.column 형식 + 존재 여부
      - sort_order.column    : table.column 형식 + 존재 여부
    """
    errors: list[str] = []

    # linked_tables
    for table in sl_result.get("linked_tables", []):
        if table not in lookup:
            errors.append(f'linked_tables: "{table}" → DB에 존재하지 않는 테이블')

    # linked_columns (v9+)
    for col_ref in sl_result.get("linked_columns", []):
        if not isinstance(col_ref, str):
            continue
        if not _valid_col(col_ref, lookup):
            errors.append(
                f'linked_columns: "{col_ref}" → 존재하지 않는 컬럼'
                f"\n    힌트: {_col_hint(col_ref, lookup)}"
            )

    # column_mappings  { "표현": "table.column" }
    for expr, col_ref in sl_result.get("column_mappings", {}).items():
        if not isinstance(col_ref, str):
            continue
        if not _valid_col(col_ref, lookup):
            errors.append(
                f'column_mappings["{expr}"]: "{col_ref}" → 존재하지 않는 컬럼'
                f"\n    힌트: {_col_hint(col_ref, lookup)}"
            )

    # filter_conditions  { "table.column": value, ... }
    for col_ref in sl_result.get("filter_conditions", {}).keys():
        if not _valid_col(col_ref, lookup):
            errors.append(
                f'filter_conditions 키: "{col_ref}" → 존재하지 않는 컬럼'
                f"\n    힌트: {_col_hint(col_ref, lookup)}"
            )

    # filter_columns (v9+)
    for col_ref in sl_result.get("filter_columns", []):
        if not isinstance(col_ref, str):
            continue
        if not _valid_col(col_ref, lookup):
            errors.append(
                f'filter_columns: "{col_ref}" → 존재하지 않는 컬럼'
                f"\n    힌트: {_col_hint(col_ref, lookup)}"
            )

    # time_column
    time_col = sl_result.get("time_column")
    if time_col and not _valid_col(time_col, lookup):
        errors.append(
            f'time_column: "{time_col}" → 존재하지 않는 컬럼'
            f"\n    힌트: {_col_hint(time_col, lookup)}"
        )

    # temporal_columns (v9+)
    for col_ref in sl_result.get("temporal_columns", []):
        if not isinstance(col_ref, str):
            continue
        if not _valid_col(col_ref, lookup):
            errors.append(
                f'temporal_columns: "{col_ref}" → 존재하지 않는 컬럼'
                f"\n    힌트: {_col_hint(col_ref, lookup)}"
            )

    # group_by_columns
    for col_ref in sl_result.get("group_by_columns", []):
        if not _valid_col(col_ref, lookup):
            errors.append(
                f'group_by_columns: "{col_ref}" → 존재하지 않는 컬럼'
                f"\n    힌트: {_col_hint(col_ref, lookup)}"
            )

    # sort_order.column
    sort_order = sl_result.get("sort_order")
    if sort_order and isinstance(sort_order, dict):
        col_ref = sort_order.get("column", "")
        if col_ref and not _valid_col(col_ref, lookup):
            errors.append(
                f'sort_order.column: "{col_ref}" → 존재하지 않는 컬럼'
                f"\n    힌트: {_col_hint(col_ref, lookup)}"
            )

    return errors


def find_ir_errors(
    ir_result: dict[str, Any],
    lookup: dict[str, frozenset[str]],
    sl_result: dict[str, Any] | None = None,
) -> list[str]:
    """IR JSON 결과를 검증하고 오류 메시지 목록을 반환.

    검사 대상:
      - FROM            : 테이블 존재 여부 + SL 허용 여부
      - JOIN/FILTER/
        GROUP_BY/
        AGGREGATE/
        HAVING/
        ORDER_BY/
        SELECT/
        COMPUTED        : 표현식 내 table.column 참조 존재 여부 + SL 허용 여부
      - FILTER/HAVING   : 컬럼 타입 vs 리터럴 타입 불일치 (integer/boolean + string literal)
      - JOIN            : 컬럼 간 타입 불일치 (enum vs varchar 등)
    """
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    allowed_sl_tables = _allowed_sl_tables(sl_result)
    allowed_sl_cols = _allowed_sl_columns(sl_result)
    col_types = _get_column_types()

    from_tables = [t for t in ir_result.get("FROM", []) if isinstance(t, str)]
    if not from_tables:
        errors.append("FROM: 테이블 목록이 비어 있습니다")
        return errors

    for table in from_tables:
        if table not in lookup:
            errors.append(f'FROM: "{table}" → DB에 존재하지 않는 테이블')
            continue
        if allowed_sl_tables and table not in allowed_sl_tables:
            allowed_tables_str = ", ".join(sorted(allowed_sl_tables))
            errors.append(
                f'FROM: "{table}" → SL에 없는 테이블'
                f"\n    허용 테이블: {allowed_tables_str}"
            )

    expr_fields = ("JOIN", "FILTER", "GROUP_BY", "AGGREGATE", "HAVING", "ORDER_BY", "SELECT", "COMPUTED")
    for field in expr_fields:
        for expr in ir_result.get(field, []):
            if not isinstance(expr, str):
                continue

            # 컬럼 존재 여부 + SL 범위 검사
            for col_ref in sorted(_extract_col_refs(expr)):
                key = (field, col_ref)
                if key in seen:
                    continue
                seen.add(key)
                if not _valid_col(col_ref, lookup):
                    errors.append(
                        f'{field}: "{col_ref}" → 존재하지 않는 컬럼'
                        f"\n    힌트: {_col_hint(col_ref, lookup)}"
                    )
                    continue
                if allowed_sl_cols and col_ref not in allowed_sl_cols:
                    allowed_cols_str = ", ".join(sorted(allowed_sl_cols))
                    errors.append(
                        f'{field}: "{col_ref}" → SL에 없는 컬럼'
                        f"\n    허용 컬럼: {allowed_cols_str}"
                    )

            # 타입 불일치 검사
            if field in ("FILTER", "HAVING"):
                errors.extend(_check_filter_type_errors(field, expr, col_types))
            elif field == "JOIN":
                errors.extend(_check_join_type_errors(expr, col_types))

    return errors


_SQL_FROM_RE = re.compile(r"\bFROM\b", re.IGNORECASE)
_SQL_SELECT_RE = re.compile(r"\b(SELECT|WITH)\b", re.IGNORECASE)


def find_sql_errors(sql: str) -> list[str]:
    """생성된 SQL 문자열에 대한 기본 구문 검사.

    검사 항목:
      - SELECT 또는 WITH 로 시작하는지 (SQL generator가 설명문을 앞에 붙이는 경우 방어)
      - FROM 절 존재 여부
    """
    errors: list[str] = []
    text = (sql or "").strip()
    if not text:
        errors.append("SQL이 비어 있습니다")
        return errors
    if not _SQL_SELECT_RE.search(text):
        errors.append("SQL에 SELECT/WITH 절이 없습니다")
    if not _SQL_FROM_RE.search(text):
        errors.append("SQL에 FROM 절이 없습니다")
    return errors


def build_correction_suffix(
    sl_result: dict[str, Any],
    errors: list[str],
) -> str:
    """재링킹 요청 시 원본 프롬프트 끝에 추가할 섹션 생성.

    LLM이 1차 결과와 검증 오류를 확인하고 올바른 JSON을 재출력하도록 유도한다.
    """
    sl_json = json.dumps(sl_result, ensure_ascii=False, indent=2)
    error_lines = "\n".join(f"- {e}" for e in errors)

    return (
        "\n\n---\n"
        "## [자동 검증] 1차 스키마 링킹 결과에 오류가 있습니다\n\n"
        "아래 JSON은 방금 생성된 1차 결과입니다.\n"
        "```json\n"
        f"{sl_json}\n"
        "```\n\n"
        "## 검증 오류 목록\n"
        f"{error_lines}\n\n"
        "위 오류를 **모두 수정**하여 올바른 JSON만 출력하세요.\n"
        "JSON 외 텍스트(설명, 이유 등)는 절대 포함하지 마세요."
    )
