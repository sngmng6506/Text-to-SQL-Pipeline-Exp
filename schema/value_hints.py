"""
Schema value hint service.

schema_linking 결과의 컬럼을 기준으로 DB에서 허용값(DISTINCT)과
시간 컬럼 min/max를 조회해 SQL 생성 보조 힌트를 만든다.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Any

from psycopg2 import sql
from psycopg2.extras import RealDictCursor

from config import (
    VALUE_HINT_ALLOWED_MAX,
    VALUE_HINT_DISTINCT_LIMIT,
    VALUE_HINT_EXCLUDE_BOOLEAN_COLUMNS,
    VALUE_HINT_EXCLUDE_ID_LIKE_COLUMNS,
    VALUE_HINT_EXCLUDE_PK_FK,
    VALUE_HINT_INCLUDE_FILTER_COLUMNS,
    VALUE_HINT_INCLUDE_MAPPED_COLUMNS,
    VALUE_HINT_INCLUDE_TEMPORAL_COLUMNS,
    VALUE_HINT_SAMPLE_MAX,
    VALUE_HINT_TIMEOUT_MS,
)
from db.client import get_conn


class SchemaValueHintService:
    """schema_linking 결과 기반 value hints 조회 서비스."""

    _IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    _ID_LIKE_RE = re.compile(r"(^id$|_id$)", re.IGNORECASE)

    @classmethod
    def _parse_col_ref(cls, col_ref: str) -> tuple[str, str] | None:
        if not col_ref or "." not in col_ref:
            return None
        table, _, column = col_ref.partition(".")
        if not cls._IDENT_RE.match(table or ""):
            return None
        if not cls._IDENT_RE.match(column or ""):
            return None
        return table, column

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_pk_fk_columns() -> set[str]:
        """
        public 스키마의 PK/FK 컬럼 목록을 캐싱하여 반환.

        반환 형식: {"table.column", ...}
        """
        refs: set[str] = set()
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        kcu.table_name,
                        kcu.column_name
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    WHERE tc.table_schema = 'public'
                      AND tc.constraint_type IN ('PRIMARY KEY', 'FOREIGN KEY')
                    """
                )
                for row in cur.fetchall():
                    table = row.get("table_name")
                    col = row.get("column_name")
                    if table and col:
                        refs.add(f"{table}.{col}")
        return refs

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_column_types() -> dict[str, str]:
        """public 스키마 내 모든 컬럼의 data_type을 캐싱.

        반환 형식: {"table.column": "data_type"}
        """
        result: dict[str, str] = {}
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT table_name, column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                    """
                )
                for row in cur.fetchall():
                    table = row.get("table_name")
                    col = row.get("column_name")
                    dtype = row.get("data_type")
                    if table and col and dtype:
                        result[f"{table}.{col}"] = dtype
        return result

    @staticmethod
    @lru_cache(maxsize=1)
    def _get_boolean_columns() -> set[str]:
        """
        public 스키마의 boolean 타입 컬럼 목록을 캐싱하여 반환.

        반환 형식: {"table.column", ...}
        """
        refs: set[str] = set()
        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        table_name,
                        column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND data_type = 'boolean'
                    """
                )
                for row in cur.fetchall():
                    table = row.get("table_name")
                    col = row.get("column_name")
                    if table and col:
                        refs.add(f"{table}.{col}")
        return refs

    @staticmethod
    def _build_target_columns(sl_result: dict[str, Any]) -> tuple[list[str], list[str]]:
        filter_cols = [c for c in sl_result.get("filter_columns", []) if isinstance(c, str)]
        temporal_cols = [c for c in sl_result.get("temporal_columns", []) if isinstance(c, str)]
        mapped_cols = [
            c
            for c in (sl_result.get("column_mappings", {}) or {}).values()
            if isinstance(c, str)
        ]
        selected_cols: list[str] = []
        if VALUE_HINT_INCLUDE_FILTER_COLUMNS:
            selected_cols.extend(filter_cols)
        if VALUE_HINT_INCLUDE_TEMPORAL_COLUMNS:
            selected_cols.extend(temporal_cols)
        if VALUE_HINT_INCLUDE_MAPPED_COLUMNS:
            selected_cols.extend(mapped_cols)
        target_cols = list(dict.fromkeys(selected_cols))

        # PK/FK는 high-cardinality 가능성이 높아 value hints 대상에서 기본 제외
        if VALUE_HINT_EXCLUDE_PK_FK:
            pk_fk_cols = SchemaValueHintService._get_pk_fk_columns()
            filtered_target_cols = [c for c in target_cols if c not in pk_fk_cols]
            filtered_temporal_cols = [c for c in temporal_cols if c not in pk_fk_cols]
        else:
            filtered_target_cols = target_cols
            filtered_temporal_cols = temporal_cols

        # boolean 컬럼은 값 관측 편향이 semantic 해석을 오염시킬 수 있어 기본 제외
        if VALUE_HINT_EXCLUDE_BOOLEAN_COLUMNS:
            boolean_cols = SchemaValueHintService._get_boolean_columns()
            filtered_target_cols = [c for c in filtered_target_cols if c not in boolean_cols]
            filtered_temporal_cols = [c for c in filtered_temporal_cols if c not in boolean_cols]

        # PK/FK 메타정보에 잡히지 않는 id-like 컬럼을 보조적으로 제외
        if VALUE_HINT_EXCLUDE_ID_LIKE_COLUMNS:
            def _is_id_like(col_ref: str) -> bool:
                parsed = SchemaValueHintService._parse_col_ref(col_ref)
                if not parsed:
                    return False
                _, column = parsed
                return bool(SchemaValueHintService._ID_LIKE_RE.search(column))

            filtered_target_cols = [c for c in filtered_target_cols if not _is_id_like(c)]
            filtered_temporal_cols = [c for c in filtered_temporal_cols if not _is_id_like(c)]

        return filtered_target_cols, filtered_temporal_cols

    def fetch_column_hints(
        self,
        *,
        column_refs: list[str],
        temporal_column_refs: list[str] | None = None,
        distinct_limit: int = VALUE_HINT_DISTINCT_LIMIT,
        statement_timeout_ms: int = VALUE_HINT_TIMEOUT_MS,
    ) -> dict[str, dict[str, Any]]:
        temporal_set = set(temporal_column_refs or [])
        hints: dict[str, dict[str, Any]] = {}
        col_types = SchemaValueHintService._get_column_types()

        deduped_refs = list(
            dict.fromkeys([c for c in column_refs if isinstance(c, str) and c.strip()])
        )

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET LOCAL statement_timeout = %s", (statement_timeout_ms,))

                for col_ref in deduped_refs:
                    parsed = self._parse_col_ref(col_ref)
                    if not parsed:
                        continue
                    table, column = parsed
                    item: dict[str, Any] = {}
                    dtype = col_types.get(col_ref)
                    if dtype:
                        item["data_type"] = dtype

                    try:
                        cur.execute(
                            sql.SQL(
                                """
                                SELECT DISTINCT {col}::text AS v
                                FROM {tbl}
                                WHERE {col} IS NOT NULL
                                ORDER BY 1
                                LIMIT %s
                                """
                            ).format(col=sql.Identifier(column), tbl=sql.Identifier(table)),
                            (distinct_limit,),
                        )
                        vals = [r["v"] for r in cur.fetchall() if r.get("v") is not None]
                        # low-cardinality 컬럼만 allowed_values로 사용 (정확 매칭용)
                        item["allowed_values"] = vals if len(vals) <= VALUE_HINT_ALLOWED_MAX else []
                        # sample_values는 high-cardinality 컬럼 포함 참고용 힌트
                        item["sample_values"] = vals[: VALUE_HINT_SAMPLE_MAX]
                        item["value_count"] = len(vals)
                    except Exception:
                        item["allowed_values"] = []
                        item["sample_values"] = []
                        item["value_count"] = 0

                    if col_ref in temporal_set:
                        try:
                            cur.execute(
                                sql.SQL(
                                    """
                                    SELECT
                                        MIN({col})::text AS min_value,
                                        MAX({col})::text AS max_value
                                    FROM {tbl}
                                    WHERE {col} IS NOT NULL
                                    """
                                ).format(col=sql.Identifier(column), tbl=sql.Identifier(table))
                            )
                            row = cur.fetchone() or {}
                            item["min"] = row.get("min_value")
                            item["max"] = row.get("max_value")
                        except Exception:
                            item["min"] = None
                            item["max"] = None

                    hints[col_ref] = item

        return hints

    @staticmethod
    def _all_source_columns(sl_result: dict[str, Any]) -> list[str]:
        """SL 결과에서 제외 정책과 무관하게 모든 후보 컬럼을 반환."""
        cols: list[str] = []
        cols.extend(c for c in sl_result.get("filter_columns", []) if isinstance(c, str))
        cols.extend(c for c in sl_result.get("temporal_columns", []) if isinstance(c, str))
        cols.extend(
            c for c in (sl_result.get("column_mappings", {}) or {}).values()
            if isinstance(c, str)
        )
        return list(dict.fromkeys(cols))

    def fetch_from_schema_linking(
        self,
        sl_result: dict[str, Any],
        *,
        distinct_limit: int = VALUE_HINT_DISTINCT_LIMIT,
        statement_timeout_ms: int = VALUE_HINT_TIMEOUT_MS,
    ) -> dict[str, dict[str, Any]]:
        target_cols, temporal_cols = self._build_target_columns(sl_result)
        hints = self.fetch_column_hints(
            column_refs=target_cols,
            temporal_column_refs=temporal_cols,
            distinct_limit=distinct_limit,
            statement_timeout_ms=statement_timeout_ms,
        ) if target_cols else {}

        # 제외된 컬럼(boolean, pk/fk 등)에도 data_type 메타만 주입
        col_types = SchemaValueHintService._get_column_types()
        for col_ref in self._all_source_columns(sl_result):
            dtype = col_types.get(col_ref)
            if not dtype:
                continue
            if col_ref not in hints:
                hints[col_ref] = {
                    "data_type": dtype,
                    "allowed_values": [],
                    "sample_values": [],
                    "value_count": 0,
                }
            else:
                hints[col_ref].setdefault("data_type", dtype)

        return hints

