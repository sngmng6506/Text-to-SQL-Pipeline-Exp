"""
PostgreSQL 실행 유틸

주의: 파괴적 SQL을 막기 위해 SELECT/WITH만 허용합니다.
"""

from __future__ import annotations

import os
import re
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor



_SAFE_SQL_RE = re.compile(r"^\s*(select|with)\b", flags=re.IGNORECASE)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        port=int(os.getenv("DB_PORT", "5432")),
    )


def execute_sql(sql: str) -> list[dict[str, Any]]:
    if not _SAFE_SQL_RE.search(sql or ""):
        raise ValueError("Only SELECT/WITH statements are allowed.")
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
