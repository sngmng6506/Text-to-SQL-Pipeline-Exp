"""
PostgreSQL 스키마 추출 → JSON 저장

실행:
  python -m schema.extract
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor

from config import SCHEMA_JSON_PATH


_SCHEMA_QUERY = """
SELECT
    cols.table_name,
    string_agg(
        cols.column_name || ' (' || cols.data_type || ')' ||
        CASE WHEN pk.column_name IS NOT NULL THEN ' [PK]' ELSE '' END,
        ', ' ORDER BY cols.ordinal_position
    ) AS schema_details
FROM
    information_schema.columns cols
LEFT JOIN (
    SELECT kcu.table_name, kcu.column_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
    WHERE tc.constraint_type = 'PRIMARY KEY'
) pk ON cols.table_name = pk.table_name AND cols.column_name = pk.column_name
WHERE
    cols.table_schema = 'public'
GROUP BY
    cols.table_name;
"""


def get_postgres_schema() -> list[dict]:
    db_config = {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "port": os.getenv("DB_PORT", 5432),
    }
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(_SCHEMA_QUERY)
        rows = cur.fetchall()
        return [
            {
                "table": row["table_name"],
                "schema_text": f"Table: {row['table_name']} | Columns: {row['schema_details']}",
            }
            for row in rows
        ]
    except Exception as e:
        print(f"Connection Failed: {e}")
        return []
    finally:
        if "conn" in locals() and conn:
            cur.close()
            conn.close()


def save_schema(schema_data: list[dict], output_path: Path = SCHEMA_JSON_PATH) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(schema_data, f, ensure_ascii=False, indent=4)
    print(f"저장 완료: {output_path} ({len(schema_data)}개 테이블)")


def main() -> None:
    load_dotenv()
    schemas = get_postgres_schema()
    print(f"총 {len(schemas)}개의 테이블 정보를 가져왔습니다.")
    save_schema(schemas)

    # 저장 확인
    with open(SCHEMA_JSON_PATH, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    print(f"[확인] 총 {len(loaded)}개 테이블")
    if loaded:
        import json as _json
        print(f"첫 번째 샘플:\n{_json.dumps(loaded[0], indent=2, ensure_ascii=False)}")


if __name__ == "__main__":
    main()
