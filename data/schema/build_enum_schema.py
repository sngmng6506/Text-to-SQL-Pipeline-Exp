"""
DB에서 categorical 컬럼의 실제 허용값(distinct values)을 조회하여
table_schema_column_enum_description.json 을 생성한다.

동작:
  1. table_schema_column_description.json 을 기반으로 테이블/컬럼 목록 파악
  2. varchar / character varying / USER-DEFINED 타입 컬럼에 대해
     SELECT DISTINCT 조회
  3. distinct 값이 MAX_DISTINCT 이하인 컬럼에만 "values" 필드 추가
  4. 결과를 table_schema_column_enum_description.json 에 저장

실행:
  cd c:\\Users\\hanwha\\Desktop\\TAG-test
  python data/schema/build_enum_schema.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------

SCHEMA_DIR   = Path(__file__).parent
INPUT_JSON   = SCHEMA_DIR / "table_schema_column_description.json"
OUTPUT_JSON  = SCHEMA_DIR / "table_schema_column_enum_description.json"

# distinct 값이 이 수 이하인 컬럼만 허용값으로 등록
MAX_DISTINCT = 30

# 이름 기준으로 자유 텍스트 / 민감 / 비카테고리 컬럼 제외
FREE_TEXT_KEYWORDS = {
    # 자유 텍스트
    "description", "message", "detail", "content", "remark",
    "comment", "memo", "note", "title", "process_message",
    "process_method", "file_id_list", "loc_all", "loc_unit",
    "batch_number", "order_number", "sku_key", "loc_raw",
    # 민감 정보
    "password", "email", "phone_number", "try_ip",
    # 식별자성 코드 (데이터 ID, 자유값)
    "code", "name", "equipment_name", "equipment_code",
    "container_no", "sku_id", "user_id",
    # 색상 / 시스템 설정값
    "load_warning_color_crane", "load_danger_color_crane",
    "load_warning_color_gantry", "load_danger_color_gantry",
}

# 컬럼명에 이 문자열이 포함되면 제외 (패턴 기반)
FREE_TEXT_PATTERNS = {"color", "order_flow"}

# categorical 후보 타입
CATEGORICAL_TYPES = {"character varying", "varchar", "user-defined", "text"}


# ---------------------------------------------------------------------------
# DB 연결
# ---------------------------------------------------------------------------

def get_conn():
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        port=int(os.getenv("DB_PORT", "5432")),
    )


# ---------------------------------------------------------------------------
# 허용값 조회
# ---------------------------------------------------------------------------

def fetch_distinct_values(cur, table: str, column: str) -> list[str] | None:
    """테이블.컬럼의 DISTINCT 값 조회.

    distinct 수가 MAX_DISTINCT 초과면 None 반환 (자유 텍스트로 간주).
    조회 실패(뷰 권한 등)시 None 반환.
    """
    try:
        # 먼저 카운트 확인 (대용량 테이블에서 전체 스캔 방지)
        cur.execute(
            f'SELECT COUNT(DISTINCT "{column}") FROM "{table}"'  # noqa: S608
        )
        count = cur.fetchone()[0]
        if count > MAX_DISTINCT:
            return None

        cur.execute(
            f'SELECT DISTINCT "{column}" FROM "{table}" '  # noqa: S608
            f'WHERE "{column}" IS NOT NULL ORDER BY "{column}"'
        )
        rows = cur.fetchall()
        return [str(r[0]) for r in rows]
    except Exception as e:  # noqa: BLE001
        print(f"    [SKIP] {table}.{column}: {e}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def main() -> None:
    schema = json.loads(INPUT_JSON.read_text(encoding="utf-8"))

    conn = get_conn()
    cur  = conn.cursor()

    print(f"입력: {INPUT_JSON}")
    print(f"출력: {OUTPUT_JSON}")
    print(f"MAX_DISTINCT = {MAX_DISTINCT}\n")

    result = []
    for table_entry in schema:
        table = table_entry["table"]
        new_columns = []

        for col in table_entry.get("columns", []):
            col_name  = col["name"]
            col_type  = col.get("type", "").lower()
            new_col   = dict(col)

            col_lower           = col_name.lower()
            is_categorical_type = col_type in CATEGORICAL_TYPES
            is_free_text        = (
                col_lower in FREE_TEXT_KEYWORDS
                or any(p in col_lower for p in FREE_TEXT_PATTERNS)
            )

            if is_categorical_type and not is_free_text:
                print(f"  조회 중: {table}.{col_name} ({col_type})")
                values = fetch_distinct_values(cur, table, col_name)
                # 빈 문자열만 있거나 단일 의미없는 값이면 제외
                meaningful = [v for v in (values or []) if v.strip()]
                if meaningful:
                    new_col["values"] = meaningful
                    print(f"    → {meaningful}")
                else:
                    print(f"    → (의미있는 값 없음, 스킵)")

            new_columns.append(new_col)

        result.append({
            "table":       table_entry["table"],
            "description": table_entry.get("description", ""),
            "columns":     new_columns,
        })

    cur.close()
    conn.close()

    OUTPUT_JSON.write_text(
        json.dumps(result, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n완료: {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
