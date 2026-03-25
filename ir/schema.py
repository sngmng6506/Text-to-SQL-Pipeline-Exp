"""
IR / Schema Linking JSON Schema 정의

vLLM guided_json 파라미터에 전달하여 항상 유효한 JSON을 생성하도록 강제한다.
파싱 실패율 0%, max_tokens 대폭 축소 가능.
"""

SCHEMA_LINKING_JSON_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "linked_tables": {
            "type": "array",
            "items": {"type": "string"},
            "description": "질문 이해에 필요한 테이블 목록"
        },
        "linked_columns": {
            "type": "array",
            "items": {"type": "string"},
            "description": "테이블.컬럼 형식의 컬럼 목록"
        },
        "column_mappings": {
            "type": "object",
            "additionalProperties": {"type": "string"},
            "description": "질문 표현 → 테이블.컬럼 매핑"
        },
        "filter_columns": {
            "type": "array",
            "items": {"type": "string"},
            "description": "WHERE 조건에 사용될 테이블.컬럼 목록"
        },
        "temporal_columns": {
            "type": "array",
            "items": {"type": "string"},
            "description": "시간/날짜 관련 테이블.컬럼 목록"
        },
    },
    "required": ["linked_tables", "linked_columns", "column_mappings", "filter_columns", "temporal_columns"],
    "additionalProperties": False,
}


IR_JSON_SCHEMA: dict = {
    "type": "object",
    "properties": {
        "FROM": {
            "type": "array",
            "items": {"type": "string"},
            "description": "조회 대상 테이블 목록"
        },
        "JOIN": {
            "type": "array",
            "items": {"type": "string"},
            "description": "JOIN 조건 표현식 목록"
        },
        "FILTER": {
            "type": "array",
            "items": {"type": "string"},
            "description": "WHERE 조건 표현식 목록"
        },
        "GROUP_BY": {
            "type": "array",
            "items": {"type": "string"},
            "description": "GROUP BY 컬럼 목록"
        },
        "AGGREGATE": {
            "type": "array",
            "items": {"type": "string"},
            "description": "집계 표현식 목록 (예: COUNT(*), SUM(col))"
        },
        "HAVING": {
            "type": "array",
            "items": {"type": "string"},
            "description": "HAVING 조건 표현식 목록"
        },
        "ORDER_BY": {
            "type": "array",
            "items": {"type": "string"},
            "description": "ORDER BY 표현식 목록 (예: col DESC)"
        },
        "LIMIT": {
            "type": ["integer", "null"],
            "description": "결과 건수 제한 (없으면 null)"
        },
        "SELECT": {
            "type": "array",
            "items": {"type": "string"},
            "description": "SELECT 대상 컬럼·표현식 목록"
        },
        "COMPUTED": {
            "type": "array",
            "items": {"type": "string"},
            "description": "CASE WHEN 기반 계산 컬럼 표현식 목록 (예: CASE WHEN col > x THEN 'A' ELSE 'B' END AS label)"
        },
    },
    "required": ["FROM", "JOIN", "FILTER", "GROUP_BY", "AGGREGATE", "HAVING", "ORDER_BY", "LIMIT", "SELECT", "COMPUTED"],
    "additionalProperties": False,
}
