"""
SQL AST 유틸리티

sqlglot을 이용해 SQL을 정규화·비교한다.

주요 기능:
  - normalize_sql   : SQL을 정규화된 문자열로 변환 (공백·대소문자·조건 순서 통일)
  - sql_ast_key     : 다수결 그룹핑에 쓸 비교 키 반환
  - ast_majority_vote : 후보 SQL 리스트에서 AST 기준 다수결로 최선 SQL 선택

의존:
  pip install sqlglot
"""

from __future__ import annotations

import json
import re
from collections import Counter
from typing import Sequence


# ---------------------------------------------------------------------------
# 정규화
# ---------------------------------------------------------------------------

def normalize_sql(sql: str, dialect: str = "postgres") -> str:
    """
    SQL을 sqlglot으로 파싱 후 정규화된 문자열로 반환한다.

    정규화 내용:
    - 키워드 대소문자 통일
    - 불필요한 공백 제거
    - 식별자 소문자 변환
    - 파싱 실패 시 소문자 strip 문자열 반환 (폴백)

    Parameters
    ----------
    sql     : 원본 SQL 문자열
    dialect : sqlglot 방언 (기본 'postgres')
    """
    try:
        import sqlglot
        parsed = sqlglot.parse_one(sql, read=dialect)
        raw = parsed.sql(dialect=dialect, pretty=False)
        return re.sub(r"\s+", " ", raw).strip().lower()
    except Exception:
        return re.sub(r"\s+", " ", sql).strip().lower()


def _sort_commutative(node: object) -> object:
    """
    AND / OR 조건의 피연산자를 SQL 문자열 기준으로 정렬한다.

    WHERE a = 1 AND b = 2  ↔  WHERE b = 2 AND a = 1 을 같은 키로 만든다.
    sqlglot.Expression.transform() 콜백으로 사용한다.
    """
    try:
        import sqlglot.expressions as exp
        if isinstance(node, (exp.And, exp.Or)):
            left_key  = node.left.sql(pretty=False).lower()
            right_key = node.right.sql(pretty=False).lower()
            if left_key > right_key:
                cls = type(node)
                return cls(this=node.right.copy(), expression=node.left.copy())
    except Exception:
        pass
    return node


def sql_ast_key(sql: str, dialect: str = "postgres") -> str:
    """
    SQL의 AST 정규화 키를 반환한다.

    동일한 의미의 SQL이면 (공백·대소문자·AND/OR 조건 순서 무관) 동일한 키를 반환한다.
    다수결 그룹핑 키로 사용한다.
    """
    try:
        import sqlglot
        # 후행 세미콜론 제거 후 파싱
        parsed = sqlglot.parse_one(sql.rstrip(";").strip(), read=dialect)
        # AND/OR 피연산자를 정렬해 조건 순서 차이를 제거
        parsed = parsed.transform(_sort_commutative)
        # AST 자체를 직렬화해 포맷팅(개행/공백) 차이를 완전히 제거
        return json.dumps(parsed.dump(), ensure_ascii=False, sort_keys=True)
    except Exception:
        return normalize_sql(sql, dialect=dialect)


# ---------------------------------------------------------------------------
# AST 다수결 선택
# ---------------------------------------------------------------------------

def ast_majority_vote(
    sqls: Sequence[str],
    dialect: str = "postgres",
) -> tuple[str, str, list[tuple[str, int]]]:
    """
    후보 SQL 리스트에서 AST 기준 다수결로 대표 SQL을 선택한다.

    동일한 AST 키를 가진 SQL 그룹 중 가장 많이 등장한 것을 선택한다.
    동률이면 첫 번째 후보(first_valid)를 반환한다.

    Parameters
    ----------
    sqls    : 후보 SQL 문자열 리스트 (구문 통과한 것들)
    dialect : sqlglot 방언

    Returns
    -------
    (selected_sql, method, ranking)

    selected_sql : 선택된 SQL
    method       : "majority_vote (AST)" | "first_valid"
    ranking      : [(sql, vote_count), ...] 득표 내림차순
    """
    if not sqls:
        return "", "no_candidate", []

    key_to_sql: dict[str, str] = {}
    keys: list[str] = []

    for sql in sqls:
        k = sql_ast_key(sql, dialect=dialect)
        keys.append(k)
        if k not in key_to_sql:
            key_to_sql[k] = sql  # 그룹의 첫 번째 SQL을 대표로 사용

    counter = Counter(keys)
    ranking = [(key_to_sql[k], cnt) for k, cnt in counter.most_common()]

    best_sql, best_count = ranking[0]
    method = "majority_vote (AST)" if best_count > 1 else "first_valid"

    return best_sql, method, ranking
