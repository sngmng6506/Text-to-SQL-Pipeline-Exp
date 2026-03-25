"""
WRONG 항목 분석 스크립트

사용법:
  python eval-analysis/show_wrong.py --input eval-analysis/results/v10/analysis_20260313_112900.xlsx
  python eval-analysis/show_wrong.py --input <파일경로> [--diff 1-2] [--err-type 스키마오류]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("[ERROR] pandas가 없습니다. pip install pandas openpyxl")
    sys.exit(1)

# ── 컬럼명 (eval-analysis/run.py 기준) ───────────────────────────────────────
COL_NO        = "번호"
COL_TABLE     = "대상 테이블"
COL_QUESTION  = "자연어 질문"
COL_DIFF      = "실행 난이도"
COL_GT_SQL    = "정답 SQL"
COL_GEN_SQL   = "생성_SQL"
COL_GEN_RESULT= "생성_실행결과"
COL_EXEC_ERR  = "실행_오류"
COL_CORRECT   = "정답여부"
COL_ERR_TYPE  = "오류타입"
COL_COMMENT   = "분석코멘트"
COL_FAIL_STAGE = "실패_단계"


def _col(df: pd.DataFrame, want: str) -> str | None:
    """공백 차이를 무시하고 컬럼 매칭."""
    if want in df.columns:
        return want
    m = {str(c).strip(): str(c) for c in df.columns}
    return m.get(want)


def _val(row: pd.Series, df: pd.DataFrame, col_name: str) -> str:
    col = _col(df, col_name)
    return str(row[col]).strip() if col and col in row.index else "(없음)"


def load_wrong(path: str, diff_filter: str | None, err_filter: str | None) -> pd.DataFrame:
    df = pd.read_excel(path)

    col_correct = _col(df, COL_CORRECT)
    if col_correct is None:
        print(f"[ERROR] '{COL_CORRECT}' 컬럼을 찾을 수 없습니다.")
        print(f"  발견된 컬럼: {df.columns.tolist()}")
        sys.exit(1)

    wrong = df[df[col_correct].astype(str).str.strip().str.upper() == "WRONG"].copy()

    if diff_filter:
        col_diff = _col(df, COL_DIFF)
        if col_diff:
            wrong = wrong[wrong[col_diff].astype(str).str.strip() == diff_filter]

    if err_filter:
        col_err = _col(df, COL_ERR_TYPE)
        if col_err:
            wrong = wrong[wrong[col_err].astype(str).str.contains(err_filter, na=False)]

    return wrong, df


def print_wrong(wrong: pd.DataFrame, df: pd.DataFrame) -> None:
    total_wrong = len(wrong)
    if total_wrong == 0:
        print("WRONG 항목이 없습니다.")
        return

    # 난이도별 집계
    col_diff = _col(df, COL_DIFF)
    col_err  = _col(df, COL_ERR_TYPE)

    print(f"{'=' * 70}")
    print(f"  WRONG 총 {total_wrong}건")
    if col_diff:
        diff_counts = wrong[col_diff].value_counts().sort_index()
        print(f"  난이도별: { {k: int(v) for k, v in diff_counts.items()} }")
    if col_err:
        err_counts = wrong[col_err].value_counts()
        print(f"  오류타입별: { {k: int(v) for k, v in err_counts.items()} }")
    print(f"{'=' * 70}\n")

    for i, (_, row) in enumerate(wrong.iterrows(), 1):
        no       = _val(row, df, COL_NO)
        table    = _val(row, df, COL_TABLE)
        question = _val(row, df, COL_QUESTION)
        diff     = _val(row, df, COL_DIFF)
        err_type = _val(row, df, COL_ERR_TYPE)
        fail_stg = _val(row, df, COL_FAIL_STAGE)
        comment  = _val(row, df, COL_COMMENT)
        gt_sql   = _val(row, df, COL_GT_SQL)
        gen_sql  = _val(row, df, COL_GEN_SQL)
        exec_err = _val(row, df, COL_EXEC_ERR)

        print(f"[{i:>3}] 번호={no}  난이도={diff}  테이블={table}")
        print(f"       질문: {question}")
        print(f"       오류타입: {err_type}  |  실패단계: {fail_stg}")
        if comment not in ("(없음)", "nan", ""):
            print(f"       코멘트: {comment}")
        print(f"       정답 SQL : {gt_sql}")
        print(f"       생성 SQL : {gen_sql}")
        if exec_err not in ("(없음)", "nan", ""):
            print(f"       실행오류 : {exec_err}")
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description="WRONG 항목 분석")
    parser.add_argument("--input", required=True, help="분석 결과 xlsx 경로")
    parser.add_argument("--diff",     default=None, help="난이도 필터 (예: 1-2)")
    parser.add_argument("--err-type", default=None, help="오류타입 키워드 필터")
    args = parser.parse_args()

    path = Path(args.input)
    if not path.exists():
        print(f"[ERROR] 파일 없음: {path}")
        sys.exit(1)

    wrong, df = load_wrong(str(path), args.diff, args.err_type)
    print_wrong(wrong, df)


if __name__ == "__main__":
    main()
