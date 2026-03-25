"""
TAG 파이프라인 평가 스크립트

SQL-dataset.xlsx의 자연어 질문을 파싱해 파이프라인을 일괄 실행하고
생성된 SQL 및 실행 결과를 정답과 함께 xlsx로 저장합니다.

사용법 (TAG-test 루트에서):
  python eval/run.py ir-use
  python eval/run.py ir-use --resume
  python eval/run.py ir-use --resume result.xlsx
  python eval/run.py ir-use --dataset SQL-dataset-multi.xlsx

  python eval/run.py simple
  python eval/run.py simple --resume
  python eval/run.py simple --dataset SQL-dataset-multi.xlsx
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_root))

from dotenv import load_dotenv
load_dotenv(_root / ".env", override=True)

import pandas as pd

from config import EVAL_OUTPUT_DIR
from config.simple import SIMPLE_EVAL_OUTPUT_DIR, SIMPLE_VLLM_BASE_URL, SIMPLE_VLLM_MODEL, SIMPLE_VLLM_TIMEOUT_SEC, SIMPLE_N
from config.common import SCHEMA_TOP_K, SQL_MAX_TOKENS, EVAL_DATA_DIR, EVAL_DATASET_PATH

# ── xlsx 컬럼명 (입력) ────────────────────────────────────────────────────────
COL_NO       = "번호"
COL_TABLE    = "대상 테이블"
COL_QUESTION = "자연어 질문"
COL_GT_SQL   = "정답 SQL"
COL_GT_RESULT= "SQL 실행 결과"
COL_DIFF     = "실행 난이도"

# ── 공통 출력 컬럼 ───────────────────────────────────────────────────────────
COL_GEN_SQL    = "생성_SQL"
COL_GEN_RESULT = "생성_실행결과"
COL_EXEC_ERROR = "실행_오류"
COL_TOTAL      = "시간_합계(s)"

# ── IR-use 전용 출력 컬럼 ────────────────────────────────────────────────────
COL_SCHEMA_LINKING = "스키마_링킹_결과"
COL_IR_JSON        = "IR_JSON"
COL_T1             = "시간_엔티티(s)"
COL_T2             = "시간_스키마(s)"
COL_T3             = "시간_링킹(s)"
COL_T4             = "시간_재작성(s)"
COL_T5             = "시간_SQL생성(s)"
COL_T6             = "시간_DB실행(s)"

# ── Simple 전용 출력 컬럼 ────────────────────────────────────────────────────
COL_SELECT_METHOD = "선택_방식"
COL_T_SCHEMA      = "시간_스키마(s)"
COL_T_GENERATE    = "시간_SQL생성(s)"
COL_T_VALIDATE    = "시간_검증(s)"


# ── 공통 유틸 ────────────────────────────────────────────────────────────────

def _resolve_col(df: pd.DataFrame, want: str) -> str | None:
    """컬럼명을 유연하게 탐색한다 (공백 무시, 부분 일치 포함).

    1순위: 정확히 일치
    2순위: strip 후 일치
    3순위: 'want'를 포함하는 첫 번째 컬럼
    """
    if want in df.columns:
        return want
    stripped = {str(c).strip(): str(c) for c in df.columns}
    if want in stripped:
        return stripped[want]
    for col in df.columns:
        if want in str(col):
            return str(col)
    return None


def _normalize_diff(v: object) -> str:
    """난이도 셀 값을 문자열로 정규화한다.

    Excel이 숫자(1, 2, 3)를 날짜 시리얼로 파싱해
    datetime으로 반환하는 경우를 복원한다.
    (Excel 날짜 시리얼 1 = 1900-01-01이므로 역산해 원래 숫자를 복구)
    """
    import datetime as _dt
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    if isinstance(v, _dt.datetime):
        return str((v.date() - _dt.date(1899, 12, 31)).days)
    if isinstance(v, float) and v == int(v):
        return str(int(v))
    return str(v).strip()


def _load_dataset(filename: str | None = None) -> tuple[pd.DataFrame, str | None]:
    """데이터셋을 로드하고 (DataFrame, 실제 난이도 컬럼명)을 반환한다."""
    path = EVAL_DATA_DIR / filename if filename else EVAL_DATASET_PATH
    if not path.exists():
        print(f"[ERROR] 데이터셋 파일을 찾을 수 없습니다: {path}", file=sys.stderr)
        sys.exit(1)
    df = pd.read_excel(path)
    df = df[df[COL_QUESTION].notna()].reset_index(drop=True)
    diff_col = _resolve_col(df, COL_DIFF)
    if diff_col:
        df[diff_col] = df[diff_col].apply(_normalize_diff)
    return df, diff_col


def _setup_output(output_dir: Path, resume: str | None) -> tuple[Path, list, int]:
    """출력 경로 및 시작 인덱스를 결정한다. (out_path, records, start_idx)"""
    output_dir.mkdir(parents=True, exist_ok=True)

    if resume == "auto":
        candidates = sorted(output_dir.glob("result_*.xlsx"),
                            key=lambda p: p.stat().st_mtime, reverse=True)
        if not candidates:
            print("[ERROR] 이어서 실행할 result_*.xlsx 파일을 찾을 수 없습니다.", file=sys.stderr)
            sys.exit(1)
        resume_path = candidates[0]
        print(f"[AUTO-RESUME] 최신 파일: {resume_path}")
    elif resume is not None:
        resume_path = Path(resume)
        if not resume_path.exists():
            print(f"[ERROR] 파일을 찾을 수 없습니다: {resume_path}", file=sys.stderr)
            sys.exit(1)
    else:
        resume_path = None

    if resume_path:
        existing   = pd.read_excel(resume_path)
        records    = existing.to_dict("records")
        start_idx  = len(records)
        out_path   = resume_path
        print(f"[RESUME] {resume_path.name} 에서 이어서 실행합니다.")
    else:
        ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path  = output_dir / f"result_{ts}.xlsx"
        records   = []
        start_idx = 0

    return out_path, records, start_idx


# ── IR-use 평가 ──────────────────────────────────────────────────────────────

def run_evaluate_ir_use(resume: str | None, dataset: str | None = None) -> None:
    from clients.chat import default_vllm_client
    from pipelines.ir_use import run_pipeline

    model_id = os.getenv("VLLM_MODEL")
    if not model_id:
        print("[ERROR] VLLM_MODEL 환경변수가 없습니다.", file=sys.stderr)
        sys.exit(1)

    client  = default_vllm_client()
    valid, diff_col = _load_dataset(dataset)
    total   = len(valid)
    out_path, records, start_idx = _setup_output(EVAL_OUTPUT_DIR, resume)

    print(f"총 {total}건 중 {total - start_idx}건 평가 시작")
    print(f"저장 경로: {out_path}\n")

    for i, row in valid.iterrows():
        if i < start_idx:
            continue

        question  = str(row[COL_QUESTION]).strip()
        gt_sql    = str(row.get(COL_GT_SQL, "")).strip()
        gt_result = str(row.get(COL_GT_RESULT, "")).strip()
        diff      = str(row.get(diff_col, "") if diff_col else "").strip()

        print(f"\n{'─'*60}", flush=True)
        print(f"  [{i+1}/{total}] (난이도: {diff or '?'})  {question[:55]}", flush=True)
        print(f"{'─'*60}", flush=True)

        _item_start  = time.perf_counter()
        pipeline_out = run_pipeline(
            question,
            top_k=SCHEMA_TOP_K,
            max_tokens=SQL_MAX_TOKENS,
            model_id=model_id,
            client=client,
        )
        elapsed = time.perf_counter() - _item_start

        gen_sql  = pipeline_out["sql"]
        pipe_err = pipeline_out["error"] or ""
        exec_err = pipeline_out.get("exec_error") or ""
        timings  = pipeline_out.get("timings") or {}

        gen_result = ""
        if pipeline_out.get("db_result") is not None:
            gen_result = json.dumps(pipeline_out["db_result"], ensure_ascii=False, default=str)

        records.append({
            COL_NO             : row.get(COL_NO, i + 1),
            COL_TABLE          : row.get(COL_TABLE, ""),
            COL_QUESTION       : question,
            COL_DIFF           : diff,
            COL_GT_SQL         : gt_sql,
            COL_GT_RESULT      : gt_result,
            COL_GEN_SQL        : gen_sql if not pipe_err else f"[ERROR] {pipe_err}",
            COL_GEN_RESULT     : gen_result,
            COL_EXEC_ERROR     : exec_err or pipe_err,
            COL_SCHEMA_LINKING : pipeline_out.get("schema_linking_json", ""),
            COL_IR_JSON        : pipeline_out.get("rewritten", ""),
            COL_T1             : timings.get("t1_entity"),
            COL_T2             : timings.get("t2_schema"),
            COL_T3             : timings.get("t3_linking"),
            COL_T4             : timings.get("t4_rewrite", timings.get("t3_rewrite")),
            COL_T5             : timings.get("t5_sql",     timings.get("t4_sql")),
            COL_T6             : timings.get("t6_db",      timings.get("t5_db")),
            COL_TOTAL          : timings.get("total"),
        })

        _print_progress(i + 1, total, start_idx, elapsed, gen_sql, pipe_err, exec_err)
        pd.DataFrame(records).to_excel(out_path, index=False)

    print(f"\n평가 완료. 결과 저장: {out_path}")


# ── Simple 평가 ──────────────────────────────────────────────────────────────

def run_evaluate_simple(resume: str | None, dataset: str | None = None) -> None:
    from clients.chat import VllmChatClient
    from pipelines.simple import run_pipeline

    if not SIMPLE_VLLM_BASE_URL or not SIMPLE_VLLM_MODEL:
        raise EnvironmentError(
            ".env에 SIMPLE_VLLM_BASE_URL 과 SIMPLE_VLLM_MODEL 을 설정해야 합니다."
        )

    client = VllmChatClient(
        base_url=f"{SIMPLE_VLLM_BASE_URL.rstrip('/')}/v1",
        timeout_sec=SIMPLE_VLLM_TIMEOUT_SEC,
    )

    valid, diff_col = _load_dataset(dataset)
    total   = len(valid)
    out_path, records, start_idx = _setup_output(SIMPLE_EVAL_OUTPUT_DIR, resume)

    print(f"총 {total}건 중 {total - start_idx}건 평가 시작")
    print(f"저장 경로: {out_path}\n")

    for i, row in valid.iterrows():
        if i < start_idx:
            continue

        question  = str(row[COL_QUESTION]).strip()
        gt_sql    = str(row.get(COL_GT_SQL, "")).strip()
        gt_result = str(row.get(COL_GT_RESULT, "")).strip()
        diff      = str(row.get(diff_col, "") if diff_col else "").strip()

        print(f"\n{'─'*60}", flush=True)
        print(f"  [{i+1}/{total}] (난이도: {diff or '?'})  {question[:55]}", flush=True)
        print(f"{'─'*60}", flush=True)

        _item_start  = time.perf_counter()
        pipeline_out = run_pipeline(
            question,
            client=client,
            model_id=SIMPLE_VLLM_MODEL,
            n_candidates=SIMPLE_N,
            top_k=SCHEMA_TOP_K,
            max_tokens=SQL_MAX_TOKENS,
        )
        elapsed = time.perf_counter() - _item_start

        gen_sql  = pipeline_out["sql"]
        pipe_err = pipeline_out["error"] or ""
        exec_err = pipeline_out.get("exec_error") or ""
        timings  = pipeline_out.get("timings") or {}

        gen_result = ""
        if pipeline_out.get("db_result") is not None:
            gen_result = json.dumps(pipeline_out["db_result"], ensure_ascii=False, default=str)

        records.append({
            COL_NO            : row.get(COL_NO, i + 1),
            COL_TABLE         : row.get(COL_TABLE, ""),
            COL_QUESTION      : question,
            COL_DIFF          : diff,
            COL_GT_SQL        : gt_sql,
            COL_GT_RESULT     : gt_result,
            COL_GEN_SQL       : gen_sql if not pipe_err else f"[ERROR] {pipe_err}",
            COL_GEN_RESULT    : gen_result,
            COL_EXEC_ERROR    : exec_err or pipe_err,
            COL_SELECT_METHOD : pipeline_out.get("select_method", ""),
            COL_T_SCHEMA      : timings.get("t1_schema"),
            COL_T_GENERATE    : timings.get("t2_generate"),
            COL_T_VALIDATE    : timings.get("t3_validate"),
            COL_TOTAL         : timings.get("total"),
        })

        _print_progress(i + 1, total, start_idx, elapsed, gen_sql, pipe_err, exec_err)
        pd.DataFrame(records).to_excel(out_path, index=False)

    print(f"\n평가 완료. 결과 저장: {out_path}")


# ── 공통 진행 출력 ────────────────────────────────────────────────────────────

def _print_progress(
    idx: int, total: int, start_idx: int,
    elapsed: float, gen_sql: str, pipe_err: str, exec_err: str,
) -> None:
    print(f"\n  완료  ({elapsed:.1f}s)", flush=True)
    if pipe_err:
        print(f"  [파이프라인 오류] {pipe_err[:80]}", flush=True)
    elif exec_err:
        print(f"  [DB 오류] {exec_err[:80]}", flush=True)
    else:
        print(f"  SQL: {gen_sql[:80]}", flush=True)
    done = idx - start_idx
    print(f"  진행: {done}/{total - start_idx}건 완료, 잔여 {total - idx}건", flush=True)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="TAG 파이프라인 평가")
    sub = parser.add_subparsers(dest="pipeline", required=True)

    common_parent = argparse.ArgumentParser(add_help=False)
    common_parent.add_argument(
        "--resume",
        nargs="?",
        const="auto",
        default=None,
        metavar="XLSX",
        help="중단된 결과 xlsx에서 이어서 실행. 경로 생략 시 최신 파일 자동 선택.",
    )
    common_parent.add_argument(
        "--dataset",
        default=None,
        metavar="FILENAME",
        help="데이터셋 파일명 (data/eval_data/ 기준, 예: SQL-dataset-multi.xlsx). 생략 시 SQL-dataset.xlsx 사용.",
    )

    sub.add_parser(
        "ir-use",
        parents=[common_parent],
        help="IR 기반 파이프라인 평가",
    ).set_defaults(func=lambda a: run_evaluate_ir_use(a.resume, a.dataset))

    sub.add_parser(
        "simple",
        parents=[common_parent],
        help="Simple 파이프라인 평가 (로컬 모델, 1회 로드 후 루프)",
    ).set_defaults(func=lambda a: run_evaluate_simple(a.resume, a.dataset))

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
