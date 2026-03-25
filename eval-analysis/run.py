"""
TAG 파이프라인 평가 결과 분석 스크립트

평가 결과 xlsx를 읽어 OpenAI API로 정답 여부 및 오류 타입을 분석하고
결과를 새 xlsx로 저장합니다.

사용법 (eval-analysis 루트에서):
  python run.py --pipeline ir     --input data/eval_data/results/v10/result_XXX.xlsx
  python run.py --pipeline simple --input data/eval_data/results-simple/v1/result_XXX.xlsx
  python run.py --pipeline simple --input result.xlsx --start 10   # 10번 행부터 이어서
  python run.py --pipeline ir     --input result.xlsx --model gpt-5.2
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

try:
    import httpx
    import pandas as pd
    from jinja2 import Environment, FileSystemLoader
    from openai import OpenAI
except ImportError as e:
    print(f"[ERROR] 필요한 패키지가 없습니다: {e}")
    print("pip install openai pandas openpyxl jinja2 python-dotenv httpx")
    sys.exit(1)


# ── 경로 설정 ──────────────────────────────────────────────
_root = Path(__file__).resolve().parent
PROMPTS_DIR = _root / "prompts"

_tag_root = _root.parent
sys.path.insert(0, str(_tag_root))
from config import ANALYSIS_OUTPUT_DIR
from config.simple import SIMPLE_ANALYSIS_OUTPUT_DIR

_PIPELINE_OUTPUT_DIRS = {
    "ir":     ANALYSIS_OUTPUT_DIR,
    "simple": SIMPLE_ANALYSIS_OUTPUT_DIR,
}


# ── xlsx 컬럼명 (TAG-test 평가 결과 기준) ───────────────────
COL_NO        = "번호"
COL_TABLE     = "대상 테이블"
COL_QUESTION  = "자연어 질문"
COL_DIFF      = "실행 난이도"
COL_GT_SQL    = "정답 SQL"
COL_GT_RESULT = "SQL 실행 결과"
COL_GEN_SQL   = "생성_SQL"
COL_GEN_RESULT= "생성_실행결과"
COL_EXEC_ERR  = "실행_오류"
COL_IR_JSON   = "IR_JSON"
COL_SCHEMA_LINKING_JSON = "스키마_링킹_결과"


# ── 추가 출력 컬럼 ──────────────────────────────────────────
COL_CORRECT   = "정답여부"
COL_ERR_TYPE  = "오류타입"
COL_COMMENT   = "분석코멘트"
COL_FAIL_STAGE = "실패_단계"
COL_SCHEMA_JUDGEMENT = "스키마링킹_판단"
COL_SCHEMA_COMMENT = "스키마링킹_코멘트"
COL_IR_JUDGEMENT = "IR_판단"
COL_IR_COMMENT = "IR_코멘트"


def _resolve_column(df: "pd.DataFrame", want: str) -> str | None:
    """
    엑셀 컬럼명에 trailing/leading space가 섞여도 매칭되도록 처리.
    예: '실행 난이도', '실행 난이도 ', ' 실행 난이도'
    """
    if want in df.columns:
        return want
    stripped_map = {str(c).strip(): str(c) for c in df.columns}
    return stripped_map.get(want)


def _difficulty_sort_key(x: object) -> tuple[int, int, str]:
    """난이도 표기를 숫자 기준으로 정렬.

    '1-2' 형식과 '1' 형식 모두 지원한다.
    """
    s = str(x).strip()
    try:
        if "-" in s:
            a, b = s.split("-", 1)
            return (int(a), int(b), s)
        return (int(s), 0, s)
    except Exception:
        return (10**9, 10**9, s)


# ── 프롬프트 렌더러 ─────────────────────────────────────────
_jinja_env = Environment(
    loader=FileSystemLoader(str(PROMPTS_DIR)),
    autoescape=False,
)


def _render_prompt(
    question: str,
    gt_sql: str,
    gen_sql: str,
    schema_linking_json: str,
    ir_json: str,
    gt_result: str,
    gen_result: str,
    exec_error: str,
) -> str:
    return _jinja_env.get_template("analyze.j2").render(
        question=question,
        gt_sql=gt_sql or "(없음)",
        gen_sql=gen_sql or "(없음)",
        schema_linking_json=schema_linking_json or "(없음)",
        ir_json=ir_json or "(없음)",
        gt_result=gt_result or "(없음)",
        gen_result=gen_result or "(없음)",
        exec_error=exec_error or "(없음)",
    )


def _infer_failure_stage(correctness: str, error_type: str) -> str:
    if correctness == "CORRECT":
        return "none"

    _execution = {"NO_SQL", "SQL_SYNTAX", "EXECUTION_ERROR"}
    _schema = {"WRONG_TABLE", "WRONG_COLUMN", "WRONG_JOIN"}
    _ir = {
        "MISSING_FILTER",
        "WRONG_FILTER_COLUMN",
        "WRONG_FILTER_VALUE",
        "WRONG_FILTER_OPERATOR",
        "TYPE_MISMATCH",
        "WRONG_AGGREGATE_FUNC",
        "MISSING_GROUP_BY",
        "WRONG_GROUP_BY",
        "WRONG_HAVING",
        "WRONG_ORDER_BY",
        "WRONG_LIMIT",
        "WRONG_PROJECTION",
        "DUPLICATE_HANDLING",
    }

    if error_type in _execution:
        return "execution"
    if error_type in _schema:
        return "schema_linking"
    if error_type in _ir:
        return "ir_generation"
    return "sql_generation"


# ── OpenAI 호출 ─────────────────────────────────────────────
def _analyze_one(
    client: OpenAI,
    model: str,
    prompt: str,
    timeout: int,
) -> dict:
    """프롬프트를 OpenAI에 보내고 JSON 결과를 반환. 실패 시 기본값 반환."""
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                timeout=timeout,
                response_format={"type": "json_object"},
            )
            content = resp.choices[0].message.content or ""
            return json.loads(content)
        except Exception as e:
            wait = 2 ** attempt
            print(f"  [WARN] OpenAI 오류 (시도 {attempt+1}/3), {wait}s 후 재시도: {e}", flush=True)
            time.sleep(wait)

    return {
        "correctness": "ERROR",
        "error_type": "API_FAIL",
        "failure_stage": "sql_generation",
        "comment": "OpenAI 호출 실패",
    }


# ── 메인 분석 루프 ──────────────────────────────────────────
def run_analyze(
    input_path: Path,
    model: str,
    timeout: int,
    start_idx: int,
    pipeline: str = "ir",
) -> None:
    output_dir = _PIPELINE_OUTPUT_DIRS[pipeline]
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_excel(input_path)
    total = len(df)
    diff_col = _resolve_column(df, COL_DIFF)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = output_dir / f"analysis_{ts}.xlsx"
    print(f"총 {total}건 분석 시작 (start={start_idx})")
    print(f"입력: {input_path}")
    print(f"저장: {out_path}\n")

    # 회사 네트워크 SSL 인터셉션 대응: verify=False로 자체 서명 인증서 우회
    _http_client = httpx.Client(verify=False)
    client = OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        timeout=timeout,
        http_client=_http_client,
    )

    records = []
    for i, row in df.iterrows():
        if i < start_idx:
            continue

        question   = str(row.get(COL_QUESTION,  "")).strip()
        gt_sql     = str(row.get(COL_GT_SQL,    "")).strip()
        gen_sql    = str(row.get(COL_GEN_SQL,   "")).strip()
        schema_linking_json = str(row.get(COL_SCHEMA_LINKING_JSON, "")).strip()
        ir_json    = str(row.get(COL_IR_JSON,   "")).strip()
        gt_result  = str(row.get(COL_GT_RESULT, "")).strip()
        gen_result = str(row.get(COL_GEN_RESULT,"")).strip()
        exec_error = str(row.get(COL_EXEC_ERR,  "")).strip()
        diff_val   = str(row.get(diff_col, "")).strip() if diff_col else ""

        if diff_val:
            print(f"\n[{i+1}/{total}] (난이도: {diff_val}) {question[:60]}", flush=True)
        else:
            print(f"\n[{i+1}/{total}] {question[:60]}", flush=True)

        prompt = _render_prompt(
            question,
            gt_sql,
            gen_sql,
            schema_linking_json,
            ir_json,
            gt_result,
            gen_result,
            exec_error,
        )
        result = _analyze_one(client, model, prompt, timeout)

        correctness = result.get("correctness", "ERROR")
        error_type  = result.get("error_type",  "UNKNOWN")
        failure_stage = result.get("failure_stage", "").strip() or _infer_failure_stage(correctness, error_type)
        schema_judgement = result.get("schema_linking_judgement", "UNKNOWN")
        schema_comment = result.get("schema_linking_comment", "")
        ir_judgement = result.get("ir_judgement", "UNKNOWN")
        ir_comment = result.get("ir_comment", "")
        comment     = result.get("comment",     "")

        print(f"  → {correctness}  [{error_type}]  <{failure_stage}>  {comment[:60]}", flush=True)

        record = dict(row)
        record[COL_CORRECT]  = correctness
        record[COL_ERR_TYPE] = error_type
        record[COL_FAIL_STAGE] = failure_stage
        record[COL_SCHEMA_JUDGEMENT] = schema_judgement
        record[COL_SCHEMA_COMMENT] = schema_comment
        record[COL_IR_JUDGEMENT] = ir_judgement
        record[COL_IR_COMMENT] = ir_comment
        record[COL_COMMENT]  = comment
        records.append(record)

        # 건별 즉시 저장
        pd.DataFrame(records).to_excel(out_path, index=False)

    result_df = pd.DataFrame(records)
    json_path = out_path.with_name(out_path.stem + "_summary.json")
    print(f"\n분석 완료. 결과 저장: {out_path}")
    _print_summary(result_df, diff_col=diff_col, json_path=json_path)
    print(f"요약 JSON 저장: {json_path}")


# ── 요약 출력 + JSON 저장 ────────────────────────────────────
def _print_summary(
    df: "pd.DataFrame",
    *,
    diff_col: str | None,
    json_path: Path | None = None,
) -> None:
    print("\n" + "=" * 50)
    print("  난이도별 정답률 요약")
    print("=" * 50)

    total_all   = len(df)
    correct_all = int((df[COL_CORRECT] == "CORRECT").sum())

    summary: dict = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total": total_all,
        "correct": correct_all,
        "accuracy_pct": round(correct_all / total_all * 100, 2) if total_all else 0.0,
        "by_difficulty": {},
        "by_error_type": {},
        "by_failure_stage": {},
        "timings_sec": {},
    }

    # 난이도별
    if diff_col and diff_col in df.columns:
        stats = []
        for diff, group in df.groupby(diff_col, dropna=False):
            n       = len(group)
            correct = int((group[COL_CORRECT] == "CORRECT").sum())
            pct     = round(correct / n * 100, 2) if n else 0.0
            stats.append((diff, correct, int(n), pct))
        for diff, correct, n, pct in sorted(stats, key=lambda t: _difficulty_sort_key(t[0])):
            print(f"  {str(diff):<12} {correct:>3}/{n:<3}  ({pct:.1f}%)")
            summary["by_difficulty"][str(diff)] = {"correct": correct, "total": n, "accuracy_pct": pct}
    else:
        print("  (난이도 컬럼을 찾지 못해 난이도별 정답률을 생략합니다.)")

    pct_all = round(correct_all / total_all * 100, 2) if total_all else 0.0
    print(f"  {'전체':<12} {correct_all:>3}/{total_all:<3}  ({pct_all:.1f}%)")

    # 오류 타입별
    print("\n" + "=" * 50)
    print("  오류 타입별 분포")
    print("=" * 50)
    wrong = df[df[COL_CORRECT] != "CORRECT"]
    if len(wrong):
        for err_type, group in wrong.groupby(COL_ERR_TYPE, dropna=False):
            print(f"  {str(err_type):<25} {len(group)}건")
            summary["by_error_type"][str(err_type)] = int(len(group))
    else:
        print("  오류 없음")

    # 실패 단계별
    if COL_FAIL_STAGE in df.columns:
        print("\n" + "=" * 50)
        print("  실패 단계별 분포")
        print("=" * 50)
        wrong2 = df[df[COL_CORRECT] != "CORRECT"]
        if len(wrong2):
            for stage, group in wrong2.groupby(COL_FAIL_STAGE, dropna=False):
                print(f"  {str(stage):<25} {len(group)}건")
                summary["by_failure_stage"][str(stage)] = int(len(group))
        else:
            print("  오류 없음")

    # 단계별 평균 소요 시간
    _TIMING_COLS = [
        ("시간_엔티티(s)",  "엔티티 추출"),
        ("시간_스키마(s)",  "스키마 검색"),
        ("시간_링킹(s)",    "스키마 링킹"),
        ("시간_재작성(s)",  "질의 재작성"),
        ("시간_SQL생성(s)", "SQL 생성   "),
        ("시간_DB실행(s)",  "DB 실행    "),
        ("시간_합계(s)",    "전체 합계  "),
    ]
    timing_rows = [(label, col) for col, label in _TIMING_COLS if col in df.columns]
    if timing_rows:
        print("\n" + "=" * 50)
        print("  단계별 평균 소요 시간")
        print("=" * 50)
        for label, col in timing_rows:
            series = pd.to_numeric(df[col], errors="coerce").dropna()
            if len(series):
                avg = round(float(series.mean()), 3)
                mn  = round(float(series.min()),  3)
                mx  = round(float(series.max()),  3)
                print(f"  {label}  avg={avg:.2f}s  min={mn:.2f}s  max={mx:.2f}s  (n={len(series)})")
                summary["timings_sec"][label.strip()] = {"avg": avg, "min": mn, "max": mx, "n": int(len(series))}
            else:
                print(f"  {label}  데이터 없음")

    if json_path is not None:
        json_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


# ── 기존 분석 결과 요약만 출력 ─────────────────────────────
def run_summary_only(input_path: Path) -> None:
    """이미 분석된 xlsx(정답여부 컬럼 포함)에서 요약만 출력하고 JSON도 저장."""
    df = pd.read_excel(input_path)
    if COL_CORRECT not in df.columns:
        print(f"[ERROR] '{COL_CORRECT}' 컬럼이 없습니다. 분석이 완료된 파일을 지정하세요.", file=sys.stderr)
        sys.exit(1)
    diff_col = _resolve_column(df, COL_DIFF)
    json_path = Path(input_path).with_suffix("").with_name(Path(input_path).stem + "_summary.json")
    _print_summary(df, diff_col=diff_col, json_path=json_path)
    print(f"요약 JSON 저장: {json_path}")


# ── CLI ─────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",    required=True, help="평가 결과 xlsx 경로")
    parser.add_argument("--model",    default=os.getenv("OPENAI_MODEL", "gpt-4o"), help="OpenAI 모델명")
    parser.add_argument("--timeout",  type=int, default=int(os.getenv("OPENAI_TIMEOUT_SEC", "60")))
    parser.add_argument("--start",    type=int, default=0, help="시작 행 인덱스 (0-based)")
    parser.add_argument("--pipeline", choices=["ir", "simple"], default="simple",
                        help="파이프라인 종류 (ir → results/v10/, simple → results/simple/v1/)")
    parser.add_argument("--summary-only", action="store_true",
                        help="이미 분석된 xlsx에서 난이도별 정답률 요약만 출력 (OpenAI 호출 없음)")
    args = parser.parse_args()

    if args.summary_only:
        run_summary_only(Path(args.input))
        return

    if not os.getenv("OPENAI_API_KEY"):
        print("[ERROR] OPENAI_API_KEY가 설정되지 않았습니다. .env를 확인하세요.", file=sys.stderr)
        sys.exit(1)

    run_analyze(
        input_path=Path(args.input),
        model=args.model,
        timeout=args.timeout,
        start_idx=args.start,
        pipeline=args.pipeline,
    )


if __name__ == "__main__":
    main()
