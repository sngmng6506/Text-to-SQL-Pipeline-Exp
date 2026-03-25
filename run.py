"""
파이프라인 라우터

서브커맨드로 ir-use / simple 파이프라인을 선택해 실행한다.
모든 실행 파라미터는 config/ 에서 관리한다.

사용법:
  python run.py ir-use  --question "어제 발생한 알람 건수 보여줘"
  python run.py simple  --question "어제 발생한 알람 건수 보여줘"
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env", override=True)

from config.common import SCHEMA_TOP_K, SQL_MAX_TOKENS
from config.simple import SIMPLE_VLLM_BASE_URL, SIMPLE_VLLM_MODEL, SIMPLE_VLLM_TIMEOUT_SEC, SIMPLE_N


def _print_result(result: dict) -> None:
    if result.get("error"):
        print(f"\n[ERROR] {result['error']}", file=sys.stderr)
        sys.exit(1)

    print(f"\n[최종 SQL]\n{result['sql']}")

    if result.get("db_result") is not None:
        rows = result["db_result"]
        print(f"\n[실행 결과] {len(rows)}행")
        for row in rows[:10]:
            print(" ", row)

    timings = result.get("timings") or {}
    if timings.get("total"):
        print(f"\n총 소요: {timings['total']:.2f}s")


# ── ir-use ────────────────────────────────────────────────────────────────────

def _run_ir_use(args: argparse.Namespace) -> None:
    from pipelines.ir_use import run_pipeline

    model_id = os.getenv("VLLM_MODEL")
    if not model_id:
        print("[ERROR] VLLM_MODEL 환경변수가 설정되지 않았습니다. .env 확인.", file=sys.stderr)
        sys.exit(1)

    result = run_pipeline(
        args.question,
        top_k=SCHEMA_TOP_K,
        max_tokens=SQL_MAX_TOKENS,
        model_id=model_id,
    )
    _print_result(result)


# ── simple ────────────────────────────────────────────────────────────────────

def _run_simple(args: argparse.Namespace) -> None:
    from pipelines.simple import run_pipeline
    from clients.chat import VllmChatClient

    if not SIMPLE_VLLM_BASE_URL or not SIMPLE_VLLM_MODEL:
        raise EnvironmentError(
            ".env에 SIMPLE_VLLM_BASE_URL 과 SIMPLE_VLLM_MODEL 을 설정해야 합니다."
        )

    client = VllmChatClient(
        base_url=f"{SIMPLE_VLLM_BASE_URL.rstrip('/')}/v1",
        timeout_sec=SIMPLE_VLLM_TIMEOUT_SEC,
    )

    result = run_pipeline(
        args.question,
        client=client,
        model_id=SIMPLE_VLLM_MODEL,
        n_candidates=SIMPLE_N,
        top_k=SCHEMA_TOP_K,
        max_tokens=SQL_MAX_TOKENS,
    )
    _print_result(result)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="TAG Text-to-SQL 파이프라인 라우터",
    )
    sub = parser.add_subparsers(dest="pipeline", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--question", required=True, help="자연어 질문")

    sub.add_parser(
        "ir-use",
        parents=[common],
        help="IR 기반 파이프라인 (vLLM, pipelines/ir_use.py)",
    ).set_defaults(func=_run_ir_use)

    sub.add_parser(
        "simple",
        parents=[common],
        help="간소화 파이프라인 (로컬 모델, pipelines/simple.py)",
    ).set_defaults(func=_run_simple)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
