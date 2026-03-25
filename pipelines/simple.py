"""
Simple 파이프라인 (IR 생략, vLLM 기반)

흐름:
  1. 스키마 검색 (임베딩, schema/search.py 재사용)
  2. SQL 후보 N개 생성 (스키마 rotation × N, ThreadPoolExecutor 병렬)
  3. 구문 검증 + AST 다수결 선택
  4. 대표 SQL DB 실행 (오류 시 재시도)

진입점: python run.py simple --question "..."
"""

from __future__ import annotations

import re
import time
from typing import Any

from jinja2 import Environment, FileSystemLoader

from clients.chat import VllmChatClient
from services.sql_ast import sql_ast_key, ast_majority_vote
from config import (
    SCHEMA_TOP_K,
    SCHEMA_CANDIDATES,
    SQL_MAX_TOKENS,
    SIMPLE_GENERATE_SQL_TEMPLATE_PATH,
    SIMPLE_SQL_DB_MAX_RETRIES,
    SIMPLE_FEW_SHOT_TOP_K,
    SIMPLE_MQS_POOL_NPZ_PATH,
    SIMPLE_VALUE_HINT_TIMEOUT_MS,
    SIMPLE_VALUE_HINT_DISTINCT_LIMIT,
    SIMPLE_SQL_SELECT_METHOD,
    SIMPLE_SQL_JUDGE_TOP_K,
    SIMPLE_SQL_JUDGE_MODEL,
    SIMPLE_SQL_JUDGE_BASE_URL,
    SIMPLE_SQL_JUDGE_TIMEOUT_SEC,
    SIMPLE_SQL_JUDGE_TEMPERATURE,
    SIMPLE_SQL_JUDGE_MAX_TOKENS,
    SIMPLE_SQL_JUDGE_EXEC_PREVIEW,
    SIMPLE_SQL_JUDGE_EXEC_PREVIEW_ROWS,
    SIMPLE_TRANSLATE_ENABLED,
    SIMPLE_TRANSLATE_MAX_TOKENS,
    SIMPLE_MQS_POOL_EN_NPZ_PATH,
    SCHEMA_JSON_PATH,
)
from db.client import execute_sql
from schema.search import query_schema
from schema.validator import find_sql_errors
from services.exec_preview import fetch_exec_preview


# ── SQL 후처리 ───────────────────────────────────────────────────────────────

_SQL_FENCE_RE = re.compile(r"^```[a-zA-Z]*\n?|\n?```$", re.MULTILINE)
_SELECT_RE    = re.compile(r"\b(SELECT|WITH)\b", re.IGNORECASE)



def _to_v1_base_url(url: str) -> str:
    u = (url or "").strip().rstrip("/")
    return u if u.endswith("/v1") else f"{u}/v1"


def _strip_sql(text: str) -> str:
    """코드 펜스 제거 후 SELECT/WITH 앞부분을 잘라낸다."""
    text = _SQL_FENCE_RE.sub("", text).strip()
    m = _SELECT_RE.search(text)
    return text[m.start():].strip() if m else text.strip()


# ── SQL 생성 프롬프트 (Jinja2 파일 로드) ──────────────────────────────────────

def _render_sql_prompt(
    question: str,
    schema_text: str,
    value_hints: str = "",
    few_shot_examples: list[dict] | None = None,
    question_en: str | None = None,
) -> str:
    """prompts/simple/<version>/generate_sql.j2 를 렌더링하여 사용자 메시지를 반환한다."""
    tpl_path = SIMPLE_GENERATE_SQL_TEMPLATE_PATH
    env = Environment(
        loader=FileSystemLoader(str(tpl_path.parent)),
        keep_trailing_newline=True,
    )
    tpl = env.get_template(tpl_path.name)
    return tpl.render(
        question=question,
        question_en=question_en or "",
        schema_candidates=schema_text,
        value_hints=value_hints,
        few_shot_examples=few_shot_examples or [],
    )


def _build_sql_prompt(
    question: str,
    schema_text: str,
    value_hints: str = "",
    few_shot_examples: list[dict] | None = None,
    question_en: str | None = None,
) -> list[dict[str, str]]:
    """질문 + 스키마 → Jinja2 렌더링된 단일 user 메시지 프롬프트."""
    return [{"role": "user", "content": _render_sql_prompt(
        question, schema_text,
        value_hints=value_hints,
        few_shot_examples=few_shot_examples,
        question_en=question_en,
    )}]


# ── Value Hints 조회 ──────────────────────────────────────────────────────────

def _fetch_value_hints(schema_results: list[dict]) -> str:
    """
    스키마 검색 결과로부터 text/varchar/enum 컬럼의 실제 DB 값을 조회해
    프롬프트용 문자열로 반환한다. 실패 시 빈 문자열 반환.
    """
    import json as _json
    from schema.value_hints import SchemaValueHintService

    TEXT_TYPES = {"character varying", "varchar", "text", "char", "bpchar"}
    TIME_TYPES = {
        "timestamp", "date", "time",
        "timestamp without time zone", "timestamp with time zone",
    }
    # 표준 타입이 아닌 것 (enum, user-defined type 등) — 허용값 조회 대상
    STANDARD_TYPES = TEXT_TYPES | TIME_TYPES | {
        "integer", "bigint", "smallint", "int4", "int8", "int2",
        "numeric", "decimal", "float4", "float8", "real", "double precision",
        "boolean", "bool", "serial", "bigserial", "bytea", "json", "jsonb",
        "uuid", "oid", "array",
    }
    SKIP_RE = re.compile(r"(^id$|_id$|_seq$|_pk$)", re.IGNORECASE)

    # schema JSON 로드 → 검색된 테이블의 컬럼 타입 확인
    try:
        with open(str(SCHEMA_JSON_PATH), encoding="utf-8") as _f:
            schema_data = _json.load(_f)
    except Exception:
        return ""

    table_set = {r["table"] for r in schema_results}
    column_refs: list[str] = []
    temporal_refs: list[str] = []

    for entry in schema_data:
        tbl = entry.get("table", "")
        if tbl not in table_set:
            continue
        for col in entry.get("columns", []):
            col_name = col.get("name", "")
            col_type = col.get("type", "").lower()
            if not col_name or SKIP_RE.search(col_name):
                continue
            ref = f"{tbl}.{col_name}"
            if any(t in col_type for t in TEXT_TYPES):
                column_refs.append(ref)
            elif any(t in col_type for t in TIME_TYPES):
                temporal_refs.append(ref)
            elif not any(t in col_type for t in STANDARD_TYPES):
                # 표준 타입에 속하지 않음 → enum / user-defined 타입으로 판단
                column_refs.append(ref)

    if not column_refs and not temporal_refs:
        return ""

    try:
        svc = SchemaValueHintService()
        hints = svc.fetch_column_hints(
            column_refs=column_refs + temporal_refs,
            temporal_column_refs=temporal_refs,
            distinct_limit=SIMPLE_VALUE_HINT_DISTINCT_LIMIT,
            statement_timeout_ms=SIMPLE_VALUE_HINT_TIMEOUT_MS,
        )
    except Exception:
        return ""

    if not hints:
        return ""

    lines: list[str] = []
    for col_ref, meta in hints.items():
        allowed = meta.get("allowed_values", [])
        sample  = meta.get("sample_values", [])
        mn, mx  = meta.get("min"), meta.get("max")
        if allowed:
            lines.append(f"{col_ref}: {allowed}")
        elif mn is not None or mx is not None:
            lines.append(f"{col_ref}: min={mn}, max={mx}")
        elif sample:
            lines.append(f"{col_ref} (sample): {sample}")

    return "\n".join(lines)


# ── Few-Shot 검색 ─────────────────────────────────────────────────────────────

def _fetch_few_shots(question: str) -> list[dict]:
    """MQS pool에서 유사 질문-SQL 예시를 검색한다. pool 파일 없으면 []."""
    if SIMPLE_FEW_SHOT_TOP_K <= 0:
        return []
    if not SIMPLE_MQS_POOL_NPZ_PATH.exists():
        return []
    try:
        from services.few_shot import retrieve_few_shots
        return retrieve_few_shots(
            rewritten=question,
            npz_path=str(SIMPLE_MQS_POOL_NPZ_PATH),
            top_k=SIMPLE_FEW_SHOT_TOP_K,
        )
    except Exception:
        return []


# ── AST 다수결은 schema.sql_ast 모듈로 위임 ──────────────────────────────────
# sql_ast_key, ast_majority_vote 는 상단 import에서 로드


# ── 메인 파이프라인 ───────────────────────────────────────────────────────────

def run_pipeline(
    question: str,
    *,
    client: VllmChatClient,
    model_id: str | None = None,
    n_candidates: int = 3,
    top_k: int | None = None,
    max_tokens: int | None = None,
    skip_exec: bool = False,
) -> dict[str, Any]:
    """
    간소화 파이프라인 실행.

    Returns
    -------
    {
      "sql"        : 최종 선택 SQL,
      "db_result"  : DB 실행 결과 (skip_exec=True이면 None),
      "exec_error" : 실행 오류 문자열 (없으면 None),
      "candidates" : 모든 후보 메타 목록,
      "select_method": 선택 방식,
      "timings"    : 단계별 소요 시간,
      "error"      : 파이프라인 전체 오류 (없으면 None),
    }
    """
    _top_k     = top_k     if top_k     is not None else SCHEMA_TOP_K
    _max_tok   = max_tokens if max_tokens is not None else SQL_MAX_TOKENS
    _timings: dict[str, float] = {}
    _start = time.perf_counter()

    def _log(step: str, body: str = "", elapsed: float = 0.0) -> None:
        print(f"\n{'='*50}", flush=True)
        print(f"  {step}  ({elapsed:.2f}s)", flush=True)
        print(f"{'='*50}", flush=True)
        if body:
            print(body, flush=True)

    try:
        # ── STEP 1: 번역 → 스키마 검색 ──────────────────────────────────────
        print("\n[1/3] 스키마 검색 중...", flush=True)
        _t = time.perf_counter()
        question_en: str | None = None

        if SIMPLE_TRANSLATE_ENABLED and SIMPLE_SQL_JUDGE_BASE_URL:
            from services.translate import translate_to_english

            _judge_client_tr = VllmChatClient(
                base_url=_to_v1_base_url(SIMPLE_SQL_JUDGE_BASE_URL),
                timeout_sec=SIMPLE_SQL_JUDGE_TIMEOUT_SEC,
            )
            question_en = translate_to_english(
                question,
                client=_judge_client_tr,
                model_id=SIMPLE_SQL_JUDGE_MODEL or model_id,
                max_tokens=SIMPLE_TRANSLATE_MAX_TOKENS,
            )

        if question_en:
            print(f"  [translate] {question_en}", flush=True)
        elif SIMPLE_TRANSLATE_ENABLED:
            print(f"  [translate] 실패 (fallback: 한국어 풀)", flush=True)

        results = query_schema(question, top_k=_top_k)
        _timings["t1_schema"] = round(time.perf_counter() - _t, 2)

        if not results:
            return _error("스키마 후보 없음", _timings, _start)

        candidates_pool = results[: min(SCHEMA_CANDIDATES, len(results))]
        schema_text = "\n\n".join(
            (f"-- {r['description']}\n" if r.get("description") else "") + r["schema_text"]
            for r in candidates_pool
        )
        schema_summary = "\n".join(
            f"  {i}. [{r['score']:.4f}] {r['table']}"
            for i, r in enumerate(results, 1)
        )
        _log("STEP 1 · SCHEMA SEARCH", schema_summary, _timings["t1_schema"])

        # ── Value Hints 조회 ─────────────────────────────────────────────────
        _t = time.perf_counter()
        value_hints = _fetch_value_hints(candidates_pool)
        _timings["t1b_value_hints"] = round(time.perf_counter() - _t, 2)
        if value_hints:
            print(f"  [value-hints] {len(value_hints.splitlines())}개 컬럼 힌트 조회 완료 ({_timings['t1b_value_hints']:.2f}s)", flush=True)

        # ── Few-Shot 검색 (영어 풀 우선, 없으면 한국어 풀 fallback) ─────────
        _t = time.perf_counter()
        few_shots: list[dict] = []
        _few_shot_lang = "ko"

        if question_en and SIMPLE_MQS_POOL_EN_NPZ_PATH.exists():
            try:
                from services.few_shot import retrieve_few_shots_en
                few_shots = retrieve_few_shots_en(
                    question_en=question_en,
                    npz_path=str(SIMPLE_MQS_POOL_EN_NPZ_PATH),
                    top_k=SIMPLE_FEW_SHOT_TOP_K,
                )
                _few_shot_lang = "en"
            except Exception:
                few_shots = []

        if not few_shots:
            few_shots = _fetch_few_shots(question)
            _few_shot_lang = "ko"

        _timings["t1c_few_shot"] = round(time.perf_counter() - _t, 2)
        if few_shots:
            print(f"  [few-shot/{_few_shot_lang}] {len(few_shots)}개 예시 검색 완료 ({_timings['t1c_few_shot']:.2f}s)", flush=True)
            for _i, _ex in enumerate(few_shots, 1):
                _q_display = _ex.get("question_en") or _ex.get("question_ko", "")
                print(f"    [{_i}] Q: {_q_display}", flush=True)
                print(f"         SQL: {_ex.get('sql', '').strip()}", flush=True)

        # ── STEP 2: 스키마 Rotation × N 병렬 SQL 생성 ───────────────────────
        print(f"\n[2/3] SQL 후보 {n_candidates}개 생성 중 (Rotation ×{n_candidates})...", flush=True)
        _t = time.perf_counter()

        # rotation i: candidates_pool[i:] + candidates_pool[:i] (순환)
        _n_rot = max(n_candidates, 1)
        _pool_len = len(candidates_pool)

        def _schema_text_for_rotation(rot_i: int) -> str:
            rot = candidates_pool[rot_i % _pool_len:] + candidates_pool[:rot_i % _pool_len]
            return "\n\n".join(
                (f"-- {r['description']}\n" if r.get("description") else "") + r["schema_text"]
                for r in rot
            )

        def _generate_one(rot_i: int) -> tuple[str, list[dict]]:
            """rotation rot_i로 SQL 1개 생성 후 (raw_sql, base_messages) 반환."""
            rot_schema = _schema_text_for_rotation(rot_i)
            rot_messages = _build_sql_prompt(
                question, rot_schema,
                value_hints=value_hints,
                few_shot_examples=few_shots,
                question_en=question_en,
            )
            temp = 0.0 if _n_rot == 1 else 0.5
            raw = client.chat_completions(
                messages=rot_messages,
                model=model_id,
                temperature=temp,
                max_tokens=_max_tok,
            )
            return raw, rot_messages

        if _n_rot == 1:
            _results = [_generate_one(0)]
        else:
            from concurrent.futures import ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=_n_rot) as _pool:
                _futures = [_pool.submit(_generate_one, i) for i in range(_n_rot)]
                _results = [f.result() for f in _futures]

        raw_sqls        = [r for r, _ in _results]
        _rot_messages   = [m for _, m in _results]  # rotation별 베이스 메시지 (재시도용)

        _timings["t2_generate"] = round(time.perf_counter() - _t, 2)

        # ── STEP 3: 구문 검증 → AST 다수결 → 대표 SQL DB 실행 (재시도) ────────
        print("\n[3/3] 검증 및 후보 선택 중...", flush=True)
        _t = time.perf_counter()

        cand_meta: list[dict[str, Any]] = []

        # ── 3-A: 구문 검사 ───────────────────────────────────────────────────
        valid_sqls: list[str] = []
        # rotation별 base messages 보존 (재시도 시 해당 rotation의 스키마 컨텍스트 유지)
        _valid_sql_to_messages: dict[str, list[dict]] = {}

        for idx, raw in enumerate(raw_sqls):
            sql = _strip_sql(raw)
            meta: dict[str, Any] = {
                "idx": idx, "sql": sql,
                "syntax_errors": [], "exec_error": None,
                "result": None, "selected": False,
            }
            syntax_errors = find_sql_errors(sql)
            if syntax_errors:
                meta["syntax_errors"] = syntax_errors
                print(f"  [Rotation-{idx}] 구문 오류: {syntax_errors[0][:60]}", flush=True)
            else:
                valid_sqls.append(sql)
                # 같은 AST 키가 이미 있으면 첫 번째 rotation 메시지 유지
                _key = sql_ast_key(sql)
                if _key not in _valid_sql_to_messages:
                    _valid_sql_to_messages[_key] = _rot_messages[idx]
            cand_meta.append(meta)

        _timings["t3_ast"] = round(time.perf_counter() - _t, 2)

        if not valid_sqls:
            _timings["total"] = round(time.perf_counter() - _start, 2)
            return {
                "sql": "", "db_result": None,
                "exec_error": "모든 후보가 구문 오류",
                "candidates": cand_meta, "select_method": "none",
                "timings": _timings, "error": "유효한 SQL 후보 없음",
            }

        # ── 3-B: AST 다수결로 실행할 SQL 순위 결정 ──────────────────────────
        _best_sql, select_method, ast_ranking = ast_majority_vote(valid_sqls)
        _syntax_fail = len(raw_sqls) - len(valid_sqls)
        print(
            f"  구문 통과 {len(valid_sqls)}/{len(raw_sqls)}개"
            + (f" ({_syntax_fail}개 오류)" if _syntax_fail else "")
            + f"  →  AST 그룹 {len(ast_ranking)}개",
            flush=True,
        )
        for _gi, (_gsql, _gvotes) in enumerate(ast_ranking, 1):
            print(f"    [그룹{_gi} | {_gvotes}표] {_gsql}", flush=True)

        # ── 3-B2: 필요 시 LLM Judge로 AST 그룹 대표 SQL 재정렬 ───────────────
        if SIMPLE_SQL_SELECT_METHOD == "llm_judge" and len(ast_ranking) > 1:
            _t_judge = time.perf_counter()
            _judge_groups = ast_ranking[: max(1, SIMPLE_SQL_JUDGE_TOP_K)]
            _rest_groups = ast_ranking[len(_judge_groups):]
            try:
                from services.sql_judge import rerank_sql_groups

                _judge_client = client
                if SIMPLE_SQL_JUDGE_BASE_URL:
                    _judge_client = VllmChatClient(
                        base_url=_to_v1_base_url(SIMPLE_SQL_JUDGE_BASE_URL),
                        timeout_sec=SIMPLE_SQL_JUDGE_TIMEOUT_SEC,
                    )
                _judge_model = SIMPLE_SQL_JUDGE_MODEL or model_id

                # Judge 전 각 후보를 LIMIT N으로 실행해 결과 미리보기 수집
                _exec_previews: list[str | None] = []
                if SIMPLE_SQL_JUDGE_EXEC_PREVIEW:
                    for _gsql, _ in _judge_groups:
                        _exec_previews.append(
                            fetch_exec_preview(_gsql, SIMPLE_SQL_JUDGE_EXEC_PREVIEW_ROWS)
                        )

                _reranked, _judge_meta, _judge_raw = rerank_sql_groups(
                    question=question,
                    schema_text=schema_text,
                    groups=_judge_groups,
                    client=_judge_client,
                    model_id=_judge_model,
                    temperature=SIMPLE_SQL_JUDGE_TEMPERATURE,
                    max_tokens=SIMPLE_SQL_JUDGE_MAX_TOKENS,
                    exec_previews=_exec_previews or None,
                )
                ast_ranking = _reranked + _rest_groups
                select_method = "llm_judge (AST groups)"
                _judge_elapsed = time.perf_counter() - _t_judge
                print(f"  [Judge] AST 대표 그룹 {len(_judge_groups)}개 rerank 완료 ({_judge_elapsed:.2f}s)", flush=True)
                _used_fallback = any(_m.get("score_source") != "judge" for _m in _judge_meta)
                if _used_fallback:
                    print("  [Judge WARN] score 파싱 실패, 득표순 fallback", flush=True)
                for _m in _judge_meta:
                    _tag = "선택" if _m.get("selected") else "후보"
                    _src = _m.get("score_source", "unknown")
                    print(
                        f"    [{_tag} {int(_m.get('idx', 0)) + 1}] score={_m.get('score')} ({_src}) votes={_m.get('votes')}",
                        flush=True,
                    )
                    if _m.get("comment"):
                        print(f"      → {_m['comment']}", flush=True)
            except Exception as _e:
                print(f"  [Judge WARN] Judge rerank 실패, AST 득표순 유지: {_e}", flush=True)
                # ast_ranking 유지 (득표순)
                select_method = f"{select_method} (judge_fallback)"

        # 득표 많은 것부터 시도 순서 결정
        _candidates_to_try = [sql for sql, _ in ast_ranking]

        if skip_exec:
            selected_sql = _candidates_to_try[0]
            selected_result: list[dict] = []
            for m in cand_meta:
                if m["sql"] == selected_sql:
                    m["selected"] = True
                    break
            select_method = f"{select_method} (skip_exec)"
            _timings["t3_validate"] = round(time.perf_counter() - _t, 2)
        else:
            # ── 3-C: 득표 순으로 DB 실행 시도 (재시도 포함) ─────────────────
            selected_sql = ""
            selected_result = []
            exec_error_last = ""

            for _rank, _candidate_sql in enumerate(_candidates_to_try):
                _current_sql = _candidate_sql
                _current_raw = _current_sql
                # 해당 후보를 생성한 rotation의 베이스 메시지 사용
                _base = _valid_sql_to_messages.get(sql_ast_key(_candidate_sql), _rot_messages[0])
                _correction_messages = list(_base)

                for _try in range(SIMPLE_SQL_DB_MAX_RETRIES + 1):
                    try:
                        rows = execute_sql(_current_sql)
                        selected_sql = _current_sql
                        selected_result = rows
                        # 메타 업데이트
                        for m in cand_meta:
                            if sql_ast_key(m["sql"]) == sql_ast_key(_current_sql) and not m["syntax_errors"]:
                                m["result"] = rows
                                m["selected"] = True
                                break
                        print(f"  [AST그룹 {_rank+1}] 실행 성공  {len(rows)}행", flush=True)
                        exec_error_last = ""
                        break
                    except Exception as e:
                        exec_error_last = str(e)
                        if _try < SIMPLE_SQL_DB_MAX_RETRIES:
                            print(f"  [AST그룹 {_rank+1}] DB 오류 → 재시도 ({_try+1}/{SIMPLE_SQL_DB_MAX_RETRIES}): {str(e)[:60]}", flush=True)
                            _correction_messages = _correction_messages + [
                                {"role": "assistant", "content": _current_raw},
                                {"role": "user", "content": (
                                    f"SQL 실행 시 DB 오류가 발생했습니다:\n{e}\n\n"
                                    f"오류를 수정한 올바른 PostgreSQL SQL을 다시 출력하세요."
                                )},
                            ]
                            _current_raw = client.chat_completions(
                                messages=_correction_messages,
                                model=model_id,
                                temperature=0.0,
                                max_tokens=_max_tok,
                            )
                            _current_sql = _strip_sql(_current_raw)
                        else:
                            print(f"  [AST그룹 {_rank+1}] 실행 실패 — 최대 재시도 도달", flush=True)

                if selected_sql:
                    break

            if not selected_sql:
                _timings["total"] = round(time.perf_counter() - _start, 2)
                return {
                    "sql": "", "db_result": None,
                    "exec_error": exec_error_last,
                    "candidates": cand_meta, "select_method": "none",
                    "timings": _timings, "error": "모든 후보 DB 실행 실패",
                }

            _timings["t3_validate"] = round(time.perf_counter() - _t, 2)

        _timings["total"] = round(time.perf_counter() - _start, 2)

        _log(
            "STEP 3 · 선택 결과",
            f"방식: {select_method}\n\n{selected_sql}",
            _timings["t3_validate"],
        )
        print(f"\n{'='*50}", flush=True)
        print(f"  파이프라인 완료  총 소요: {_timings['total']:.2f}s", flush=True)
        print(f"{'='*50}", flush=True)

        return {
            "sql": selected_sql,
            "db_result": selected_result if not skip_exec else None,
            "exec_error": None,
            "candidates": cand_meta,
            "select_method": select_method,
            "timings": _timings,
            "error": None,
        }

    except Exception as e:
        _timings["total"] = round(time.perf_counter() - _start, 2)
        return _error(str(e), _timings, _start)


def _error(msg: str, timings: dict, start: float) -> dict[str, Any]:
    timings["total"] = round(time.perf_counter() - start, 2)
    return {
        "sql": "", "db_result": None, "exec_error": None,
        "candidates": [], "select_method": "none",
        "timings": timings, "error": msg,
    }


