"""
IR-use 파이프라인 (vLLM 기반)

흐름:
  1. 엔티티 추출 (LLM)           — steps/extract_entity.py
  2. 스키마 후보 검색 (TEI 임베딩) — schema/search.py
  3. 스키마 링킹 (LLM)           — steps/schema_linking.py
  4. 질의 재작성 / IR 생성 (LLM) — steps/rewrite_query.py
  5. SQL 생성 (LLM)              — steps/generate_sql.py
  6. DB 실행 (PostgreSQL)        — db/client.py

진입점: python run.py ir-use --question "..."
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from typing import Any

from clients.chat import default_vllm_client
from config import (
    REWRITE_IR_MODE as _REWRITE_IR_MODE,
    SCHEMA_TOP_K,
    SCHEMA_CANDIDATES,
    ENTITY_MAX_TOKENS,
    SCHEMA_LINKING_MAX_TOKENS,
    SCHEMA_LINKING_MAX_RETRIES,
    REWRITE_MAX_TOKENS,
    SQL_MAX_TOKENS,
    MQS_POOL_NPZ_PATH,
    FEW_SHOT_TOP_K,
    IR_CANDIDATES_N,
    IR_CANDIDATES_TEMP,
    IR_GUIDED_JSON,
    SCHEMA_LINKING_GUIDED_JSON,
    GUIDED_DECODING_BACKEND,
    IR_SELECT_METHOD,
    IR_JUDGE_COT,
    SL_CANDIDATES_N,
    VALUE_HINT_DISTINCT_LIMIT,
    VALUE_HINT_TIMEOUT_MS,
    ENTITY_EXTRACTION_TEMPERATURE,
    SCHEMA_LINKING_TEMPERATURE,
    SCHEMA_CORRECTION_TEMPERATURE,
    REWRITE_SINGLE_TEMPERATURE,
    SQL_GENERATION_TEMPERATURE,
    SQL_DB_MAX_RETRIES,
)
from db.client import execute_sql
from schema.search import query_schema
from schema.value_hints import SchemaValueHintService
from ir.selector import majority_vote as _ir_majority_vote
from ir.llm_judge import IRLLMJudgeService
from ir.schema import IR_JSON_SCHEMA, SCHEMA_LINKING_JSON_SCHEMA
from steps.extract_entity import render_prompt as render_entity_prompt
from steps.schema_linking import entity_extraction_enabled
from steps.schema_linking import is_enabled as schema_linking_enabled
from steps.schema_linking import render_prompt as render_schema_linking_prompt
from schema.validator import find_errors, find_ir_errors, find_sql_errors, get_schema_lookup
from steps.rewrite_query import render_prompt as render_rewrite_prompt
from steps.generate_sql import render_prompt as render_sql_prompt
from services.few_shot import retrieve_few_shots



_JSON_OBJ_RE = re.compile(r"\{[\s\S]*\}")



def _parse_json_object(text: str) -> dict[str, Any]:
    """LLM 응답에서 첫 JSON 객체를 파싱. 앞뒤 텍스트가 섞여도 동작."""
    text = (text or "").strip()
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        m = _JSON_OBJ_RE.search(text)
        if not m:
            return {}
        try:
            obj = json.loads(m.group(0))
            return obj if isinstance(obj, dict) else {}
        except Exception:
            return {}


def _entities_to_search_query(entities: dict[str, Any]) -> str:
    """엔티티 추출 결과에서 스키마 임베딩 검색 키워드를 추출.

    테이블/컬럼 검색에 유효한 명사형 엔티티만 사용한다.
    - v3: entity_phrases 우선, 없으면 모든 list 값 수집 (v1/v2 하위 호환)
    - time_phrases, aggregation_intent, filter_conditions 는 IR에 노이즈이므로 제외
    """
    # v3 포맷: entity_phrases만 사용
    if "entity_phrases" in entities:
        phrases = [p for p in entities["entity_phrases"] if isinstance(p, str)]
        if phrases:
            return " ".join(phrases).strip()

    # v1/v2 하위 호환: 모든 list/dict 값 수집
    parts: list[str] = []
    for v in entities.values():
        if isinstance(v, list):
            for item in v[:5]:
                if isinstance(item, str):
                    parts.append(item)
        elif isinstance(v, dict):
            for cols in v.values():
                if isinstance(cols, list):
                    parts.extend(c for c in cols[:2] if isinstance(c, str))
    return " ".join(parts).strip()


def _strip_sql_fences(text: str) -> str:
    """LLM이 ```sql ... ``` 형태로 감싸서 반환할 때 코드 펜스를 제거."""
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        # 첫 줄(```sql 또는 ```) 과 마지막 줄(```) 제거
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        text = "\n".join(inner).strip()
    return text


def _extract_rewritten_question(raw: str) -> str:
    """
    rewrite 단계 LLM 응답에서 최종 결과를 추출.

    - v9 IR 모드: JSON 객체를 그대로 반환 (generate_sql.j2의 rewritten_question 변수로 전달됨)
    - 구버전: 태그 형태(<table.col = '...'>) 한 줄 추출
    """
    text = (raw or "").strip()

    if _REWRITE_IR_MODE:
        # JSON IR: 코드펜스만 제거하고 JSON 텍스트 그대로 반환
        if text.startswith("```"):
            lines = text.splitlines()
            inner = []
            in_block = False
            for ln in lines:
                if ln.startswith("```") and not in_block:
                    in_block = True
                    continue
                if ln.startswith("```") and in_block:
                    break
                if in_block:
                    inner.append(ln)
            text = "\n".join(inner).strip()
        return text

    # 구버전: 'Rewritten Question:' 접두 제거
    if "Rewritten Question:" in text:
        text = text.split("Rewritten Question:", 1)[1].strip()

    # <table.column = '...'> 패턴이 있는 마지막 줄 우선
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in reversed(lines):
        if "<" in ln and ">" in ln and "=" in ln:
            return ln
    return lines[0] if lines else text


def _normalize_for_dedupe(value: Any) -> str:
    """중복 제거용 정규화 문자열을 반환한다."""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value or "").strip()


def run_pipeline(
    question: str,
    *,
    top_k: int | None = None,
    max_tokens: int | None = None,
    model_id: str | None = None,
    client=None,
    skip_exec: bool = False,
) -> dict[str, Any]:
    """
    단일 자연어 질문을 받아 파이프라인을 실행하고 결과를 반환.

    Returns:
        {
            "entities": dict,
            "schema_linking_json": str,       # 스키마 링킹 결과 JSON 문자열
            "rewritten": str,                 # 재작성 질문 또는 IR JSON 문자열
            "sql": str,
            "db_result": list[dict] | None,  # DB 실행 결과 (skip_exec=True면 None)
            "exec_error": str | None,         # DB 실행 오류
            "error": str | None,              # 파이프라인 전체 실패 시
        }
    """
    if client is None:
        client = default_vllm_client()
    if model_id is None:
        model_id = os.getenv("VLLM_MODEL")

    # 파이프라인 파라미터 — 인자 우선, 없으면 config 기본값 사용
    _top_k          = top_k if top_k is not None else SCHEMA_TOP_K
    _n_candidates   = SCHEMA_CANDIDATES
    _entity_tokens  = ENTITY_MAX_TOKENS
    _linking_tokens = SCHEMA_LINKING_MAX_TOKENS
    _rewrite_tokens = REWRITE_MAX_TOKENS
    _sql_tokens     = SQL_MAX_TOKENS
    _use_schema_linking    = schema_linking_enabled()
    _use_entity_extraction = entity_extraction_enabled()
    # v4: 엔티티 추출 없음 → 단계 수 감소
    _total_steps = (6 if _use_schema_linking else 5) if _use_entity_extraction else (5 if _use_schema_linking else 4)
    if max_tokens is not None:
        _entity_tokens  = min(_entity_tokens,  max_tokens)
        _linking_tokens = min(_linking_tokens,  max_tokens)
        _rewrite_tokens = min(_rewrite_tokens, max_tokens)
        _sql_tokens     = min(_sql_tokens,     max_tokens)

    _pipeline_start = time.perf_counter()

    def _log(step: str, header: str, body: str = "", elapsed: float = 0.0) -> None:
        print(f"\n{'='*50}", flush=True)
        print(f"  {step} · {header}  ({elapsed:.2f}s)", flush=True)
        print(f"{'='*50}", flush=True)
        if body:
            print(body, flush=True)

    def _dedupe_ir_raw_candidates(raw_candidates: list[str]) -> tuple[list[str], int]:
        """완전 동일한 IR 후보를 제거하고 (deduped, removed_count)를 반환한다."""
        deduped: list[str] = []
        seen: set[str] = set()
        removed = 0
        for raw in raw_candidates:
            ir_text = _extract_rewritten_question(raw)
            ir = _parse_json_object(ir_text) if _REWRITE_IR_MODE else {}
            key = _normalize_for_dedupe(ir if ir else ir_text)
            if key in seen:
                removed += 1
                continue
            seen.add(key)
            deduped.append(raw)
        return deduped, removed

    def _dedupe_pair_candidates(pairs: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
        """완전 동일한 (SL, IR) 쌍만 제거하고 idx를 재부여한다."""
        deduped: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        removed = 0
        for pair in pairs:
            key = (
                _normalize_for_dedupe(pair.get("sl") or pair.get("sl_json", "{}")),
                _normalize_for_dedupe(pair.get("ir") or pair.get("ir_raw", "")),
            )
            if key in seen:
                removed += 1
                continue
            seen.add(key)
            deduped.append(dict(pair))
        for i, pair in enumerate(deduped):
            pair["idx"] = i
        return deduped, removed

    def _brief_validator_error(message: str) -> str:
        """validator 에러 메시지에서 첫 줄만 추출해 로그를 짧게 유지."""
        return str(message or "").splitlines()[0].strip()

    def _filter_invalid_ir_raw_candidates(
        raw_candidates: list[str],
        sl_result: dict[str, Any] | None = None,
    ) -> tuple[list[str], list[tuple[int, list[str]]]]:
        """DB schema에 없는 테이블/컬럼을 참조하는 IR 후보를 제거."""
        lookup = get_schema_lookup()
        valid: list[str] = []
        dropped: list[tuple[int, list[str]]] = []
        for i, raw in enumerate(raw_candidates):
            ir_text = _extract_rewritten_question(raw)
            ir = _parse_json_object(ir_text) if _REWRITE_IR_MODE else {}
            errors = find_ir_errors(ir, lookup, sl_result=sl_result) if ir else ["IR JSON 파싱 실패"]
            if errors:
                dropped.append((i, errors))
                continue
            valid.append(raw)
        return valid, dropped

    def _filter_invalid_ir_pairs(
        pairs: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[tuple[int, int, list[str]]]]:
        """DB schema에 없는 테이블/컬럼을 참조하는 (SL, IR) 후보를 제거."""
        lookup = get_schema_lookup()
        valid: list[dict[str, Any]] = []
        dropped: list[tuple[int, int, list[str]]] = []
        for pair in pairs:
            ir = pair.get("ir") or {}
            errors = find_ir_errors(ir, lookup, sl_result=pair.get("sl")) if ir else ["IR JSON 파싱 실패"]
            if errors:
                dropped.append((pair.get("sl_idx", 0), pair.get("ir_idx", 0), errors))
                continue
            valid.append(pair)
        for i, pair in enumerate(valid):
            pair["idx"] = i
        return valid, dropped

    _timings: dict[str, float] = {}
    _value_hint_service = SchemaValueHintService()
    _ir_reranker = None
    if IR_SELECT_METHOD == "cross_encoder":
        from ir.reranker import IRRerankerService
        from config import RERANKER_MODEL_PATH
        _ir_reranker = IRRerankerService(RERANKER_MODEL_PATH)
    _ir_judge = IRLLMJudgeService(client, model_id, use_cot=IR_JUDGE_COT) if IR_SELECT_METHOD == "llm_judge" else None

    try:
        schema_linking_json = "{}"
        value_hints_json = "{}"
        entity_json = "{}"
        entities: dict = {}
        _step = 0

        # 1. 엔티티 추출 (v4는 extract_entity.j2 없으므로 스킵)
        if _use_entity_extraction:
            _step += 1
            print(f"\n[{_step}/{_total_steps}] 엔티티 추출 중...", flush=True)
            _t = time.perf_counter()
            content = client.chat_completions(
                messages=[{"role": "user", "content": render_entity_prompt(question)}],
                model=model_id,
                temperature=ENTITY_EXTRACTION_TEMPERATURE,
                max_tokens=_entity_tokens,
            )
            entities = _parse_json_object(content)
            entity_json = json.dumps(entities, ensure_ascii=False, indent=2)
            _timings["t1_entity"] = round(time.perf_counter() - _t, 2)
            if not entities:
                print("[WARN] 엔티티 추출 결과 없음 — 원본 질문으로 스키마 검색", file=sys.stderr, flush=True)
            _log("STEP 1", "ENTITY", entity_json, _timings["t1_entity"])

        # IR 쿼리: v3는 entity_phrases, v4는 원본 질문 사용
        schema_query = (_entities_to_search_query(entities) or question) if _use_entity_extraction else question

        # 2. 스키마 후보 검색
        _step += 1
        print(f"\n[{_step}/{_total_steps}] 스키마 검색 중...", flush=True)
        _t = time.perf_counter()
        results = query_schema(schema_query, top_k=_top_k)
        _timings["t2_schema"] = round(time.perf_counter() - _t, 2)
        if not results:
            return {"entities": entities, "rewritten": "", "sql": "", "schema_results": [], "db_result": None, "exec_error": None, "timings": _timings, "error": "스키마 후보 없음"}
        schema_lines = "\n".join(
            f"  {i}. [{r['score']:.4f}] {r['table']}"
            + (f"\n     └ {r['description']}" if r.get("description") else "")
            for i, r in enumerate(results, 1)
        )
        _log("STEP 2", f"SCHEMA SEARCH (top {_top_k})", schema_lines, _timings["t2_schema"])

        candidates = results[: min(_n_candidates, len(results))]
        schema_candidates = "\n\n".join([
            (f"-- {r['description']}\n" if r.get("description") else "") + r["schema_text"]
            for r in candidates
        ])

        # ── SL + IR 생성 경로 분기 ─────────────────────────────────────────────
        _use_pair_mode = (
            _use_schema_linking
            and _REWRITE_IR_MODE
            and SL_CANDIDATES_N >= 2
            and IR_SELECT_METHOD == "llm_judge"
            and _ir_judge is not None
        )

        if _use_pair_mode:
            # ══════════════════════════════════════════════════════════════════
            # Rotation Pair 모드: 회전된 SL 후보들을 병렬 생성하고,
            # 각 SL마다 IR 후보를 생성한 뒤 Pair Judge로 선택
            # ══════════════════════════════════════════════════════════════════
            from concurrent.futures import ThreadPoolExecutor
            from ir.selector import _parse_ir

            _step += 1
            print(f"\n[{_step}/{_total_steps}] 스키마 링킹 (Rotation ×{SL_CANDIDATES_N}) 생성 중...", flush=True)
            _t = time.perf_counter()

            # SL_CANDIDATES_N개의 rotation 순서 생성
            # rotation i: candidates[i:] + candidates[:i]  (i=0 → 원본순서)
            _n_sl = min(SL_CANDIDATES_N, len(candidates))  # candidates 수를 초과하지 않도록
            _rotated_schema_texts: list[str] = []
            _sl_labels: list[str] = []
            for _rot_i in range(_n_sl):
                _rot = candidates[_rot_i:] + candidates[:_rot_i]
                _rotated_schema_texts.append("\n\n".join([
                    (f"-- {r['description']}\n" if r.get("description") else "") + r["schema_text"]
                    for r in _rot
                ]))
                _sl_labels.append("원본순서" if _rot_i == 0 else f"Rotation-{_rot_i}")

            _sl_extra = None
            if SCHEMA_LINKING_GUIDED_JSON:
                _sl_extra = {"guided_json": SCHEMA_LINKING_JSON_SCHEMA}
                if GUIDED_DECODING_BACKEND:
                    _sl_extra["guided_decoding_backend"] = GUIDED_DECODING_BACKEND

            def _generate_sl(_schema_cands: str) -> tuple[str, str]:
                _prompt = render_schema_linking_prompt(
                    schema_candidates=_schema_cands,
                    entity_json=entity_json,
                    question=question,
                )
                return client.chat_completions(
                    messages=[{"role": "user", "content": _prompt}],
                    model=model_id,
                    temperature=SCHEMA_LINKING_TEMPERATURE,
                    max_tokens=_linking_tokens,
                    extra=_sl_extra,
                ), _prompt

            # SL N개 병렬 생성 (서로 다른 rotation 순서)
            with ThreadPoolExecutor(max_workers=_n_sl) as _pool:
                _sl_futures = [_pool.submit(_generate_sl, _txt) for _txt in _rotated_schema_texts]
                _sl_raw_and_prompts = [f.result() for f in _sl_futures]
            _sl_gen_elapsed = time.perf_counter() - _t

            _sl_results = [_parse_json_object(raw) for raw, _ in _sl_raw_and_prompts]
            _sl_prompts = [prompt for _, prompt in _sl_raw_and_prompts]
            for _i, (_sl, _label) in enumerate(zip(_sl_results, _sl_labels)):
                _log(
                    f"STEP {_step}",
                    f"SL 후보 {_i + 1} ({_label})",
                    json.dumps(_sl, ensure_ascii=False, indent=2),
                    _sl_gen_elapsed,
                )

            # ── 각 SL 후보에 validator 적용 ─────────────────────────────────────
            _t_validator = time.perf_counter()
            _lookup = get_schema_lookup()
            _validated_sls: list[dict] = []
            for _i, (_sl, _sl_prompt) in enumerate(zip(_sl_results, _sl_prompts)):
                _label = f"SL후보{_i + 1}"
                for _retry in range(SCHEMA_LINKING_MAX_RETRIES + 1):
                    _errors = find_errors(_sl, _lookup)
                    if not _errors:
                        print(f"  [{_label} validator] 검증 통과", flush=True)
                        break
                    if _retry == SCHEMA_LINKING_MAX_RETRIES:
                        print(f"  [{_label} validator] {len(_errors)}건 오류 — 최대 재링킹 도달, 현재 결과로 진행", flush=True)
                        break
                    print(f"  [{_label} validator] {len(_errors)}건 오류 → 재링킹 ({_retry + 1}/{SCHEMA_LINKING_MAX_RETRIES})", flush=True)
                    _err_lines = "\n".join(f"- {e.splitlines()[0]}" for e in _errors)
                    _sl_content = client.chat_completions(
                        messages=[
                            {"role": "user", "content": _sl_prompt},
                            {"role": "assistant", "content": json.dumps(_sl, ensure_ascii=False, indent=2)},
                            {"role": "user", "content": (
                                f"위 JSON에 오류가 있습니다:\n{_err_lines}\n\n"
                                f"위 오류를 모두 수정하여 올바른 JSON만 출력하세요."
                            )},
                        ],
                        model=model_id,
                        temperature=SCHEMA_CORRECTION_TEMPERATURE,
                        max_tokens=_linking_tokens,
                        extra=_sl_extra,
                    )
                    _sl = _parse_json_object(_sl_content)
                _validated_sls.append(_sl)
            _timings["t3_validator"] = round(time.perf_counter() - _t_validator, 2)
            _timings["t3_linking"] = round(time.perf_counter() - _t, 2)

            # ── SL별 value hints 병렬 조회 (IR 생성 전) ────────────────────────
            _t_hints = time.perf_counter()

            def _fetch_hints_for_sl(_sl_dict: dict) -> tuple[dict, str]:
                """SL 하나에 대해 value hints를 조회해 (dict, json_str) 반환."""
                try:
                    _h = _value_hint_service.fetch_from_schema_linking(
                        _sl_dict,
                        distinct_limit=VALUE_HINT_DISTINCT_LIMIT,
                        statement_timeout_ms=VALUE_HINT_TIMEOUT_MS,
                    )
                    return _h, json.dumps(_h, ensure_ascii=False, indent=2)
                except Exception as _e:
                    print(f"  [value-hints] 조회 실패 (무시): {_e}", flush=True)
                    return {}, "{}"

            with ThreadPoolExecutor(max_workers=len(_validated_sls)) as _pool:
                _hints_futures = [_pool.submit(_fetch_hints_for_sl, _sl) for _sl in _validated_sls]
                _sl_hints_list: list[tuple[dict, str]] = [f.result() for f in _hints_futures]
            _timings["t3b_value_hints"] = round(time.perf_counter() - _t_hints, 2)

            for _sl_i, ((_h_dict, _), _label_i) in enumerate(zip(_sl_hints_list, _sl_labels)):
                _hint_lines: list[str] = []
                for _col, _meta in _h_dict.items():
                    _allowed = _meta.get("allowed_values", []) if isinstance(_meta, dict) else []
                    _sample = _meta.get("sample_values", []) if isinstance(_meta, dict) else []
                    _count = _meta.get("value_count", 0) if isinstance(_meta, dict) else 0
                    _line = f"{_col}: allowed={len(_allowed)}, sample={len(_sample)}, count={_count}"
                    _hint_lines.append(_line)
                _hint_body = "\n".join(_hint_lines) if _hint_lines else "(조회 대상 컬럼 없음)"
                _log("VALUE HINTS", f"SL{_sl_i + 1} ({_label_i})", _hint_body, _timings["t3b_value_hints"])

            # ── 각 SL 기반 IR IR_CANDIDATES_N개씩 병렬 생성 → Pair Judge ─────────
            _n_ir = max(1, IR_CANDIDATES_N)
            _step += 1
            print(
                f"\n[{_step}/{_total_steps}] IR 생성 "
                f"(SL {len(_validated_sls)}개 × IR {_n_ir}개 = 총 {len(_validated_sls) * _n_ir}쌍) 중...",
                flush=True,
            )
            _t_ir = time.perf_counter()

            _rewrite_system = "You are a Query Rewriter. Output ONLY a valid JSON IR object. No explanations, no markdown fences."
            _ir_extra = None
            if IR_GUIDED_JSON:
                _ir_extra = {"guided_json": IR_JSON_SCHEMA}
                if GUIDED_DECODING_BACKEND:
                    _ir_extra["guided_decoding_backend"] = GUIDED_DECODING_BACKEND

            def _generate_irs_for_sl(_sl_dict: dict, _hints_json: str) -> list[str]:
                """SL 하나에 대해 IR을 _n_ir개 생성해 반환."""
                _sl_json_str = json.dumps(_sl_dict, ensure_ascii=False, indent=2)
                _msgs = [
                    {"role": "system", "content": _rewrite_system},
                    {"role": "user", "content": render_rewrite_prompt(
                        question=question,
                        schema_candidates=schema_candidates,
                        entity_json=entity_json,
                        schema_linking_json=_sl_json_str,
                        value_hints_json=_hints_json,
                    )},
                ]
                if _n_ir >= 2:
                    return client.chat_completions_n(
                        messages=_msgs,
                        model=model_id,
                        n=_n_ir,
                        temperature=IR_CANDIDATES_TEMP,
                        max_tokens=_rewrite_tokens,
                        extra=_ir_extra,
                    )
                return [client.chat_completions(
                    messages=_msgs,
                    model=model_id,
                    temperature=REWRITE_SINGLE_TEMPERATURE,
                    max_tokens=_rewrite_tokens,
                    extra=_ir_extra,
                )]

            with ThreadPoolExecutor(max_workers=len(_validated_sls)) as _pool:
                _ir_futures = [
                    _pool.submit(_generate_irs_for_sl, _sl, _h_json)
                    for _sl, (_, _h_json) in zip(_validated_sls, _sl_hints_list)
                ]
                _all_ir_raws: list[list[str]] = [f.result() for f in _ir_futures]
            _ir_gen_elapsed = time.perf_counter() - _t_ir

            _pairs: list[dict[str, Any]] = []
            for _sl_i, (_sl, _ir_raws_for_sl) in enumerate(zip(_validated_sls, _all_ir_raws)):
                _sl_json_str = json.dumps(_sl, ensure_ascii=False, indent=2)
                for _ir_j, _ir_raw in enumerate(_ir_raws_for_sl):
                    _ir_text = _extract_rewritten_question(_ir_raw)
                    _ir_parsed = _parse_ir(_ir_text) if _ir_text else {}
                    _pairs.append({
                        "idx": len(_pairs),
                        "sl_idx": _sl_i,
                        "ir_idx": _ir_j,
                        "sl": _sl,
                        "sl_json": _sl_json_str,
                        "ir_raw": _ir_text,
                        "ir": _ir_parsed,
                        "score": 0.0,
                        "selected": False,
                        "reasoning": "",
                    })
                    _log(
                        f"STEP {_step}",
                        f"SL{_sl_i + 1} → IR{_ir_j + 1}",
                        f"SL tables: {_sl.get('linked_tables', [])}\nIR:\n{_ir_text}",
                        _ir_gen_elapsed,
                    )

            _pairs, _removed_pair_dupes = _dedupe_pair_candidates(_pairs)
            if _removed_pair_dupes:
                print(f"  [dedupe] 완전 동일한 (SL, IR) 쌍 {_removed_pair_dupes}개 제거", flush=True)

            _pairs, _dropped_ir_pairs = _filter_invalid_ir_pairs(_pairs)
            if _dropped_ir_pairs:
                print(f"  [IR validator] 유효하지 않은 (SL, IR) 쌍 {len(_dropped_ir_pairs)}개 탈락", flush=True)
                for _sl_i, _ir_j, _errors in _dropped_ir_pairs:
                    print(
                        f"    · SL{_sl_i + 1}-IR{_ir_j + 1}: {_brief_validator_error(_errors[0])}",
                        flush=True,
                    )
            if not _pairs:
                _timings["t4_rewrite"] = round(time.perf_counter() - _t_ir, 2)
                return {
                    "entities": entities,
                    "schema_linking_json": "{}",
                    "value_hints_json": "{}",
                    "rewritten": "",
                    "sql": "",
                    "schema_results": results,
                    "db_result": None,
                    "exec_error": None,
                    "timings": _timings,
                    "error": "IR validator에서 모든 (SL, IR) 후보가 탈락했습니다",
                }

            # ── Pair Judge ──────────────────────────────────────────────────────
            _t_judge = time.perf_counter()
            _sl_tables_list = [frozenset(_sl.get("linked_tables", [])) for _sl in _validated_sls]
            if len(set(_sl_tables_list)) == 1:
                # 모든 SL이 동일한 테이블을 선택 → 모든 IR 후보를 IR Judge로 선택
                _ir_texts = [p["ir_raw"] for p in _pairs]
                _unique_irs = set(t.strip() for t in _ir_texts)
                if len(_unique_irs) == 1:
                    _winner_idx = 0
                    _pairs[0]["selected"] = True
                    _pairs[0]["score"] = 10.0
                    _judge_reasoning = ""
                    _select_method = "SL·IR 모두 동일 (skip)"
                else:
                    # IR 후보 전체(SL×IR)를 IR judge로 판단
                    _, _ir_meta = _ir_judge.select(
                        question, _ir_texts, schema_linking_json=_pairs[0]["sl_json"]
                    )
                    for _m in _ir_meta:
                        _pairs[_m["idx"]]["score"] = _m.get("score", 0)
                        _pairs[_m["idx"]]["selected"] = _m["selected"]
                        if _m["selected"]:
                            _pairs[_m["idx"]]["reasoning"] = _m.get("reasoning", "")
                    _winner_idx = next(m["idx"] for m in _ir_meta if m["selected"])
                    _judge_reasoning = next((_m.get("reasoning", "") for _m in _ir_meta if _m.get("reasoning")), "")
                    _select_method = f"IR LLM Judge (SL 동일, IR {len(_ir_texts)}개)"
            else:
                # SL이 다르면 전체 (SL, IR) 쌍을 Pair Judge로 선택
                _winner_idx, _judge_reasoning, _pairs = _ir_judge.select_pairs(question, _pairs)
                _select_method = f"Pair LLM Judge ({len(_pairs)}쌍)"
            _judge_elapsed = time.perf_counter() - _t_judge

            _score_lines: list[str] = []
            for _p in _pairs:
                _tables_str = str(_p["sl"].get("linked_tables", []))
                _tag = f"★선택  score={_p.get('score', '-')}" if _p["selected"] else f"  score={_p.get('score', '-')}"
                _sl_i = _p.get("sl_idx", _p["idx"])
                _ir_j = _p.get("ir_idx", 0)
                _score_lines.append(f"[SL{_sl_i + 1}-IR{_ir_j + 1}] tables={_tables_str} {_tag}")
            _winner_p = _pairs[_winner_idx]
            _w_sl = _winner_p.get("sl_idx", _winner_idx)
            _w_ir = _winner_p.get("ir_idx", 0)
            _judge_body = "\n".join(_score_lines) + f"\n\n→ SL{_w_sl + 1}-IR{_w_ir + 1} 선택"
            if _judge_reasoning:
                _judge_body += f"\n\n[Judge 추론]\n{_judge_reasoning}"
            _log(f"STEP {_step} Judge", f"선택 ({_select_method})", _judge_body, _judge_elapsed)

            _winner = _pairs[_winner_idx]
            sl_result = _winner["sl"]
            schema_linking_json = _winner["sl_json"]
            rewritten = _winner["ir_raw"]
            _timings["t4_rewrite"] = round(_ir_gen_elapsed + _judge_elapsed, 2)

            # ── value hints: winner SL의 이미 조회된 결과 재사용 ────────────────
            _winner_sl_idx = _winner.get("sl_idx", 0)
            _, value_hints_json = _sl_hints_list[_winner_sl_idx]

        else:
            # ══════════════════════════════════════════════════════════════════
            # 기존 모드: SL 단일 → IR N개 → IR judge
            # ══════════════════════════════════════════════════════════════════

            # 3. 스키마 링킹 (v3+ 전용, 템플릿 없으면 자동 스킵)
            if _use_schema_linking:
                _step += 1
                print(f"\n[{_step}/{_total_steps}] 스키마 링킹 중...", flush=True)
                _t = time.perf_counter()
                _sl_prompt = render_schema_linking_prompt(
                    schema_candidates=schema_candidates,
                    entity_json=entity_json,
                    question=question,
                )
                _sl_extra = None
                if SCHEMA_LINKING_GUIDED_JSON:
                    _sl_extra = {"guided_json": SCHEMA_LINKING_JSON_SCHEMA}
                    if GUIDED_DECODING_BACKEND:
                        _sl_extra["guided_decoding_backend"] = GUIDED_DECODING_BACKEND
                sl_content = client.chat_completions(
                    messages=[{"role": "user", "content": _sl_prompt}],
                    model=model_id,
                    temperature=SCHEMA_LINKING_TEMPERATURE,
                    max_tokens=_linking_tokens,
                    extra=_sl_extra,
                )
                sl_result = _parse_json_object(sl_content)

                _t_validator = time.perf_counter()
                _lookup = get_schema_lookup()
                for _retry in range(SCHEMA_LINKING_MAX_RETRIES + 1):
                    _errors = find_errors(sl_result, _lookup)
                    if not _errors:
                        print("  [validator] 검증 통과", flush=True)
                        break
                    if _retry == SCHEMA_LINKING_MAX_RETRIES:
                        print(
                            f"  [validator] {len(_errors)}건 오류 — 최대 재링킹 횟수({SCHEMA_LINKING_MAX_RETRIES}) 도달, 현재 결과로 진행",
                            flush=True,
                        )
                        for _err in _errors:
                            print(f"    · {_err}", flush=True)
                        break
                    print(
                        f"  [validator] {len(_errors)}건 오류 발견 → 재링킹 시도 ({_retry + 1}/{SCHEMA_LINKING_MAX_RETRIES})",
                        flush=True,
                    )
                    for _err in _errors:
                        print(f"    · {_err}", flush=True)
                    _sl_err_lines = "\n".join(f"- {e.splitlines()[0]}" for e in _errors)
                    sl_content = client.chat_completions(
                        messages=[
                            {"role": "user", "content": _sl_prompt},
                            {"role": "assistant", "content": json.dumps(sl_result, ensure_ascii=False, indent=2)},
                            {"role": "user", "content": (
                                f"위 JSON에 오류가 있습니다:\n{_sl_err_lines}\n\n"
                                f"위 오류를 모두 수정하여 올바른 JSON만 출력하세요."
                            )},
                        ],
                        model=model_id,
                        temperature=SCHEMA_CORRECTION_TEMPERATURE,
                        max_tokens=_linking_tokens,
                        extra=_sl_extra,
                    )
                    sl_result = _parse_json_object(sl_content)
                    print(f"  [validator] 재링킹 완료 ({_retry + 1}/{SCHEMA_LINKING_MAX_RETRIES})", flush=True)
                _timings["t3_validator"] = round(time.perf_counter() - _t_validator, 2)

                schema_linking_json = json.dumps(sl_result, ensure_ascii=False, indent=2)
                _timings["t3_linking"] = round(time.perf_counter() - _t, 2)
                _log("STEP 3", "SCHEMA LINKING", schema_linking_json, _timings["t3_linking"])

                _t_hints = time.perf_counter()
                try:
                    _hints = _value_hint_service.fetch_from_schema_linking(
                        sl_result,
                        distinct_limit=VALUE_HINT_DISTINCT_LIMIT,
                        statement_timeout_ms=VALUE_HINT_TIMEOUT_MS,
                    )
                    value_hints_json = json.dumps(_hints, ensure_ascii=False, indent=2)
                    _timings["t3b_value_hints"] = round(time.perf_counter() - _t_hints, 2)

                    _hint_lines: list[str] = []
                    for _col, _meta in _hints.items():
                        _allowed = _meta.get("allowed_values", []) if isinstance(_meta, dict) else []
                        _sample = _meta.get("sample_values", []) if isinstance(_meta, dict) else []
                        _count = _meta.get("value_count", 0) if isinstance(_meta, dict) else 0
                        _min = _meta.get("min") if isinstance(_meta, dict) else None
                        _max = _meta.get("max") if isinstance(_meta, dict) else None
                        _line = f"{_col}: allowed={len(_allowed)}, sample={len(_sample)}, count={_count}"
                        if _min is not None or _max is not None:
                            _line += f", min={_min}, max={_max}"
                        _hint_lines.append(_line)

                    _hint_body = "\n".join(_hint_lines) if _hint_lines else "(조회 대상 컬럼 없음)"
                    _log("STEP 3-B", "VALUE HINTS", _hint_body, _timings["t3b_value_hints"])
                except Exception as _e:
                    _timings["t3b_value_hints"] = round(time.perf_counter() - _t_hints, 2)
                    value_hints_json = "{}"
                    print(f"  [value-hints] 조회 실패 (무시): {_e}", flush=True)

            # 4. 질의 재작성 / IR 생성
            _step += 1
            _rewrite_label = "IR 생성 중..." if _REWRITE_IR_MODE else "질의 재작성 중..."
            print(f"\n[{_step}/{_total_steps}] {_rewrite_label}", flush=True)
            _t = time.perf_counter()
            _rewrite_system = (
                "You are a Query Rewriter. Output ONLY a valid JSON IR object. No explanations, no markdown fences."
                if _REWRITE_IR_MODE
                else "You are a rewriting function. Do NOT output reasoning. Output ONLY the rewritten question as a single line in Korean. No explanations."
            )
            if _REWRITE_IR_MODE and IR_GUIDED_JSON:
                _rewrite_extra = {"guided_json": IR_JSON_SCHEMA}
                if GUIDED_DECODING_BACKEND:
                    _rewrite_extra["guided_decoding_backend"] = GUIDED_DECODING_BACKEND
            elif _REWRITE_IR_MODE:
                _rewrite_extra = {}
            else:
                _rewrite_extra = {"stop": ["\n"]}
            _rewrite_messages = [
                {"role": "system", "content": _rewrite_system},
                {"role": "user", "content": render_rewrite_prompt(
                    question=question,
                    schema_candidates=schema_candidates,
                    entity_json=entity_json,
                    schema_linking_json=schema_linking_json,
                    value_hints_json=value_hints_json,
                )},
            ]

            _use_batch_ir = _REWRITE_IR_MODE and IR_CANDIDATES_N > 1
            if _use_batch_ir:
                _step_name = "STEP 4" if _use_schema_linking else "STEP 3"

                _ir_extra = None
                if IR_GUIDED_JSON:
                    _ir_extra = {"guided_json": IR_JSON_SCHEMA}
                    if GUIDED_DECODING_BACKEND:
                        _ir_extra["guided_decoding_backend"] = GUIDED_DECODING_BACKEND
                _t_gen = time.perf_counter()
                _raw_candidates = client.chat_completions_n(
                    messages=_rewrite_messages,
                    model=model_id,
                    n=IR_CANDIDATES_N,
                    temperature=IR_CANDIDATES_TEMP,
                    max_tokens=_rewrite_tokens,
                    extra=_ir_extra,
                )
                _gen_elapsed = time.perf_counter() - _t_gen

                _gen_lines = [
                    f"[후보 {i + 1}]\n{raw.strip()}"
                    for i, raw in enumerate(_raw_candidates)
                ]
                _log(_step_name, f"IR 후보 {IR_CANDIDATES_N}개 생성", "\n\n".join(_gen_lines), _gen_elapsed)

                _raw_candidates, _removed_ir_dupes = _dedupe_ir_raw_candidates(_raw_candidates)
                if _removed_ir_dupes:
                    print(f"  [dedupe] 완전 동일한 IR 후보 {_removed_ir_dupes}개 제거", flush=True)

                _raw_candidates, _dropped_ir_candidates = _filter_invalid_ir_raw_candidates(
                    _raw_candidates,
                    sl_result=sl_result if _use_schema_linking else None,
                )
                if _dropped_ir_candidates:
                    print(f"  [IR validator] 유효하지 않은 IR 후보 {len(_dropped_ir_candidates)}개 탈락", flush=True)
                    for _cand_i, _errors in _dropped_ir_candidates:
                        print(
                            f"    · 후보 {_cand_i + 1}: {_brief_validator_error(_errors[0])}",
                            flush=True,
                        )
                if not _raw_candidates:
                    _timings["t4_rewrite" if _use_schema_linking else "t3_rewrite"] = round(time.perf_counter() - _t, 2)
                    return {
                        "entities": entities,
                        "schema_linking_json": schema_linking_json,
                        "value_hints_json": value_hints_json,
                        "rewritten": "",
                        "sql": "",
                        "schema_results": results,
                        "db_result": None,
                        "exec_error": None,
                        "timings": _timings,
                        "error": "IR validator에서 모든 IR 후보가 탈락했습니다",
                    }

                _t_judge = time.perf_counter()
                _unique_candidates = set(c.strip() for c in _raw_candidates)
                if len(_unique_candidates) == 1:
                    rewritten = _raw_candidates[0].strip()
                    _ir_meta = [{"idx": i, "raw": r, "selected": i == 0, "score": "-", "reasoning": ""} for i, r in enumerate(_raw_candidates)]
                    _select_method = "동일 (skip)"
                elif IR_SELECT_METHOD == "llm_judge" and _ir_judge is not None:
                    rewritten, _ir_meta = _ir_judge.select(
                        question, _raw_candidates, schema_linking_json=schema_linking_json
                    )
                    _select_method = "LLM Judge"
                elif IR_SELECT_METHOD == "cross_encoder" and _ir_reranker is not None:
                    rewritten, _ir_meta = _ir_reranker.select(question, _raw_candidates)
                    _select_method = "Cross Encoder"
                else:
                    rewritten, _ir_meta = _ir_majority_vote(_raw_candidates)
                    _select_method = "다수결"
                _judge_elapsed = time.perf_counter() - _t_judge

                _score_lines: list[str] = []
                for _m in _ir_meta:
                    if IR_SELECT_METHOD in ("llm_judge", "cross_encoder"):
                        _score_str = f"  score={_m.get('score', '-')}"
                        _tag = f"선택{_score_str}" if _m["selected"] else _score_str
                    else:
                        _vote_str = f"  득표 {_m.get('vote_count', '-')}"
                        _tag = f"선택{_vote_str}" if _m["selected"] else _vote_str
                    _score_lines.append(f"[후보 {_m['idx'] + 1}] {_tag}")

                _winner_idx = next((m["idx"] + 1 for m in _ir_meta if m["selected"]), "?")
                _judge_body = "\n".join(_score_lines) + f"\n\n→ 후보 {_winner_idx} 선택"

                _judge_reasoning = next(
                    (_m.get("reasoning", "") for _m in _ir_meta if _m.get("reasoning")),
                    "",
                )
                if _judge_reasoning:
                    _judge_body += f"\n\n[Judge 추론]\n{_judge_reasoning}"

                _log(_step_name + " Judge", f"IR 선택 ({_select_method})", _judge_body, _judge_elapsed)

                _timings["t4_rewrite" if _use_schema_linking else "t3_rewrite"] = round(_gen_elapsed + _judge_elapsed, 2)
            else:
                raw_rewrite = client.chat_completions(
                    messages=_rewrite_messages,
                    model=model_id,
                    temperature=REWRITE_SINGLE_TEMPERATURE,
                    max_tokens=_rewrite_tokens,
                    extra=_rewrite_extra,
                )
                rewritten = _extract_rewritten_question(raw_rewrite) or question
                if _REWRITE_IR_MODE:
                    _single_ir = _parse_json_object(rewritten)
                    _single_ir_errors = (
                        find_ir_errors(
                            _single_ir,
                            get_schema_lookup(),
                            sl_result=sl_result if _use_schema_linking else None,
                        )
                        if _single_ir else ["IR JSON 파싱 실패"]
                    )
                    if _single_ir_errors:
                        return {
                            "entities": entities,
                            "schema_linking_json": schema_linking_json,
                            "value_hints_json": value_hints_json,
                            "rewritten": "",
                            "sql": "",
                            "schema_results": results,
                            "db_result": None,
                            "exec_error": None,
                            "timings": _timings,
                            "error": f"IR validator 탈락: {_single_ir_errors[0]}",
                        }
                _step_name = "STEP 4" if _use_schema_linking else "STEP 3"
                _timings["t4_rewrite" if _use_schema_linking else "t3_rewrite"] = round(time.perf_counter() - _t, 2)
                _log_label = "IR JSON" if _REWRITE_IR_MODE else "REWRITTEN QUESTION"
                _log(_step_name, _log_label, rewritten, time.perf_counter() - _t)

        # MQS Few-Shot 검색 (FEW_SHOT_TOP_K > 0 이고 pool 파일이 있을 때만)
        few_shots: list[dict] = []
        if FEW_SHOT_TOP_K > 0 and MQS_POOL_NPZ_PATH.exists():
            _t_mqs = time.perf_counter()
            try:
                # IR JSON을 그대로 임베딩 검색 쿼리로 사용
                _search_query = rewritten
                _mqs_prefix = "[Pool 검색]\n"

                few_shots = retrieve_few_shots(
                    rewritten=_search_query,
                    npz_path=str(MQS_POOL_NPZ_PATH),
                    top_k=FEW_SHOT_TOP_K,
                )
                _mqs_elapsed = time.perf_counter() - _t_mqs
                _mqs_body = _mqs_prefix + f"[검색 결과 ({len(few_shots)}건)]"
                for _i, _fs in enumerate(few_shots, 1):
                    _mqs_body += (
                        f"\n  {_i}. Q: {_fs['question_ko']}"
                        f"\n     S: {_fs['sql']}"
                    )
                _log("MQS", "FEW-SHOT EXAMPLES", _mqs_body, _mqs_elapsed)
            except Exception as _e:
                print(f"  [few-shot] 검색 실패 (무시): {_e}", flush=True)

        # 5+6. SQL 생성 → DB 실행 (통합 재시도 루프, 최대 SQL_DB_MAX_RETRIES회)
        _step += 1
        print(f"\n[{_step}/{_total_steps}] SQL 생성 중...", flush=True)
        _t = time.perf_counter()
        _sql_step_name = "STEP 5" if _use_schema_linking else "STEP 4"
        _db_step_name  = "STEP 6" if _use_schema_linking else "STEP 5"
        _sql_timing_key = "t5_sql" if _use_schema_linking else "t4_sql"
        _db_timing_key  = "t6_db"  if _use_schema_linking else "t5_db"

        _sql_user_prompt = render_sql_prompt(
            question=question,
            rewritten_question=rewritten,
            schema_candidates=schema_candidates,
            value_hints_json=value_hints_json,
            entity_json=entity_json,
            schema_linking_json=schema_linking_json,
            few_shot_examples=few_shots,
        )
        _sql_system = "You are a SQL generator. Output ONLY SQL starting with SELECT or WITH. No explanations."
        _sql_base_messages: list[dict] = [
            {"role": "system", "content": _sql_system},
            {"role": "user", "content": _sql_user_prompt},
        ]

        sql_text = ""
        db_result: list[dict] | None = None
        exec_error: str | None = None
        _sql_errors: list[str] = []
        _correction_msg = ""

        for _try in range(SQL_DB_MAX_RETRIES + 1):
            # ── SQL 생성 ────────────────────────────────────────────────────
            if _try == 0:
                _sql_messages = _sql_base_messages
            else:
                _sql_messages = _sql_base_messages + [
                    {"role": "assistant", "content": sql_text},
                    {"role": "user", "content": _correction_msg},
                ]
                print(f"  [SQL 재시도 {_try}/{SQL_DB_MAX_RETRIES}] {_correction_msg.splitlines()[0]}", flush=True)

            _t_sql = time.perf_counter()
            sql_text = _strip_sql_fences(client.chat_completions(
                messages=_sql_messages,
                model=model_id,
                temperature=SQL_GENERATION_TEMPERATURE,
                max_tokens=_sql_tokens,
            ))
            _timings[_sql_timing_key] = round(time.perf_counter() - _t_sql, 2)
            _log(_sql_step_name, f"SQL (시도 {_try + 1})", sql_text, time.perf_counter() - _t_sql)

            # ── 구문 검사 ───────────────────────────────────────────────────
            _sql_errors = find_sql_errors(sql_text)
            if _sql_errors:
                _err_lines = "\n".join(f"- {e}" for e in _sql_errors)
                _correction_msg = (
                    f"위 SQL에 구문 오류가 있습니다:\n{_err_lines}\n\n"
                    f"FROM 절을 반드시 포함한 완전한 PostgreSQL SQL을 다시 출력하세요."
                )
                continue

            # ── DB 실행 ─────────────────────────────────────────────────────
            if skip_exec:
                break

            _step_db = _step + 1
            print(f"\n[{_step_db}/{_total_steps}] DB 실행 중...", flush=True)
            _t_db = time.perf_counter()
            try:
                db_result = execute_sql(sql_text)
                _timings[_db_timing_key] = round(time.perf_counter() - _t_db, 2)
                _log(_db_step_name, "DB RESULT", json.dumps(db_result, ensure_ascii=False, indent=2, default=str), time.perf_counter() - _t_db)
                exec_error = None
                break  # 성공
            except Exception as _db_exc:
                _timings[_db_timing_key] = round(time.perf_counter() - _t_db, 2)
                exec_error = str(_db_exc)
                print(f"  DB 실행 오류 ({time.perf_counter() - _t_db:.2f}s): {exec_error}", file=sys.stderr, flush=True)
                _correction_msg = (
                    f"위 SQL 실행 시 DB 오류가 발생했습니다:\n{exec_error}\n\n"
                    f"오류를 수정한 올바른 PostgreSQL SQL을 다시 출력하세요."
                )

        if _sql_errors:
            print(f"  [SQL validator] 최대 재시도 후에도 구문 오류 — 탈락:", flush=True)
            for _e in _sql_errors:
                print(f"    · {_e}", flush=True)
            return {
                "entities": entities,
                "schema_linking_json": schema_linking_json,
                "value_hints_json": value_hints_json,
                "rewritten": rewritten,
                "sql": sql_text,
                "schema_results": results,
                "db_result": None,
                "exec_error": None,
                "timings": _timings,
                "error": f"SQL validator 탈락: {_sql_errors[0]}",
            }

        if skip_exec:
            _step += 1
            print(f"\n[{_step + 1}/{_total_steps}] DB 실행 생략 (--skip-exec)", flush=True)
        else:
            _step += 1

        _timings["total"] = round(time.perf_counter() - _pipeline_start, 2)
        print(f"\n{'='*50}", flush=True)
        print(f"  파이프라인 완료  총 소요: {_timings['total']:.2f}s", flush=True)
        print(f"{'='*50}", flush=True)

        return {
            "entities": entities,
            "schema_linking_json": schema_linking_json,
            "value_hints_json": value_hints_json,
            "rewritten": rewritten,
            "sql": sql_text,
            "schema_results": results,
            "db_result": db_result,
            "exec_error": exec_error,
            "timings": _timings,
            "error": None,
        }

    except Exception as e:
        _timings["total"] = round(time.perf_counter() - _pipeline_start, 2)
        return {"entities": {}, "schema_linking_json": "{}", "value_hints_json": "{}", "rewritten": "", "sql": "", "schema_results": [], "db_result": None, "exec_error": None, "timings": _timings, "error": str(e)}


