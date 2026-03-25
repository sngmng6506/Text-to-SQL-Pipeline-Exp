"""
IR LLM Judge Service

vLLM(Gemma 등)을 Judge로 사용해 IR 후보 중 원본 질문과 가장 잘 맞는 것을 선택한다.

사용 흐름:
    judge = IRLLMJudgeService(client, model_id, use_cot=True)
    selected_json, meta = judge.select(question, raw_candidates, schema_linking_json)

CoT=True (기본):
  1단계: 자유 텍스트 CoT 추론 → "SCORES: [...]" 파싱 성공 시 완료 (LLM 1회)
  2단계: 파싱 실패 시 CoT를 assistant context로 주고 자유 텍스트로 scores 재추출 (LLM 2회)

CoT=False:
  자유 텍스트 + SCORES 형식으로 scores 직접 추출 (LLM 1회)

reasoning은 선택된 후보(winner)의 meta에만 저장한다.
"""
from __future__ import annotations

import json
import re
from typing import Any

from config import (
    IR_JUDGE_REASONING_MAX_TOKENS,
    IR_JUDGE_SCORE_MAX_TOKENS,
    IR_JUDGE_TEMPERATURE,
    PAIR_JUDGE_REASONING_MAX_TOKENS,
    PAIR_JUDGE_SCORE_MAX_TOKENS,
    PAIR_JUDGE_TEMPERATURE,
)
from ir.selector import _parse_ir, _completeness, _FIELD_ORDER


_COMMON_CRITERIA = """\
- FILTER 조건이 질문 의도와 일치하는가 (날짜 범위 vs Top-K 혼동에 특히 주의)
- 집계(AGGREGATE), 정렬(ORDER_BY), 건수(LIMIT)가 적절한가
- 비율 비교(A가 B의 k%보다 크다/작다)는 가능하면 나눗셈(A/B) 대신 곱셈 비교(A > B*k)를 사용했는가
- 나눗셈이 사용된 경우 division by zero 위험(order_count = 0 등)이 없는가\
- 질문 의도와 맞는 테이블·컬럼 선택을 집계 완전성보다 우선한다.
- 개수 질문에서는 COUNT 집계가 있는 후보를 선호한다.
"""

_SYSTEM_PROMPT = f"""\
당신은 PostgreSQL 기반 Text-to-SQL 전문가입니다.
원본 질문에 대해 생성된 IR(중간 표현, SQL 의도 구조) 후보들을 평가합니다.

평가 기준:
1. 질문에서 요구하는 테이블·컬럼이 올바르게 선택됐는가
2. 불필요한 조건이 추가됐거나 필요한 조건이 누락됐는가
3. {_COMMON_CRITERIA}
4. 스키마 링킹 결과(실제 DB 컬럼)와 대조했을 때 존재하지 않는 컬럼을 참조하지 않는가\
"""

_PAIR_SYSTEM_PROMPT = f"""\
당신은 PostgreSQL 기반 Text-to-SQL 전문가입니다.
원본 질문에 대해 (스키마 링킹 + IR) 쌍 후보들을 평가합니다.
각 후보는 서로 다른 스키마 링킹 결과를 기반으로 생성된 IR입니다.

평가 기준:
1. 스키마 링킹이 질문에 필요한 테이블·컬럼을 올바르게 선택했는가
2. 불필요한 테이블이 포함됐거나 필요한 테이블이 누락되지 않았는가
3. {_COMMON_CRITERIA}
4. 스키마 링킹과 IR이 일관성 있게 대응하는가\
"""

_REASONING_PROMPT_SUFFIX = (
    "\n\n각 후보를 위 기준으로 분석하고, "
    "마지막에 반드시 다음 형식으로 점수를 출력하세요:\n"
    "SCORES: [점수1, 점수2, ...]\n"
    "(점수는 1~10 정수, 후보 순서대로)"
)

_SCORE_LINE_RE = re.compile(r"SCORES\s*:\s*\[([^\]]+)\]", re.IGNORECASE)


class IRLLMJudgeService:
    """LLM-as-Judge 기반 IR 후보 선택 서비스."""

    def __init__(
        self,
        client: Any,
        model_id: str | None = None,
        use_cot: bool = True,
    ) -> None:
        self._client = client
        self._model_id = model_id
        self._use_cot = use_cot

    def select(
        self,
        question: str,
        raw_candidates: list[str],
        schema_linking_json: str = "{}",
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        IR 후보 중 LLM Judge score가 가장 높은 것을 선택한다.

        Parameters
        ----------
        question            : 원본 한국어 질문
        raw_candidates      : LLM이 반환한 IR raw 문자열 목록
        schema_linking_json : 스키마 링킹 결과 JSON 문자열 (컬럼 존재 검증용)

        Returns
        -------
        (selected_json_str, meta_list)
            - selected_json_str : 선택된 IR의 정규화된 JSON 문자열
            - meta_list         : 각 후보의 메타. reasoning은 winner에만 저장.
        """
        meta: list[dict[str, Any]] = []
        for i, raw in enumerate(raw_candidates):
            ir = _parse_ir(raw)
            meta.append({
                "idx": i,
                "raw": raw,
                "ir": ir,
                "completeness": _completeness(ir) if ir else 0,
                "score": float("-inf"),
                "selected": False,
                "reasoning": "",
            })

        valid = [m for m in meta if m["ir"]]
        if not valid:
            meta[0]["selected"] = True
            return raw_candidates[0], meta

        # 유효 후보가 1개뿐이면 judge 호출 없이 바로 선택
        if len(valid) == 1:
            valid[0]["selected"] = True
            valid[0]["score"] = 10.0
            return json.dumps(
                {k: valid[0]["ir"].get(k, []) for k in _FIELD_ORDER},
                ensure_ascii=False,
            ), meta

        scores, reasoning = self._judge(question, valid, schema_linking_json)
        for m, score in zip(valid, scores):
            m["score"] = score

        winner = max(valid, key=lambda m: m["score"])
        winner["selected"] = True
        winner["reasoning"] = reasoning  # reasoning은 winner에만 저장

        selected_str = json.dumps(
            {k: winner["ir"].get(k, []) for k in _FIELD_ORDER},
            ensure_ascii=False,
        )
        return selected_str, meta

    def _build_base_messages(
        self,
        question: str,
        candidates_text: str,
        schema_section: str,
        include_score_suffix: bool,
    ) -> list[dict[str, str]]:
        """공통 프롬프트 메시지를 구성한다."""
        suffix = _REASONING_PROMPT_SUFFIX if include_score_suffix else ""
        return [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {
                "role": "user",
                "content": (
                    f"원본 질문: {question}\n"
                    f"{schema_section}"
                    f"{candidates_text}"
                    f"{suffix}"
                ),
            },
        ]

    @staticmethod
    def _build_pair_candidate_summary(i: int, pair: dict[str, Any]) -> str:
        """(SL, IR) 쌍 후보를 핵심 필드만 포함한 일반형 요약 문자열로 만든다."""
        sl = pair.get("sl") or {}
        ir = pair.get("ir") or {}

        lines = [f"[후보 {i}]"]
        lines.append(f"SL tables: {json.dumps(sl.get('linked_tables', []), ensure_ascii=False)}")

        column_mappings = sl.get("column_mappings", {}) or {}
        if column_mappings:
            lines.append(f"SL mappings: {json.dumps(column_mappings, ensure_ascii=False, sort_keys=True)}")

        lines.append(f"IR FROM: {json.dumps(ir.get('FROM', []), ensure_ascii=False)}")
        lines.append(f"IR FILTER: {json.dumps(ir.get('FILTER', []), ensure_ascii=False)}")
        lines.append(f"IR AGGREGATE: {json.dumps(ir.get('AGGREGATE', []), ensure_ascii=False)}")
        lines.append(f"IR SELECT: {json.dumps(ir.get('SELECT', []), ensure_ascii=False)}")

        if ir.get("JOIN"):
            lines.append(f"IR JOIN: {json.dumps(ir.get('JOIN', []), ensure_ascii=False)}")
        if ir.get("GROUP_BY"):
            lines.append(f"IR GROUP_BY: {json.dumps(ir.get('GROUP_BY', []), ensure_ascii=False)}")
        if ir.get("ORDER_BY"):
            lines.append(f"IR ORDER_BY: {json.dumps(ir.get('ORDER_BY', []), ensure_ascii=False)}")
        if ir.get("HAVING"):
            lines.append(f"IR HAVING: {json.dumps(ir.get('HAVING', []), ensure_ascii=False)}")
        if ir.get("COMPUTED"):
            lines.append(f"IR COMPUTED: {json.dumps(ir.get('COMPUTED', []), ensure_ascii=False)}")

        return "\n".join(lines)

    def _judge_direct(
        self,
        n: int,
        score_messages: list[dict[str, str]],
        fallback_meta: list[dict[str, Any]],
        *,
        temperature: float,
        max_tokens: int,
    ) -> list[float]:
        """
        scores를 추출한다.

        1차: 자유 텍스트 + SCORES 형식 파싱 (가장 안정적)
        2차: completeness fallback
        """
        # ── 마지막 user 메시지에 SCORES 출력 지시 결합 ─────────────────────────
        _messages = [dict(msg) for msg in score_messages]
        if _messages and _messages[-1]["role"] == "user":
            _messages[-1] = {
                "role": "user",
                "content": _messages[-1]["content"] + (
                    f"\n\n{n}개 후보의 점수를 1~10 정수로 매기세요. "
                    f"반드시 다음 형식으로만 응답:\nSCORES: [점수1, 점수2, ...]"
                ),
            }

        try:
            raw = self._client.chat_completions(
                messages=_messages,
                model=self._model_id,
                temperature=temperature,
                max_tokens=max_tokens,
            )
            m = _SCORE_LINE_RE.search(raw)
            if m:
                parts = [p.strip() for p in m.group(1).split(",")]
                try:
                    parsed = [float(p) for p in parts if p]
                except ValueError:
                    parsed = []
                if len(parsed) == n:
                    return parsed
            print(f"[WARN] LLM Judge scores 파싱 실패: {raw[:200]}", flush=True)
        except Exception as e:
            print(f"[WARN] LLM Judge scores 호출 실패: {e}", flush=True)

        # ── Fallback: completeness ────────────────────────────────────────────
        return [float(m["completeness"]) for m in fallback_meta]

    def _judge(
        self,
        question: str,
        valid_meta: list[dict[str, Any]],
        schema_linking_json: str = "{}",
    ) -> tuple[list[float], str]:
        """
        CoT 설정에 따라 LLM Judge 채점을 수행한다.
        실패 시 completeness 점수로 fallback.
        """
        n = len(valid_meta)
        candidates_text = "\n\n".join(
            f"[후보 {m['idx'] + 1}]\n{json.dumps(m['ir'], ensure_ascii=False, indent=2)}"
            for m in valid_meta
        )
        schema_section = (
            f"스키마 링킹 결과 (사용 가능한 테이블·컬럼 근거):\n"
            f"```json\n{schema_linking_json}\n```\n\n"
            if schema_linking_json and schema_linking_json.strip() not in ("{}", "")
            else ""
        )

        reasoning = ""

        if self._use_cot:
            # ── 1단계: CoT 추론 (자유 텍스트, SCORES 줄 포함) ─────────────────
            base_messages = self._build_base_messages(
                question, candidates_text, schema_section, include_score_suffix=True
            )
            try:
                raw_reasoning = self._client.chat_completions(
                    messages=base_messages,
                    model=self._model_id,
                    temperature=IR_JUDGE_TEMPERATURE,
                    max_tokens=IR_JUDGE_REASONING_MAX_TOKENS,
                )
                reasoning = raw_reasoning.strip()

                m = _SCORE_LINE_RE.search(reasoning)
                if m:
                    parts = [p.strip() for p in m.group(1).split(",")]
                    try:
                        parsed = [float(p) for p in parts if p]
                    except ValueError:
                        parsed = []
                    if len(parsed) == n:
                        return parsed, reasoning
            except Exception as e:
                print(f"[WARN] LLM Judge CoT 실패: {e}", flush=True)

            # ── 2단계: CoT를 context로 주고 _judge_direct로 scores 재추출 ───────
            print("[INFO] LLM Judge 2단계 (자유 텍스트) 진입", flush=True)
            score_messages = base_messages + [
                msg for msg in [
                    {"role": "assistant", "content": reasoning} if reasoning else None,
                    {
                        "role": "user",
                        "content": f"위 분석을 바탕으로 {n}개 후보의 점수를 JSON으로만 응답하세요.",
                    },
                ] if msg
            ]
        else:
            # ── CoT 없이 바로 _judge_direct로 scores 추출 ────────────────────
            score_messages = self._build_base_messages(
                question, candidates_text, schema_section, include_score_suffix=False
            )

        scores = self._judge_direct(
            n,
            score_messages,
            valid_meta,
            temperature=IR_JUDGE_TEMPERATURE,
            max_tokens=IR_JUDGE_SCORE_MAX_TOKENS,
        )
        return scores, reasoning

    # ── (SL, IR) 쌍 평가 ──────────────────────────────────────────────────────

    def select_pairs(
        self,
        question: str,
        pairs: list[dict[str, Any]],
    ) -> tuple[int, str, list[dict[str, Any]]]:
        """
        (SL, IR) 쌍 후보 중 LLM Judge score가 가장 높은 것을 선택한다.

        Parameters
        ----------
        question : 원본 한국어 질문
        pairs    : [{"sl_json": str, "ir_raw": str, "ir": dict, ...}, ...]

        Returns
        -------
        (winner_idx, reasoning, pairs_with_scores)
        """
        n = len(pairs)
        if n == 0:
            return 0, "", pairs
        if n == 1:
            pairs[0]["score"] = 10.0
            pairs[0]["selected"] = True
            return 0, "", pairs

        candidates_text = "\n\n".join(
            self._build_pair_candidate_summary(i + 1, p)
            for i, p in enumerate(pairs)
        )

        reasoning = ""
        fallback_meta = [{"completeness": _completeness(p.get("ir") or {})} for p in pairs]

        if self._use_cot:
            # ── CoT: 자유 추론 후 SCORES 추출 ────────────────────────────────
            suffix = _REASONING_PROMPT_SUFFIX
            messages: list[dict[str, str]] = [
                {"role": "system", "content": _PAIR_SYSTEM_PROMPT},
                {"role": "user", "content": f"원본 질문: {question}\n{candidates_text}{suffix}"},
            ]
            try:
                raw_reasoning = self._client.chat_completions(
                    messages=messages,
                    model=self._model_id,
                    temperature=PAIR_JUDGE_TEMPERATURE,
                    max_tokens=PAIR_JUDGE_REASONING_MAX_TOKENS,
                )
                reasoning = raw_reasoning.strip()
                m = _SCORE_LINE_RE.search(reasoning)
                if m:
                    parts = [p.strip() for p in m.group(1).split(",")]
                    try:
                        parsed = [float(p) for p in parts if p]
                    except ValueError:
                        parsed = []
                    if len(parsed) == n:
                        for pair, score in zip(pairs, parsed):
                            pair["score"] = score
                            pair["selected"] = False
                        winner_idx = max(range(n), key=lambda i: parsed[i])
                        pairs[winner_idx]["selected"] = True
                        pairs[winner_idx]["reasoning"] = reasoning
                        return winner_idx, reasoning, pairs
                print(f"[WARN] Pair Judge CoT scores 파싱 실패: {raw_reasoning[:200]}", flush=True)
            except Exception as e:
                print(f"[WARN] Pair Judge CoT 호출 실패: {e}", flush=True)

            # CoT 파싱 실패 시 _judge_direct 스타일로 재추출
            score_messages: list[dict[str, str]] = messages + (
                [{"role": "assistant", "content": reasoning}] if reasoning else []
            )
        else:
            # ── CoT 없이 점수만 직접 추출 ────────────────────────────────────
            score_messages = [
                {"role": "system", "content": _PAIR_SYSTEM_PROMPT},
                {"role": "user", "content": f"원본 질문: {question}\n{candidates_text}"},
            ]

        # 직접 점수 추출 (use_cot=False 기본 경로 or CoT fallback)
        _score_msgs = [dict(msg) for msg in score_messages]
        if _score_msgs and _score_msgs[-1]["role"] == "user":
            _score_msgs[-1] = {
                "role": "user",
                "content": _score_msgs[-1]["content"] + (
                    f"\n\n{n}개 후보의 점수를 1~10 정수로 매기세요. "
                    f"반드시 다음 형식으로만 응답:\nSCORES: [점수1, 점수2, ...]"
                ),
            }
        try:
            raw = self._client.chat_completions(
                messages=_score_msgs,
                model=self._model_id,
                temperature=PAIR_JUDGE_TEMPERATURE,
                max_tokens=PAIR_JUDGE_SCORE_MAX_TOKENS,
            )
            m = _SCORE_LINE_RE.search(raw)
            if m:
                parts = [p.strip() for p in m.group(1).split(",")]
                try:
                    parsed = [float(p) for p in parts if p]
                except ValueError:
                    parsed = []
                if len(parsed) == n:
                    for pair, score in zip(pairs, parsed):
                        pair["score"] = score
                        pair["selected"] = False
                    winner_idx = max(range(n), key=lambda i: parsed[i])
                    pairs[winner_idx]["selected"] = True
                    return winner_idx, reasoning, pairs
            print(f"[WARN] Pair Judge scores 파싱 실패: {raw[:200]}", flush=True)
        except Exception as e:
            print(f"[WARN] Pair Judge 점수 추출 실패: {e}", flush=True)

        # Fallback: completeness 기반
        for i, p in enumerate(pairs):
            p["score"] = float(fallback_meta[i]["completeness"])
            p["selected"] = False
        winner_idx = max(range(n), key=lambda i: pairs[i]["score"])
        pairs[winner_idx]["selected"] = True
        return winner_idx, reasoning, pairs
