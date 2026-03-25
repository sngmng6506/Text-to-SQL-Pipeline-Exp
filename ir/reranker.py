"""
IR Reranker Service

Cross Encoder를 사용해 IR 후보 중 원본 질문과 가장 관련 높은 하나를 선택한다.

사용 흐름:
    reranker = IRRerankerService(model_path)
    selected_json, meta = reranker.select(question, raw_candidates)

(질문, IR JSON 직렬화 문자열) 쌍을 Cross Encoder에 배치 입력하여
relevance score를 계산하고 최고 점수 후보를 반환한다.
"""
from __future__ import annotations

from typing import Any

from ir.selector import _parse_ir, _completeness, _FIELD_ORDER
import json


class IRRerankerService:
    """Cross Encoder 기반 IR 후보 선택 서비스."""

    def __init__(self, model_path: str) -> None:
        self._model_path = model_path
        self._tokenizer: Any = None
        self._model: Any = None
        self._device: str = "cpu"

    def _ensure_loaded(self) -> None:
        """모델이 로드되지 않았으면 로드한다 (지연 초기화)."""
        if self._model is not None:
            return

        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        import torch

        # transformers 5.x에서 제거된 함수를 jina-reranker 커스텀 코드가 요구함
        # monkey patch로 호환성 유지
        import transformers.models.xlm_roberta.modeling_xlm_roberta as _xlm_mod
        if not hasattr(_xlm_mod, "create_position_ids_from_input_ids"):
            def _create_position_ids_from_input_ids(
                input_ids, padding_idx, past_key_values_length: int = 0
            ):
                mask = input_ids.ne(padding_idx).int()
                incremental_indices = (
                    torch.cumsum(mask, dim=1).type_as(mask) + past_key_values_length
                ) * mask
                return incremental_indices.long() + padding_idx
            _xlm_mod.create_position_ids_from_input_ids = _create_position_ids_from_input_ids

        self._tokenizer = AutoTokenizer.from_pretrained(
            self._model_path,
            trust_remote_code=True,
        )
        import transformers as _tf
        _major = int(_tf.__version__.split(".")[0])
        _load_kwargs: dict[str, Any] = {"trust_remote_code": True}
        if _major >= 5:
            _load_kwargs["dtype"] = "auto"
        else:
            _load_kwargs["torch_dtype"] = "auto"
        self._model = AutoModelForSequenceClassification.from_pretrained(
            self._model_path,
            **_load_kwargs,
        )
        self._device = "cuda" if torch.cuda.is_available() else "cpu"
        self._model = self._model.to(self._device)
        self._model.eval()

    def select(
        self,
        question: str,
        raw_candidates: list[str],
    ) -> tuple[str, list[dict[str, Any]]]:
        """
        IR 후보 중 Cross Encoder score가 가장 높은 것을 선택한다.

        Parameters
        ----------
        question       : 원본 한국어 질문
        raw_candidates : LLM이 반환한 IR raw 문자열 목록

        Returns
        -------
        (selected_json_str, meta_list)
            - selected_json_str : 선택된 IR의 정규화된 JSON 문자열
            - meta_list         : 각 후보의 메타
                                  (idx, raw, ir, natural, score, selected)
        """
        import torch

        self._ensure_loaded()

        meta: list[dict[str, Any]] = []
        for i, raw in enumerate(raw_candidates):
            ir = _parse_ir(raw)
            ir_str = json.dumps(ir, ensure_ascii=False) if ir else ""
            meta.append({
                "idx": i,
                "raw": raw,
                "ir": ir,
                "ir_str": ir_str,
                "completeness": _completeness(ir) if ir else 0,
                "score": float("-inf"),
                "selected": False,
            })

        valid = [m for m in meta if m["ir"]]
        if not valid:
            meta[0]["selected"] = True
            return raw_candidates[0], meta

        pairs = [[question, m["ir_str"]] for m in valid]
        with torch.no_grad():
            inputs = self._tokenizer(
                pairs,
                padding=True,
                truncation=True,
                return_tensors="pt",
                max_length=512,
            )
            inputs = {k: v.to(self._device) for k, v in inputs.items()}
            logits = self._model(**inputs, return_dict=True).logits
            scores = logits.view(-1).float().tolist()

        for m, score in zip(valid, scores):
            m["score"] = round(score, 4)

        winner = max(valid, key=lambda m: m["score"])
        winner["selected"] = True

        selected_str = json.dumps(
            {k: winner["ir"].get(k, []) for k in _FIELD_ORDER},
            ensure_ascii=False,
        )
        return selected_str, meta
