"""
로컬 transformers 기반 Chat 클라이언트

VllmChatClient와 동일한 인터페이스(chat_completions / chat_completions_n)를 제공하므로
pipelines/simple.py 외의 기존 코드도 그대로 사용 가능.

특징:
  - 모델을 지연 초기화(최초 호출 시 1회 로드)
  - chat_completions_n → num_return_sequences 로 GPU/CPU 단일 패스 병렬 생성
  - device 기본값 "cpu" (테스트 환경 대응)
"""
from __future__ import annotations

from typing import Any


class LocalChatClient:
    """transformers AutoModelForCausalLM 기반 로컬 추론 클라이언트."""

    def __init__(
        self,
        model_path: str,
        device: str = "cpu",
        torch_dtype: str = "float32",
    ) -> None:
        """
        Parameters
        ----------
        model_path  : 로컬 모델 디렉터리 경로
        device      : "cpu" | "cuda" | "cuda:0" | "cuda:1" 등
        torch_dtype : "float32" | "bfloat16" | "float16"
                      CPU는 float32, GPU는 bfloat16 권장
        """
        self._model_path = model_path
        self._device = device
        self._torch_dtype_str = torch_dtype
        self._tokenizer: Any = None
        self._model: Any = None

    # ── 지연 초기화 ──────────────────────────────────────────────────────────

    def _ensure_loaded(self) -> None:
        """최초 호출 시 모델·토크나이저를 로드한다."""
        if self._model is not None:
            return

        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        dtype_map = {
            "float32": torch.float32,
            "bfloat16": torch.bfloat16,
            "float16": torch.float16,
        }
        torch_dtype = dtype_map.get(self._torch_dtype_str, torch.float32)

        print(f"[LocalChatClient] 모델 로딩: {self._model_path}", flush=True)
        print(f"  device={self._device}, dtype={self._torch_dtype_str}", flush=True)

        self._tokenizer = AutoTokenizer.from_pretrained(
            self._model_path,
            trust_remote_code=True,
        )

        self._model = AutoModelForCausalLM.from_pretrained(
            self._model_path,
            torch_dtype=torch_dtype,
            device_map={"": self._device},
            trust_remote_code=True,
        )
        self._model.eval()
        print("[LocalChatClient] 모델 로딩 완료", flush=True)

    # ── 내부 생성 헬퍼 ───────────────────────────────────────────────────────

    def _generate(
        self,
        messages: list[dict[str, str]],
        *,
        n: int = 1,
        temperature: float = 0.0,
        max_new_tokens: int = 512,
    ) -> list[str]:
        """
        messages를 chat template으로 변환 후 num_return_sequences=n 으로 생성.

        Returns
        -------
        n개의 생성 텍스트 (입력 프롬프트 제외)
        """
        import torch

        self._ensure_loaded()

        text = self._tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True,
        )
        inputs = self._tokenizer(text, return_tensors="pt")
        inputs = {k: v.to(self._device) for k, v in inputs.items()}
        input_len = inputs["input_ids"].shape[1]

        do_sample = (temperature > 0.0) and (n >= 1)
        gen_kwargs: dict[str, Any] = {
            "max_new_tokens": max_new_tokens,
            "num_return_sequences": n,
            "do_sample": do_sample,
            "pad_token_id": self._tokenizer.eos_token_id,
        }
        if do_sample:
            gen_kwargs["temperature"] = temperature

        with torch.no_grad():
            output_ids = self._model.generate(**inputs, **gen_kwargs)

        results: list[str] = []
        for seq in output_ids:
            # 입력 토큰 제거 → 생성 부분만 디코딩
            new_ids = seq[input_len:]
            text_out = self._tokenizer.decode(new_ids, skip_special_tokens=True)
            results.append(text_out.strip())
        return results

    # ── 공개 API (VllmChatClient 호환) ──────────────────────────────────────

    def chat_completions(
        self,
        *,
        messages: list[dict[str, str]],
        model: str | None = None,          # 무시 (로컬 고정)
        temperature: float = 0.0,
        max_tokens: int = 512,
        extra: dict[str, Any] | None = None,  # 무시 (guided_json 등 미지원)
        **_: Any,
    ) -> str:
        """단일 응답 생성."""
        results = self._generate(
            messages,
            n=1,
            temperature=temperature,
            max_new_tokens=max_tokens,
        )
        return results[0]

    def chat_completions_n(
        self,
        *,
        messages: list[dict[str, str]],
        model: str | None = None,
        n: int = 3,
        temperature: float = 0.5,
        max_tokens: int = 512,
        extra: dict[str, Any] | None = None,
        **_: Any,
    ) -> list[str]:
        """
        n개 후보를 num_return_sequences로 한 번의 패스에서 병렬 생성.

        temperature=0 이면 모든 후보가 동일해지므로 강제로 0.3 이상으로 보정.
        """
        effective_temp = max(temperature, 0.3)
        return self._generate(
            messages,
            n=n,
            temperature=effective_temp,
            max_new_tokens=max_tokens,
        )


# ── 기본 인스턴스 생성 헬퍼 ──────────────────────────────────────────────────

def default_local_client(model_path: str, device: str = "cpu") -> LocalChatClient:
    """CPU 기본값으로 LocalChatClient를 생성한다."""
    return LocalChatClient(
        model_path=model_path,
        device=device,
        torch_dtype="float32" if device == "cpu" else "bfloat16",
    )
