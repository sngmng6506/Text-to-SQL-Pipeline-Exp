"""
vLLM(OpenAI-compatible) Chat Completions 클라이언트

- endpoint 예: http://172.22.51.221:8000/v1/chat/completions
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import requests


def _extract_one_line_from_reasoning(reasoning: str) -> str | None:
    """
    vLLM reasoning 모드에서 message.content=None, message.reasoning만 채워지는 경우가 있어
    reasoning 문자열에서 '최종 한 줄'을 최대한 안전하게 뽑아낸다.
    """
    if not reasoning:
        return None
    text = reasoning.strip()

    # 1) 'Rewritten Question:' 라인이 있으면 그 뒤 한 줄
    key = "Rewritten Question:"
    if key in text:
        tail = text.split(key, 1)[1].strip()
        return tail.splitlines()[0].strip() if tail else None

    # 2) 마지막으로 "<table.column = '...'>" 패턴이 포함된 라인 우선
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for ln in reversed(lines):
        if "<" in ln and ">" in ln and "=" in ln:
            return ln

    # 3) 그 외에는 첫 줄만(너무 길게 나오는 걸 방지)
    return lines[0] if lines else None


@dataclass(frozen=True)
class VllmChatClient:
    base_url: str
    api_key: str | None = None
    timeout_sec: int = 60

    def _api_v1_base(self) -> str:
        """
        base_url이
        - http://host:port 인 경우 → http://host:port/v1
        - http://host:port/v1 인 경우 → 그대로 사용
        """
        base = self.base_url.rstrip("/")
        return base if base.endswith("/v1") else f"{base}/v1"

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    def chat_completions(
        self,
        *,
        messages: list[dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
        extra: Optional[dict[str, Any]] = None,
    ) -> str:
        """
        POST /v1/chat/completions
        반환: assistant content 문자열
        """
        url = f"{self._api_v1_base()}/chat/completions"

        payload: dict[str, Any] = {
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": False,
        }
        if model:
            payload["model"] = model
        if extra:
            payload.update(extra)

        r = requests.post(url, headers=self._headers(), json=payload, timeout=self.timeout_sec)
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code} {url}\n{r.text}")
        data = r.json()

        try:
            choice0 = data["choices"][0]
            msg = choice0.get("message")
            if isinstance(msg, dict) and isinstance(msg.get("content"), str):
                return msg["content"]
            # 일부 reasoning 모드에서 content=None, reasoning만 채워짐
            if isinstance(msg, dict) and msg.get("content") is None and isinstance(msg.get("reasoning"), str):
                extracted = _extract_one_line_from_reasoning(msg["reasoning"])
                if extracted:
                    return extracted
        except Exception:
            pass
        raise RuntimeError(f"Unexpected chat/completions response shape: {data}")


def default_vllm_client() -> VllmChatClient:
    base_url = os.getenv("VLLM_CHAT_BASE_URL") or os.getenv("VLLM_BASE_URL", "http://172.22.51.221:8000")
    api_key = os.getenv("VLLM_API_KEY")
    timeout_sec = int(os.getenv("VLLM_TIMEOUT_SEC", "60"))
    return VllmChatClient(base_url=base_url, api_key=api_key, timeout_sec=timeout_sec)
