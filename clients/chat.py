"""
vLLM(OpenAI-compatible) Chat Completions
endpoint: {base_url}/chat/completions (base_url에 /v1 포함 가정)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import time

import requests
from requests.exceptions import ConnectionError as RequestsConnectionError


@dataclass(frozen=True)
class VllmChatClient:
    base_url: str  # e.g. http://host:8000/v1
    api_key: str | None = None
    timeout_sec: int = 60

    def chat_completions(
        self,
        *,
        messages: list[dict[str, str]],
        model: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
        extra: Optional[dict[str, Any]] = None,
    ) -> str:
        url = f"{self.base_url.rstrip('/')}/chat/completions"
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

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=self.timeout_sec)
                break
            except RequestsConnectionError as e:
                last_exc = e
                wait = 2 ** attempt
                print(f"[WARN] 연결 오류 (시도 {attempt+1}/3), {wait}s 후 재시도...", flush=True)
                time.sleep(wait)
        else:
            raise RuntimeError(f"연결 실패 (3회 재시도 초과): {last_exc}")

        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code} {url}\n{r.text}")
        data = r.json()

        choice = data.get("choices", [{}])[0]
        finish_reason = choice.get("finish_reason", "")
        if finish_reason == "length":
            print(f"[WARN] 응답이 max_tokens에서 잘림 (finish_reason=length, max_tokens={max_tokens})", flush=True)
        msg = choice.get("message") or {}
        content = msg.get("content")
        if isinstance(content, str):
            return content
        raise RuntimeError(f"Unexpected response: {data}")

    def chat_completions_n(
        self,
        *,
        messages: list[dict[str, str]],
        model: Optional[str] = None,
        n: int = 3,
        temperature: float = 0.4,
        max_tokens: int = 512,
        extra: Optional[dict[str, Any]] = None,
    ) -> list[str]:
        """n개의 완성 후보를 한 번의 요청으로 반환한다 (vLLM n 파라미터 활용)."""
        url = f"{self.base_url.rstrip('/')}/chat/completions"
        payload: dict[str, Any] = {
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "n": n,
            "stream": False,
        }
        if model:
            payload["model"] = model
        if extra:
            payload.update(extra)

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        last_exc: Exception | None = None
        for attempt in range(3):
            try:
                r = requests.post(url, headers=headers, json=payload, timeout=self.timeout_sec)
                break
            except RequestsConnectionError as e:
                last_exc = e
                wait = 2 ** attempt
                print(f"[WARN] 연결 오류 (시도 {attempt+1}/3), {wait}s 후 재시도...", flush=True)
                time.sleep(wait)
        else:
            raise RuntimeError(f"연결 실패 (3회 재시도 초과): {last_exc}")

        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code} {url}\n{r.text}")
        data = r.json()

        results: list[str] = []
        for choice in data.get("choices", []):
            msg = (choice.get("message") or {})
            content = msg.get("content")
            if isinstance(content, str):
                results.append(content)
        if not results:
            raise RuntimeError(f"Unexpected response (no choices): {data}")
        return results


def default_vllm_client() -> VllmChatClient:
    base = os.getenv("VLLM_CHAT_BASE_URL") or os.getenv("VLLM_BASE_URL", "http://172.22.51.221:8000")
    base_url = base if base.rstrip("/").endswith("/v1") else f"{base.rstrip('/')}/v1"
    return VllmChatClient(
        base_url=base_url,
        api_key=os.getenv("VLLM_API_KEY"),
        timeout_sec=int(os.getenv("VLLM_TIMEOUT_SEC", "180")),
    )


