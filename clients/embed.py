"""
TEI(Text Embeddings Inference) 클라이언트

- endpoint 예: http://172.22.51.221:8080/embed
- payload: {"input": ["text1", "text2", ...]}
- model 필드 없음 (TEI는 서버 시작 시 모델이 고정됨)
"""

from __future__ import annotations

import os
from dataclasses import dataclass

import requests


@dataclass(frozen=True)
class TeiEmbedClient:
    base_url: str
    timeout_sec: int = 60

    def _embed_url(self) -> str:
        return self.base_url.rstrip("/") + "/embed"

    def embeddings(self, texts: list[str]) -> list[list[float]]:
        """
        POST /embed
        반환: [[float, ...], ...] — texts 순서와 동일
        """
        url = self._embed_url()
        r = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json={"inputs": texts},
            timeout=self.timeout_sec,
        )
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code} {url}\n{r.text}")
        data = r.json()

        # TEI 응답: [[float, ...], ...] 리스트 직접 반환
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
            return data

        # OpenAI 호환 형식 fallback: {"data": [{"embedding": [...], "index": N}, ...]}
        if isinstance(data, dict) and "data" in data:
            items = sorted(data["data"], key=lambda x: x["index"])
            return [item["embedding"] for item in items]

        raise RuntimeError(f"Unexpected TEI embeddings response: {data}")


def default_embed_client() -> TeiEmbedClient:
    base_url = os.getenv("TEI_BASE_URL", "http://172.22.51.221:8080")
    timeout_sec = int(os.getenv("VLLM_TIMEOUT_SEC", "60"))
    return TeiEmbedClient(base_url=base_url, timeout_sec=timeout_sec)
