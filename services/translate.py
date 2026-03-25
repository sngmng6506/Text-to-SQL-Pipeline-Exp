"""
translate.py

한국어 질의를 영어로 번역하는 유틸리티.

- gpt-oss 등 LLM을 이용해 도메인 용어를 보존하면서 번역한다.
- 실패 시 None을 반환하며 파이프라인을 중단하지 않는다.
"""

from __future__ import annotations

from typing import Any

_SYSTEM_PROMPT = (
    "You are a translator. "
    "Translate the Korean database query into concise English. "
    "Preserve domain-specific terms (table/column names, status codes, etc.) as-is. "
    "Output only the translated sentence, nothing else."
)


def translate_to_english(
    question: str,
    client: Any,
    model_id: str | None = None,
    max_tokens: int = 128,
    temperature: float = 0.0,
) -> str | None:
    """
    Korean 질의를 English로 번역한다.

    Parameters
    ----------
    question   : 번역할 한국어 질의
    client     : VllmChatClient (chat_completions 메서드 보유)
    model_id   : 사용할 모델명 (None이면 client 기본값)
    max_tokens : 번역 결과 최대 토큰 수
    temperature: 생성 temperature (번역은 결정론적으로 0.0 권장)

    Returns
    -------
    번역된 영어 문자열, 또는 실패 시 None
    """
    try:
        messages = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ]
        result = client.chat_completions(
            messages=messages,
            model=model_id,
            temperature=temperature,
            max_tokens=max_tokens,
            extra={"chat_template_kwargs": {"enable_thinking": False}},
        )
        translated = (result or "").strip()
        return translated if translated else None
    except Exception:
        return None
