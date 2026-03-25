"""
MQS(Masked Question Similarity) Few-Shot 검색

rewritten_question에서 <table.column = '설명'> 태그를 [COL]로 마스킹한 뒤
pool_embeddings.npz의 question_ko 임베딩과 코사인 유사도를 계산하여
상위 K개의 (question_ko, sql) 쌍을 반환한다.
"""

from __future__ import annotations

import re
import numpy as np

from clients.embed import default_embed_client

# <table.column = '설명'> 또는 <table.column 연산자 값> 형태 모두 마스킹
_TAG_RE = re.compile(r"<[^>]+>")

# pool 캐시 (언어별, 프로세스 내 최초 1회만 로드)
_pool_cache: tuple[np.ndarray, list[str], list[str], list[str], list[str]] | None = None
_pool_en_cache: tuple[np.ndarray, list[str], list[str], list[str], list[str]] | None = None


def mask_rewritten_question(rewritten: str) -> str:
    """
    rewritten_question의 <…> 태그를 [COL]로 치환.

    예:
      <crane_cell.equipment_id = '크레인 장비'>  →  [COL]
      <products.stock_quantity < 10>             →  [COL]
    """
    return _TAG_RE.sub("[COL]", rewritten).strip()


def _load_pool_en(npz_path: str) -> tuple[np.ndarray, list[str], list[str], list[str], list[str]]:
    """pool_embeddings_en.npz 로드 (캐시). 영어 임베딩 풀 전용."""
    global _pool_en_cache
    if _pool_en_cache is None:
        data = np.load(npz_path, allow_pickle=True)
        embeddings    = data["embeddings"].astype(np.float32)
        questions_en  = data["questions_en"].tolist()           # 임베딩 검색용 (masked)
        sqls          = data["sqls"].tolist()
        masked_sqls   = data["masked_sqls"].tolist() if "masked_sqls" in data else sqls
        orig_q_en     = (
            data["original_questions_en"].tolist()
            if "original_questions_en" in data
            else questions_en
        )
        _pool_en_cache = (embeddings, questions_en, sqls, masked_sqls, orig_q_en)
    return _pool_en_cache


def _load_pool(npz_path: str) -> tuple[np.ndarray, list[str], list[str], list[str], list[str]]:
    """pool_embeddings.npz 로드 (캐시)."""
    global _pool_cache
    if _pool_cache is None:
        data = np.load(npz_path, allow_pickle=True)
        embeddings   = data["embeddings"].astype(np.float32)
        questions_ko = data["questions_ko"].tolist()          # 임베딩 검색용 (masked)
        sqls         = data["sqls"].tolist()
        masked_sqls  = data["masked_sqls"].tolist() if "masked_sqls" in data else sqls
        # 원본 한국어 질문 (LLM 표시용); 없으면 questions_ko로 fallback
        orig_questions_ko = (
            data["original_questions_ko"].tolist()
            if "original_questions_ko" in data
            else questions_ko
        )
        _pool_cache = (embeddings, questions_ko, sqls, masked_sqls, orig_questions_ko)
    return _pool_cache


def retrieve_few_shots(
    rewritten: str,
    npz_path: str,
    top_k: int = 3,
) -> list[dict]:
    """
    rewritten_question을 마스킹 → 임베딩 → pool과 유사도 계산 → top-K 반환.

    Args:
        rewritten: 재작성된 질문 (태그 포함)
        npz_path:  pool_embeddings.npz 경로
        top_k:     반환할 예시 수

    Returns:
        [{"question_ko": str, "sql": str, "masked_sql": str}, ...]  유사도 높은 순
    """
    masked = mask_rewritten_question(rewritten)

    client = default_embed_client()
    vec = client.embeddings([masked])[0]
    query_vec = np.array(vec, dtype=np.float32)

    # L2 정규화 (pool과 동일하게 맞춤)
    norm = np.linalg.norm(query_vec)
    if norm > 0:
        query_vec = query_vec / norm

    embeddings, questions_ko, sqls, masked_sqls, orig_questions_ko = _load_pool(npz_path)

    # 코사인 유사도 = 내적 (양측 모두 L2 정규화됨)
    scores = embeddings @ query_vec
    top_indices = np.argsort(scores)[::-1][:top_k]

    return [
        {
            "question_ko": orig_questions_ko[i],  # 원본 한국어 (LLM 표시용)
            "sql":         sqls[i],
            "masked_sql":  masked_sqls[i],
        }
        for i in top_indices
    ]


def retrieve_few_shots_en(
    question_en: str,
    npz_path: str,
    top_k: int = 3,
) -> list[dict]:
    """
    영어 질의를 임베딩 → 영어 풀(pool_embeddings_en.npz)과 유사도 계산 → top-K 반환.

    Args:
        question_en: 영어로 번역된 질의
        npz_path:    pool_embeddings_en.npz 경로
        top_k:       반환할 예시 수

    Returns:
        [{"question_en": str, "sql": str, "masked_sql": str}, ...]  유사도 높은 순
    """
    client = default_embed_client()
    vec = client.embeddings([question_en])[0]
    query_vec = np.array(vec, dtype=np.float32)

    norm = np.linalg.norm(query_vec)
    if norm > 0:
        query_vec = query_vec / norm

    embeddings, questions_en, sqls, masked_sqls, orig_q_en = _load_pool_en(npz_path)

    scores = embeddings @ query_vec
    top_indices = np.argsort(scores)[::-1][:top_k]

    return [
        {
            "question_en": orig_q_en[i],  # 원본 영어 (LLM 표시용)
            "sql":         sqls[i],
            "masked_sql":  masked_sqls[i],
        }
        for i in top_indices
    ]
