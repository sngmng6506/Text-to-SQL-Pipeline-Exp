"""
스키마 임베딩 검색

사전 빌드된 .npz 임베딩을 로드하여 자연어 질의와 관련된 스키마를 검색한다.
런타임에서 매 질의마다 호출된다.

실행:
  python -m schema.search --query "장비 알람 이력"
  python -m schema.search --query "처리되지 않은 알람 수" --top-k 5
"""

from __future__ import annotations

import numpy as np

from config import SCHEMA_EMBEDDINGS_NPZ_PATH
from schema.build_schema import embed_texts

EMBEDDINGS_FILE = str(SCHEMA_EMBEDDINGS_NPZ_PATH)


# ---------------------------------------------------------------------------
# 임베딩 로드
# ---------------------------------------------------------------------------

def load_embeddings(
    npz_path: str = EMBEDDINGS_FILE,
) -> tuple[np.ndarray, list[str], list[str], list[str]]:
    """저장된 임베딩 로드.

    Returns:
        embeddings, tables, schema_texts, descriptions
    """
    data = np.load(npz_path, allow_pickle=True)
    embeddings   = data["embeddings"]
    tables       = data["tables"].tolist()
    schema_texts = data["schema_texts"].tolist()
    descriptions = (
        data["descriptions"].tolist()
        if "descriptions" in data
        else [""] * len(tables)
    )
    return embeddings, tables, schema_texts, descriptions


# ---------------------------------------------------------------------------
# 유사도 검색
# ---------------------------------------------------------------------------

def query_schema(
    query: str,
    embeddings_path: str = EMBEDDINGS_FILE,
    top_k: int = 5,
) -> list[dict]:
    """자연어 질의와 관련된 스키마를 유사도 순으로 반환.

    Args:
        query: 사용자 질의 (예: "장비 알람 이력 조회")
        embeddings_path: 사전 구축된 스키마 임베딩 .npz 파일
        top_k: 반환할 상위 개수

    Returns:
        [{"table": str, "schema_text": str, "description": str, "score": float}, ...]
    """
    embeddings, tables, schema_texts, descriptions = load_embeddings(embeddings_path)

    query_embedding = embed_texts([query])[0]

    # 코사인 유사도 (이미 L2 정규화됨 → 내적 = 코사인)
    scores = np.dot(embeddings, query_embedding.T).flatten()
    top_indices = np.argsort(scores)[::-1][:top_k]

    return [
        {
            "table":       tables[i],
            "schema_text": schema_texts[i],
            "description": descriptions[i],
            "score":       float(scores[i]),
        }
        for i in top_indices
    ]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="스키마 유사도 검색")
    parser.add_argument("--query", type=str, required=True, help="검색할 자연어 질의")
    parser.add_argument("--top-k", type=int, default=10, help="반환할 상위 개수")
    parser.add_argument("--npz", type=str, default=EMBEDDINGS_FILE, help="NPZ 파일 경로")
    args = parser.parse_args()

    results = query_schema(args.query, embeddings_path=args.npz, top_k=args.top_k)
    print(f"\n질의: {args.query}\n")
    for i, r in enumerate(results, 1):
        print(f"{i}. {r['table']} (score: {r['score']:.4f})")
        print(f"   {r['schema_text'][:120]}...")


if __name__ == "__main__":
    main()
