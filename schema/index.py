"""
스키마 임베딩 및 질의 매핑

TEI(Text Embeddings Inference) 서버의 /embed API를 사용하여
DB 스키마를 벡터화하고, 자연어 질의가 들어오면 관련 스키마를 검색합니다.

환경변수:
  TEI_BASE_URL  - TEI 서버 주소 (예: http://172.22.51.221:8080)
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from clients.embed import default_embed_client
from config import SCHEMA_JSON_PATH, SCHEMA_EMBEDDINGS_NPZ_PATH

SCHEMA_FILE = str(SCHEMA_JSON_PATH)
EMBEDDINGS_FILE = str(SCHEMA_EMBEDDINGS_NPZ_PATH)

# Query 쪽은 instruction-aware 임베딩 권장 (영문 권장)
_QUERY_TASK_INSTRUCT = "Given a database schema search query, retrieve relevant tables and columns."


def _get_query_text(query: str) -> str:
    return f"Instruct: {_QUERY_TASK_INSTRUCT}\nQuery:{query}"


def embed_texts(texts: list[str], *, batch_size: int = 32) -> np.ndarray:
    """
    TEI /embed API로 텍스트 임베딩 생성.
    반환: (N, D) float32 numpy (L2 정규화됨)
    """
    client = default_embed_client()
    outs: list[np.ndarray] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        vectors = client.embeddings(batch)
        arr = np.array(vectors, dtype=np.float32)
        norms = np.linalg.norm(arr, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        outs.append(arr / norms)
    return np.concatenate(outs, axis=0)


def load_schema(schema_path: str = SCHEMA_FILE) -> list[dict]:
    """table_schema.json에서 스키마 로드"""
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_embeddings(
    schema_path: str = SCHEMA_FILE,
    output_path: str = EMBEDDINGS_FILE,
) -> tuple[np.ndarray, list[dict]]:
    """
    스키마를 numpy 배열로 임베딩하여 .npz로 저장

    Returns:
        embeddings: (N, D) 형태의 numpy 배열
        schemas: 스키마 리스트 (table, schema_text)
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    schemas = load_schema(schema_path)
    texts = [s["schema_text"] for s in schemas]

    print(f"임베딩 생성 중 ({len(texts)}개 스키마)...")
    embeddings = embed_texts(texts)

    np.savez(
        output_path,
        embeddings=embeddings,
        tables=[s["table"] for s in schemas],
        schema_texts=texts,
    )
    print(f"저장 완료: {output_path} ({embeddings.shape})")

    return embeddings, schemas


def load_embeddings(npz_path: str = EMBEDDINGS_FILE) -> tuple[np.ndarray, list[str], list[str]]:
    """저장된 임베딩 로드"""
    data = np.load(npz_path, allow_pickle=True)
    embeddings = data["embeddings"]
    tables = data["tables"].tolist()
    schema_texts = data["schema_texts"].tolist()
    return embeddings, tables, schema_texts


def query_schema(
    query: str,
    embeddings_path: str = EMBEDDINGS_FILE,
    top_k: int = 5,
) -> list[dict]:
    """
    자연어 질의와 관련된 스키마를 유사도 순으로 반환

    Args:
        query: 사용자 질의 (예: "장비 알람 이력 조회")
        embeddings_path: 사전 구축된 스키마 임베딩 .npz 파일
        top_k: 반환할 상위 개수

    Returns:
        [{"table": str, "schema_text": str, "score": float}, ...]
    """
    embeddings, tables, schema_texts = load_embeddings(embeddings_path)

    query_text = _get_query_text(query)
    query_embedding = embed_texts([query_text])[0]

    # 코사인 유사도 (이미 정규화됨 → 내적 = 코사인)
    scores = np.dot(embeddings, query_embedding.T).flatten()
    top_indices = np.argsort(scores)[::-1][:top_k]

    return [
        {
            "table": tables[i],
            "schema_text": schema_texts[i],
            "score": float(scores[i]),
        }
        for i in top_indices
    ]


def main() -> None:
    import argparse
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true", help="임베딩 구축")
    parser.add_argument("--query", type=str, help="질의 예시 실행")
    parser.add_argument("--top-k", type=int, default=10, help="반환할 상위 개수")
    args = parser.parse_args()

    if args.build:
        build_embeddings()
        return

    if args.query:
        results = query_schema(args.query, top_k=args.top_k)
        print(f"\n질의: {args.query}\n")
        for i, r in enumerate(results, 1):
            print(f"{i}. {r['table']} (score: {r['score']:.4f})")
            print(f"   {r['schema_text'][:100]}...")
        return

    print("사용법:")
    print("  python -m schema.index --build          # 임베딩 구축")
    print("  python -m schema.index --query '장비 알람 이력'  # 질의 매핑")


if __name__ == "__main__":
    main()
