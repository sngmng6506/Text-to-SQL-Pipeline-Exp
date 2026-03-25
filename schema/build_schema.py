"""
스키마 임베딩 빌드

TEI(Text Embeddings Inference) 서버로 DB 스키마를 벡터화하여 .npz로 저장한다.
새 스키마 JSON이 추가되거나 변경될 때 1회 실행한다.

실행:
  python -m schema.build_schema --build
  python -m schema.build_schema --build --schema data/schema/table_schema_column_enum_description.json
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from clients.embed import default_embed_client
from config import SCHEMA_EMBEDDINGS_NPZ_PATH, SCHEMA_JSON_PATH

SCHEMA_FILE    = str(SCHEMA_JSON_PATH)
EMBEDDINGS_FILE = str(SCHEMA_EMBEDDINGS_NPZ_PATH)


# ---------------------------------------------------------------------------
# 임베딩 생성
# ---------------------------------------------------------------------------

def embed_texts(texts: list[str], *, batch_size: int = 8) -> np.ndarray:
    """TEI /embed API로 텍스트 임베딩 생성.

    Returns:
        (N, D) float32 numpy, L2 정규화됨
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


# ---------------------------------------------------------------------------
# 스키마 JSON 파싱
# ---------------------------------------------------------------------------

def _normalize_schema_entry(raw: dict) -> dict:
    """JSON 포맷이 달라도 통일된 dict로 변환.

    지원 포맷:
      A) 구포맷: {"table": ..., "schema_text": ..., "description": ...}
      B) 신포맷: {"table": ..., "description": ..., "columns": [...]}
         columns 항목에 "values" 필드가 있으면 [허용값: ...] 형태로 schema_text에 포함
    """
    if "schema_text" in raw:
        return raw

    table      = raw["table"]
    table_desc = raw.get("description", "")
    columns: list[dict] = raw.get("columns", [])

    # schema_text: 프롬프트(schema_linking, SQL gen)에 표시할 텍스트
    # values가 있는 컬럼은 허용값을 함께 표시하여 LLM이 정확한 값을 사용하도록 유도
    col_parts = []
    for col in columns:
        name  = col.get("name", "")
        ctype = col.get("type", "")
        cdesc = col.get("description", "")
        vals  = col.get("values")
        part  = f"{name} ({ctype})"
        if cdesc:
            part += f" — {cdesc}"
        if vals:
            part += f" [허용값: {', '.join(vals)}]"
        col_parts.append(part)
    schema_text = f"Table: {table} | Columns: {', '.join(col_parts)}"

    # embed_text: IR 임베딩에 사용할 텍스트 (컬럼 설명 풍부하게)
    col_lines = "\n".join(
        f"  - {col.get('name', '')}: {col.get('description', col.get('type', ''))}"
        for col in columns
    )
    embed_text = f"{table_desc}\n{col_lines}" if table_desc else col_lines

    return {
        "table":       table,
        "schema_text": schema_text,
        "description": table_desc,
        "embed_text":  embed_text,
    }


def load_schema(schema_path: str = SCHEMA_FILE) -> list[dict]:
    """스키마 JSON을 로드하고 통일된 포맷으로 정규화."""
    with open(schema_path, encoding="utf-8") as f:
        raw_list = json.load(f)
    return [_normalize_schema_entry(r) for r in raw_list]


# ---------------------------------------------------------------------------
# 임베딩 빌드 및 저장
# ---------------------------------------------------------------------------

def build_embeddings(
    schema_path: str = SCHEMA_FILE,
    output_path: str = EMBEDDINGS_FILE,
) -> tuple[np.ndarray, list[dict]]:
    """스키마를 numpy 배열로 임베딩하여 .npz로 저장.

    Returns:
        embeddings: (N, D) numpy 배열
        schemas: 정규화된 스키마 리스트
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    schemas = load_schema(schema_path)

    texts = []
    for s in schemas:
        if s.get("embed_text"):
            texts.append(s["embed_text"])
        else:
            t = s["schema_text"]
            if s.get("description"):
                t += "\n" + s["description"]
            texts.append(t)

    print(f"임베딩 생성 중 ({len(texts)}개 스키마)...")
    embeddings = embed_texts(texts)

    np.savez(
        output_path,
        embeddings=embeddings,
        tables=[s["table"] for s in schemas],
        schema_texts=[s["schema_text"] for s in schemas],
        descriptions=[s.get("description", "") for s in schemas],
    )
    print(f"저장 완료: {output_path} ({embeddings.shape})")

    return embeddings, schemas


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    import argparse

    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="스키마 임베딩 빌드")
    parser.add_argument("--build", action="store_true", help="임베딩 구축")
    parser.add_argument("--schema", type=str, default=SCHEMA_FILE, help="스키마 JSON 경로")
    parser.add_argument("--output", type=str, default=EMBEDDINGS_FILE, help="출력 NPZ 경로")
    args = parser.parse_args()

    if args.build:
        build_embeddings(args.schema, args.output)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
