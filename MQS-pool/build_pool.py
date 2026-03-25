"""
MQS Pool Builder

Spider 데이터를 파싱하여 질문/SQL을 분리하고,
OpenAI로 한글 번역 후 TEI로 임베딩하여 pool 파일을 생성합니다.

train 소스의 경우 query_toks + question_toks를 활용해 DB 특화 토큰을 마스킹하고,
masked_question_ko를 임베딩합니다 (DAIL-SQL MQS 방식).

폴더 구조:
  MQS-pool/spider/
    ├── dev.sql               ← 원본
    ├── train_spider.json     ← 원본
    ├── dev/                  ← dev pool 산출물
    │   ├── questions_en.txt
    │   ├── sqls.txt
    │   ├── pool.json
    │   └── pool_embeddings.npz
    └── train/                ← train pool 산출물
        ├── questions_en.txt
        ├── sqls.txt
        ├── pool.json
        └── pool_embeddings.npz

실행 (TAG-test 루트에서):
  python MQS-pool/main_build_pool.py                        # dev, 처음부터
  python MQS-pool/main_build_pool.py --source train         # train, 처음부터
  python MQS-pool/main_build_pool.py --source train --resume
  python MQS-pool/main_build_pool.py --source train --embed-only
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
from dotenv import load_dotenv

# ── 경로 설정 ────────────────────────────────────────────────
_root = Path(__file__).resolve().parent.parent   # TAG-test 루트
sys.path.insert(0, str(_root))

SPIDER_DIR      = Path(__file__).resolve().parent / "spider"
DEV_SQL_PATH    = SPIDER_DIR / "dev.sql"
TRAIN_JSON_PATH = SPIDER_DIR / "train_spider.json"

load_dotenv(_root / "eval-analysis" / ".env")
load_dotenv(_root / ".env")

OPENAI_API_KEY    = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL      = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OPENAI_TIMEOUT    = int(os.getenv("OPENAI_TIMEOUT_SEC", "60"))
TRANSLATE_BATCH   = 25   # 배치 크기 (10 → 25)
TRANSLATE_WORKERS = 4    # 병렬 스레드 수
EMBED_BATCH       = 8    # TEI 서버 최대 배치 크기
EMBED_WORKERS     = 4    # TEI 병렬 요청 수
EMBED_SAVE_EVERY  = 500  # N건마다 npz 중간 저장 (중단 후 이어하기용)


def _out_dir(source: str) -> Path:
    """소스별 출력 디렉토리"""
    d = SPIDER_DIR / source
    d.mkdir(parents=True, exist_ok=True)
    return d


# ── 1. 마스킹 ────────────────────────────────────────────────

_SQL_KEYWORDS = {
    "select", "from", "where", "group", "by", "order", "having",
    "join", "on", "as", "and", "or", "not", "in", "like", "is", "null",
    "between", "exists", "all", "any", "case", "when", "then", "else", "end",
    "intersect", "union", "except", "with", "distinct", "limit", "offset",
    "count", "sum", "avg", "max", "min",
    "asc", "desc",
    "inner", "outer", "left", "right", "cross", "natural", "full",
    "(", ")", ",", "*", "+", "-", "/",
    ">", "<", "=", ">=", "<=", "!=", "<>", ";",
    "t1", "t2", "t3", "t4", "t5",
    "value",                     # query_toks_no_value placeholder
}

_NUM_RE = re.compile(r"^\d+(\.\d+)?$")


_SQL_PURE_KW = {
    "select", "from", "where", "group", "by", "order", "having",
    "join", "on", "as", "and", "or", "not", "in", "like", "is", "null",
    "between", "exists", "all", "any", "case", "when", "then", "else", "end",
    "intersect", "union", "except", "with", "distinct", "limit", "offset",
    "count", "sum", "avg", "max", "min", "asc", "desc",
    "inner", "outer", "left", "right", "cross", "natural", "full",
}
_SQL_OPS = {"(", ")", ",", "*", "+", "-", "/", ">", "<", "=", ">=", "<=", "!=", "<>", ";"}
_SQL_ALIAS = {"t1", "t2", "t3", "t4", "t5"}


def mask_sql(item: dict) -> str:
    """
    query_toks_no_value를 활용해 SQL의 테이블명·컬럼명을 [COL]로,
    값 placeholder(value)를 [VAL]로 치환한다.

    예:
      query_toks_no_value: select count ( * ) from head where age > value
      masked_sql:          SELECT count ( * ) FROM [COL] WHERE [COL] > [VAL]
    """
    toks = item.get("query_toks_no_value", [])
    if not toks:
        return item.get("query", "")

    result = []
    for tok in toks:
        tl = tok.lower()
        if tl in _SQL_PURE_KW:
            result.append(tok.upper())
        elif tok in _SQL_OPS:
            result.append(tok)
        elif tl in _SQL_ALIAS:
            result.append(tok)       # 별칭은 그대로 유지
        elif tl == "value":
            result.append("[VAL]")
        else:
            result.append("[COL]")   # 테이블명 또는 컬럼명

    return " ".join(result)


def mask_question_en(item: dict) -> str:
    """
    query_toks에서 DB 특화 토큰(테이블명·컬럼명·값)을 추출하여
    question_toks의 대응 단어를 [COL]로 치환한다.

    예:
      query:    SELECT count(*) FROM head WHERE age > 56
      question: How many heads of the departments are older than 56 ?
      masked:   How many [COL] of the [COL] are older than [COL] ?
    """
    query_toks    = item.get("query_toks", [])
    question_toks = item.get("question_toks", [])

    if not query_toks or not question_toks:
        return item.get("question", "")

    # SQL 키워드/연산자가 아닌 토큰 = 테이블명·컬럼명·값
    db_tokens = {
        t.lower()
        for t in query_toks
        if t.lower() not in _SQL_KEYWORDS
    }

    masked = []
    for tok in question_toks:
        tl = tok.lower()
        if tl in db_tokens or _NUM_RE.match(tok):
            masked.append("[COL]")
        else:
            masked.append(tok)

    return " ".join(masked)


# ── 2. 파싱 ─────────────────────────────────────────────────

_RE_Q = re.compile(r"^Question \d+:\s+(.*?)\s+\|\|\|")
_RE_S = re.compile(r"^SQL:\s+(.+)")


def parse_dev_sql(path: Path) -> list[dict]:
    """dev.sql → records (masked_question_en / masked_sql 없음)"""
    records: list[dict] = []
    lines = path.read_text(encoding="utf-8").splitlines()
    pending_q: str | None = None
    idx = 0
    for line in lines:
        line = line.strip()
        if not line:
            continue
        mq = _RE_Q.match(line)
        if mq:
            pending_q = mq.group(1).strip()
            continue
        ms = _RE_S.match(line)
        if ms and pending_q is not None:
            idx += 1
            records.append({
                "id": idx,
                "question_en": pending_q,
                "masked_question_en": None,  # dev는 마스킹 불가
                "masked_sql": None,          # dev는 query_toks 없음
                "sql": ms.group(1).strip(),
            })
            pending_q = None
    return records


def parse_train_json(path: Path) -> list[dict]:
    """train_spider.json → records (masked_question_en / masked_sql 포함)"""
    data = json.loads(path.read_text(encoding="utf-8"))
    records = []
    for idx, item in enumerate(data, 1):
        q = item.get("question", "").strip()
        s = item.get("query", "").strip()
        if q and s:
            records.append({
                "id": idx,
                "question_en": q,
                "masked_question_en": mask_question_en(item),
                "masked_sql": mask_sql(item),
                "sql": s,
            })
    return records


def save_raw_files(records: list[dict], out: Path) -> None:
    (out / "questions_en.txt").write_text(
        "\n".join(r["question_en"] for r in records), encoding="utf-8"
    )
    (out / "sqls.txt").write_text(
        "\n".join(r["sql"] for r in records), encoding="utf-8"
    )
    print(f"[parse] questions_en.txt → {out / 'questions_en.txt'}")
    print(f"[parse] sqls.txt         → {out / 'sqls.txt'}")
    print(f"[parse] 총 {len(records)}건")


# ── 3. 번역 ─────────────────────────────────────────────────

def _build_openai_client():
    try:
        from openai import OpenAI
        import httpx
    except ImportError as e:
        print(f"[ERROR] {e} — pip install openai httpx")
        sys.exit(1)
    if not OPENAI_API_KEY:
        print("[ERROR] OPENAI_API_KEY 없음. eval-analysis/.env 확인")
        sys.exit(1)
    return OpenAI(
        api_key=OPENAI_API_KEY,
        http_client=httpx.Client(verify=False, timeout=OPENAI_TIMEOUT),
    )


def _translate_batch(client, questions: list[str]) -> list[str]:
    prompt = (
        "다음 영어 질문들을 자연스러운 한국어로 번역하세요.\n"
        "[COL]이 포함된 경우 반드시 [COL] 그대로 보존하세요.\n"
        "입력과 동일한 순서로 JSON 문자열 배열만 반환하세요.\n"
        "설명, 코드펜스, 추가 텍스트 없이 JSON 배열만 출력하세요.\n\n"
        f"입력: {json.dumps(questions, ensure_ascii=False)}"
    )
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.0,
        max_completion_tokens=2048,
    )
    content = resp.choices[0].message.content.strip()
    content = re.sub(r"^```[a-z]*\n?", "", content)
    content = re.sub(r"\n?```$", "", content)
    parsed = json.loads(content)
    if isinstance(parsed, list) and len(parsed) == len(questions):
        return [str(x) for x in parsed]
    raise ValueError(f"응답 길이 불일치: {len(parsed)} vs {len(questions)}")


def _translate_one(client, question: str) -> str:
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {
                "role": "system",
                "content": (
                    "영어 문장을 자연스러운 한국어로 번역하세요. "
                    "[COL]이 있으면 반드시 [COL] 그대로 보존하세요. "
                    "번역 결과만 출력하세요."
                ),
            },
            {"role": "user", "content": question},
        ],
        temperature=0.0,
        max_completion_tokens=256,
    )
    return resp.choices[0].message.content.strip()


def _do_translate_batch(
    client,
    batch: list[dict],
    field: str,
    results: dict[int, str],
    failed: set[int],
    lock: threading.Lock,
) -> int:
    """batch의 field 값을 번역하여 results에 thread-safe하게 저장. 완료 건수 반환."""
    questions = [r[field] for r in batch]
    ids       = [r["id"] for r in batch]
    try:
        translated = _translate_batch(client, questions)
        with lock:
            for rid, ko in zip(ids, translated):
                results[rid] = ko
    except Exception as e:
        print(f"\n  [WARN] 배치 실패 ({field}, id {ids[0]}~{ids[-1]}): {e} → 단건 재시도")
        for r in batch:
            try:
                ko = _translate_one(client, r[field])
                with lock:
                    results[r["id"]] = ko
            except Exception as e2:
                print(f"  [WARN] 단건 실패 id={r['id']}: {e2} → 원본 사용")
                with lock:
                    results[r["id"]] = r[field]
                    failed.add(r["id"])
            time.sleep(0.3)
    return len(batch)


def _save_pool(
    records: list[dict],
    ko_map: dict[int, str],
    ko_fail: set[int],
    mko_map: dict[int, str],
    mko_fail: set[int],
    pool_path: Path,
) -> None:
    pool = []
    for r in records:
        has_mask = r["masked_question_en"] is not None
        pool.append({
            "id":                  r["id"],
            "question_en":         r["question_en"],
            "masked_question_en":  r["masked_question_en"],
            "question_ko":         ko_map.get(r["id"], r["question_en"]),
            "masked_question_ko":  mko_map.get(r["id"]) if has_mask else None,
            "translated":          r["id"] in ko_map and r["id"] not in ko_fail,
            "masked_translated":   (r["id"] in mko_map and r["id"] not in mko_fail)
                                   if has_mask else None,
            "sql":                 r["sql"],
            "masked_sql":          r.get("masked_sql"),
        })
    pool_path.write_text(json.dumps(pool, ensure_ascii=False, indent=2), encoding="utf-8")


def translate_records(
    records: list[dict],
    out: Path,
    resume: bool = False,
) -> list[dict]:
    client   = _build_openai_client()
    pool_path = out / "pool.json"
    total     = len(records)
    has_mask  = any(r["masked_question_en"] is not None for r in records)

    # resume: 기존 pool.json 로드
    ko_map:   dict[int, str] = {}
    ko_fail:  set[int]       = set()
    mko_map:  dict[int, str] = {}
    mko_fail: set[int]       = set()

    if resume and pool_path.exists():
        existing = json.loads(pool_path.read_text(encoding="utf-8"))
        for r in existing:
            qko = r.get("question_ko", "")
            qen = r.get("question_en", "")
            if qko and qko != qen:
                # 실제로 번역된 경우만 재사용 (영어 fallback은 재번역 대상)
                ko_map[r["id"]] = qko
            else:
                ko_fail.add(r["id"])
            if has_mask and r.get("masked_question_en") is not None:
                mqko = r.get("masked_question_ko", "")
                mqen = r.get("masked_question_en", "")
                if mqko and mqko != mqen:
                    # 실제로 번역된 경우만 재사용
                    mko_map[r["id"]] = mqko
                else:
                    mko_fail.add(r["id"])
        print(
            f"[translate] resume — "
            f"question_ko: 성공 {len(ko_map)}건 / 실패(재시도) {len(ko_fail)}건 | "
            f"masked_question_ko: 성공 {len(mko_map)}건 / 실패(재시도) {len(mko_fail)}건"
        )

    def _run_parallel(
        todo: list[dict],
        field: str,
        res_map: dict[int, str],
        res_fail: set[int],
        label: str,
    ) -> None:
        """todo를 TRANSLATE_BATCH 단위 배치로 나눠 TRANSLATE_WORKERS 스레드로 병렬 번역."""
        batches   = [todo[i : i + TRANSLATE_BATCH] for i in range(0, len(todo), TRANSLATE_BATCH)]
        lock      = threading.Lock()
        todo_done = [0]   # todo 내에서 완료된 건수만 추적
        save_lock = threading.Lock()
        save_interval = TRANSLATE_WORKERS * 2        # N배치마다 중간 저장

        completed_batches = [0]

        # 각 스레드는 자체 OpenAI client 사용 (httpx.Client는 스레드 안전하지 않음)
        def worker(batch: list[dict]) -> int:
            c = _build_openai_client()
            return _do_translate_batch(c, batch, field, res_map, res_fail, lock)

        print(f"[translate] {label}: {len(todo)}/{total}건 "
              f"(배치 {TRANSLATE_BATCH}, 스레드 {TRANSLATE_WORKERS})")

        with ThreadPoolExecutor(max_workers=TRANSLATE_WORKERS) as ex:
            futures = {ex.submit(worker, b): b for b in batches}
            for fut in as_completed(futures):
                try:
                    n = fut.result()
                except Exception as e:
                    print(f"\n  [ERROR] worker 예외: {e}")
                    n = 0
                completed_batches[0] += 1
                with lock:
                    todo_done[0] += n
                print(f"  [{label}] {todo_done[0]}/{len(todo)}", end="\r", flush=True)
                # 주기적 중간 저장 (save_lock으로 동시 쓰기 방지)
                if completed_batches[0] % save_interval == 0:
                    with save_lock:
                        _save_pool(records, ko_map, ko_fail, mko_map, mko_fail, pool_path)

        # 최종 저장
        _save_pool(records, ko_map, ko_fail, mko_map, mko_fail, pool_path)
        print()

    # ── Pass 1: question_en → question_ko ──────────────────
    todo_ko = [r for r in records if r["id"] not in ko_map]
    _run_parallel(todo_ko, "question_en", ko_map, ko_fail, "translate/ko")

    # ── Pass 2: masked_question_en → masked_question_ko ────
    if has_mask:
        todo_mko = [r for r in records
                    if r["masked_question_en"] and r["id"] not in mko_map]
        _run_parallel(todo_mko, "masked_question_en", mko_map, mko_fail, "translate/masked_ko")

    ok_ko  = total - len(ko_fail)
    ok_mko = total - len(mko_fail) if has_mask else "-"
    print(
        f"[translate] 완료 — "
        f"question_ko: {ok_ko}/{total} | "
        f"masked_question_ko: {ok_mko}/{total if has_mask else '-'}"
    )
    print(f"[translate] pool 저장: {pool_path}")

    return json.loads(pool_path.read_text(encoding="utf-8"))


# ── 4. 임베딩 ────────────────────────────────────────────────

def embed_pool(pool: list[dict], npz_path: Path, embed_lang: str = "ko") -> None:
    """
    pool을 임베딩하여 npz로 저장한다.

    embed_lang="ko" (기본):
        임베딩 텍스트: masked_question_ko (없으면 question_ko)
        npz 키: embeddings, ids, questions_ko, original_questions_ko, sqls, masked_sqls

    embed_lang="en":
        임베딩 텍스트: masked_question_en (없으면 question_en)
        npz 키: embeddings, ids, questions_en, original_questions_en, sqls, masked_sqls
        출력 파일: pool_embeddings_en.npz (npz_path 인자 무시하고 _en suffix 추가)
    """
    from clients.embed import default_embed_client

    if embed_lang == "en":
        npz_path = npz_path.with_name(npz_path.stem + "_en" + npz_path.suffix)

    client = default_embed_client()
    total  = len(pool)

    # resume: 기존 npz 로드
    done_ids: set[int]             = set()
    done_emb: dict[int, np.ndarray] = {}
    if npz_path.exists():
        try:
            ex = np.load(npz_path, allow_pickle=True)
            # 언어별 필수 키 확인
            if embed_lang == "en":
                required = ("original_questions_en", "masked_sqls")
            else:
                required = ("original_questions_ko", "masked_sqls")
            missing = [k for k in required if k not in ex]
            if missing:
                print(f"[embed] 구버전 npz 감지 ({', '.join(missing)} 없음) → 전체 재임베딩")
            else:
                for i, eid in enumerate(ex["ids"].tolist()):
                    done_ids.add(int(eid))
                    done_emb[int(eid)] = ex["embeddings"][i]
                print(f"[embed] resume: 기존 {len(done_ids)}건 로드")
        except Exception as e:
            print(f"[embed] 기존 npz 로드 실패 (처음부터 시작): {e}")

    if embed_lang == "en":
        # 영어 질문이 있는 항목만 포함 (dev에서 dev.sql 파싱 시 question_en 있음)
        pool = [r for r in pool if r.get("question_en")]
    else:
        # 번역이 실제로 된 항목만 임베딩 (영어 fallback 제외)
        pool = [
            r for r in pool
            if r.get("question_ko") and r.get("question_ko") != r.get("question_en")
        ]

    skipped = total - len(pool)
    if skipped:
        print(f"[embed] 제외 항목: {skipped}건")

    total = len(pool)
    todo = [r for r in pool if r["id"] not in done_ids]
    print(f"[embed] 임베딩 대상: {len(todo)}/{total}건 (lang={embed_lang})")

    pool_map = {r["id"]: r for r in pool}

    def _save_npz(all_emb: dict[int, np.ndarray], label: str = "") -> None:
        ordered = sorted(all_emb.keys())
        if not ordered:
            return
        embs    = np.stack([all_emb[i] for i in ordered])
        ids_arr = np.array(ordered, dtype=np.int32)
        sql_arr  = np.array([pool_map[i]["sql"] for i in ordered])
        msql_arr = np.array([pool_map[i].get("masked_sql") or pool_map[i]["sql"] for i in ordered])

        if embed_lang == "en":
            q_arr    = np.array([pool_map[i].get("masked_question_en") or pool_map[i]["question_en"] for i in ordered])
            orig_arr = np.array([pool_map[i]["question_en"] for i in ordered])
            save_kwargs = dict(
                embeddings=embs, ids=ids_arr,
                questions_en=q_arr, original_questions_en=orig_arr,
                sqls=sql_arr, masked_sqls=msql_arr,
            )
        else:
            q_arr    = np.array([pool_map[i].get("masked_question_ko") or pool_map[i]["question_ko"] for i in ordered])
            orig_arr = np.array([pool_map[i]["question_ko"] for i in ordered])
            save_kwargs = dict(
                embeddings=embs, ids=ids_arr,
                questions_ko=q_arr, original_questions_ko=orig_arr,
                sqls=sql_arr, masked_sqls=msql_arr,
            )

        tmp = npz_path.with_suffix(".tmp.npz")
        np.savez(tmp, **save_kwargs)
        tmp.replace(npz_path)
        suffix = f" ({label})" if label else ""
        print(f"\n  [embed] 중간 저장{suffix}: {len(ordered)}건 → {npz_path}", flush=True)

    new_emb: dict[int, np.ndarray] = {}
    batches  = [todo[i : i + EMBED_BATCH] for i in range(0, len(todo), EMBED_BATCH)]
    done_cnt = [0]
    emb_lock = threading.Lock()
    save_lock = threading.Lock()

    def _embed_batch(batch: list[dict]) -> int:
        if embed_lang == "en":
            texts = [r.get("masked_question_en") or r["question_en"] for r in batch]
        else:
            texts = [r.get("masked_question_ko") or r["question_ko"] for r in batch]
        vectors = client.embeddings(texts)
        arr = np.array(vectors, dtype=np.float32)
        norms = np.linalg.norm(arr, axis=1, keepdims=True)
        arr = arr / np.where(norms == 0, 1.0, norms)
        with emb_lock:
            for j, r in enumerate(batch):
                new_emb[r["id"]] = arr[j]
            done_cnt[0] += len(batch)
            cnt = done_cnt[0]
        print(f"  [embed] {len(done_ids) + cnt}/{total}", end="\r", flush=True)
        return cnt

    with ThreadPoolExecutor(max_workers=EMBED_WORKERS) as ex:
        futures = [ex.submit(_embed_batch, b) for b in batches]
        last_saved = [0]
        for fut in as_completed(futures):
            try:
                cnt = fut.result()
            except Exception as e:
                print(f"\n  [embed][ERROR] {e}")
                continue
            if cnt - last_saved[0] >= EMBED_SAVE_EVERY:
                with save_lock:
                    if cnt - last_saved[0] >= EMBED_SAVE_EVERY:
                        with emb_lock:
                            snapshot = {**done_emb, **new_emb}
                        _save_npz(snapshot, f"{len(done_ids) + cnt}/{total}")
                        last_saved[0] = cnt

    print()
    all_emb = {**done_emb, **new_emb}
    _save_npz(all_emb)
    print(f"[embed] 저장 완료: {len(all_emb)}건 → {npz_path}")


# ── CLI ──────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="MQS Pool Builder")
    parser.add_argument("--source", choices=["dev", "train"], default="dev")
    parser.add_argument("--resume",     action="store_true", help="이어서 실행")
    parser.add_argument("--embed-only", action="store_true", help="임베딩만 실행")
    parser.add_argument(
        "--embed-lang", choices=["ko", "en"], default="ko",
        help="임베딩 언어: ko(기본, masked_question_ko) / en(masked_question_en → pool_embeddings_en.npz)",
    )
    args = parser.parse_args()

    out = _out_dir(args.source)

    # 1. 파싱
    if args.source == "dev":
        print(f"[parse] dev.sql: {DEV_SQL_PATH}")
        records = parse_dev_sql(DEV_SQL_PATH)
    else:
        print(f"[parse] train_spider.json: {TRAIN_JSON_PATH}")
        records = parse_train_json(TRAIN_JSON_PATH)
    save_raw_files(records, out)

    # 2. 번역 (en 임베딩은 번역 불필요)
    pool_path = out / "pool.json"
    if args.embed_only or args.embed_lang == "en":
        if not pool_path.exists():
            print(f"[ERROR] {pool_path} 없음. 먼저 번역을 완료하세요.")
            sys.exit(1)
        pool = json.loads(pool_path.read_text(encoding="utf-8"))
        print(f"[pool] 로드: {len(pool)}건")
    else:
        pool = translate_records(records, out=out, resume=args.resume)

    # 3. 임베딩
    embed_pool(pool, out / "pool_embeddings.npz", embed_lang=args.embed_lang)

    npz_name = "pool_embeddings_en.npz" if args.embed_lang == "en" else "pool_embeddings.npz"
    print("\n=== 완료 ===")
    print(f"  소스:      {args.source}")
    print(f"  언어:      {args.embed_lang}")
    print(f"  출력:      {out}")
    print(f"  pool:      {pool_path}")
    print(f"  임베딩:    {out / npz_name}")


if __name__ == "__main__":
    main()
