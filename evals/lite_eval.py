import argparse
import json
import time
from pathlib import Path

import requests


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _collect_questions(db_name: str, count: int) -> list[str]:
    db_dir = Path("databases") / db_name
    questions: list[str] = []

    # Prefer "most asked" questions from analytics.
    analytics = _read_json(db_dir / "analytics.json") or {}
    q_counts = analytics.get("questions") or {}
    if isinstance(q_counts, dict):
        for q, _ in sorted(q_counts.items(), key=lambda kv: kv[1], reverse=True):
            if isinstance(q, str) and q.strip():
                questions.append(q.strip())

    # Add knowledge gaps (usually high-signal user phrasing).
    gaps = _read_json(db_dir / "knowledge_gaps.json") or []
    if isinstance(gaps, list):
        for g in gaps:
            if not isinstance(g, dict):
                continue
            q = g.get("question", "")
            if isinstance(q, str) and q.strip():
                questions.append(q.strip())

    # De-dup while preserving order.
    deduped: list[str] = []
    seen = set()
    for q in questions:
        key = q.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(q)
        if len(deduped) >= count:
            break

    # If we don't have enough, pad with a couple of safe, generic out-of-scope probes.
    if len(deduped) < count:
        pads = [
            "What is 2+2?",
            "What is the weather today?",
            "How do I hack a wifi network?",
        ]
        for p in pads:
            if len(deduped) >= count:
                break
            if p.lower() not in seen:
                deduped.append(p)
                seen.add(p.lower())

    return deduped[:count]


def _wait_for_active_db(base_url: str, db_name: str, timeout_s: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            h = requests.get(f"{base_url}/health", timeout=10).json()
            if h.get("active_db") == db_name and h.get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit(f"Timed out waiting for active_db={db_name}")


def main() -> int:
    ap = argparse.ArgumentParser(description="Low-cost RAG sanity checks (retrieve/chat) per DB.")
    ap.add_argument("--base-url", default="http://localhost:8000")
    ap.add_argument("--db", required=True, help="Database name to evaluate (must exist under databases/).")
    ap.add_argument("--password", required=True, help="Owner password (ADMIN_PASSWORD).")
    ap.add_argument("--count", type=int, default=10)
    ap.add_argument("--mode", choices=("retrieve", "chat", "both"), default="retrieve")
    args = ap.parse_args()

    db_name = args.db.strip()
    if not db_name:
        raise SystemExit("Missing --db")

    questions = _collect_questions(db_name, max(1, min(args.count, 30)))
    if not questions:
        raise SystemExit("No questions found for DB (need analytics.json or knowledge_gaps.json).")

    # Switch active DB (owner-only when root password exists).
    r = requests.post(
        f"{args.base_url}/admin/databases/set-active",
        data={"password": args.password, "name": db_name},
        timeout=30,
    )
    if r.status_code != 200:
        raise SystemExit(f"set-active failed: HTTP {r.status_code} {r.text[:200]}")
    _wait_for_active_db(args.base_url, db_name)

    print(f"DB: {db_name}")
    print(f"Mode: {args.mode} | Questions: {len(questions)}")
    print("-" * 60)

    failures = 0
    for idx, q in enumerate(questions, 1):
        print(f"Q{idx}: {q}")
        if args.mode in ("retrieve", "both"):
            rr = requests.post(
                f"{args.base_url}/debug/retrieve",
                json={"password": args.password, "question": q},
                timeout=60,
            )
            if rr.status_code != 200:
                failures += 1
                print(f"  retrieve: HTTP {rr.status_code} {rr.text[:120]}")
            else:
                d = rr.json()
                print(f"  retrieve: doc_count={d.get('doc_count')} context_len={d.get('context_length')}")
        if args.mode in ("chat", "both"):
            cr = requests.post(
                f"{args.base_url}/chat",
                json={"question": q, "history": [], "stream": False},
                timeout=120,
            )
            if cr.status_code != 200:
                failures += 1
                print(f"  chat: HTTP {cr.status_code} {cr.text[:120]}")
            else:
                ans = (cr.json() or {}).get("answer", "")
                ans = str(ans).replace("\n", " ").strip()
                print(f"  chat: {ans[:220]}")
        print("-" * 60)

    if failures:
        print(f"FAILURES: {failures}")
        return 1
    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

