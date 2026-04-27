import argparse
import time

import requests
from evals import eval_v1


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

    # Switch active DB (owner-only when root password exists).
    r = requests.post(
        f"{args.base_url}/admin/databases/set-active",
        data={"password": args.password, "name": db_name},
        timeout=30,
    )
    if r.status_code != 200:
        raise SystemExit(f"set-active failed: HTTP {r.status_code} {r.text[:200]}")
    _wait_for_active_db(args.base_url, db_name)

    items = eval_v1._collect_eval_items(args.base_url, args.password, db_name, max(1, min(args.count, 30)))
    questions = [item.q for item in items]
    if not questions:
        raise SystemExit("No DB-grounded eval questions were found for this tenant.")

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
