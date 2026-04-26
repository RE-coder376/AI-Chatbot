import argparse
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests


IDK_SIGS = (
    "i don't have",
    "i do not have",
    "not in my",
    "i'm not sure",
    "i cannot find",
    "i can't find",
    "no information",
    "not available",
)

REFUSE_SIGS = (
    "i can't help with that",
    "i cannot help with that",
    "outside that scope",
    "i specialize in helping with",
)


@dataclass
class EvalItem:
    q: str
    expect: str  # ANSWER | IDK | REFUSE


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _collect_eval_items(db_name: str, count: int) -> list[EvalItem]:
    """
    No-money default: pull real user questions from analytics + gaps (expected=ANSWER),
    then add a couple of out-of-scope probes (expected=REFUSE) and a couple unknowns (expected=IDK).
    """
    db_dir = Path("databases") / db_name
    items: list[EvalItem] = []

    analytics = _read_json(db_dir / "analytics.json") or {}
    q_counts = analytics.get("questions") or {}
    if isinstance(q_counts, dict):
        for q, _ in sorted(q_counts.items(), key=lambda kv: kv[1], reverse=True):
            if isinstance(q, str) and q.strip():
                items.append(EvalItem(q=q.strip(), expect="ANSWER"))

    gaps = _read_json(db_dir / "knowledge_gaps.json") or []
    if isinstance(gaps, list):
        for g in gaps:
            if not isinstance(g, dict):
                continue
            q = g.get("question", "")
            if isinstance(q, str) and q.strip():
                items.append(EvalItem(q=q.strip(), expect="ANSWER"))

    # De-dup while preserving order.
    deduped: list[EvalItem] = []
    seen = set()
    for it in items:
        k = it.q.lower()
        if k in seen:
            continue
        seen.add(k)
        deduped.append(it)
        if len(deduped) >= count:
            break

    # Add small fixed probes (kept short to save tokens if chat mode is enabled).
    pads = [
        EvalItem("What is 2+2?", "REFUSE"),
        EvalItem("What is the weather today?", "REFUSE"),
        EvalItem("What is the capital of Japan?", "REFUSE"),
        EvalItem("What is your return policy for a specific item?", "IDK"),
    ]
    for p in pads:
        if len(deduped) >= count:
            break
        if p.q.lower() not in seen:
            deduped.append(p)
            seen.add(p.q.lower())

    return deduped[:count]


def _wait_health(base_url: str, db_name: str, timeout_s: int = 120) -> None:
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


def _is_idk(text: str) -> bool:
    t = (text or "").lower()
    return any(s in t for s in IDK_SIGS)


def _is_refuse(text: str) -> bool:
    t = (text or "").lower()
    return any(s in t for s in REFUSE_SIGS)


def main() -> int:
    ap = argparse.ArgumentParser(description="Eval v1: lightweight scored checks per DB.")
    ap.add_argument("--base-url", default="http://localhost:8000")
    ap.add_argument("--db", required=True)
    ap.add_argument("--password", required=True, help="Owner password (ADMIN_PASSWORD).")
    ap.add_argument("--count", type=int, default=10)
    ap.add_argument("--mode", choices=("retrieve", "chat", "both"), default="retrieve")
    ap.add_argument("--out-dir", default="evals/results")
    args = ap.parse_args()

    db = args.db.strip()
    items = _collect_eval_items(db, max(5, min(args.count, 25)))
    if not items:
        raise SystemExit("No eval questions found for DB")

    # Switch DB (owner-only when root password exists).
    r = requests.post(
        f"{args.base_url}/admin/databases/set-active",
        data={"password": args.password, "name": db},
        timeout=30,
    )
    if r.status_code != 200:
        raise SystemExit(f"set-active failed: HTTP {r.status_code} {r.text[:200]}")
    _wait_health(args.base_url, db)

    results = []
    failures = 0
    for it in items:
        row = {"q": it.q, "expect": it.expect}

        if args.mode in ("retrieve", "both"):
            rr = requests.post(
                f"{args.base_url}/debug/retrieve",
                json={"password": args.password, "question": it.q},
                timeout=60,
            )
            row["retrieve_http"] = rr.status_code
            if rr.status_code == 200:
                d = rr.json()
                row["doc_count"] = d.get("doc_count")
                row["context_length"] = d.get("context_length")
            else:
                failures += 1
                row["retrieve_error"] = rr.text[:200]

        if args.mode in ("chat", "both"):
            cr = requests.post(
                f"{args.base_url}/chat",
                json={"question": it.q, "history": [], "stream": False},
                timeout=120,
            )
            row["chat_http"] = cr.status_code
            if cr.status_code == 200:
                ans = (cr.json() or {}).get("answer", "")
                ans_s = str(ans).strip()
                row["answer_preview"] = ans_s[:300]
                if it.expect == "REFUSE" and not _is_refuse(ans_s):
                    failures += 1
                    row["fail"] = "SHOULD_REFUSE"
                elif it.expect == "IDK" and not _is_idk(ans_s):
                    failures += 1
                    row["fail"] = "SHOULD_IDK"
                elif it.expect == "ANSWER" and (_is_refuse(ans_s)):
                    failures += 1
                    row["fail"] = "WRONG_REFUSE"
            else:
                failures += 1
                row["chat_error"] = cr.text[:200]

        results.append(row)

    out_dir = Path(args.out_dir) / db
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"eval_{ts}.json"
    out_path.write_text(json.dumps({"db": db, "mode": args.mode, "failures": failures, "results": results}, indent=2), encoding="utf-8")

    print(str(out_path))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

