"""Router-model A/B: run the client-style 50Q through the REAL router->deterministic
pipeline (answer_catalog_query, which fires extract_plan) against the pulled
diecaststation chroma, with ROUTER_MODEL as the ONLY variable. Grades expect/forbid.
Follow-ups need conversational state the offline single-call path lacks -> reported
separately (they fail in BOTH variants equally, so don't bias the delta)."""
import sys, os, json, sqlite3, collections, time
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services import catalog_query as cq

DB = os.path.join(os.path.dirname(__file__), "diecaststation", "chroma.sqlite3")
JSON = os.path.join(os.path.dirname(__file__), "..", "DIECAST_50Q_CLIENT_STYLE_20260627.json")
MODELS = [("llama-3.3-70b (baseline)", ""), ("qwen/qwen3-32b", "qwen/qwen3-32b")]
RUNS = int(os.getenv("AB_RUNS", "2"))

class _Coll:
    def __init__(s, docs, metas): s._docs, s._metas = docs, metas
    def count(s): return len(s._docs)
    def get(s, limit=0, include=None): return {"documents": s._docs, "metadatas": s._metas}

def build_db(path):
    c = sqlite3.connect(path)
    rows = collections.defaultdict(dict)
    for eid, k, sv in c.execute("select id,key,string_value from embedding_metadata"):
        if sv is not None: rows[eid][k] = sv
    docs, metas = [], []
    for d in rows.values():
        docs.append(d.get("chroma:document", ""))
        metas.append({k: v for k, v in d.items() if k != "chroma:document"})
    class _DB: pass
    db = _DB(); db._collection = _Coll(docs, metas)
    return db

def grade_one(q, expect, forbid, db):
    for _ in range(3):
        try:
            ans, _ = cq.answer_catalog_query(q, db, None, 50)
            break
        except Exception:
            ans = None; time.sleep(1.0)
    ans = ans or ""
    low = ans.lower()
    miss = [e for e in expect if e.lower() not in low]
    forb = [f for f in forbid if f.lower() in low]
    return (not miss and not forb and bool(ans)), miss, forb, ans

def run(model_label, model_id, db, qs):
    os.environ["ROUTER_MODEL"] = model_id
    npass = nfollowskip = 0
    follow = []
    per = {}
    for it in qs:
        k = str(it.get("kind", ""))
        if "follow" in k:
            nfollowskip += 1; follow.append(it["id"]); continue
        ok, miss, forb, ans = grade_one(it["q"], it.get("expect", []), it.get("forbid", []), db)
        per[it["id"]] = (ok, miss, forb, ans[:80], it["q"], k)
        if ok: npass += 1
        time.sleep(0.3)
    graded = len(qs) - nfollowskip
    return npass, graded, follow, per

if __name__ == "__main__":
    db = build_db(DB)
    qs = json.load(open(JSON, encoding="utf-8"))["results"]
    results = {}
    for label, mid in MODELS:
        scores = []
        last_per = None
        for r in range(RUNS):
            p, g, follow, per = run(label, mid, db, qs)
            scores.append(p); last_per = per
            print(f"[{label}] run {r+1}: {p}/{g} (follow-skip {len(follow)})")
        results[label] = (scores, g, follow, last_per)
    summary = {"graded_non_follow": results[MODELS[0][0]][1],
               "follow_skipped": len(results[MODELS[0][0]][2]),
               "models": {}}
    print("\n=== SUMMARY (non-follow graded) ===")
    base_per = results[MODELS[0][0]][3]
    for label, _ in MODELS:
        scores, g, follow, per = results[label]
        summary["models"][label] = {"runs": scores, "mean": sum(scores)/len(scores), "graded": g}
        print(f"{label}: mean {sum(scores)/len(scores):.1f}/{g}  runs={scores}")
    json.dump(summary, open(os.path.join(os.path.dirname(__file__), "router_ab_out.json"), "w"), indent=2)
    # per-question divergence on last run
    qwen_per = results[MODELS[1][0]][3]
    print("\n=== Qwen vs baseline divergences (last run) ===")
    for qid in sorted(base_per):
        b = base_per[qid][0]; w = qwen_per[qid][0]
        if b != w:
            who = "QWEN+ baseline-" if w else "QWEN- baseline+"
            print(f"  Q{qid:02d} {who} :: {base_per[qid][4][:60]}")
            print(f"      base: {base_per[qid][3]}")
            print(f"      qwen: {qwen_per[qid][3]}")
