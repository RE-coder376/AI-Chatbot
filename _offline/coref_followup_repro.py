"""Repro Codex 100Q follow-up fails end-to-end deterministically:
resolve_followup(q,history) -> parse -> _execute_spec, against pulled chroma."""
import sys, os, sqlite3, collections
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services import catalog_query as cq
from services import coref

DB = os.path.join(os.path.dirname(__file__), "diecaststation", "chroma.sqlite3")


class _Coll:
    def __init__(self, docs, metas):
        self._docs, self._metas = docs, metas
    def count(self):
        return len(self._docs)
    def get(self, limit=0, include=None):
        return {"documents": self._docs, "metadatas": self._metas}


class FakeDB:
    def __init__(self, path):
        c = sqlite3.connect(path)
        rows = collections.defaultdict(dict)
        for eid, k, sv in c.execute("select id,key,string_value from embedding_metadata"):
            if sv is not None:
                rows[eid][k] = sv
        docs, metas = [], []
        for d in rows.values():
            docs.append(d.get("chroma:document", ""))
            metas.append({k: v for k, v in d.items() if k != "chroma:document"})
        self._collection = _Coll(docs, metas)


CASES = [
    (74, "available ones from that set?",
     [{"role": "user", "content": "BMW S1000RR vs Kawasaki Ninja H2R 1/12 stock"},
      {"role": "assistant", "content": "BMW S1000RR is out of stock, Kawasaki Ninja H2R 1/12 is available."}],
     ["Kawasaki Ninja H2R 1/12", "available"]),
    (75, "total bhi batao",
     [{"role": "user", "content": "BMW M8 plus Iron Man"},
      {"role": "assistant", "content": "BMW M8 is Rs.4,950 and Iron Man is Rs.3,650."}],
     ["8600"]),
    (77, "aur cheapest buyable?",
     [{"role": "user", "content": "RC Construction full list"},
      {"role": "assistant", "content": "Only Mini RC Excavator is available; others are sold out."}],
     ["Mini RC Excavator", "5450"]),
    (78, "does it have old price?",
     [{"role": "user", "content": "BMW M8 now price"},
      {"role": "assistant", "content": "BMW M8 is Rs.4,950."}],
     ["7350"]),
    (79, "sold out count kya tha?",
     [{"role": "user", "content": "RC Construction breakdown"},
      {"role": "assistant", "content": "RC Construction has 4 total, 1 available, 3 sold out."}],
     ["3 sold out", "3 out of stock"]),
    (82, "uska price range?",
     [{"role": "user", "content": "Mercedes AMG ONE 1:24 available hai?"},
      {"role": "assistant", "content": "Mercedes AMG ONE 1:24 is available."}],
     ["4345", "4545"]),
]


def _norm(s):
    import re
    return re.sub(r"\s+", " ", str(s).lower().replace(",", "")).strip()


def run():
    db = FakeDB(DB)
    rows = cq.load_rows(db)
    for cid, q, hist, want in CASES:
        rw = coref.resolve_followup(q, hist)
        # live engine: multi-product (compare/order/basket) path runs before parse
        mp = cq._parse_multi(rw)
        if mp:
            res = cq._answer_multi(mp, rows)
            ans = res[0] if res else None
            spec = None
        else:
            spec = cq.parse(rw)
            ans, _ = cq._execute_spec(spec, rows, None, 50)
        ans = ans or "<None/abstain>"
        hit = any(_norm(w) in _norm(ans) for w in want)
        print(f"\n==== Q{cid}  {'PASS' if hit else 'FAIL'}  want~{want}")
        print("  q       :", q)
        print("  rewrite :", rw)
        if spec is not None:
            print("  agg:", spec.agg, "anchors:", spec.anchors, "in_stock:", spec.in_stock, "oos:", spec.out_of_stock)
        else:
            print("  multi:", {k: mp[k] for k in ("op", "names")})
        print("  ->", ans.replace("\n", " ")[:320])


if __name__ == "__main__":
    run()
