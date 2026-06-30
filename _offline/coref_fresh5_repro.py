"""Fresh5 follow-up fails repro (deterministic). Same harness as fresh4; threads the
exact eval history. Targets the Roman-Urdu set-reference / filler gaps."""
import sys, os, sqlite3, collections, re
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services import catalog_query as cq
from services import coref

DB = os.path.join(os.path.dirname(__file__), "diecaststation", "chroma.sqlite3")


class _Coll:
    def __init__(self, d, m): self._docs, self._metas = d, m
    def count(self): return len(self._docs)
    def get(self, limit=0, include=None): return {"documents": self._docs, "metadatas": self._metas}


class FakeDB:
    def __init__(self, path):
        c = sqlite3.connect(path); rows = collections.defaultdict(dict)
        for eid, k, sv in c.execute("select id,key,string_value from embedding_metadata"):
            if sv is not None: rows[eid][k] = sv
        docs = [d.get("chroma:document", "") for d in rows.values()]
        metas = [{k: v for k, v in d.items() if k != "chroma:document"} for d in rows.values()]
        self._collection = _Coll(docs, metas)


def H(u, a): return [{"role": "user", "content": u}, {"role": "assistant", "content": a}]

CASES = [
    (40, "us teen me sabse mehnga konsa", H("Rolls Phantom Spectre Cullinan dikhao",
        "Rolls Royce Phantom 1/24 Rs.4,330, Rolls Royce Spectre 1/24 Rs.4,420, Rolls Royce SUV Cullinan 1:20 Big Scale Rs.4,780."),
        ["cullinan", "4780", "4,780"]),
    (42, "repeat just the sold-out tally", H("RC Cars in/out tally",
        "RC Cars has 19 total, 9 available, 10 out of stock."), ["10 out of stock", "10 sold out"]),
    (43, "un me se sirf jo abhi khareed sakta hun", H("Harley Sportster S Diecast Model vs Kawasaki Ninja H2R 1/12 Model",
        "Harley Sportster S Diecast Model is out of stock; Kawasaki Ninja H2R 1/12 Model is available at Rs.3,650."),
        ["kawasaki", "available"]),
    (44, "wahi do dobara total karke batao", H("Suzuki GS125 1:12 Scale and Kawasaki Ninja H2R 1/12 Model price",
        "Suzuki GS125 1:12 Scale is Rs.3,250 and Kawasaki Ninja H2R 1/12 Model is Rs.3,650."), ["6900", "6,900"]),
    (45, "now just give the live headcount", H("SUVs availability split",
        "SUVs has 7 total, 5 available, 2 out of stock."), ["5 available"]),
    (46, "sort those same three cheapest first", H("Iron Man Flash Goku figures prices",
        "Iron Man Action Figure ( 12 inches ) Rs.3,650, Flash Action Figure ( 12 inches ) Rs.3,950, Dragon Ball Z Son Goku Action Figure Rs.4,500."),
        ["iron man", "goku"]),
    (47, "us category ka gone wala count", H("Bikes available vs sold out",
        "Bikes has 10 total, 4 available, 6 out of stock."), ["6 out of stock", "6 sold out"]),
]


def _norm(s): return re.sub(r"\s+", " ", str(s).lower().replace(",", "")).strip()


def run():
    db = FakeDB(DB); rows = cq.load_rows(db); npass = 0
    for cid, q, hist, want in CASES:
        rw = coref.resolve_followup(q, hist)
        mp = cq._parse_multi(rw)
        if mp:
            res = cq._answer_multi(mp, rows); ans = res[0] if res else None
        else:
            spec = cq.parse(rw); ans, _ = cq._execute_spec(spec, rows, None, 50)
        ans = ans or "<None>"
        hit = any(_norm(w) in _norm(ans) for w in want); npass += hit
        print(f"Q{cid} {'PASS' if hit else 'FAIL'} want~{want}")
        print(f"   rw: {rw}")
        print(f"   ->: {ans.replace(chr(10),' ')[:150]}")
    print(f"=== FRESH5-followups: {npass}/{len(CASES)} ===")


if __name__ == "__main__":
    run()
