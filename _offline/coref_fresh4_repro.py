"""Fresh4 follow-up coref repro (deterministic, live-faithful): uses the EXACT
synthetic history the runner threads (run_diecast_100q sends item['history'] verbatim).
resolve_followup -> _parse_multi/_answer_multi or parse/_execute_spec, vs pulled chroma."""
import sys, os, sqlite3, collections, re
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services import catalog_query as cq
from services import coref

DB = os.path.join(os.path.dirname(__file__), "diecaststation", "chroma.sqlite3")


class _Coll:
    def __init__(self, docs, metas): self._docs, self._metas = docs, metas
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

# (id, q, history, want-any-of)  — Fresh4 fails
FRESH4 = [
    (77, "gone bucket names now", H("Formula One live vs gone tally",
        "Formula One has five live and three gone products."),
        ["out of stock", "sold out"]),
    (78, "repeat live count only", H("RC Cars collection in/out tally",
        "RC Cars has 19 total, 9 available, 10 out of stock."), ["9 available"]),
    (79, "repeat gone count only", H("RC Cars collection in/out tally",
        "RC Cars has 19 total, 9 available, 10 out of stock."),
        ["10 out of stock", "10 sold out"]),
    (81, "cheaper option stock mein hai?", H("Mini Can RC vs Desktop Drift",
        "Mini Can is Rs.2,980 and Desktop is Rs.3,620."), ["available", "in stock"]),
    (83, "sort same three again please", H("Rolls Phantom Spectre Cullinan prices",
        "Phantom Rs.4,330, Spectre Rs.4,420, Cullinan Rs.4,780."),
        ["phantom", "spectre", "cullinan"]),
    (84, "only buyable from those two bikes", H("Harley Sportster vs Ducati Diavel stock",
        "Both Harley Sportster and Ducati Diavel are out of stock."), ["out of stock", "sold out"]),
    (85, "price of the product that is available", H("BMW XM vs Lamborghini URUS stock",
        "BMW XM is out of stock. Lamborghini URUS is available at Rs.4,250."), ["4250", "4,250"]),
    (87, "which one is highest price in that trio?", H("Iron Man Goku Deadpool prices",
        "Iron Man 3650, Deadpool 4040, Goku 4500."), ["4500", "goku"]),
]

# Regression guards — must STAY passing (from coref_followup_repro CASES)
GUARDS = [
    (74, "available ones from that set?", H("BMW S1000RR vs Kawasaki Ninja H2R 1/12 stock",
        "BMW S1000RR is out of stock, Kawasaki Ninja H2R 1/12 is available."),
        ["kawasaki", "available"]),
    (75, "total bhi batao", H("BMW M8 plus Iron Man", "BMW M8 is Rs.4,950 and Iron Man is Rs.3,650."),
        ["8600", "8,600"]),
    (177, "aur cheapest buyable?", H("RC Construction full list",
        "Only Mini RC Excavator is available; others are sold out."), ["excavator"]),
    (179, "sold out count kya tha?", H("RC Construction breakdown",
        "RC Construction has 4 total, 1 available, 3 sold out."), ["3 sold out", "3 out of stock"]),
]


def _norm(s): return re.sub(r"\s+", " ", str(s).lower().replace(",", "")).strip()


def run(cases, tag):
    db = FakeDB(DB); rows = cq.load_rows(db); npass = 0
    for cid, q, hist, want in cases:
        rw = coref.resolve_followup(q, hist)
        mp = cq._parse_multi(rw)
        if mp:
            res = cq._answer_multi(mp, rows); ans = res[0] if res else None
        else:
            spec = cq.parse(rw); ans, _ = cq._execute_spec(spec, rows, None, 50)
        ans = ans or "<None/abstain>"
        hit = any(_norm(w) in _norm(ans) for w in want)
        npass += hit
        print(f"[{tag}] Q{cid} {'PASS' if hit else 'FAIL'}  want~{want}")
        print(f"     q : {q}")
        print(f"     rw: {rw}")
        print(f"     ->: {ans.replace(chr(10),' ')[:200]}")
    print(f"=== {tag}: {npass}/{len(cases)} ===\n")
    return npass, len(cases)


if __name__ == "__main__":
    run(GUARDS, "GUARD")
    run(FRESH4, "FRESH4")
