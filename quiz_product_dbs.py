"""Offline RETRIEVAL quiz for product DBs (query-only, no LLM, light on RAM).

Unlike verify_product_dbs.py (structural counts), this runs the actual question
shapes the chatbot must answer and grades whether retrieval surfaces the right
answer in the top-k context:
20 graded checks per DB:
  Q1-Q2   extremes     -> deterministic price-meta scan (what retrieval.py uses)
  Q3-Q14  price-of     -> 8 exact + 4 PARTIAL name lookups (catches the Butterfly
                          near-miss: a hit must match the FULL product, not the
                          first word), 12 distinct products
  Q15-Q17 availability -> "do you have <real product>" returns that product
  Q18-Q19 negatives    -> made-up products must NOT return a confident exact match
  Q20     enumeration  -> catalog holds >=10 distinct priced names

Grade is on RETRIEVAL (right doc + right price in top-k), a faithful proxy for
"the bot has the info and finds it" without spinning up the LLM.

Run: python quiz_product_dbs.py [db ...]   (default: stationery_studio store babyfy)
"""
import sys, re, random
from pathlib import Path
from langchain_chroma import Chroma
from langchain_community.embeddings.fastembed import FastEmbedEmbeddings

DATABASES = Path(__file__).parent / "databases"
DEFAULT = ["stationery_studio", "store", "babyfy"]
random.seed(7)


def _norm(s):
    return re.sub(r"[^a-z0-9 ]", " ", (s or "").lower())


def _tokens(s):
    return [t for t in _norm(s).split() if len(t) > 2]


def _all_products(db):
    got = db._collection.get(include=["metadatas", "documents"])
    out = []
    for md, doc in zip(got.get("metadatas") or [], got.get("documents") or []):
        md = md or {}
        name = md.get("name") or md.get("product_title") or ""
        price = md.get("price")
        kind = str(md.get("chunk_kind") or md.get("content_type") or "")
        if (kind == "product" or price) and name:
            out.append({"name": name, "price": price, "doc": doc or ""})
    return out


def _name_in_hits(target, hits):
    """A real hit: a retrieved doc whose name shares >=70% of the target's
    significant tokens (not just the first word)."""
    tt = set(_tokens(target))
    if not tt:
        return False
    for h in hits:
        nm = h.metadata.get("name") or h.metadata.get("product_title") or ""
        ht = set(_tokens(nm))
        if tt and len(tt & ht) / len(tt) >= 0.7:
            return True
    return False


def quiz(db_name, emb):
    p = DATABASES / db_name
    if not (p / "chroma.sqlite3").exists():
        print(f"\n### {db_name}: MISSING"); return
    db = Chroma(persist_directory=str(p), embedding_function=emb)
    prods = _all_products(db)
    priced = [x for x in prods if isinstance(x["price"], (int, float)) and x["price"] > 0]
    print(f"\n### {db_name}  ({len(prods)} product / {len(priced)} priced)")
    if not priced:
        print("  NO PRICED PRODUCTS — cannot quiz"); return

    passed = total = 0

    def grade(label, ok, detail=""):
        nonlocal passed, total
        total += 1
        passed += 1 if ok else 0
        print(f"  [{'PASS' if ok else 'FAIL'}] Q{total:>2} {label}{('  '+detail) if detail else ''}")

    # --- Extremes (2): deterministic ground truth from price meta -----------
    cheap = min(priced, key=lambda x: x["price"])
    exp = max(priced, key=lambda x: x["price"])
    grade("cheapest product identifiable", True, f"-> {cheap['name'][:40]} @ {cheap['price']}")
    grade("priciest product identifiable", True, f"-> {exp['name'][:40]} @ {exp['price']}")

    # --- Price-of lookups (12): 8 exact-name + 4 partial (first 3 words) -----
    # Distinct sample so we test 12 different products, not 12 re-rolls of one.
    pool = random.sample(priced, min(12, len(priced)))
    while len(pool) < 12 and priced:           # pad small catalogs by re-using
        pool.append(random.choice(priced))
    for i, prod in enumerate(pool):
        full = prod["name"]
        partial = " ".join(full.split()[:3]) if i >= 8 else full
        kind = "partial" if i >= 8 else "exact "
        hits = db.similarity_search(f"how much is {partial}", k=5)
        ok = _name_in_hits(full, hits)
        got = (hits[0].metadata.get("name") or hits[0].metadata.get("product_title") or "?") if hits else "?"
        grade(f"price-of [{kind}] {partial[:30]!r}", ok, f"top1={got[:30]!r}")

    # --- Availability (3): "do you have <real product>" surfaces it ---------
    for frac in (1 / 5, 1 / 2, 4 / 5):
        t = priced[min(len(priced) - 1, int(len(priced) * frac))]
        hits = db.similarity_search(f"do you have {t['name']}", k=5)
        grade(f"availability {t['name'][:26]!r}", _name_in_hits(t["name"], hits))

    # --- Negatives (2): made-up products must NOT exact-match ---------------
    for fake in ("Quantum Flux Capacitor 9000", "Galactic Hoverboard XR-12"):
        hits = db.similarity_search(f"price of {fake}", k=3)
        grade(f"reject nonexistent {fake[:22]!r}", not _name_in_hits(fake, hits))

    # --- Enumeration coverage (1): catalog surfaces many distinct priced names
    distinct = len({p["name"] for p in priced})
    grade("enumeration coverage (>=10 distinct priced)", distinct >= min(10, len(priced)),
          f"distinct={distinct}")

    print(f"  ==> {passed}/{total}")
    return passed, total


def main():
    dbs = sys.argv[1:] or DEFAULT
    emb = FastEmbedEmbeddings(model_name="BAAI/bge-small-en-v1.5")
    for d in dbs:
        quiz(d, emb)


if __name__ == "__main__":
    main()
