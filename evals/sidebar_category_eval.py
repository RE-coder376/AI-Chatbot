"""Sidebar-category eval — tests the bot the way a CUSTOMER browses: pick storefront
collections (the website sidebar/menu groups), ask the bot to list them, and verify
against the LIVE collection pages (/collections/<handle>/products.json) — NOT
/products.json (which has no collection membership and was the original blind spot).

Also tests the reverse "location" question: name a product, expect its category.

MEASURED only. Run:  python evals/sidebar_category_eval.py
"""
import sys, os, re, random, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import chromadb
from services.catalog_api import build_catalog_docs, fetch_collection_map, fetch_all_products
from services import catalog_query as CQ

random.seed(42)

STORES = {
    "babyfy": "https://babyfy.pk/",
    "tsc": "https://thestationerycompany.pk",
    "stationery_studio": "https://stationerystudio.pk",
}

def norm(s): return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()

def build_db(docs):
    col = chromadb.PersistentClient(path=tempfile.mkdtemp()).get_or_create_collection("evalcol")
    col.add(ids=[str(i) for i in range(len(docs))], embeddings=[[0.0]*8 for _ in docs],
            metadatas=[dict(d.metadata) for d in docs], documents=[d.page_content for d in docs])
    class DB: _collection = col
    return DB()

def answer_titles(ans):
    out = []
    for line in (ans or "").splitlines():
        m = re.match(r"\s*\d+[.)]\s*(.+?)\s*-\s*Rs", line) or re.match(r"\s*[-*]\s*(.+?)\s*-\s*Rs", line)
        if m: out.append(m.group(1).strip())
    return out

def run_store(name, url):
    print(f"\n{'='*70}\n{name}  ({url})\n{'='*70}")
    products = fetch_all_products(url) or []
    handle_title = {str(p.get("handle")): re.sub(r"\s+"," ",str(p.get("title") or "")).strip() for p in products}
    colmap = fetch_collection_map(url)
    col_products = {}
    for h, cols in colmap.items():
        t = handle_title.get(h)
        if not t: continue
        for c in cols:
            col_products.setdefault(c, set()).add(t)
    db = build_db(build_catalog_docs(url))
    eligible = [(c, ts) for c, ts in col_products.items() if 2 <= len(ts) <= 12]
    random.shuffle(eligible)
    sample = eligible[:6]
    p = f = 0
    for c, gt in sample:
        gt_n = {norm(t) for t in gt}
        for phrasing in [f"show all products listed in {c}", f"what's in the {c} category"]:
            ans, _ = CQ.answer_catalog_query(phrasing, db)
            got = answer_titles(ans); got_n = {norm(t) for t in got}
            extra = [t for t in got if norm(t) not in gt_n]
            missing = [t for t in gt if norm(t) not in got_n]
            ok = bool(ans) and not extra and not missing
            p += ok; f += (not ok)
            print(f"  [{'PASS' if ok else 'FAIL'}] {phrasing!r} (gt={len(gt)})")
            if not ok:
                if not ans: print("        -> RAG (no list)")
                if missing: print(f"        -> MISSING {missing[:5]}")
                if extra:   print(f"        -> EXTRA   {extra[:5]}")
    # reverse location question
    lp = lf = 0
    for prod, cat in [(t, c) for c, ts in sample for t in list(ts)[:1]][:4]:
        ans, _ = CQ.answer_catalog_query(f"which category is {prod} in", db)
        ok = bool(ans) and cat.lower() in (ans or "").lower()
        lp += ok; lf += (not ok)
        print(f"  [{'PASS' if ok else 'FAIL'}] loc: 'which category is {prod[:40]}… in' -> {'has '+cat if ok else (ans or 'RAG')[:60]}")
    print(f"\n  {name}: listing {p}/{p+f} | location {lp}/{lp+lf}")
    return p, f, lp, lf

if __name__ == "__main__":
    P = F = LP = LF = 0
    for n, u in STORES.items():
        try:
            a, b, c, d = run_store(n, u); P += a; F += b; LP += c; LF += d
        except Exception as e:
            import traceback; traceback.print_exc(); print(f"  {n}: ERROR {e}")
    print(f"\n{'#'*70}\nTOTAL listing: {P}/{P+F}   |   location: {LP}/{LP+LF}\n{'#'*70}")
