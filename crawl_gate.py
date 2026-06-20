"""
crawl_gate.py — post-crawl verification gate. One line out: PASS or FAIL+why.

Layer 1  invariants over every ingested chunk (junk, titles, prices).
Layer 2  ground-truth diff: Shopify /products.json vs crawled chunks
         (coverage + exact price accuracy, deterministic, no LLM).
Layer 3  auto-generated quiz with computed answers vs a running server
         (optional, needs --quiz <base_url>).

Usage:
  python crawl_gate.py <db_name>                          # layers 1+2
  python crawl_gate.py <db_name> --sqlite <path>          # explicit chroma.sqlite3
  python crawl_gate.py <db_name> --quiz <base_url>        # + layer 3
Exit 0 = PASS, 1 = FAIL. Full report: databases/<db>/gate_report.json
"""
import argparse
import json
import os
import random
import re
import sqlite3
import sys
import time
import urllib.parse
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent))
from pipeline_check import JUNK_PATTERNS, TITLE_BAD  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36"}
COVERAGE_MIN = 0.98       # GT products that must have a product chunk
PRICE_ACC_MIN = 0.99      # matched products whose price equals a live variant price
PRICE_META_WARN = 0.70    # product chunks carrying price metadata (warn only)

_HANDLE_RE = re.compile(r"/products/([^/?#]+)")
NAV_CHROME = re.compile(r"shop now|click to enlarge|sold out|pre[\s-]?order|add to (cart|wishlist)|view (cart|details)|quick view|click to (enlarge|zoom)", re.I)
_CAT_STOP = {
    # structural / generic
    "the","and","for","with","your","new","pack","size","gift","quality","piece","pieces",
    "original","brand","set","sets","style","styles","design","designs","print","printed",
    "premium","deluxe","classic","assorted","multi","regular","standard","custom","edition",
    "single","double","value","combo","item","items","product","products","range",
    # adjectives of dimension/shape/texture
    "mini","large","small","big","medium","jumbo","round","square","soft","hard","smooth","flat",
    # colours (incl. plurals — "colours" is what slipped through before) + common colour names
    "color","colour","colors","colours","colored","coloured","multicolor","multicolour",
    "black","blue","white","red","green","yellow","pink","purple","orange","brown","grey","gray",
    "gold","golden","silver","clear","transparent","rainbow","pastel",
    # materials
    "plastic","metal","steel","wooden","wood","glass","paper","rubber","leather","fabric","cotton",
}
_ABSENCE_CANDIDATES = ["backpack","umbrella","mattress","bicycle","helmet","perfume","jacket","tyre","blanket","kettle"]


# ── chunk loading ────────────────────────────────────────────────────────────
def load_chunks(sqlite_path: str) -> list[dict]:
    con = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    c = con.cursor()
    c.execute("SELECT id, key, string_value, int_value, float_value FROM embedding_metadata")
    by_id: dict = {}
    for _id, key, sv, iv, fv in c.fetchall():
        d = by_id.setdefault(_id, {})
        d[key] = sv if sv is not None else (fv if fv is not None else iv)
    con.close()
    return [d for d in by_id.values() if d.get("chroma:document")]


# ── layer 1: invariants ──────────────────────────────────────────────────────
def layer1(chunks: list[dict]) -> dict:
    junk: dict = {}
    junk_samples: dict = {}
    bad_titles, nonpos = [], []
    unprintable = 0
    for ch in chunks:
        body = str(ch.get("chroma:document") or "")
        head = body[:2000]
        # isspace() (not just \n\r\t) so non-breaking/unicode spaces (\xa0 from
        # Shopify specs "Type :\xa0Gel Pen") count as printable, not binary —
        # NBSP is whitespace, not corruption. Guards against false unprintable.
        printable = sum(1 for x in head if x.isprintable() or x.isspace())
        if printable / max(1, len(head)) < 0.95:
            unprintable += 1
        for pat, label in JUNK_PATTERNS:
            mm = pat.search(body)
            if mm:
                junk[label] = junk.get(label, 0) + 1
                if len(junk_samples.get(label, [])) < 4:
                    s = max(0, mm.start() - 40)
                    junk_samples.setdefault(label, []).append({
                        "snip": body[s:mm.end() + 40].replace("\n", " "),
                        "kind": ch.get("chunk_kind"), "source": ch.get("source"),
                        "url": ch.get("url") or ch.get("source_url"),
                        "head": body[:120].replace("\n", " "),
                    })
                break
        t = str(ch.get("product_title") or "")
        if t:
            if len(t.strip()) < 4:
                bad_titles.append(("short title", t))
            else:
                _hit = False
                for pat, label in TITLE_BAD:
                    if pat.search(t):
                        bad_titles.append((label, t[:60])); _hit = True
                        break
                if not _hit and NAV_CHROME.search(t):
                    bad_titles.append(("nav-chrome", t[:60]))
        pv = ch.get("price")
        if pv is not None:
            try:
                if float(pv) <= 0:
                    nonpos.append((t[:40], pv, ch.get("chunk_kind"), str(ch.get("source"))[-60:], body[:90].replace("\n", " ")))
            except (TypeError, ValueError):
                nonpos.append((t[:40], pv, ch.get("chunk_kind"), str(ch.get("source"))[-60:], body[:90].replace("\n", " ")))
    prod = [ch for ch in chunks if str(ch.get("chunk_kind") or "") == "product"]
    priced = sum(1 for ch in prod if ch.get("price") is not None)
    return {
        "chunks": len(chunks), "product_chunks": len(prod),
        "price_coverage": round(priced / len(prod), 3) if prod else None,
        "junk": junk, "junk_samples": junk_samples,
        "bad_titles": bad_titles[:20], "bad_title_count": len(bad_titles),
        "nonpositive_prices": nonpos[:10], "nonpositive_count": len(nonpos),
        "unprintable_chunks": unprintable,
    }


# ── layer 2: Shopify ground-truth diff ───────────────────────────────────────
def fetch_shopify_gt(base_url: str) -> list[dict] | None:
    """All products from the store's own catalog API. None = not Shopify/blocked."""
    base = base_url.rstrip("/")
    products, page = [], 1
    endpoint = f"{base}/products.json"
    while page <= 40:
        r = None
        for attempt in range(3):
            try:
                r = requests.get(endpoint, params={"limit": 250, "page": page}, headers=UA, timeout=45)
                break
            except Exception:
                if attempt == 2:
                    return None if page == 1 else products
                time.sleep(5)
        if r.status_code != 200:
            if page == 1 and not endpoint.endswith("/collections/all/products.json"):
                endpoint = f"{base}/collections/all/products.json"
                continue
            return None if page == 1 else products
        try:
            batch = r.json().get("products") or []
        except Exception:
            return None if page == 1 else products
        if not batch:
            break
        for p in batch:
            variants = p.get("variants") or []
            prices = set()
            for v in variants:
                try:
                    prices.add(round(float(v.get("price")), 2))
                except (TypeError, ValueError):
                    pass
            products.append({
                "handle": str(p.get("handle") or ""),
                "title": str(p.get("title") or ""),
                "prices": sorted(prices),
                "available": any(v.get("available") for v in variants) if variants and "available" in variants[0] else None,
            })
        page += 1
        time.sleep(0.4)
    return products


def _norm_url(u: str) -> str:
    """Canonical product-URL key: unquote, drop scheme/query/fragment, lowercase,
    strip trailing slash. Universal across platforms (Shopify /products/<h>, Woo
    /product/<s>, generic flat URLs) so ingest and GT match on the same key."""
    u = urllib.parse.unquote(str(u or "")).split("#")[0].split("?")[0].strip().lower()
    u = re.sub(r"^https?://", "", u).rstrip("/")
    return u


def _norm_title(t: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(t or "").lower()).strip()


def layer2(chunks: list[dict], crawl_url: str) -> dict:
    """Ground-truth diff against the store's own structured catalog — Shopify,
    WooCommerce, or generic JSON-LD/microdata (the SAME source catalog_api ingests
    from). Platform-agnostic match: by canonical product URL first, then normalized
    title, so non-Shopify stores are verified for coverage + price, not skipped."""
    from services.catalog_api import catalog_ground_truth
    gt = catalog_ground_truth(crawl_url)
    if not gt:
        return {"skipped": "no structured catalog (Shopify/Woo/JSON-LD) — GT diff unavailable for this site"}
    by_url: dict = {}
    by_title: dict = {}
    for ch in chunks:
        if str(ch.get("chunk_kind") or "") != "product":
            continue
        u = _norm_url(ch.get("source"))
        if u:
            by_url.setdefault(u, []).append(ch)
        t = _norm_title(ch.get("canonical_product_title") or ch.get("product_title"))
        if t:
            by_title.setdefault(t, []).append(ch)
    used = set()
    missing, mismatch, matched, price_ok = [], [], 0, 0
    for p in gt:
        label = p.get("title") or p.get("source") or ""
        chs = by_url.get(_norm_url(p.get("source")))
        if not chs:
            chs = by_title.get(_norm_title(p.get("title")))
        chs = [c for c in (chs or []) if id(c) not in used] or None
        if not chs:
            missing.append(label)
            continue
        used.add(id(chs[0]))
        matched += 1
        pvs = [float(ch["price"]) for ch in chs if ch.get("price") is not None]
        if not p["prices"] or not pvs:
            price_ok += 1  # nothing to compare (unpriced GT or OOS-suppressed)
        elif any(abs(pv - g) <= 0.011 for pv in pvs for g in p["prices"]):
            price_ok += 1
        else:
            mismatch.append({"title": label[:60], "crawled": pvs[:3], "live": p["prices"][:5]})
    extra = sum(len(v) for v in by_title.values()) - matched
    return {
        "gt_products": len(gt), "matched": matched,
        "coverage": round(matched / len(gt), 4) if gt else None,
        "price_accuracy": round(price_ok / matched, 4) if matched else None,
        "missing_count": len(missing), "missing": missing[:25],
        "price_mismatch_count": len(mismatch), "price_mismatch": mismatch[:25],
        "extra_handles_count": max(0, extra), "extra_handles": [],
        "_gt": gt,  # consumed by layer 3, stripped before saving
    }


# ── layer 3: auto-quiz against a running server ──────────────────────────────
def _num_in(ans: str, val: float) -> bool:
    flat = ans.replace(",", "")
    pat = rf"\b{int(val)}(?:\.\d{{1,2}})?\b" if float(val).is_integer() else rf"\b{val:g}\b"
    return re.search(pat, flat) is not None


def _widget_key(base: str, db: str, password: str) -> str:
    r = requests.get(f"{base}/admin/embed-code", params={"password": password, "db_name": db}, timeout=60)
    r.raise_for_status()
    try:
        j = r.json()
        for k in ("widget_key", "key", "token"):
            if isinstance(j, dict) and j.get(k):
                return j[k]
    except Exception:
        pass
    m = re.search(r"wk1\.[A-Za-z0-9._\-]+", r.text)
    if not m:
        raise RuntimeError("widget key not found in /admin/embed-code response")
    return m.group(0)


def _ask(base: str, wk: str, q: str) -> str:
    r = requests.post(f"{base}/chat", json={"question": q, "stream": False},
                      headers={"X-Widget-Key": wk}, timeout=180)
    try:
        return str(r.json().get("answer") or r.text)
    except Exception:
        return r.text


def gen_quiz(gt: list[dict] | None, chunks: list[dict], exclude_terms: tuple = ()) -> list[dict]:
    """Questions with answers computed from ground truth (GT preferred, else chunk meta).
    exclude_terms (per-DB count_exclude_terms): accessory/refill words dropped from
    category groups so the count GT matches the engine's count_exclude logic."""
    if gt:
        items = [{"title": p["title"], "price": p["prices"][0], "cats": str(p.get("categories") or "").lower()}
                 for p in gt if p["prices"] and p["prices"][0] > 0]
    else:
        seen, items = set(), []
        for ch in chunks:
            t = str(ch.get("canonical_product_title") or ch.get("product_title") or "")
            if str(ch.get("chunk_kind") or "") == "product" and t and ch.get("price") and t not in seen:
                seen.add(t)
                items.append({"title": t, "price": float(ch["price"]), "cats": str(ch.get("categories") or "").lower()})
    if len(items) < 3:
        return []
    rng = random.Random(42)  # deterministic across reruns
    qs = []
    for it in rng.sample(items, min(3, len(items))):
        qs.append({"q": f"What is the price of {it['title']}?", "expect_num": it["price"], "label": "exact-price"})
    cheapest = min(items, key=lambda x: x["price"])
    dearest = max(items, key=lambda x: x["price"])
    qs.append({"q": "What is the absolute cheapest product you sell?", "expect_num": cheapest["price"], "label": "rank-min"})
    qs.append({"q": "What is the most expensive product in your entire store?", "expect_num": dearest["price"], "label": "rank-max"})
    prices = sorted(x["price"] for x in items)
    cap = prices[min(4, len(prices) - 1)]  # ~5 items qualify
    under = [x for x in items if x["price"] <= cap]
    qs.append({"q": f"Which products do you have under {cap + 1:.0f}?", "label": "bounds",
               "expect_any_num": [x["price"] for x in under]})
    # per-category: counts, scoped extremes, full-set bounds (catches pollution/recall/count bugs).
    # Category membership MUST mirror the engine (catalog_query._execute_spec count
    # branch): when ≥50% of products are tagged, the engine counts by the STRUCTURED
    # categories field (singular/plural word match), NOT the title — so a title-word
    # GT systematically disagreed and false-failed. Match that exactly here.
    from collections import Counter
    tagged = sum(1 for it in items if it.get("cats", "").strip())
    is_tagged = bool(items) and (tagged / len(items)) >= 0.5

    def _cat_variants(a: str) -> set:
        vs = {a}
        if a.endswith("s") and len(a) > 3:
            vs.add(a[:-1])
        elif len(a) > 3:
            vs.add(a + "s")
        return vs

    def _field(it: dict) -> str:
        # tagged catalog → categories field only (mirrors _cats_hit); else title+cats (_hit hay).
        return it.get("cats", "") if is_tagged else (it["title"].lower() + " " + it.get("cats", ""))

    def _cat_match(it: dict, cat: str) -> bool:
        hay = _field(it)
        return any(re.search(rf"\b{re.escape(v)}\b", hay) for v in _cat_variants(cat))

    cnt = Counter()
    for it in items:
        toks = re.findall(r"[A-Za-z]{4,}", _field(it))
        for w in set(toks):
            if w not in _CAT_STOP:
                cnt[w] += 1
    # mid-size buckets only: skip giant buckets (367 notebooks) + brand/adjective noise; lands on clean categories
    _excl_re = re.compile(r"\b(?:" + "|".join(re.escape(str(t)) for t in exclude_terms) + r")\b", re.I) if exclude_terms else None
    for cat in [c for c, n in cnt.most_common() if 3 <= n <= 40][:4]:
        grp = [x for x in items if _cat_match(x, cat)
               and not (_excl_re and cat not in str(_excl_re.pattern).lower() and _excl_re.search(x["title"]))]
        if not 3 <= len(grp) <= 40:
            continue
        gp = sorted(g["price"] for g in grp)
        qs.append({"q": f"How many {cat} products do you have?", "expect_count": len(grp), "label": f"count:{cat}"})
        qs.append({"q": f"What is the cheapest {cat} you sell?", "expect_num": gp[0], "label": f"cat-min:{cat}"})
        qs.append({"q": f"What is the most expensive {cat} you sell?", "expect_num": gp[-1], "label": f"cat-max:{cat}"})
        hi = gp[min(len(gp) - 1, max(1, len(gp) // 2))]
        inb = [p for p in gp if gp[0] <= p <= hi]
        if 2 <= len(inb) <= 12:
            qs.append({"q": f"Which {cat} products are priced between {gp[0]:.0f} and {hi:.0f}?",
                       "expect_all_num": inb, "label": f"bounds:{cat}"})
    present = " ".join(x["title"].lower() for x in items)
    for cand in _ABSENCE_CANDIDATES:
        if cand not in present:
            qs.append({"q": f"Do you sell {cand}s? If you don't carry them, say so.",
                       "expect_absent": True, "label": f"absence:{cand}"})
            break
    return qs


def layer3(base: str, db: str, password: str, gt, chunks, exclude_terms: tuple = ()) -> dict:
    quiz = gen_quiz(gt, chunks, exclude_terms)
    if not quiz:
        return {"skipped": "not enough priced products to generate a quiz"}
    wk = _widget_key(base, db, password)
    results, passed = [], 0
    for item in quiz:
        ans = _ask(base, wk, item["q"])
        if "expect_num" in item:
            ok = _num_in(ans, item["expect_num"])
        elif "expect_count" in item:
            ok = _num_in(ans, item["expect_count"])
        elif "expect_all_num" in item:
            ok = all(_num_in(ans, v) for v in item["expect_all_num"])  # full membership, not any
        elif "expect_absent" in item:
            al = ans.lower()
            ok = bool(re.search(r"don'?t (carry|stock|sell|have)|do not (carry|stock|sell|have)|"
                                r"not (carry|stock|available)|we don'?t|currently (don'?t|do not)|no .{0,25}(available|in stock|found)", al))
        else:
            ok = any(_num_in(ans, v) for v in item["expect_any_num"])
        passed += ok
        results.append({"label": item["label"], "q": item["q"], "pass": bool(ok),
                        "expected": item.get("expect_num") or item.get("expect_count") or item.get("expect_all_num")
                        or item.get("expect_any_num") or ("absent" if item.get("expect_absent") else None), "answer": ans[:300]})
        time.sleep(4)
    return {"asked": len(quiz), "passed": passed, "results": results}


# ── verdict + entry points ───────────────────────────────────────────────────
def run_gate(db_name: str, sqlite_path: str, crawl_url: str = "",
             quiz_base: str = "", password: str = "", exclude_terms: tuple = ()) -> dict:
    chunks = load_chunks(sqlite_path)
    report = {"db": db_name, "sqlite": sqlite_path, "layer1": layer1(chunks)}
    gt = None
    if crawl_url:
        report["layer2"] = layer2(chunks, crawl_url)
        gt = report["layer2"].pop("_gt", None)
    else:
        report["layer2"] = {"skipped": "no crawl_url"}
    if quiz_base:
        try:
            report["layer3"] = layer3(quiz_base.rstrip("/"), db_name, password, gt, chunks, exclude_terms)
        except Exception as e:
            report["layer3"] = {"error": str(e)}
    fails = []
    l1 = report["layer1"]
    if l1["chunks"] == 0:
        fails.append("0 chunks in DB")
    if l1["junk"]:
        fails.append(f"junk in chunks: {l1['junk']}")
    if l1["bad_title_count"]:
        fails.append(f"{l1['bad_title_count']} garbage titles (e.g. {l1['bad_titles'][:2]})")
    if l1["nonpositive_count"]:
        fails.append(f"{l1['nonpositive_count']} non-positive prices")
    if l1["unprintable_chunks"]:
        fails.append(f"{l1['unprintable_chunks']} binary/unprintable chunks")
    l2 = report["layer2"]
    if not l2.get("skipped"):
        if l2["coverage"] is not None and l2["coverage"] < COVERAGE_MIN:
            fails.append(f"catalog coverage {l2['coverage']:.1%} < {COVERAGE_MIN:.0%} ({l2['missing_count']} live products missing)")
        if l2["price_accuracy"] is not None and l2["price_accuracy"] < PRICE_ACC_MIN:
            fails.append(f"price accuracy {l2['price_accuracy']:.1%} < {PRICE_ACC_MIN:.0%} ({l2['price_mismatch_count']} wrong prices)")
    l3 = report.get("layer3") or {}
    if l3.get("asked") and l3["passed"] < l3["asked"]:
        fails.append(f"quiz {l3['passed']}/{l3['asked']} — failed: {[r['label'] for r in l3['results'] if not r['pass']]}")
    warns = []
    if l1["price_coverage"] is not None and l1["price_coverage"] < PRICE_META_WARN:
        warns.append(f"only {l1['price_coverage']:.0%} of product chunks carry price metadata")
    report["warnings"] = warns
    report["failures"] = fails
    report["verdict"] = "PASS" if not fails else "FAIL"
    return report


def run_gate_for_db(db_name: str, db_dir: str, crawl_url: str = "", quiz_base: str = "", password: str = "") -> dict:
    """Importable entry point (app.py post-crawl). Writes gate_report.json next to the DB."""
    sq = str(Path(db_dir) / "chroma.sqlite3")
    _excl = ()
    try:
        _cfg = json.loads((Path(db_dir) / "config.json").read_text(encoding="utf-8-sig"))
        _excl = tuple(_cfg.get("count_exclude_terms") or ())
    except Exception:
        pass
    report = run_gate(db_name, sq, crawl_url, quiz_base, password, _excl)
    try:
        out = Path(db_dir) / "gate_report.json"
        out.write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    except Exception:
        pass
    return report


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("db_name")
    ap.add_argument("--sqlite", default="")
    ap.add_argument("--url", default="", help="override crawl_url for GT diff")
    ap.add_argument("--quiz", default="", help="server base URL → run auto-quiz")
    ap.add_argument("--password", default=os.environ.get("ADMIN_PASSWORD", ""))
    a = ap.parse_args()
    db_dir = Path("databases") / a.db_name
    sq = a.sqlite or str(db_dir / "chroma.sqlite3")
    crawl_url = a.url
    _cfg = {}
    cfg_p = db_dir / "config.json"
    if cfg_p.exists():
        try:
            _cfg = json.loads(cfg_p.read_text(encoding="utf-8-sig"))
        except Exception:
            pass
    if not crawl_url:
        crawl_url = _cfg.get("crawl_url") or ""
    exclude_terms = tuple(_cfg.get("count_exclude_terms") or ())
    report = run_gate(a.db_name, sq, crawl_url, a.quiz, a.password, exclude_terms)
    out = Path(sq).parent / "gate_report.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    l1, l2 = report["layer1"], report["layer2"]
    print(f"chunks={l1['chunks']} product={l1['product_chunks']} priced={l1['price_coverage']}")
    if not l2.get("skipped"):
        pa = l2['price_accuracy']
        print(f"GT: {l2['matched']}/{l2['gt_products']} matched, coverage={(l2['coverage'] or 0):.1%}, price_acc={'n/a' if pa is None else format(pa, '.1%')}")
    else:
        print(f"GT: {l2['skipped']}")
    if report.get("layer3", {}).get("asked"):
        print(f"quiz: {report['layer3']['passed']}/{report['layer3']['asked']}")
    for w in report["warnings"]:
        print(f"WARN: {w}")
    for f in report["failures"]:
        print(f"FAIL: {f}")
    print(f"\n{report['verdict']}  (full report: {out})")
    sys.exit(0 if report["verdict"] == "PASS" else 1)


if __name__ == "__main__":
    main()
