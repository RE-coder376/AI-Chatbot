"""
pipeline_check.py — offline crawl+ingestion pipeline validator.

Fetches representative live pages for EVERY site type the chatbot serves,
runs the EXACT production transform (services.page_extract.extract_page_text
→ _smart_chunk_page), and asserts universal invariants. Run this locally
before any crawl — green here means the crawl will ingest clean data.

Usage:  python pipeline_check.py            # full battery
        python pipeline_check.py <url>      # single page, verbose dump
"""

import re
import sys

import requests

sys.path.insert(0, ".")
from services.page_extract import extract_page_text  # noqa: E402
from services.crawler_utils import _smart_chunk_page, set_docs_path_hints  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36"}

# kind: expected chunk_kind of first doc ('product' | 'category' | 'any')
# price: (lo, hi) the first product chunk's price must fall in, or None = no price required
BATTERY = [
    # ── Shopify store (babyfy.pk) ────────────────────────────────────────────
    dict(url="https://babyfy.pk/products/educational-drawing-robot-toy", kind="product", price=(14000, 14500), site="shopify"),
    dict(url="https://babyfy.pk/products/2-in-1-doll-catcher-atm-claw-machine", kind="product", price=(12000, 12700), site="shopify"),
    dict(url="https://babyfy.pk/collections/all", kind=("category", "catalog"), price=None, site="shopify"),
    dict(url="https://babyfy.pk/", kind="any", price=None, site="shopify"),
    # ── Shopify store #2 (belony.pk) ─────────────────────────────────────────
    # belony.pk DNS dead as of 2026-06-11 — optional until the domain returns
    dict(url="https://belony.pk/", kind="any", price=None, site="shopify2", optional=True),
    # ── Shopify store #3 (thestationerycompany.pk / tsc_pk) ─────────────────
    dict(url="https://thestationerycompany.pk/", kind="any", price=None, site="shopify3"),
    # ── Static catalog with £ + carousels (books.toscrape.com) ──────────────
    dict(url="https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html", kind="product", price=(51.77, 51.77), site="static-catalog"),
    dict(url="https://books.toscrape.com/catalogue/born-for-this-how-to-find-the-work-you-were-meant-to-do_588/index.html", kind="product", price=(21.59, 21.59), site="static-catalog"),
    dict(url="https://books.toscrape.com/catalogue/a-piece-of-sky-a-grain-of-rice-a-memoir-in-four-meditations_878/index.html", kind="product", price=None, site="static-catalog"),
    dict(url="https://books.toscrape.com/catalogue/category/books/poetry_23/index.html", kind="category", price=None, site="static-catalog"),
    # ── Server-rendered test shop with variants (webscraper.io) ─────────────
    dict(url="https://webscraper.io/test-sites/e-commerce/allinone/product/1", kind="product", price=(24.99, 24.99), site="test-shop"),
    dict(url="https://webscraper.io/test-sites/e-commerce/allinone/product/88", kind="product", price=(370, 500), site="test-shop"),
    dict(url="https://webscraper.io/test-sites/e-commerce/allinone/phones/touch", kind="category", price=None, site="test-shop"),
    # ── Docs site (agentfactory.panaversity.org) ─────────────────────────────
    dict(url="https://agentfactory.panaversity.org/", kind="any", price=None, site="docs", docs_hints=["/roman/", "/arabic/", "/spanish/", "/hindi/", "/chinese/"]),
    dict(url="https://agentfactory.panaversity.org/docs/intro", kind="any", price=None, site="docs", docs_hints=["/roman/", "/arabic/", "/spanish/", "/hindi/", "/chinese/"], optional=True),
]

# Universal invariants applied to EVERY chunk of every page
JUNK_PATTERNS = [
    (re.compile(r'(?i)toggle navigation'), "nav boilerplate"),
    (re.compile(r'(?i)your cart is currently empty'), "cart widget"),
    (re.compile(r'(?i)subtotal:?\s*(?:rs\.?|pkr|\$|£|€)\s*0(?:\.00)?\b'), "zero subtotal"),
    (re.compile(r'&[a-z]+;|&#\d+;'), "HTML entity"),
    (re.compile(r'Â£'), "mojibake"),
]
TITLE_BAD = [
    (re.compile(r'(?i)^(?:rs\.?|pkr|\$|£|€)\s*[\d.,]*$'), "price-fragment title"),
    (re.compile(r'[–—|-]\s*(?:babyfy|books to scrape|web scraper)\s*$', re.I), "site-suffix title"),
    (re.compile(r'(?i)\b(?:in stock|tax|availability|subtotal|add to cart)\b'), "spec-word title"),
    (re.compile(r'(?i)^(?:home|index|sandbox)\b.{0,12}$'), "nav-word title"),
]


def _docs_like(url, hints):
    u = url.lower()
    return any(h in u for h in (["/docs/", "/guide"] + (hints or [])))


def check_page(case) -> list[str]:
    errors = []
    url = case["url"]
    set_docs_path_hints(case.get("docs_hints") or [])
    r = None
    for _attempt in range(3):
        try:
            r = requests.get(url, timeout=25, headers=UA)
            break
        except Exception as e:
            if _attempt == 2:
                return [f"FETCH FAILED: {e}"]
            import time
            time.sleep(4)
    if r.status_code != 200:
        return [f"HTTP {r.status_code}"]
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "charset" not in ctype:
        r.encoding = "utf-8"
    ext = extract_page_text(r.text, url, docs_like=_docs_like(url, case.get("docs_hints")))
    if not ext:
        return ["extract_page_text returned None"]
    text, meta = ext["text"], ext["page_meta"]
    # printability — catches compression/binary regressions (the brotli class)
    printable = sum(1 for ch in text[:2000] if ch.isprintable() or ch in "\n\r\t")
    if printable / max(1, len(text[:2000])) < 0.95:
        errors.append(f"low printability {printable / max(1, len(text[:2000])):.2f} — binary/compressed text?")
    docs = _smart_chunk_page(text, url, 400, 320, page_meta=meta)
    if not docs:
        errors.append("0 chunks produced")
        return errors
    d0 = docs[0]
    kind = str(d0.metadata.get("chunk_kind") or "")
    _want = case["kind"] if isinstance(case["kind"], tuple) else (case["kind"],)
    if "any" not in _want and kind not in _want:
        errors.append(f"chunk_kind={kind!r}, expected {_want!r}")
    if case.get("price"):
        lo, hi = case["price"]
        pv = d0.metadata.get("price")
        if pv is None or not (lo <= float(pv) <= hi):
            errors.append(f"price={pv}, expected within [{lo}, {hi}]")
    # universal chunk invariants
    for d in docs:
        body = d.page_content
        for pat, label in JUNK_PATTERNS:
            if pat.search(body):
                errors.append(f"{label} in chunk: {pat.search(body).group(0)[:40]!r}")
                break
        t = str(d.metadata.get("product_title") or "")
        if t:
            if len(t.strip()) < 4:
                errors.append(f"short title {t!r}")
            for pat, label in TITLE_BAD:
                if pat.search(t):
                    errors.append(f"{label}: {t[:60]!r}")
                    break
        pv = d.metadata.get("price")
        if pv is not None and float(pv) <= 0:
            errors.append(f"non-positive price {pv} (title={t[:40]!r})")
    return errors


def main():
    if len(sys.argv) > 1:
        case = dict(url=sys.argv[1], kind="any", price=None, site="manual")
        errs = check_page(case)
        print("PASS" if not errs else "FAIL")
        for e in errs:
            print("  -", e)
        return
    failed = 0
    for case in BATTERY:
        errs = check_page(case)
        if errs and case.get("optional"):
            print(f"SKIP {case['site']:15s} {case['url'][:70]}  ({errs[0]})")
            continue
        status = "PASS" if not errs else "FAIL"
        if errs:
            failed += 1
        print(f"{status} {case['site']:15s} {case['url'][:70]}")
        for e in errs[:4]:
            print(f"     - {e}")
    print(f"\n{'ALL GREEN — crawl-safe' if failed == 0 else f'{failed} page(s) FAILED — fix before crawling'}")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
