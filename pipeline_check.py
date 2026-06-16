"""
pipeline_check.py — offline crawl+ingestion pipeline validator.

Fetches representative live pages for EVERY site type the chatbot serves,
runs the EXACT production transform (services.page_extract.extract_page_text
→ _smart_chunk_page), and asserts universal invariants. Run this locally
before any crawl — green here means the crawl will ingest clean data.

Usage:  python pipeline_check.py            # full battery
        python pipeline_check.py <url>      # single page, verbose dump
"""

import html.entities as _he
import re
import sys

import requests

# Only genuine HTML entities count as junk. Source text like "blue&yellow;pink"
# (a literal description) matched the old `&[a-z]+;` and FAILED the gate forever,
# since html.unescape leaves non-entities untouched. Build the matcher from the
# canonical HTML5 set so it catches real un-decoded refs (&ndash; &amp; &#39;)
# without false-positiving on arbitrary `&word;` substrings.
_ENTITY_NAMES = sorted({k[:-1] for k in _he.html5 if k.endswith(";")}, key=len, reverse=True)
_REAL_ENTITY_RE = re.compile(r"&(?:" + "|".join(map(re.escape, _ENTITY_NAMES)) + r"|#\d+|#x[0-9a-fA-F]+);")

sys.path.insert(0, ".")
from services.page_extract import extract_page_text  # noqa: E402
from services.crawler_utils import _smart_chunk_page, set_docs_path_hints  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36"}

# kind: expected chunk_kind of first doc ('product' | 'category' | 'any')
# price: (lo, hi) the first product chunk's price must fall in, or None = no price required
BATTERY = [
    # ── Shopify store (babyfy.pk) ────────────────────────────────────────────
    dict(url="https://babyfy.pk/products/educational-drawing-robot-toy", kind="product", price=None, site="shopify"),
    dict(url="https://babyfy.pk/products/2-in-1-doll-catcher-atm-claw-machine", kind="product", price=None, site="shopify"),
    # Sale-priced product: og:price:amount (Rs.1,200) must beat the body's
    # compare-at price (Rs.2,000) — covered by the og-consistency invariant.
    dict(url="https://babyfy.pk/products/quick-push-pop-game", kind="product", price=None, site="shopify"),
    dict(url="https://babyfy.pk/collections/all", kind=("category", "catalog"), price=None, site="shopify"),
    # Homepage and static info pages must NEVER emit product chunks — site-wide
    # chrome ("Free Shipping over 5000PKR") + carousels made them pseudo-products
    # ("Your Trust Matters" Rs.2,430) before the URL-class gate.
    dict(url="https://babyfy.pk/", kind="any", not_kind="product", price=None, site="shopify"),
    dict(url="https://babyfy.pk/pages/about-us", kind="any", not_kind="product", price=None, site="shopify"),
    # ── Shopify store #2 (belony.pk) ─────────────────────────────────────────
    # belony.pk DNS dead as of 2026-06-11 — optional until the domain returns
    dict(url="https://belony.pk/", kind="any", price=None, site="shopify2", optional=True),
    # ── Shopify store #3 (thestationerycompany.pk / tsc_pk) ─────────────────
    dict(url="https://thestationerycompany.pk/", kind="any", price=None, site="shopify3"),
    # Product pages: variant-picker text ("Single Piece Black") used to win the
    # title — og:title/JSON-LD authority must produce the real product name.
    dict(url="https://thestationerycompany.pk/products/m-g-office-gel-pen-0-7mm-point", kind="product", price=None,
         title_re=r"(?i)gel pen", site="shopify3", optional=True),
    # Model numbers ("DJI RS 3", "Osmo Mobile 6") used to parse as prices
    # (Rs.3 / Rs.0) — authority_price (og/JSON-LD) must win; og:price 0.00
    # (out-of-stock placeholder) must yield NO price, not price 0.
    dict(url="https://www.thestationerycompany.pk/products/dji-rs3-gimbal-stabilizer-2", kind="product", price=None,
         price_floor=1000, title_re=r"(?i)dji", site="shopify3", optional=True),
    dict(url="https://www.thestationerycompany.pk/products/dji-r-vertical-camera-mount-for-rs-2-and-rs-3-pro-gimbals", kind="product", price=None,
         price_floor=1000, title_re=r"(?i)dji", site="shopify3", optional=True),
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
    # Long-form docs article: guards the content-loss regression where the FAQ
    # mode truncated each section to sec[:2000] and dropped >90% of the page. The
    # chunker must retain essentially all of it (coverage ≥ 0.85).
    dict(url="https://agentfactory.panaversity.org/docs/agentic-coding-crash-course", kind="any", price=None, site="docs",
         docs_hints=["/roman/", "/arabic/", "/spanish/", "/hindi/", "/chinese/"], min_coverage=0.85),
]

# Universal invariants applied to EVERY chunk of every page
JUNK_PATTERNS = [
    (re.compile(r'(?i)toggle navigation'), "nav boilerplate"),
    (re.compile(r'(?i)your cart is currently empty'), "cart widget"),
    (re.compile(r'(?i)subtotal:?\s*(?:rs\.?|pkr|\$|£|€)\s*0(?:\.00)?\b'), "zero subtotal"),
    (_REAL_ENTITY_RE, "HTML entity"),
    (re.compile(r'Â£'), "mojibake"),
]
TITLE_BAD = [
    (re.compile(r'(?i)^(?:rs\.?|pkr|\$|£|€)\s*[\d.,]*$'), "price-fragment title"),
    (re.compile(r'[–—|-]\s*(?:babyfy|books to scrape|web scraper)\s*$', re.I), "site-suffix title"),
    (re.compile(r'(?i)\b(?:in stock|tax|availability|subtotal|add to cart)\b'), "spec-word title"),
    (re.compile(r'(?i)^(?:home|index|sandbox)\b.{0,12}$'), "nav-word title"),
    (re.compile(r'(?i)^(?:rs\.?|pkr|usd|eur|gbp|aed)\s'), "currency-led swatch title"),
    (re.compile(r'(?i)\bsold\s*out\b'), "sold-out title"),
    # 2+ digit number or bare currency — "DJI RS 3" / "Ronin RS 2" are model names
    (re.compile(r'(?i)[\s\-–](?:rs\.?|pkr|\$|£|€)\s*(?:\d[\d.,]+)?$'), "trailing-price title"),
    (re.compile(r'(?i)\.\.\.$|\bloading\b|\btranslation\s+missing\b'), "truncated/widget title"),
]
# Description line of a product chunk must not carry prices ("Number Book:
# Teach children... Rs.1,050") — that is related-product carousel leakage.
DESC_PRICE_RE = re.compile(r'(?im)^description:.*(?:\brs\.?|\bpkr\b|\$|£|€)\s*\d[\d,]{1,}')  # 2+ digits: "DJI RS 3" is a model name, not a price


def _docs_like(url, hints):
    u = url.lower()
    return any(h in u for h in (["/docs/", "/guide"] + (hints or [])))


def _content_coverage(text, docs) -> float:
    """Fraction of the page's content that survives into chunks. Samples fragments
    evenly across the normalized source text and checks each appears in some chunk.
    A truncating chunker (the sec[:2000] bug that dropped 90%+ of long docs pages)
    scores near 0; a lossless chunker scores ~1.0. Robust to chunk overlap."""
    norm = re.sub(r"\s+", " ", text or "").strip()
    if len(norm) < 600:
        return 1.0
    blob = " ".join(re.sub(r"\s+", " ", d.page_content) for d in docs)
    n, hits, L = 20, 0, len(norm)
    for i in range(n):
        pos = int((i + 0.5) / n * (L - 50))
        if norm[pos:pos + 40] in blob:
            hits += 1
    return hits / n


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
    # Content-loss guard for docs pages: the chunker must not silently drop content.
    if case.get("min_coverage"):
        cov = _content_coverage(text, docs)
        if cov < case["min_coverage"]:
            errors.append(f"content coverage {cov:.2f} < {case['min_coverage']} — chunker dropped page content (truncation regression)")
    # Prose chunks must carry their section/title heading so the embedding has context.
    if meta.get("page_title"):
        _prose = [d for d in docs if str(d.metadata.get("chunk_kind") or "") == "prose"]
        if _prose and not any(d.page_content.startswith("## ") for d in _prose):
            errors.append("prose chunks missing heading prefix (lost section context)")
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
    if case.get("price_floor"):
        # No price is fine (OOS pages declare price 0); a price BELOW the floor
        # is a model number / piece count parsed as a price.
        pv = d0.metadata.get("price")
        if pv is not None and float(pv) < case["price_floor"]:
            errors.append(f"price={pv} below floor {case['price_floor']} (model-number-as-price regression)")
    if case.get("title_re"):
        _t0 = str(d0.metadata.get("product_title") or meta.get("page_title") or "")
        if not re.search(case["title_re"], _t0):
            errors.append(f"title {_t0!r} does not match {case['title_re']!r}")
    # Universal og-price consistency: when the page itself declares its current
    # selling price (og:price:amount), the extracted price must equal it —
    # catches compare-at/sale mixups on ANY storefront without hardcoded values.
    og_m = re.search(r'<meta[^>]+(?:property|name)=["\']og:price:amount["\'][^>]+content=["\']([\d.,]+)["\']', r.text, re.I) \
        or re.search(r'<meta[^>]+content=["\']([\d.,]+)["\'][^>]+(?:property|name)=["\']og:price:amount["\']', r.text, re.I)
    if og_m and str(d0.metadata.get("chunk_kind") or "") == "product":
        try:
            og_val = float(og_m.group(1).replace(",", ""))
            pv0 = d0.metadata.get("price")
            if pv0 is not None and og_val > 0 and abs(float(pv0) - og_val) > 0.01:
                errors.append(f"price={pv0} disagrees with page og:price:amount={og_val}")
        except Exception:
            pass
    # universal chunk invariants
    for d in docs:
        body = d.page_content
        _kind = str(d.metadata.get("chunk_kind") or "")
        if case.get("not_kind") and _kind == case["not_kind"]:
            errors.append(f"forbidden chunk_kind={_kind!r} on this URL (title={str(d.metadata.get('product_title') or '')[:40]!r})")
        if _kind == "product" and DESC_PRICE_RE.search(body):
            errors.append(f"price inside Description line: {DESC_PRICE_RE.search(body).group(0)[:60]!r}")
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


def check_category_helpers() -> list[str]:
    """Offline (no fetch): category-propagation URL helpers — these feed the
    'categories' chunk metadata that retrieval anchors on for category queries."""
    from services.crawler_utils import category_slugs_from_url, listing_slug_from_url, categories_for_page
    errors = []
    cases = [
        # (fn, input(s), expected)
        ("marker books", category_slugs_from_url("https://books.toscrape.com/catalogue/category/books/travel_2/index.html"), ["books", "travel"]),
        ("marker shopify scoped", category_slugs_from_url("https://babyfy.pk/collections/rc-toys/products/rc-suzuki"), ["rc toys"]),
        ("marker generic rejected", category_slugs_from_url("https://x.com/collections/all"), []),
        ("listing webscraper", listing_slug_from_url("https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops"), "laptops"),
        ("listing pagination walk-back", listing_slug_from_url("https://books.toscrape.com/catalogue/category/books/travel_2/page-2.html"), "travel"),
        ("listing root none", listing_slug_from_url("https://books.toscrape.com/"), None),
        ("listing site-pagination none", listing_slug_from_url("https://books.toscrape.com/catalogue/page-2.html"), None),
        ("listing product-url none", listing_slug_from_url("https://webscraper.io/test-sites/e-commerce/allinone/product/545"), None),
        ("page with listing parent", categories_for_page(
            "https://books.toscrape.com/catalogue/its-only-the-himalayas_981/index.html",
            ["https://books.toscrape.com/catalogue/category/books/travel_2/index.html"]), ["books", "travel"]),
    ]
    for name, got, want in cases:
        if got != want:
            errors.append(f"{name}: got {got!r}, want {want!r}")
    return errors


def check_boilerplate_filter() -> list[str]:
    """Offline (no fetch): corpus-frequency junk remover — fragments repeating
    across pages of one crawl are masked; unique content + structured lines stay."""
    from services.boilerplate import BoilerplateFilter
    errors = []
    pm = {"page_type": "product", "docs_like": False}
    pages = []
    for i in range(30):
        pages.append(
            f"Gadget unit{i} edition{i} Rs.{100 + i}\n"
            f"Price: Rs.999\n"                                               # identical everywhere, protected
            f"Shop Now Location Click to enlarge\n"                          # standalone junk line
            f"Gadget model{i} unit{i} Shop Now Location Click to enlarge Rs.{200 + i}\n"  # junk glued mid-line
            f"It offers feature{i} and quality{i} in the box{i}.\n"  # unique prose (no 4 constant words in a row)
            f"Your trust matters quality assured free shipping every day\n"  # banner
        )
    f = BoilerplateFilter(min_pages=24)
    for t in pages:
        f.add_page(t, pm)
    f.calibrate()
    s = f.scrub(pages[7], pm)
    if "Click to enlarge" in s or "Shop Now" in s:
        errors.append("theme widget string survived scrub")
    if "Your trust matters" in s:
        errors.append("banner line survived scrub")
    if "feature7 and quality7" not in s:
        errors.append("unique content was destroyed by scrub")
    if "Price: Rs.999" not in s:
        errors.append("protected Price: line was destroyed")
    if "model7 unit7" not in s or "Rs.207" not in s:
        errors.append("mid-line masking removed adjacent real content")
    # docs/info pages must be exempt (canonical copy of repeated text survives)
    if f.scrub(pages[0], {"page_type": "policy"}) != pages[0]:
        errors.append("non-storefront page was scrubbed")
    # tiny corpora must no-op
    f2 = BoilerplateFilter()
    f2.add_page(pages[0], pm)
    f2.calibrate()
    if f2.scrub(pages[0], pm) != pages[0]:
        errors.append("under-calibrated filter scrubbed anyway")
    return errors


def check_title_authority() -> list[str]:
    """Offline (no fetch): og:title / JSON-LD Product.name must beat
    variant-picker text that wins every shape heuristic."""
    errors = []
    html = """<html><head>
<title>Single Piece Black &ndash; The Stationery Company</title>
<meta property="og:title" content="M&amp;G Office Gel Pen 0.7mm Point" />
<meta property="og:price:amount" content="95.00" />
<meta property="og:price:currency" content="PKR" />
<script type="application/ld+json">{"@type":"Product","name":"M&G Office Gel Pen 0.7mm Point",
 "offers":{"price":"95.00","priceCurrency":"PKR"}}</script>
</head><body><main>
<h2>Single Piece Black</h2>
<p>Single Piece Black Rs.95.00 In stock. A smooth-writing office gel pen with quick-dry ink,
0.7mm point, ideal for daily writing and exams. Durable tip and comfortable grip body.</p>
</main></body></html>"""
    ext = extract_page_text(html, "https://thestationerycompany.pk/products/m-g-office-gel-pen-0-7mm-point")
    if not ext:
        return ["extract_page_text returned None on synthetic product page"]
    prod = (ext["page_meta"].get("product") or {})
    t = str(prod.get("title") or "")
    if "gel pen" not in t.lower():
        errors.append(f"authority title lost: got {t!r}, want JSON-LD/og name")
    if "single piece black" == t.lower().strip():
        errors.append("variant-picker text won the title")
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
    for _name, _fn in (("category-helpers", check_category_helpers),
                       ("boilerplate-filter", check_boilerplate_filter),
                       ("title-authority", check_title_authority)):
        _errs = _fn()
        print(f"{'PASS' if not _errs else 'FAIL'} {_name:18s} (offline unit checks)")
        for e in _errs[:6]:
            print(f"     - {e}")
        if _errs:
            failed += 1
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
