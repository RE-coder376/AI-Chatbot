"""
services/catalog_api.py — deterministic Shopify catalog ingestion.

Crawl-discovery (BFS/sitemap, parallel workers, rate-limit pauses) captures a
different, incomplete product set on every run → fluctuating counts and coverage.
The store's own /products.json is the authoritative, complete, order-stable
product universe (the same endpoint the gate's ground truth uses). Building
product chunks straight from it gives 100% coverage that is byte-identical on
every re-ingest, so counts/extremes become constant.

Pure + offline-testable: `build_catalog_docs(url)` does the network fetch +
transform; the ingestion sink (embeddings, Chroma write) lives in app.py.
"""
from __future__ import annotations

import html as _html
import re
import time
import urllib.parse

import requests
from langchain_core.documents import Document

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0 Safari/537.36"}
_TAG_RE = re.compile(r"<[^>]+>")

# Currency code → display symbol. The generic JSON-LD/microdata path reads an ISO
# currency CODE (USD/GBP/PKR), but _emit_doc renders a symbol prefix. Unknown codes
# fall back to the caller's default so we never invent the wrong currency.
_CUR_SYM = {"USD": "$", "EUR": "€", "GBP": "£", "PKR": "Rs.", "INR": "₹",
            "AED": "AED ", "SAR": "SAR ", "CAD": "$", "AUD": "$", "JPY": "¥"}
# URL path tokens that mark a product-detail page across platforms (Shopify
# /products/, Woo/Magento /product/, BigCommerce flat, generic /p/, /item/, /dp/).
# Used only to PREFER likely product URLs from the sitemap; the authoritative test
# is still "does the page carry schema.org Product structured data".
_PRODUCT_URL_RE = re.compile(r"/(?:products?|item|items|dp|shop|buy|p)/[^/]", re.I)
_ASSET_RE = re.compile(r"\.(?:jpg|jpeg|png|gif|webp|svg|css|js|pdf|xml|ico|woff2?|ttf|mp4|zip)(?:\?|$)", re.I)

# ── Per-run feed snapshot ──────────────────────────────────────────────────
# Ingest (build_catalog_docs) and the gate (catalog_ground_truth) resolve the
# catalog independently, so each fetches the store feed seconds apart. Any product
# the store publishes between those two fetches lands in the gate's ground truth
# but not in the stored DB → a phantom "missing" that fails the gate on a perfectly
# ingested DB. When primed (one paired ingest+gate run, same process), every
# fetch_all_products* call returns the ONE captured snapshot per base URL, so the
# gate verifies against exactly the catalog that was ingested. Off by default —
# standalone callers keep live-fetch behaviour.
_FEED_SNAPSHOT: dict[str, list | None] = {}
_FEED_SNAPSHOT_ON = False


def prime_feed_snapshot(on: bool = True) -> None:
    """Enable/disable per-run feed caching. Call prime_feed_snapshot(True) before a
    paired ingest+gate run and prime_feed_snapshot(False) after (clears the cache)."""
    global _FEED_SNAPSHOT_ON
    _FEED_SNAPSHOT_ON = on
    if not on:
        _FEED_SNAPSHOT.clear()


def _snapshot(kind: str, base_url: str, fetch):
    """Return a single cached feed per (kind, base_url) when priming is on, else
    fetch live. `fetch` is the uncached implementation called at most once."""
    if not _FEED_SNAPSHOT_ON:
        return fetch()
    key = f"{kind}::{(base_url or '').rstrip('/').lower()}"
    if key not in _FEED_SNAPSHOT:
        _FEED_SNAPSHOT[key] = fetch()
    return _FEED_SNAPSHOT[key]


def _html_to_text(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", s)
    s = re.sub(r"(?i)<br\s*/?>", "\n", s)
    s = re.sub(r"(?i)</(p|div|li|h[1-6])>", "\n", s)
    s = _TAG_RE.sub(" ", s)
    # Shopify body_html is frequently double-encoded ("&amp;amp;" → "&amp;" after one
    # pass), which leaves a literal "&amp;" in the chunk that crawl_gate flags as junk
    # and quarantines the DB. Unescape until stable (capped) so no entity survives.
    for _ in range(4):
        _u = _html.unescape(s)
        if _u == s:
            break
        s = _u
    s = re.sub(r"[ \t]{2,}", " ", s)
    s = re.sub(r"[ \t]*\n[ \t\n]*", "\n", s).strip()
    return s


def fetch_all_products(base_url: str, max_pages: int = 80) -> list[dict] | None:
    """Every product from the store catalog API. None = not Shopify/blocked.
    Returns the per-run snapshot when priming is on (see prime_feed_snapshot)."""
    return _snapshot("shopify", base_url, lambda: _fetch_all_products_impl(base_url, max_pages))


def _fetch_all_products_impl(base_url: str, max_pages: int = 80) -> list[dict] | None:
    base = (base_url or "").rstrip("/")
    if not base:
        return None
    endpoint = f"{base}/products.json"
    out, page = [], 1
    while page <= max_pages:
        r = None
        for attempt in range(3):
            try:
                r = requests.get(endpoint, params={"limit": 250, "page": page}, headers=UA, timeout=45)
                break
            except Exception:
                if attempt == 2:
                    return None if page == 1 else out
                time.sleep(5)
        if r.status_code != 200:
            if page == 1 and not endpoint.endswith("/collections/all/products.json"):
                endpoint = f"{base}/collections/all/products.json"
                continue
            return None if page == 1 else out
        try:
            batch = r.json().get("products") or []
        except Exception:
            return None if page == 1 else out
        if not batch:
            break
        out.extend(batch)
        page += 1
        time.sleep(0.3)
    return out


def _fetch_collection_map_impl(base_url: str, max_collections: int = 300, max_pages: int = 40) -> dict[str, list[str]]:
    """Map each product handle -> the Shopify COLLECTION titles it belongs to.

    Shopify collections (the storefront sidebar/menu groupings) are NOT present in
    /products.json — they live at /collections.json + /collections/<handle>/products.json.
    Without this, "show all products in <sidebar category>" has nothing to match on.
    Failure-safe: returns {} on any error so ingest behaves exactly as before."""
    base = (base_url or "").rstrip("/")
    if not base:
        return {}
    cols: list[dict] = []
    page = 1
    while page <= max_pages and len(cols) < max_collections:
        try:
            r = requests.get(f"{base}/collections.json", params={"limit": 250, "page": page}, headers=UA, timeout=45)
        except Exception:
            break
        if r.status_code != 200:
            break
        try:
            batch = r.json().get("collections") or []
        except Exception:
            break
        if not batch:
            break
        cols.extend(batch)
        page += 1
        time.sleep(0.3)
    handle_to_cols: dict[str, list[str]] = {}
    for c in cols[:max_collections]:
        chandle = str(c.get("handle") or "").strip()
        ctitle = re.sub(r"\s+", " ", str(c.get("title") or "")).strip()
        if not chandle or not ctitle:
            continue
        cpage = 1
        while cpage <= max_pages:
            try:
                r = requests.get(f"{base}/collections/{urllib.parse.quote(chandle)}/products.json",
                                 params={"limit": 250, "page": cpage}, headers=UA, timeout=45)
            except Exception:
                break
            if r.status_code != 200:
                break
            try:
                prods = r.json().get("products") or []
            except Exception:
                break
            if not prods:
                break
            for p in prods:
                phandle = str(p.get("handle") or "").strip()
                if not phandle:
                    continue
                lst = handle_to_cols.setdefault(phandle, [])
                if ctitle not in lst:
                    lst.append(ctitle)
            cpage += 1
            time.sleep(0.2)
    return handle_to_cols


def fetch_collection_map(base_url: str) -> dict[str, list[str]]:
    """Cached per-run product-handle -> collection-titles map (see _fetch_collection_map_impl)."""
    return _snapshot("shopify_collections", base_url, lambda: _fetch_collection_map_impl(base_url))


def _unescape_stable(s: str) -> str:
    """Decode HTML entities until stable — product names/categories arrive
    entity-encoded ("Dolce &amp; Gabbana", "Marc Jacobs &#8211; Daisy") and the
    crawl_gate flags any surviving entity as junk. Descriptions already pass through
    _html_to_text; titles/categories do not, so decode them here."""
    s = s or ""
    for _ in range(4):
        u = _html.unescape(s)
        if u == s:
            return s
        s = u
    return s


def _emit_doc(*, title, price, availability, ptype, categories, desc, source, canonicalize, currency, collections="") -> Document:
    """Build one canonical product Document — identical shape for Shopify + WooCommerce.
    `collections` (pipe-joined storefront collection titles) is stored as its own
    metadata field so "list products in <sidebar category>" can match EXACT collection
    membership, independent of title/tag words that merely contain the name."""
    title = _unescape_stable(title)
    categories = _unescape_stable(categories)
    collections = _unescape_stable(collections)
    ptype = _unescape_stable(ptype)
    lines = [f"Product: {title}"]
    if price is not None:
        lines.append(f"Price: {currency}{price:,.0f}" if float(price).is_integer() else f"Price: {currency}{price:,.2f}")
    lines.append(f"Availability: {availability}")
    if ptype:
        lines.append(f"Category: {ptype}")
    if desc:
        lines.append(desc)
    meta = {
        "source": source,
        "source_canonical": source,
        "product_title": title,
        "canonical_product_title": (canonicalize(title) if canonicalize else title),
        "chunk_kind": "product",
        "content_type": "product",
        "availability": availability,
        "categories": categories,
        "collections": collections,
    }
    if price is not None:
        meta["price"] = float(price)
    return Document(page_content="\n".join(lines), metadata=meta)


def _woo_price(prices: dict):
    """WC Store API prices are integer minor-units (e.g. price '2575000' with
    currency_minor_unit 2 = 25750.00; minor_unit 0 = the value as-is)."""
    try:
        raw = prices.get("price")
        if raw in (None, ""):
            return None
        val = float(raw) / (10 ** int(prices.get("currency_minor_unit") or 0))
        return val if val > 0 else None
    except (TypeError, ValueError, AttributeError):
        return None


def fetch_all_products_woo(base_url: str, max_pages: int = 80) -> list[dict] | None:
    """Every product from the WooCommerce Store API — the WordPress equivalent of
    Shopify /products.json. None = not WooCommerce / blocked.
    Returns the per-run snapshot when priming is on (see prime_feed_snapshot)."""
    return _snapshot("woo", base_url, lambda: _fetch_all_products_woo_impl(base_url, max_pages))


def _fetch_all_products_woo_impl(base_url: str, max_pages: int = 80) -> list[dict] | None:
    base = (base_url or "").rstrip("/")
    if not base:
        return None
    endpoint = f"{base}/wp-json/wc/store/products"
    out, page = [], 1
    while page <= max_pages:
        r = None
        for attempt in range(3):
            try:
                r = requests.get(endpoint, params={"per_page": 100, "page": page}, headers=UA, timeout=45)
                break
            except Exception:
                if attempt == 2:
                    return None if page == 1 else (out or None)
                time.sleep(5)
        if r.status_code != 200:
            return None if page == 1 else (out or None)
        try:
            batch = r.json()
        except Exception:
            return None if page == 1 else (out or None)
        if not isinstance(batch, list) or not batch:
            break
        out.extend(batch)
        if len(batch) < 100:
            break
        page += 1
        time.sleep(0.3)
    return out or None


def _sitemap_product_urls(base_url: str, cap: int = 6000) -> list[str]:
    """Enumerate candidate product URLs from the site's sitemap(s). Universal —
    every serious cart (Shopify, Woo, Magento, BigCommerce, Wix, Squarespace,
    hand-rolled) publishes sitemap.xml. Recurses one level of <sitemapindex>,
    preferring child sitemaps whose URL mentions 'product'. Returns deduped,
    asset-filtered URLs, product-path URLs first so the cap keeps the real ones."""
    base = (base_url or "").rstrip("/")
    if not base:
        return []
    roots: list[str] = []
    # robots.txt Sitemap: lines are authoritative; fall back to conventional paths.
    try:
        rb = requests.get(f"{base}/robots.txt", headers=UA, timeout=20)
        if rb.status_code == 200:
            roots += re.findall(r"(?im)^\s*sitemap:\s*(\S+)", rb.text)
    except Exception:
        pass
    roots += [f"{base}/sitemap.xml", f"{base}/sitemap_index.xml", f"{base}/sitemap-index.xml"]
    seen_sm, locs, q = set(), [], list(dict.fromkeys(roots))
    fetched = 0
    while q and fetched < 60:
        sm = q.pop(0)
        if sm in seen_sm:
            continue
        seen_sm.add(sm)
        try:
            r = requests.get(sm, headers=UA, timeout=30)
            if r.status_code != 200 or "<" not in r.text:
                continue
        except Exception:
            continue
        fetched += 1
        body = r.text
        child = re.findall(r"<sitemap>.*?<loc>\s*([^<\s]+)\s*</loc>", body, re.S | re.I)
        if child:
            # sitemap index — enqueue children, product sitemaps first.
            child = sorted(set(child), key=lambda u: (0 if "product" in u.lower() else 1, u))
            q[:0] = child
            continue
        locs += re.findall(r"<url>.*?<loc>\s*([^<\s]+)\s*</loc>", body, re.S | re.I) \
            or re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", body, re.I)
    # Same-host, non-asset, deduped; product-path URLs first so the cap keeps them.
    host = urllib.parse.urlparse(base).netloc.lower()
    out, seen = [], set()
    for u in locs:
        u = _html.unescape(u.strip())
        if not u or u in seen or _ASSET_RE.search(u):
            continue
        if urllib.parse.urlparse(u).netloc.lower() not in (host, ""):
            continue
        seen.add(u)
        out.append(u)
    out.sort(key=lambda u: 0 if _PRODUCT_URL_RE.search(u) else 1)
    return out[:cap]


def _extract_product_from_html(html: str, url: str) -> dict | None:
    """One product from a detail page via schema.org Product structured data
    (JSON-LD first, microdata fallback) — the universal, platform-agnostic signal.
    Returns None when the page carries no Product price (category/blog/policy)."""
    from services.page_extract import _jsonld_text, _microdata_product
    name = price = 0
    cur_code = ""
    struct_text = ""  # the structured block (JSON-LD/microdata) that carried the price
    try:
        ld_text, ld_name, ld_price = _jsonld_text(html)
    except Exception:
        ld_text, ld_name, ld_price = "", "", 0.0
    if ld_name and ld_price and ld_price > 0:
        name, price, struct_text = ld_name, float(ld_price), ld_text
        m = re.search(r"priceCurrency[\"'\s:]+([A-Z]{3})", ld_text) or re.search(r"\b([A-Z]{3})\b", ld_text)
        cur_code = m.group(1) if m else ""
    if not (name and price):
        try:
            md_name, md_price, md_cur = _microdata_product(html)
        except Exception:
            md_name, md_price, md_cur = "", 0.0, ""
        if md_name and md_price and md_price > 0:
            name, price, cur_code = md_name, float(md_price), (md_cur or "")
    if not name or not price or price <= 0:
        return None
    # Prefer the product's OWN structured availability (schema.org/InStock|OutOfStock|
    # SoldOut|PreOrder|BackOrder) over a page-wide text scan, which false-matches
    # hidden JS toggle labels and related-product cards (marked an in-stock item OOS).
    avail = "available"
    if struct_text and re.search(r"(?i)(out\s*of\s*stock|sold\s*out|backorder|discontinued)", struct_text):
        avail = "out of stock"
    elif not struct_text and re.search(r"(?i)(out\s*of\s*stock|sold\s*out|OutOfStock|currently\s+unavailable)", html):
        avail = "out of stock"
    return {"title": re.sub(r"\s+", " ", name).strip(), "price": price,
            "availability": avail, "currency_code": (cur_code or "").upper(),
            "source": url.split("#")[0]}


def fetch_all_products_generic(base_url: str, cap: int = 6000, workers: int = 8) -> list[dict] | None:
    """Deterministic full-catalog fetch for ANY site emitting schema.org Product
    data — Magento, BigCommerce, Wix, Squarespace, hand-rolled carts (no Shopify
    /products.json, no WC Store API). Returns the per-run snapshot when priming is on."""
    return _snapshot("generic", base_url, lambda: _fetch_all_products_generic_impl(base_url, cap, workers))


def _fetch_all_products_generic_impl(base_url: str, cap: int = 6000, workers: int = 8) -> list[dict] | None:
    """Enumerates products from the sitemap and extracts each via JSON-LD/microdata.
    None = no sitemap / no structured products."""
    import concurrent.futures as _cf
    urls = _sitemap_product_urls(base_url, cap=cap)
    if not urls:
        return None

    def _one(u: str) -> dict | None:
        for attempt in range(2):
            try:
                r = requests.get(u, headers=UA, timeout=30)
                if r.status_code == 200 and r.text:
                    return _extract_product_from_html(r.text, u)
                return None
            except Exception:
                if attempt == 1:
                    return None
                time.sleep(2)
        return None

    out: list[dict] = []
    with _cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for res in ex.map(_one, urls):
            if res:
                out.append(res)
    # Dedup by normalized title (variant pages collapse to one product), keep cheapest.
    best: dict[str, dict] = {}
    for p in out:
        k = re.sub(r"[^a-z0-9]+", " ", p["title"].lower()).strip()
        if not k:
            continue
        if k not in best or p["price"] < best[k]["price"]:
            best[k] = p
    return sorted(best.values(), key=lambda x: x["source"]) or None


def build_catalog_docs(base_url: str, canonicalize=None, currency: str = "Rs.") -> list[Document]:
    """Deterministic product Documents from the store's own catalog API — Shopify
    /products.json first, else the WooCommerce Store API. Same chunk format +
    metadata for both; sorted for byte-stable re-ingest. `canonicalize` is an
    optional title-normalizer (app._canonical_product_title)."""
    base = base_url.rstrip("/")
    docs: list[Document] = []
    products = fetch_all_products(base_url)
    if products:
        col_map = fetch_collection_map(base_url)
        for p in sorted(products, key=lambda x: str(x.get("handle") or "")):
            handle = str(p.get("handle") or "").strip()
            title = re.sub(r"\s+", " ", str(p.get("title") or "")).strip()
            if not handle or len(title) < 2:
                continue
            prices, any_avail = [], False
            for v in (p.get("variants") or []):
                try:
                    pv = float(v.get("price"))
                    if pv > 0:
                        prices.append(pv)
                except (TypeError, ValueError):
                    pass
                if v.get("available"):
                    any_avail = True
            ptype = str(p.get("product_type") or "").strip()
            tags = p.get("tags") or []
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]
            collections = col_map.get(handle, [])
            docs.append(_emit_doc(
                title=title, price=(min(prices) if prices else None),
                availability="available" if any_avail else "out of stock",
                ptype=ptype, categories=", ".join([t for t in ([ptype] + list(tags) + list(collections)) if t]),
                collections=" | ".join([c for c in collections if c]),
                desc=_html_to_text(str(p.get("body_html") or ""))[:1200],
                source=f"{base}/products/{urllib.parse.quote(handle)}",
                canonicalize=canonicalize, currency=currency))
        return docs
    # WooCommerce fallback — the store's WC Store API (full, authoritative catalog).
    woo = fetch_all_products_woo(base_url)
    if woo:
        for p in sorted(woo, key=lambda x: str(x.get("slug") or x.get("id") or "")):
            title = re.sub(r"\s+", " ", str(p.get("name") or "")).strip()
            if len(title) < 2:
                continue
            cats = [str(c.get("name") or "").strip() for c in (p.get("categories") or []) if c.get("name")]
            source = str(p.get("permalink") or f"{base}/product/{urllib.parse.quote(str(p.get('slug') or ''))}")
            docs.append(_emit_doc(
                title=title, price=_woo_price(p.get("prices") or {}),
                availability="available" if p.get("is_in_stock") else "out of stock",
                ptype="", categories=", ".join(cats), collections=" | ".join([c for c in cats if c]),
                desc=_html_to_text(str(p.get("description") or p.get("short_description") or ""))[:1200],
                source=source, canonicalize=canonicalize, currency=currency))
        return docs
    # Generic fallback — sitemap + schema.org Product (JSON-LD/microdata). Covers
    # Magento, BigCommerce, Wix, Squarespace, and hand-rolled carts. [[junk-fix-structured-ingest-not-regex]]
    generic = fetch_all_products_generic(base_url)
    if generic:
        for p in generic:
            title = p["title"]
            if len(title) < 2:
                continue
            cur = _CUR_SYM.get(p.get("currency_code") or "", currency)
            docs.append(_emit_doc(
                title=title, price=p.get("price"), availability=p.get("availability") or "available",
                ptype="", categories="", desc="",
                source=p["source"], canonicalize=canonicalize, currency=cur))
        return docs
    return []


def catalog_ground_truth(base_url: str) -> list[dict] | None:
    """Authoritative product universe for the GATE — the SAME source build_catalog_docs
    ingests from, so ingest and verification can never diverge. Platform-agnostic
    normalized items: {title, prices:[..], available, source}. None = no structured
    catalog (gate then skips layer-2 rather than failing a docs/structureless site)."""
    base = base_url.rstrip("/")
    shop = fetch_all_products(base_url)
    if shop:
        col_map = fetch_collection_map(base_url)
        gt = []
        for p in shop:
            handle = str(p.get("handle") or "").strip()
            title = re.sub(r"\s+", " ", str(p.get("title") or "")).strip()
            if not handle or len(title) < 2:
                continue
            prices = sorted({round(float(v.get("price")), 2) for v in (p.get("variants") or [])
                             if _safe_pos(v.get("price"))})
            vs = p.get("variants") or []
            ptype = str(p.get("product_type") or "").strip()
            tags = p.get("tags") or []
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]
            collections = col_map.get(handle, [])
            gt.append({"title": _unescape_stable(title), "prices": prices,
                       "available": (any(v.get("available") for v in vs) if vs and "available" in vs[0] else None),
                       "categories": _unescape_stable(", ".join([t for t in ([ptype] + list(tags) + list(collections)) if t])),
                       "source": f"{base}/products/{urllib.parse.quote(handle)}"})
        return gt
    woo = fetch_all_products_woo(base_url)
    if woo:
        gt = []
        for p in woo:
            title = re.sub(r"\s+", " ", str(p.get("name") or "")).strip()
            if len(title) < 2:
                continue
            pv = _woo_price(p.get("prices") or {})
            src = str(p.get("permalink") or f"{base}/product/{urllib.parse.quote(str(p.get('slug') or ''))}")
            cats = [str(c.get("name") or "").strip() for c in (p.get("categories") or []) if c.get("name")]
            gt.append({"title": _unescape_stable(title), "prices": ([round(pv, 2)] if pv else []),
                       "available": bool(p.get("is_in_stock")),
                       "categories": _unescape_stable(", ".join(cats)), "source": src})
        return gt
    generic = fetch_all_products_generic(base_url)
    if generic:
        return [{"title": p["title"], "prices": ([round(p["price"], 2)] if p.get("price") else []),
                 "available": (p.get("availability") != "out of stock"),
                 "categories": "", "source": p["source"]}
                for p in generic]
    return None


def _safe_pos(v) -> bool:
    try:
        return float(v) > 0
    except (TypeError, ValueError):
        return False
