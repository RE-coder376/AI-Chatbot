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
    """Every product from the store catalog API. None = not Shopify/blocked."""
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


def _emit_doc(*, title, price, availability, ptype, categories, desc, source, canonicalize, currency) -> Document:
    """Build one canonical product Document — identical shape for Shopify + WooCommerce."""
    title = _unescape_stable(title)
    categories = _unescape_stable(categories)
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
    Shopify /products.json. None = not WooCommerce / blocked."""
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


def build_catalog_docs(base_url: str, canonicalize=None, currency: str = "Rs.") -> list[Document]:
    """Deterministic product Documents from the store's own catalog API — Shopify
    /products.json first, else the WooCommerce Store API. Same chunk format +
    metadata for both; sorted for byte-stable re-ingest. `canonicalize` is an
    optional title-normalizer (app._canonical_product_title)."""
    base = base_url.rstrip("/")
    docs: list[Document] = []
    products = fetch_all_products(base_url)
    if products:
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
            docs.append(_emit_doc(
                title=title, price=(min(prices) if prices else None),
                availability="available" if any_avail else "out of stock",
                ptype=ptype, categories=", ".join([t for t in ([ptype] + list(tags)) if t]),
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
                ptype="", categories=", ".join(cats),
                desc=_html_to_text(str(p.get("description") or p.get("short_description") or ""))[:1200],
                source=source, canonicalize=canonicalize, currency=currency))
        return docs
    return []
