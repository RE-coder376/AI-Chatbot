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


def build_catalog_docs(base_url: str, canonicalize=None, currency: str = "Rs.") -> list[Document]:
    """Deterministic product Documents from /products.json, sorted by handle.
    `canonicalize` is an optional title-normalizer (app._canonical_product_title)."""
    products = fetch_all_products(base_url)
    if not products:
        return []
    base = base_url.rstrip("/")
    docs: list[Document] = []
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
        price = min(prices) if prices else None
        availability = "available" if any_avail else "out of stock"
        ptype = str(p.get("product_type") or "").strip()
        tags = p.get("tags") or []
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]
        categories = ", ".join([t for t in ([ptype] + list(tags)) if t])
        desc = _html_to_text(str(p.get("body_html") or ""))[:1200]
        source = f"{base}/products/{urllib.parse.quote(handle)}"
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
        docs.append(Document(page_content="\n".join(lines), metadata=meta))
    return docs
