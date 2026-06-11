"""
services/page_extract.py — pure HTML → {text, title, page_meta} transform.

Single source of truth shared by the httpx crawl fast-path (app.py) and the
offline pipeline checker (pipeline_check.py). Test THIS and the crawl path
is tested — no live crawls needed to validate extraction changes.
"""

from __future__ import annotations

import json
import re

from services.safety import _clean_text
from services.crawler_utils import _prepare_crawl_page, _canonical_source_url

CHROME_SELECTORS = (
    '[role="navigation"]', '[role="banner"]', '[role="contentinfo"]',
    '.nav', '.navigation', '.sidebar', '.side_categories', '.nav-list',
    '.breadcrumb', '.breadcrumbs', '.pagination', '.social-links',
    '.cookie-notice', '.theme-doc-sidebar-container',
    '.theme-doc-toc-mobile', '.table-of-contents',
    '.cart-drawer', '#CartDrawer', '.mini-cart', '[class*="cart-drawer"]',
    '.cart-popup', '#cart-notification',
)

_DOCS_MAIN_SELECTORS = (
    ".theme-doc-markdown", ".markdown", ".theme-doc-content",
    "[class*='docMainContainer'] article", "main article", "article",
)


def _jsonld_text(html: str) -> str:
    parts_out = []
    for raw in re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL | re.I):
        try:
            data = json.loads(raw.strip())

            def _ld(obj):
                if not obj:
                    return ''
                if isinstance(obj, list):
                    return ' '.join(_ld(o) for o in obj)
                parts = []
                if obj.get('name'):
                    parts.append('Name: ' + str(obj['name']))
                if obj.get('description'):
                    parts.append('Description: ' + str(obj['description']))
                offers = obj.get('offers', [])
                if isinstance(offers, dict):
                    offers = [offers]
                for o in offers:
                    if o.get('price'):
                        parts.append('Price: ' + str(o['price']) + ' ' + str(o.get('priceCurrency', '')))
                    if o.get('availability'):
                        parts.append('Avail: ' + str(o['availability']).replace('http://schema.org/', ''))
                if obj.get('brand') and isinstance(obj['brand'], dict):
                    if obj['brand'].get('name'):
                        parts.append('Brand: ' + obj['brand']['name'])
                if obj.get('sku'):
                    parts.append('SKU: ' + str(obj['sku']))
                if obj.get('category'):
                    parts.append('Category: ' + str(obj['category']))
                if obj.get('@graph'):
                    return ' '.join(_ld(item) for item in obj['@graph'])
                return '. '.join(parts)

            parts_out.append(_ld(data))
        except Exception:
            pass
    return ' '.join(parts_out)


def extract_page_text(html: str, page_url: str, docs_like: bool = False):
    """Transform server-rendered HTML into crawl-ready text + page metadata.

    Returns {"text", "title", "page_meta"} or None when no usable text.
    Mirrors (and replaces) the inline transform previously inside
    app.py:_requests_extract.
    """
    try:
        ld_text = _jsonld_text(html)
        title_m = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL | re.I)
        title_text = re.sub(r'<[^>]+>', '', title_m.group(1)).strip() if title_m else ''
        # og:price:amount is the storefront's own declaration of the CURRENT
        # selling price. Body text orders compare-at first ("Rs.2,000 Rs.1,200")
        # and regex-first extraction picks the crossed-out price — the og meta
        # is authoritative, so surface it as the first "Price:" line, which
        # _PRODUCT_PRICE_LINE_RE matches before any body price.
        og_price_line = ''
        og_amt = re.search(r'<meta[^>]+(?:property|name)=["\']og:price:amount["\'][^>]+content=["\']([\d.,]+)["\']', html, re.I) \
            or re.search(r'<meta[^>]+content=["\']([\d.,]+)["\'][^>]+(?:property|name)=["\']og:price:amount["\']', html, re.I)
        if og_amt:
            og_cur = re.search(r'<meta[^>]+(?:property|name)=["\']og:price:currency["\'][^>]+content=["\']([A-Za-z]{3})["\']', html, re.I) \
                or re.search(r'<meta[^>]+content=["\']([A-Za-z]{3})["\'][^>]+(?:property|name)=["\']og:price:currency["\']', html, re.I)
            _cur_code = (og_cur.group(1).upper() if og_cur else "")
            _cur_prefix = {"PKR": "Rs.", "USD": "$", "GBP": "£", "EUR": "€"}.get(_cur_code, (_cur_code + " ") if _cur_code else "Rs.")
            og_price_line = f"Price: {_cur_prefix}{og_amt.group(1)}\n"
        # Preserve tables / lists / code blocks before the broad tag strip.
        try:
            from bs4 import BeautifulSoup as _BS
            soup = _BS(html, "html.parser")
            # Strip chrome — same element list the Playwright path removes.
            for tag in soup(["script", "style", "noscript", "nav", "header", "footer", "aside", "iframe", "svg"]):
                tag.decompose()
            for sel in CHROME_SELECTORS:
                try:
                    for el in soup.select(sel):
                        el.decompose()
                except Exception:
                    continue
            for table in soup.find_all("table"):
                rows = []
                for tr in table.find_all("tr"):
                    cells = [re.sub(r"\s+", " ", c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
                    if cells:
                        rows.append(" | ".join(cells))
                if rows:
                    table.replace_with("\n\n" + "\n".join(rows) + "\n\n")
            for dl in soup.find_all("dl"):
                rows = []
                for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                    term = re.sub(r"\s+", " ", dt.get_text(" ", strip=True))
                    defn = re.sub(r"\s+", " ", dd.get_text(" ", strip=True))
                    if term or defn:
                        rows.append(f"{term}: {defn}".strip(": "))
                if rows:
                    dl.replace_with("\n\n" + "\n".join(rows) + "\n\n")
            for pre in soup.find_all("pre"):
                code = pre.get_text("\n", strip=True)
                if code and len(code) >= 20:
                    pre.replace_with("\n\n```\n" + code[:6000] + "\n```\n\n")
            for ul in soup.find_all(["ul", "ol"]):
                items = []
                for li in ul.find_all("li", recursive=False):
                    li_txt = re.sub(r"\s+", " ", li.get_text(" ", strip=True))
                    if li_txt:
                        items.append(li_txt)
                if items:
                    ul.replace_with("\n\n" + "\n".join(f"- {it}" for it in items) + "\n\n")
            if docs_like:
                for sel in _DOCS_MAIN_SELECTORS:
                    doc_main = soup.select_one(sel)
                    if doc_main and doc_main.get_text(" ", strip=True):
                        soup = doc_main
                        break
            html = str(soup)
        except Exception:
            pass
        # Preserve section boundaries before tag stripping.
        html = re.sub(r'(?is)<h2[^>]*>(.*?)</h2>', lambda m: "\n\n## " + re.sub(r'<[^>]+>', ' ', m.group(1)).strip() + "\n\n", html)
        html = re.sub(r'(?is)<h3[^>]*>(.*?)</h3>', lambda m: "\n\n### " + re.sub(r'<[^>]+>', ' ', m.group(1)).strip() + "\n\n", html)
        clean = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.I)
        clean = re.sub(r'<style[^>]*>.*?</style>', ' ', clean, flags=re.DOTALL | re.I)
        clean = re.sub(r'<h2[^>]*>', '\n## ', clean, flags=re.I)
        clean = re.sub(r'</h2>', '\n', clean, flags=re.I)
        clean = re.sub(r'<h3[^>]*>', '\n### ', clean, flags=re.I)
        clean = re.sub(r'</h3>', '\n', clean, flags=re.I)
        clean = re.sub(r'<[^>]+>', ' ', clean)
        clean = re.sub(r'[ \t]+', ' ', clean)
        clean = re.sub(r'\n{3,}', '\n\n', clean).strip()
        combined = f"{title_text}. {og_price_line}{ld_text} {clean}".strip()
        # Decode HTML entities AFTER tag-strip and concat — titles and JSON-LD
        # descriptions carry &ndash;/&amp;/&#39; too, not just the body.
        import html as _html_mod
        combined = _html_mod.unescape(combined)
        # Storefront widget scrub BEFORE product extraction: cart drawers
        # ("Subtotal: Rs.0.00"), live-viewer counters and Shopify's "Default
        # Title" variant label poison titles/prices downstream.
        combined = re.sub(r'(?is)(?:cart\s*[×x]?\s*)?your cart is currently empty\.?.{0,100}?(?:checkout|view cart)', ' ', combined)
        combined = re.sub(r'(?i)subtotal:?\s*(?:rs\.?|pkr|\$|£|€)\s*0(?:\.00)?\b', ' ', combined)
        combined = re.sub(r'(?i)\b\d+\s+people\s+are\s+viewing\s+this\s+right\s+now\.?', ' ', combined)
        combined = re.sub(r'(?i)(?:[-–—]\s*)?\bdefault\s+title\b(?:\s*[-–—])?', ' ', combined)
        combined, page_meta = _prepare_crawl_page(combined, page_url, title_hint=title_text)
        page_meta = dict(page_meta or {})
        page_meta["source_canonical"] = _canonical_source_url(page_url)
        combined = re.sub(r'[ \t]+', ' ', _clean_text(combined))
        combined = re.sub(r'\n{3,}', '\n\n', combined).strip()
        if docs_like:
            if len(combined) > 60 or (title_text and len(title_text) > 8):
                return {"text": combined, "title": title_text, "page_meta": page_meta}
        if len(combined) > 100:
            return {"text": combined, "title": title_text, "page_meta": page_meta}
        return None
    except Exception:
        return None
