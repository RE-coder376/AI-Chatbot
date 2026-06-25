"""Coverage audit — answers "is every useful thing on this site actually ingested?"
empirically, for BOTH platform stores (Shopify / WooCommerce — 'a website inside
another') AND normal/hand-rolled sites (sitemap + schema.org).

It enumerates the LIVE site's askable universe and checks each part against what the
ingest pipeline captures (build_catalog_docs), then prints coverage % + the exact
gaps. MEASURED only — no projections.

  python evals/coverage_audit.py                      # default store set
  python evals/coverage_audit.py https://babyfy.pk/   # one site

Universe audited per site:
  • products    — every product in the structured feed → ingested? priced?
  • collections — every storefront collection (sidebar/menu group) → members captured?
  • key pages   — shipping / returns / refund / privacy / terms / about / contact / faq
                  exist on the site? (prose pages need the CRAWL, not the catalog feed —
                  flagged so you know what the crawler must reach.)
"""
import sys, os, re, urllib.parse, html as _html
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import requests
from services.catalog_api import (
    UA, fetch_all_products, fetch_all_products_woo, fetch_all_products_generic,
    fetch_collection_map, build_catalog_docs, _sitemap_product_urls, prime_feed_snapshot,
)

# Cache each live feed for the whole run so detect + universe + build_catalog_docs
# share ONE fetch per (feed, site) — cheaper and not flaky on a transient empty.
prime_feed_snapshot(True)

DEFAULT_SITES = [
    "https://babyfy.pk/",                                  # Shopify
    "https://thestationerycompany.pk",                     # Shopify (large, 234 collections)
    "https://stationerystudio.pk",                         # Shopify
    "https://webscraper.io/test-sites/e-commerce/allinone",# generic / normal site
]

KEY_PAGES = {
    "shipping":  ["/pages/shipping", "/pages/shipping-policy", "/policies/shipping-policy", "/shipping"],
    "returns":   ["/pages/returns", "/pages/return-policy", "/policies/refund-policy", "/returns"],
    "refund":    ["/pages/refund-policy", "/policies/refund-policy", "/refund"],
    "privacy":   ["/pages/privacy-policy", "/policies/privacy-policy", "/privacy"],
    "terms":     ["/pages/terms", "/policies/terms-of-service", "/terms"],
    "about":     ["/pages/about-us", "/pages/about", "/about", "/about-us"],
    "contact":   ["/pages/contact", "/pages/contact-us", "/contact", "/contact-us"],
    "faq":       ["/pages/faq", "/pages/faqs", "/faq"],
}

def norm(s):
    # Unescape first — feeds serve entity-encoded titles ("Victoria&#8217;s Secret")
    # while the ingest decodes them; without this the audit reports false gaps.
    s = s or ""
    for _ in range(3):
        u = _html.unescape(s)
        if u == s: break
        s = u
    return re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()

def detect(url):
    # Each call is snapshot-cached, so this is one fetch per feed (not per call).
    if fetch_all_products(url): return "shopify"
    if fetch_all_products_woo(url): return "woocommerce"
    if fetch_all_products_generic(url): return "generic"
    return "unknown"

def live_products(url, platform):
    if platform == "shopify":
        return [re.sub(r"\s+", " ", str(p.get("title") or "")).strip() for p in (fetch_all_products(url) or [])]
    if platform == "woocommerce":
        return [re.sub(r"\s+", " ", str(p.get("name") or "")).strip() for p in (fetch_all_products_woo(url) or [])]
    if platform == "generic":
        return [p["title"] for p in (fetch_all_products_generic(url) or [])]
    return []

def live_collections(url, platform):
    """collection title -> set(member product titles), live."""
    if platform != "shopify":
        return {}
    prods = {str(p.get("handle")): re.sub(r"\s+"," ",str(p.get("title") or "")).strip() for p in (fetch_all_products(url) or [])}
    out = {}
    for h, cols in fetch_collection_map(url).items():
        t = prods.get(h)
        if not t: continue
        for c in cols:
            out.setdefault(c, set()).add(t)
    return out

def page_exists(base, paths):
    for p in paths:
        try:
            r = requests.get(base.rstrip("/") + p, headers=UA, timeout=20, allow_redirects=True)
            if r.status_code == 200 and len(r.text) > 500:
                return base.rstrip("/") + p
        except Exception:
            pass
    return None

def audit(url):
    print(f"\n{'='*72}\n{url}\n{'='*72}")
    platform = detect(url)
    print(f"platform: {platform}")
    if platform == "unknown":
        print("  no structured catalog AND no sitemap/schema.org — would need pure HTML crawl.")
        return
    docs = build_catalog_docs(url)
    ing_titles = {norm(d.metadata.get("product_title")) for d in docs}
    ing_coll = set()
    for d in docs:
        for c in (d.metadata.get("collections") or "").split("|"):
            if c.strip(): ing_coll.add(norm(c))

    # 1) PRODUCTS
    lp = live_products(url, platform)
    lp_n = {norm(t) for t in lp if t}
    missing_p = [t for t in lp if t and norm(t) not in ing_titles]
    pc = (len(lp_n) - len({norm(t) for t in missing_p})) / max(len(lp_n), 1) * 100
    priced = sum(1 for d in docs if d.metadata.get("price") is not None)
    print(f"\nPRODUCTS : {len(lp_n)} live | {len(ing_titles)} ingested | coverage {pc:.1f}% | priced {priced}/{len(docs)}")
    if missing_p: print(f"   MISSING ({len(missing_p)}): {missing_p[:8]}")

    # 2) COLLECTIONS
    lc = live_collections(url, platform)
    if lc:
        empty = [c for c in lc if norm(c) not in ing_coll]
        cc = (len(lc) - len(empty)) / max(len(lc), 1) * 100
        print(f"\nCOLLECTIONS: {len(lc)} live | captured {len(lc)-len(empty)} | coverage {cc:.1f}%")
        if empty: print(f"   NOT CAPTURED ({len(empty)}): {empty[:8]}")
    else:
        print("\nCOLLECTIONS: none (not Shopify, or store has no collections)")

    # 3) KEY PAGES (prose — needs the CRAWL, not the catalog feed)
    print("\nKEY PAGES (exist on site → must be reached by the crawler):")
    base = re.sub(r"(https?://[^/]+).*", r"\1", url)
    found, missing_pg = [], []
    for label, paths in KEY_PAGES.items():
        hit = page_exists(base, paths)
        (found if hit else missing_pg).append(label)
    print(f"   present: {found}")
    print(f"   not found at common paths: {missing_pg}")

if __name__ == "__main__":
    sites = sys.argv[1:] or DEFAULT_SITES
    for s in sites:
        try:
            audit(s)
        except Exception as e:
            import traceback; traceback.print_exc(); print(f"  ERROR {s}: {e}")
    print(f"\n{'#'*72}\nDONE. Coverage = % of the LIVE universe present in the ingest.\n"
          f"Gaps above are the exact things a customer could ask that the bot can't yet answer.\n{'#'*72}")
