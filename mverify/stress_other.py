"""Adversarial stress for the NON-Shopify product DBs: perfumeshop (WooCommerce),
store (webscraper.io demo), book (books.toscrape demo). Same type x phrasing x
random-product coverage as stress_test, but with GT pulled from each platform's
own source. Usage: python mverify/stress_other.py [N] [db ...]"""
import sys, re, json, time, random, urllib.request, urllib.error
sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
import stress_test as S   # reuse ask/key_for/gen/grade/run/_ws

random.seed()

def _get(u, timeout=40):
    req = urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0"})
    return urllib.request.urlopen(req, timeout=timeout)

def woo_catalog(base):
    """WooCommerce Store API — prices already in major units here (minor_unit 0)."""
    out, seen = [], set()
    for page in range(1, 30):
        try:
            r = _get(f"{base}/wp-json/wc/store/products?per_page=100&page={page}")
            d = json.loads(r.read().decode("utf-8", "replace"))
        except Exception:
            break
        if not d:
            break
        for p in d:
            t = (p.get("name") or "").strip()
            pr = p.get("prices", {}) or {}
            try:
                mu = int(pr.get("currency_minor_unit", 0) or 0)
                price = float(pr.get("price")) / (10 ** mu) if pr.get("price") not in (None, "") else None
            except Exception:
                price = None
            if t and price and t.lower() not in seen:
                seen.add(t.lower()); out.append({"title": __import__("html").unescape(t), "price": price})
        if len(d) < 100:
            break
    return out

def books_catalog(_base):
    out, seen = [], set()
    for page in range(1, 51):
        try:
            html = _get(f"https://books.toscrape.com/catalogue/page-{page}.html").read().decode("utf-8", "replace")
        except Exception:
            break
        rows = re.findall(r'<h3><a href="[^"]+" title="([^"]+)">.*?class="price_color">[^0-9]*([0-9.]+)', html, re.S)
        if not rows:
            break
        for t, p in rows:
            t = __import__("html").unescape(t).strip()
            if t and t.lower() not in seen:
                seen.add(t.lower()); out.append({"title": t, "price": float(p)})
    return out

def store_catalog(_base):
    out, seen = [], set()
    cats = ["computers/laptops", "computers/tablets", "phones/touch"]
    for c in cats:
        for page in range(1, 12):
            url = f"https://webscraper.io/test-sites/e-commerce/allinone/{c}?page={page}"
            try:
                html = _get(url).read().decode("utf-8", "replace")
            except Exception:
                break
            # each card: title attr + a price like $295.99
            cards = re.findall(r'class="title"[^>]*title="([^"]+)"[\s\S]*?\$([0-9.,]+)', html)
            if not cards:
                break
            for t, p in cards:
                t = __import__("html").unescape(t).strip()
                if t and t.lower() not in seen:
                    seen.add(t.lower()); out.append({"title": t, "price": float(p.replace(",", ""))})
            if "next" not in html.lower():
                break
    return out

DBS = {
    "perfumeshop": ("https://theperfumeshop.pk", woo_catalog),
    "book": ("", books_catalog),
    "store": ("", store_catalog),
}

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 6
    only = [a for a in sys.argv[2:] if a in DBS] or list(DBS)
    t = nn = 0
    for db in only:
        base, fetch = DBS[db]
        cat = fetch(base)
        if not cat:
            print(f"\n{db}: NO CATALOG FETCHED — skipping"); continue
        # monkeypatch S.catalog for this db via a closure run
        key = S.key_for(db)
        qs = S.gen(cat, n)
        print(f"\n{'='*70}\n{db}: {len(cat)} products | {len(qs)} stress Qs | key={'ok' if key else 'MISSING'}\n{'='*70}", flush=True)
        by, fails = {}, []
        for q, kind, payload in qs:
            ans = S.ask(key, q)
            v = S.grade(kind, payload, ans)
            if v is None:
                continue
            by.setdefault(kind, [0, 0]); by[kind][1] += 1; by[kind][0] += v
            if not v:
                fails.append((kind, q, ans))
            time.sleep(3)
        tot = sum(x[1] for x in by.values()); ok = sum(x[0] for x in by.values())
        print(f"\n>>> {db}: {ok}/{tot}  " + " ".join(f"{k}={by[k][0]}/{by[k][1]}" for k in by), flush=True)
        for kind, q, ans in fails:
            print(f"  FAIL [{kind}] {q[:90]}\n      -> {ans[:150].encode('ascii','replace').decode()}", flush=True)
        t += ok; nn += tot
    print(f"\n\n######## OTHER-DB STRESS TOTAL: {t}/{nn} ({100*t/max(nn,1):.1f}%) ########")
