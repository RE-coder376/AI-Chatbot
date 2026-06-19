"""Adversarial stress harness: tests the QUESTION SPACE, not a fixed set.

For each DB it samples random products across the whole /products.json catalog and
asks every question TYPE in many PARAPHRASINGS — so it surfaces the failures a
fresh external test (e.g. Codex's next round) would find, before they do. Graded
deterministically against the live catalog. Ties (equal prices) are handled.

Usage: python mverify/stress_test.py [N_per_type_per_db] [db ...]
"""
import json, re, sys, time, random, urllib.request, urllib.error

SERVE = "https://re-coder376--ai-chatbot-serve.modal.run"
random.seed()  # fresh sampling every run (no fixed seed = different products each time)
GT_JSON = r"C:\Users\User\Documents\Codex\2026-05-06\hi\product_round2_25q_ground_truth.json"

def http(url, data=None, headers=None, timeout=120):
    last = None
    for a in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers or {}), timeout=timeout) as r:
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                time.sleep(15 + a * 10); continue
            if e.code >= 500:
                time.sleep(3 + a * 3); continue
            raise
        except Exception as e:
            last = e; time.sleep(4 + a * 4)
    raise last

def catalog(base):
    out, seen = [], set()
    for page in range(1, 40):
        try:
            d = json.loads(http(f"{base}/products.json?limit=250&page={page}", timeout=40))
        except Exception:
            break
        ps = d.get("products", [])
        if not ps:
            break
        for p in ps:
            t = (p.get("title") or "").strip()
            vs = p.get("variants") or [{}]
            prices = [float(v["price"]) for v in vs if v.get("price")]
            if not t or not prices or t.lower() in seen:
                continue
            seen.add(t.lower())
            out.append({"title": t, "price": min(prices)})
        if len(ps) < 250:
            break
    return out

def key_for(db):
    raw = http(f"{SERVE}/admin/embed-code?password=iaah2006&db_name={db}", timeout=40)
    m = re.search(r"key=([^\"&\\ ]+)", raw)
    return m.group(1) if m else None

def ask(key, q):
    body = json.dumps({"question": q, "stream": False}).encode()
    try:
        return json.loads(http(f"{SERVE}/chat", data=body,
            headers={"X-Widget-Key": key, "Content-Type": "application/json"}, timeout=120)).get("answer", "") or ""
    except Exception as e:
        return f"<ERR {e}>"

def nums(s):
    return [float(x.replace(",", "")) for x in re.findall(r"(?:rs\.?|pkr|[\$£€])?\s*([\d][\d,]*(?:\.\d+)?)", s, re.I)]

def has(ans, v):
    return any(abs(x - v) <= 0.5 for x in nums(ans))

def _ws(s):
    return re.sub(r"\s+", " ", (s or "").lower()).strip()

def short(name):
    return _ws(name)[:22]

def _find(ans, name):
    return _ws(ans).find(short(name))

# many paraphrasings per type — stresses the router/parser
P_PRICE = ['What is the price of "{a}"?', 'How much does "{a}" cost?', 'How much is "{a}"?',
           "What's the going rate for \"{a}\"?", 'price of "{a}"?', "what'll \"{a}\" run me?"]
P_EXISTS = ['Do you carry "{a}"?', 'Is "{a}" in your catalog?', 'Do you have "{a}" in stock?',
            'Got "{a}"?', 'Is "{a}" available?']
P_CHEAP = ['Between "{a}" and "{b}", which is cheaper and by how much?',
           'Which is cheaper, "{a}" or "{b}", and by what amount?',
           'Of "{a}" and "{b}", which costs less and what is the gap?',
           "which of \"{a}\" or \"{b}\" is the better deal price-wise, and by how much?"]
P_MORE = ['Which costs more, "{a}" or "{b}", and by how much?',
          'Between "{a}" and "{b}", which is more expensive and by what gap?',
          'Which is pricier, "{a}" or "{b}", and by how much?']
P_ORDER_DESC = ['Order these from most to least expensive with prices: {lst}.',
                'Rank these priciest to cheapest, include prices: {lst}.',
                'Sort {lst} from highest to lowest price with each price.']
P_ORDER_ASC = ['Rank these from lowest to highest price with prices: {lst}.',
               'Order {lst} cheapest first, include each price.',
               'Sort these low to high price: {lst}.']
P_BASKET = ['What is the total cost of buying one "{a}" and one "{b}"? Show both prices and the total.',
            'If I buy "{a}" and "{b}", what do I pay altogether? List both prices.',
            'A customer buys one "{a}" plus one "{b}". What combined amount should they pay?',
            'tally up "{a}" and "{b}" for me with the total.',
            'What would "{a}", "{b}" and "{c}" come to in total?']

def lst(ps):
    return ", ".join(f'"{p["title"]}"' for p in ps)

def order_ok(ans, ps, asc):
    seq = sorted(ps, key=lambda r: (r["price"] if asc else -r["price"]))
    # group equal prices — any order within a tie group is valid
    idx = [_find(ans, p["title"]) for p in seq]
    if any(i < 0 for i in idx):
        return False
    groups, i = [], 0
    while i < len(seq):
        j = i
        while j + 1 < len(seq) and seq[j + 1]["price"] == seq[i]["price"]:
            j += 1
        groups.append((i, j)); i = j + 1
    pos = 0
    for (s, e) in groups:
        block = idx[s:e + 1]
        if min(block) < pos:
            return False
        pos = max(block)
    return True

def prior_titles():
    """Every product title used in ANY previous test (all stress runs + the round-2
    GT), so a re-run shares ZERO products with prior runs — it tests generalization,
    not memorization."""
    import glob, os
    seen = set()
    base = os.path.dirname(os.path.abspath(__file__))
    for f in glob.glob(os.path.join(base, "stress*.txt")) + glob.glob(os.path.join(base, "round2*.txt")):
        try:
            txt = open(f, encoding="utf-8", errors="ignore").read()
            for m in re.findall(r'"([^"]{4,})"', txt):
                seen.add(_ws(m))
        except Exception:
            pass
    try:
        d = json.load(open(GT_JSON, encoding="utf-8"))
        for db in d.get("databases", {}).values():
            for q in db.get("questions", []):
                e = q.get("expected", {})
                if "product" in e:
                    seen.add(_ws(e["product"]["name"]))
                for k in ("products", "ordered"):
                    for p in e.get(k, []) or []:
                        seen.add(_ws(p["name"]))
    except Exception:
        pass
    return seen

_PRIOR = prior_titles()

def gen(cat, n):
    priced = [r for r in cat if r["price"] and _ws(r["title"]) not in _PRIOR]
    qs = []
    def pk(k):
        return random.sample(priced, k)
    for _ in range(n):
        a = pk(1)[0]; qs.append((random.choice(P_PRICE).format(a=a["title"]), "price", a))
        a = pk(1)[0]; qs.append((random.choice(P_EXISTS).format(a=a["title"]), "exists", a))
        a, b = pk(2); qs.append((random.choice(P_CHEAP).format(a=a["title"], b=b["title"]), "cheaper", (a, b)))
        a, b = pk(2); qs.append((random.choice(P_MORE).format(a=a["title"], b=b["title"]), "more", (a, b)))
        g = pk(3); qs.append((random.choice(P_ORDER_DESC).format(lst=lst(g)), "order_desc", g))
        g = pk(3); qs.append((random.choice(P_ORDER_ASC).format(lst=lst(g)), "order_asc", g))
        tmpl = random.choice(P_BASKET)
        k = 3 if "{c}" in tmpl else 2
        g = pk(k)
        d = {"a": g[0]["title"], "b": g[1]["title"]}
        if k == 3: d["c"] = g[2]["title"]
        qs.append((tmpl.format(**d), "basket", g))
    return qs

def grade(kind, payload, ans):
    if ans.startswith("<ERR"):
        return None  # infra, not scored
    if kind == "price":
        return has(ans, payload["price"])
    if kind == "exists":
        return has(ans, payload["price"]) or bool(re.search(r"\b(yes|we (do )?(carry|have|stock))\b", ans, re.I))
    if kind in ("cheaper", "more"):
        a, b = payload
        lo, hi = (a, b) if a["price"] <= b["price"] else (b, a)
        who = lo if kind == "cheaper" else hi
        diff = round(abs(a["price"] - b["price"]), 2)
        return short(who["title"]) in _ws(ans) and has(ans, diff)
    if kind == "order_desc":
        return order_ok(ans, payload, asc=False)
    if kind == "order_asc":
        return order_ok(ans, payload, asc=True)
    if kind == "basket":
        return has(ans, round(sum(p["price"] for p in payload), 2))
    return False

def run(db, base, n):
    cat = catalog(base); key = key_for(db)
    qs = gen(cat, n)
    print(f"\n{'='*70}\n{db}: {len(cat)} products | {len(qs)} stress Qs | key={'ok' if key else 'MISSING'}\n{'='*70}", flush=True)
    by, fails = {}, []
    for i, (q, kind, payload) in enumerate(qs, 1):
        ans = ask(key, q)
        v = grade(kind, payload, ans)
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
    return ok, tot

if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 6
    dbs = {"babyfy": "https://babyfy.pk", "stationery_studio": "https://stationerystudio.pk",
           "tsc_pk": "https://thestationerycompany.pk", "store": "https://webscraper.io/test-sites/e-commerce/allinone",
           "book": "https://books.toscrape.com"}
    only = [a for a in sys.argv[2:] if a in dbs] or list(dbs)
    t = nn = 0
    for db in only:
        a, b = run(db, dbs[db], n); t += a; nn += b
    print(f"\n\n######## STRESS TOTAL: {t}/{nn} ({100*t/max(nn,1):.1f}%) ########")
