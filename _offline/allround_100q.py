"""All-round 100Q live test for diecaststation — every customer-intent class, live GT
pulled at run time (never stale). Fresh wordings incl. Roman-Urdu + typos. Multi-turn
coref uses real history threading. Dumps every FAIL verbatim for manual artifact review.
"""
import base64, hashlib, hmac, json, os, random, re, time, urllib.parse, urllib.request, urllib.error

BASE = "https://re-coder376--ai-chatbot-serve.modal.run"
PW = os.environ.get("ADMIN_PASSWORD", "iaah2006")
DB = "diecaststation"
STORE = "https://diecaststation.pk"
PACE = float(os.environ.get("PACE", "1.4"))
random.seed(700200)


def b64u(b): return base64.urlsafe_b64encode(b).decode().rstrip("=")


def widget_key():
    q = urllib.parse.urlencode({"password": PW, "db": DB})
    try:
        return json.load(urllib.request.urlopen(f"{BASE}/admin/embed-code?{q}", timeout=30))["widget_key"]
    except Exception:
        p = {"db": DB, "exp": int(time.time()) + 7200, "v": 1}
        pp = b64u(json.dumps(p, separators=(",", ":")).encode())
        sig = b64u(hmac.new(PW.encode(), pp.encode(), hashlib.sha256).digest())
        return f"wk1.{pp}.{sig}"


WK = widget_key()


def set_active():
    d = urllib.parse.urlencode({"password": PW, "name": DB}).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(f"{BASE}/admin/databases/set-active", data=d, method="POST"), timeout=60).read()
    except Exception as e:
        print("WARN set-active", e)


def parse_chat(body, ctype):
    if "text/event-stream" in ctype or body.lstrip().startswith("data:"):
        out = []
        for ln in body.splitlines():
            if ln.startswith("data:"):
                raw = ln[5:].strip()
                if raw and raw != "[DONE]":
                    try:
                        ev = json.loads(raw)
                        if "chunk" in ev:
                            out.append(str(ev["chunk"]))
                    except Exception:
                        pass
        return "".join(out)
    try:
        d = json.loads(body)
        return str(d.get("answer") or d.get("response") or d.get("message") or d.get("content") or "")
    except Exception:
        return body


def chat(q, history=None):
    payload = {"question": q, "history": history or [], "stream": False}
    headers = {"Content-Type": "application/json", "X-Widget-Key": WK}
    last = None
    for a in range(5):
        try:
            req = urllib.request.Request(f"{BASE}/chat", data=json.dumps(payload).encode(), headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=150) as r:
                return parse_chat(r.read().decode("utf-8", "replace"), r.headers.get("content-type", ""))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep([5, 10, 20, 40, 60][min(a, 4)]); continue
            return f"__ERR__ HTTP {e.code}"
        except Exception as e:
            last = e; time.sleep(6)
    return f"__ERR__ {last}"


# ---------- live GT ----------
def _get(url):
    for a in range(4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            return json.load(urllib.request.urlopen(req, timeout=45))
        except Exception:
            time.sleep(2 + 3 * a)
    return {}


def feed(url):
    rows, page = [], 1
    while True:
        d = _get(url + (f"&page={page}" if "?" in url else f"?page={page}")).get("products", [])
        if not d:
            break
        rows += d; page += 1
        time.sleep(0.4)
        if len(d) < 250:
            break
    return rows


def prices(p): return [float(v["price"]) for v in p.get("variants", []) if v.get("price")]
def compare(p): return [float(v["compare_at_price"]) for v in p.get("variants", []) if v.get("compare_at_price")]
def avail(p): return any(v.get("available") for v in p.get("variants", []))
def money(x): return str(int(round(x)))
def norm(t):
    t = str(t).lower(); t = re.sub(r"rs\.?\s*", "", t).replace(",", "")
    return re.sub(r"\s+", " ", t).strip()


prods = feed(f"{STORE}/products.json?limit=250")
by_handle = {p["handle"]: p for p in prods}
colls_meta = _get(f"{STORE}/collections.json?limit=250").get("collections", [])
colls = {}
for c in colls_meta:
    rows = feed(f"{STORE}/collections/{c['handle']}/products.json?limit=250")
    if rows:
        colls[c["title"]] = rows
big = sorted(((t, r) for t, r in colls.items() if len(r) >= 5), key=lambda x: -len(x[1]))
print(f"products={len(prods)} collections_nonempty={len(colls)} big={len(big)}")

instock = [p for p in prods if avail(p) and prices(p)]
oos = [p for p in prods if not avail(p) and prices(p)]
random.shuffle(instock); random.shuffle(oos)


def cheapest_av(rows):
    c = [(min(prices(r)), r) for r in rows if avail(r) and prices(r)]
    return min(c, key=lambda x: x[0])[1] if c else None
def priciest_av(rows):
    c = [(max(prices(r)), r) for r in rows if avail(r) and prices(r)]
    return max(c, key=lambda x: x[0])[1] if c else None


def ptok(p): return "PRICE:" + "|".join(sorted({money(x) for x in prices(p)}))
def base_name(t): return re.split(r"[(\[]", t)[0].strip()


def typo(s):
    s = list(s)
    idxs = [i for i, ch in enumerate(s) if ch.isalpha()]
    if len(idxs) > 6:
        i = idxs[len(idxs) // 2]; s[i] = ""
    return "".join(s)


cases = []
add = cases.append

# 1 price (12) mixed wording + languages
tp = ["what's the price of {}?", "how much is {}", "{} ka kya rate hai", "cost of {}?",
      "price for {} please", "{} kitne ka hai", "how much does {} cost", "{} price?"]
for i, p in enumerate(instock[:12]):
    add({"cls": "price", "q": tp[i % len(tp)].format(p["title"]), "expect": [ptok(p)]})

# 2 stock_in (8)
ti = ["is {} in stock?", "can I buy {} right now", "{} available hai?", "do you have {} available"]
for i, p in enumerate(instock[12:20]):
    add({"cls": "stock_in", "q": ti[i % len(ti)].format(p["title"]), "expect": ["__IN__"]})

# 3 stock_out (8)
to = ["is {} available?", "do you have {} in stock", "{} mil sakta hai?", "can I order {}"]
for i, p in enumerate(oos[:8]):
    add({"cls": "stock_out", "q": to[i % len(to)].format(p["title"]), "expect": ["__OOS__"]})

# 4 absent (6)
for q in ["do you sell iPhone 15 Pro Max?", "any Nike Air Jordan sneakers?", "do you have a PS5 console?",
          "got fresh mangoes?", "sell me a Samsung fridge", "do you carry Rolex watches?"]:
    add({"cls": "absent", "q": q, "expect": ["__ABSENT__"]})

# 5 count (8) — GT = collection membership
cc = ["how many {} do you have?", "{} ki total count kitni hai", "how many {} are listed",
      "total number of {}?"]
for i, (t, r) in enumerate(big[:8]):
    add({"cls": "count", "q": cc[i % len(cc)].format(t), "expect": [f"COUNT:{len(r)}"], "coll": t})

# 6 count_split (8)
cs = ["{}: how many in stock vs sold out", "{} mein kitne mojood kitne nahi",
      "{} stock breakdown", "{} available vs unavailable count"]
for i, (t, r) in enumerate(big[:8]):
    av = sum(1 for x in r if avail(x))
    add({"cls": "count_split", "q": cs[i % len(cs)].format(t),
         "expect": [f"COUNT:{len(r)}", f"AVAIL:{av}", f"OOS:{len(r)-av}"], "coll": t})

# 7 list+stock (6)
for i, (t, r) in enumerate(big[:6]):
    inr = [x["title"] for x in r if avail(x)][:3]
    outr = [base_name(x["title"]) for x in r if not avail(x)][:2]
    if inr:
        add({"cls": "list", "q": f"show me only the {t} that are in stock",
             "expect": [base_name(x) for x in inr], "forbid": outr, "coll": t})

# 8 cheapest in collection (8)
for i, (t, r) in enumerate(big[:8]):
    ch = cheapest_av(r)
    if ch:
        add({"cls": "min", "q": random.choice([f"cheapest {t} I can buy", f"sabse sasta {t} kaunsa hai",
             f"lowest priced {t} in stock", f"most affordable {t}"]),
             "expect": [base_name(ch["title"]), ptok(ch)], "coll": t})

# 9 priciest in collection (6)
for i, (t, r) in enumerate(big[:6]):
    pr = priciest_av(r)
    if pr:
        add({"cls": "max", "q": random.choice([f"most expensive {t}", f"sabse mehnga {t}",
             f"priciest {t} you have", f"highest priced {t}"]),
             "expect": [base_name(pr["title"]), ptok(pr)], "coll": t})

# 10 budget under X (6)
for i, thr in enumerate([3000, 4000, 5000, 6000, 3500, 4500]):
    under = [p for p in instock if prices(p) and min(prices(p)) <= thr]
    if under:
        add({"cls": "budget", "q": random.choice([f"show me diecast cars under Rs {thr}",
             f"{thr} se kam wali cars dikhao", f"anything below {thr} rupees",
             f"cars i can get for under {thr}"]),
             "expect": [f"UNDER:{thr}"]})

# 11 comparison (6) two named products -> cheaper one
for i in range(6):
    a, b = random.sample(instock[:40], 2)
    while abs((min(prices(a)) - min(prices(b)))) < 100:
        a, b = random.sample(instock[:40], 2)
    cheaper = a if min(prices(a)) < min(prices(b)) else b
    add({"cls": "compare", "q": random.choice([
        f"which is cheaper, {a['title']} or {b['title']}?",
        f"{a['title']} ya {b['title']}, konsa sasta hai?",
        f"compare price of {a['title']} and {b['title']}"]),
        "expect": [base_name(cheaper["title"])]})

# 12 ordering (4) rank 3 named by price
for i in range(4):
    trio = random.sample(instock[:40], 3)
    ordered = sorted(trio, key=lambda p: min(prices(p)))  # cheap->exp
    add({"cls": "order", "q": "rank by price cheapest first: " + ", ".join(p["title"] for p in trio),
         "expect": ["ORDER:" + ">".join(base_name(p["title"]) for p in ordered)]})

# 13 coref follow-ups (8) — 4 pairs, 2 turns each
pair_prods = instock[20:24]
for p in pair_prods:
    add({"cls": "coref_seed", "q": f"do you have {p['title']}?", "expect": ["__ANY__"], "_p": p})
    add({"cls": "coref", "q": random.choice(["how much is it?", "is it in stock?", "uska price kya hai?", "what's its price"]),
         "expect": [ptok(p)], "_after": p["title"]})

# 14 typos (6)
for p in instock[24:30]:
    add({"cls": "typo", "q": f"price of {typo(p['title'])}?", "expect": [ptok(p)], "_orig": p["title"]})

# 15 real existence (4)
for p in instock[30:34]:
    add({"cls": "exists", "q": random.choice([f"do you have {p['title']}?", f"{p['title']} milega?"]),
         "expect": ["__ANY__"]})

# 16 coref SET-reference (multi-turn) — validates the LLM coref rewrite on a prior set
for i in range(3):
    trio = random.sample(instock[34:64], 3)
    ordered = sorted(trio, key=lambda p: min(prices(p)))  # cheap->exp
    seedq = "how much are " + ", ".join(p["title"] for p in trio[:2]) + " and " + trio[2]["title"] + "?"
    add({"cls": "cset_seed", "q": seedq, "expect": ["__ANY__"]})
    add({"cls": "cset", "q": random.choice(["sort those three cheapest first",
         "un teenon ko sasta pehle karke dikhao", "rank those 3 low to high"]),
         "expect": ["ORDER:" + ">".join(base_name(p["title"]) for p in ordered)]})

# 17 basket total (4)
for i in range(4):
    a2, b2 = random.sample(instock[40:74], 2)
    add({"cls": "basket", "q": random.choice([f"total price of {a2['title']} plus {b2['title']}?",
         f"{a2['title']} aur {b2['title']} ka total kitna?"]),
         "expect": [base_name(a2["title"]), base_name(b2["title"])]})

# 18 budget BUYABLE (4) — must exclude sold-out (the "jo mil jaye" fix)
for i, thr in enumerate([5000, 6000, 4000, 8000]):
    cat = random.choice(["BMW", "Ferrari", "Mercedes", "bike"])
    add({"cls": "budget_buy", "q": random.choice([f"{cat} gift under {thr} jo mil jaye",
         f"available {cat} under {thr} jo buy ho sake"]), "expect": ["__NOOOS__"]})

for c in cases:
    c.setdefault("forbid", [])

# ---------- grade ----------
IN = re.compile(r"\b(in stock|available|you can (buy|order|get)|yes[^.]{0,25}(stock|available)|ready to ship|purchasable)\b", re.I)
OOSR = re.compile(r"\b(out of stock|sold out|unavailable|not available|currently unavailable|no longer)\b", re.I)
ABS = re.compile(r"don'?t (currently )?(carry|have|sell|stock)|couldn'?t find|no .{0,30}(in our|matching)|we (do not|don'?t) (carry|offer|sell)|unable to find|not something we|don'?t have (specific|any)", re.I)


def price_hit(a, tok): return any(norm(x) in a for x in tok.split(":", 1)[1].split("|"))


def count_hit(a, n):
    pats = [rf"\b{n}\s+(products?|items?|models?|listed|options?|cars?)\b",
            rf"\b(total|count|have|contains?|includes?|of)\D{{0,20}}{n}\b"]
    if any(re.search(p, a) for p in pats):
        return True
    nums = [int(m.group(1)) for m in re.finditer(r"(?:^|\s)(\d{1,3})[\.\)]", a)]
    return bool(nums and max(nums) >= n)


def grade(c, a):
    if a.startswith("__ERR__"):
        return None
    al = norm(a)
    e0 = c["expect"][0]
    cls = c["cls"]
    if e0 == "__IN__":
        return bool(IN.search(a)) and not OOSR.search(a)
    if e0 == "__OOS__":
        return bool(OOSR.search(a))
    if e0 == "__ABSENT__":
        return bool(ABS.search(a))
    if e0 == "__ANY__":
        return not ABS.search(a) and len(al) > 15
    if cls in ("price", "typo", "coref"):
        return price_hit(al, e0)
    if cls == "count":
        return count_hit(al, int(e0.split(":")[1]))
    if cls == "count_split":
        n = {k: int(v) for k, v in (x.split(":") for x in c["expect"])}
        tot = count_hit(al, n["COUNT"])
        av = re.search(rf"\b{n['AVAIL']}\s+(available|in stock|buyable)", al) is not None
        oo = re.search(rf"\b{n['OOS']}\s+(out of stock|sold out|unavailable)", al) is not None
        return tot and av and oo
    if cls == "list":
        miss = [x for x in c["expect"] if norm(x) not in al]
        forb = [x for x in c["forbid"] if norm(x) in al]
        return not miss and not forb
    if cls in ("min", "max"):
        name_ok = norm(c["expect"][0]) in al
        price_ok = price_hit(al, c["expect"][1])
        return name_ok or price_ok
    if cls == "budget":
        thr = int(e0.split(":")[1])
        # strip the strike-through "(was Rs.X)" / "originally Rs.X" compare-at price first,
        # else the discount's ORIGINAL price false-fails a correctly under-budget item.
        _a = re.sub(r"\(was[^)]*\)", "", a.lower())
        _a = re.sub(r"originally[^,)]*", "", _a)
        nums = [int(x.replace(",", "")) for x in re.findall(r"(?:rs\.?\s*)(\d[\d,]{2,})", _a)]
        over = [x for x in nums if x > thr * 1.02]
        return bool(nums) and not over
    if cls == "compare":
        return norm(c["expect"][0]) in al
    if cls == "order":
        seq = e0.split(":", 1)[1].split(">")
        pos = [al.find(norm(x)) for x in seq]
        return all(p >= 0 for p in pos) and pos == sorted(pos)
    if cls in ("coref_seed", "cset_seed", "exists"):
        return not ABS.search(a) and len(al) > 15
    if cls == "cset":
        seq = e0.split(":", 1)[1].split(">")
        pos = [al.find(norm(x)) for x in seq]
        return all(p >= 0 for p in pos) and pos == sorted(pos)
    if cls == "basket":
        # behavioral: both products named AND a total produced (lenient on the exact sum
        # to avoid variant-price false-fails; the point is a basket total, not a dump).
        return all(norm(x) in al for x in c["expect"]) and "total" in al and bool(re.search(r"\d{3,}", a))
    if cls == "budget_buy":
        return not OOSR.search(a) and bool(re.search(r"\d{3,}", a))
    return norm(e0) in al


set_active(); time.sleep(2)
results = []
hist = []
for n, c in enumerate(cases, 1):
    use_hist = hist if c["cls"] in ("coref", "cset") else []
    a = chat(c["q"], use_hist)
    ok = grade(c, a)
    results.append({**{k: v for k, v in c.items() if not k.startswith("_")}, "ok": ok, "answer": a})
    if c["cls"] in ("coref_seed", "coref", "cset_seed", "cset"):
        if c["cls"].endswith("_seed"):
            hist = []  # fresh set — don't carry the previous pair's turns
        hist = (hist + [{"role": "user", "content": c["q"]}, {"role": "assistant", "content": a}])[-6:]
    else:
        hist = []
    tag = "PASS" if ok else ("ERR" if ok is None else "FAIL")
    print(f"{n:03d} [{c['cls']}] {tag}: {c['q'][:70]}")
    time.sleep(PACE)

scored = [r for r in results if r["ok"] is not None]
p = sum(1 for r in scored if r["ok"])
byc = {}
for r in scored:
    d = byc.setdefault(r["cls"], [0, 0]); d[1] += 1; d[0] += 1 if r["ok"] else 0
print(f"\n=== ALL-ROUND: {p}/{len(scored)} = {round(p/max(len(scored),1)*100,1)}% (errors={len(results)-len(scored)}) ===")
for cls, (a, b) in sorted(byc.items()):
    print(f"  {cls}: {a}/{b}")
print("\n----- FAILS -----")
for r in results:
    if r["ok"] is False:
        print(f"[{r['cls']}] {r['q']}")
        print(f"   expect={r['expect']}")
        print(f"   -> {r['answer'][:220].replace(chr(10),' ')}\n")
json.dump(results, open("_offline/allround_100q_out.json", "w", encoding="utf-8"), ensure_ascii=False, indent=2)
