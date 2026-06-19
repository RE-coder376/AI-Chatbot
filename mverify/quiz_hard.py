"""Live HARD-tier quiz: compare / order / basket multi-product questions against
Modal /chat, graded on the COMPUTATION (cheaper+exact difference, correct ordering,
exact basket total) — the tier Codex scored ~30%. Ground truth from products.json.
Usage: python mverify/quiz_hard.py [db ...]"""
import json, re, sys, time, urllib.request, urllib.error

SERVE = "https://re-coder376--ai-chatbot-serve.modal.run"

def http(url, data=None, headers=None, timeout=120):
    last = None
    for attempt in range(5):
        try:
            req = urllib.request.Request(url, data=data, headers=headers or {})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                time.sleep(20 + attempt * 10); continue
            raise
        except Exception as e:
            last = e; time.sleep(5 + attempt * 5)
    raise last

def catalog(base):
    out = []
    for page in range(1, 20):
        try:
            d = json.loads(http(f"{base}/products.json?limit=250&page={page}", timeout=40))
        except Exception:
            break
        ps = d.get("products", [])
        if not ps:
            break
        for p in ps:
            vs = p.get("variants") or [{}]
            prices = [float(v.get("price")) for v in vs if v.get("price")]
            if not prices:
                continue
            out.append({"title": p.get("title", "").strip(), "price": min(prices)})
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
        r = http(f"{SERVE}/chat", data=body,
                 headers={"X-Widget-Key": key, "Content-Type": "application/json"}, timeout=120)
        return json.loads(r).get("answer", "") or ""
    except Exception as e:
        return f"<ERR {e}>"

def nums(s):
    return [float(x.replace(",", "")) for x in re.findall(r"(?:rs\.?|pkr|[\$£€])?\s*([\d][\d,]*(?:\.\d+)?)", s, re.I)]

def near(vals, target, tol=0.5):
    return any(abs(v - target) <= tol for v in vals)

def build(cat):
    """Pick 15 distinct-product hard questions: 5 compare, 5 order(3), 5 basket(2)."""
    c = [r for r in cat if r["price"] and len(r["title"]) > 4]
    # spread picks across the catalog so we exercise varied SKUs, no reuse
    step = max(1, len(c) // 40)
    pool = c[::step]
    seen = set(); pick = []
    for r in pool:
        k = r["title"].lower()
        if k in seen:
            continue
        seen.add(k); pick.append(r)
    qs = []; i = 0
    def take(n):
        nonlocal i
        grp = pick[i:i+n]; i += n; return grp
    for _ in range(5):
        a, b = take(2) if len(pick) - i >= 2 else ([], [])[0:0] or (None, None)
        if not a or not b:
            break
        lo, hi = (a, b) if a["price"] <= b["price"] else (b, a)
        diff = round(hi["price"] - lo["price"], 2)
        q = f'Between "{a["title"]}" and "{b["title"]}", which is cheaper and by exactly how much?'
        qs.append((q, "compare", {"cheaper": lo["title"], "diff": diff}))
    for _ in range(5):
        g = take(3)
        if len(g) < 3:
            break
        ordered = sorted(g, key=lambda r: -r["price"])
        names = ", ".join(f'"{r["title"]}"' for r in g)
        q = f"Order these from most to least expensive and include each price: {names}."
        qs.append((q, "order", {"order": [r["title"] for r in ordered]}))
    for _ in range(5):
        g = take(2)
        if len(g) < 2:
            break
        total = round(sum(r["price"] for r in g), 2)
        q = (f'What is the total cost of buying one "{g[0]["title"]}" and one '
             f'"{g[1]["title"]}"? Show both prices and the total.')
        qs.append((q, "basket", {"total": total}))
    return qs

def grade(ans, op, exp):
    a = ans.lower()
    v = nums(ans)
    if op == "compare":
        last = a.rsplit("\n", 1)[-1]
        name_ok = exp["cheaper"].lower()[:24] in a and "cheaper" in a
        diff_ok = near(v, exp["diff"])
        return ("PASS" if (name_ok and diff_ok) else "FAIL",
                f"cheaper~{exp['cheaper'][:24]!r} diff={exp['diff']} | name={name_ok} diff={diff_ok}")
    if op == "order":
        idx = [a.find(n.lower()[:24]) for n in exp["order"]]
        ok = all(x >= 0 for x in idx) and idx == sorted(idx)
        return ("PASS" if ok else "FAIL", f"order={[n[:18] for n in exp['order']]} idx={idx}")
    if op == "basket":
        ok = near(v, exp["total"])
        return ("PASS" if ok else "FAIL", f"total={exp['total']} nums={v[-4:]}")
    return "FAIL", "?"

def run(db, base):
    cat = catalog(base)
    key = key_for(db)
    qs = build(cat)
    print(f"\n{'='*78}\n{db}: {len(cat)} priced products | {len(qs)} hard Qs | key={'ok' if key else 'MISSING'}\n{'='*78}")
    p = 0
    for n, (q, op, exp) in enumerate(qs, 1):
        ans = ask(key, q)
        verdict, why = grade(ans, op, exp)
        p += verdict == "PASS"
        print(f"\nH{n} [{verdict}] ({op}) {q[:90]}\n   why: {why}\n   ans: {ans[:220].replace(chr(10),' | ')}", flush=True)
        time.sleep(5)
    print(f"\n>>> {db} HARD: {p}/{len(qs)}")
    return p, len(qs)

if __name__ == "__main__":
    dbs = {"babyfy": "https://babyfy.pk",
           "stationery_studio": "https://stationerystudio.pk",
           "tsc_pk": "https://thestationerycompany.pk"}
    only = sys.argv[1:] or list(dbs)
    t = n = 0
    for db in only:
        if db not in dbs:
            continue
        a, b = run(db, dbs[db]); t += a; n += b
    print(f"\n\n######## HARD GRAND TOTAL: {t}/{n} ########")
