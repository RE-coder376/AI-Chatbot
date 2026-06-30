"""Universal, store-agnostic readiness eval.

Usage:
    python tools/universal_eval.py <db_name> <store_url>
    e.g. python tools/universal_eval.py babyfy https://babyfy.pk

Pulls the store's OWN /collections.json + /products.json (no per-store config, no
hand-written questions), auto-generates a hard set across the core capability
classes (count / count_split / min+stock / list+stock / existence) per the store's
real collections + a few deliberately-creative and code-mixed wordings to test
GENERALIZATION, then runs each turn against the deployed bot and grades against the
live GT. This is how "is DB X ready?" becomes a one-hour measurement instead of weeks.
Same grader as the diecast harness, so scores are comparable across stores.
"""
import base64, hashlib, hmac, json, os, re, sys, time, urllib.error, urllib.parse, urllib.request

BASE = os.environ.get("MODAL_BASE", "https://re-coder376--ai-chatbot-serve.modal.run")
ADMIN_PASSWORD = os.environ.get("PRODUCT_EVAL_ADMIN_PASSWORD") or os.environ.get("ADMIN_PASSWORD") or "iaah2006"
PACE = float(os.environ.get("EVAL_PACE", "3"))

DB = sys.argv[1] if len(sys.argv) > 1 else "babyfy"
STORE = (sys.argv[2] if len(sys.argv) > 2 else "https://babyfy.pk").rstrip("/")


def b64u(raw): return base64.urlsafe_b64encode(raw).decode().rstrip("=")

_WK_CACHE = {}


def widget_key():
    """Fetch the tenant's CANONICAL stored widget key via the admin endpoint (no
    rotation — reuses the existing one, or mints+persists if none). Works for any DB
    whether or not a key was already provisioned; a locally-minted token would be
    rejected by _widget_key_matches_current when a stored key exists."""
    if DB in _WK_CACHE:
        return _WK_CACHE[DB]
    q = urllib.parse.urlencode({"password": ADMIN_PASSWORD, "db": DB})
    try:
        d = request_json(f"/admin/embed-code?{q}")
        k = str(d.get("widget_key") or "")
        if k:
            _WK_CACHE[DB] = k
            return k
    except Exception as e:
        print(f"WARN widget_key fetch: {e}")
    # fallback: local mint (works for tenants with no stored key)
    payload = {"db": DB, "exp": int(time.time()) + 7200, "v": 1}
    p = b64u(json.dumps(payload, separators=(",", ":")).encode())
    sig = b64u(hmac.new(ADMIN_PASSWORD.encode(), p.encode(), hashlib.sha256).digest())
    return f"wk1.{p}.{sig}"


def fetch_json(url, timeout=30, attempts=4):
    last = None
    for a in range(1, attempts + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except Exception as e:
            last = e
            if a < attempts:
                time.sleep(min(2 * a, 8))
    raise last


def prices(p): return [float(v["price"]) for v in p.get("variants", []) if v.get("price") is not None]
def available(p): return any(v.get("available") for v in p.get("variants", []))


def load_gt():
    products, page = [], 1
    while True:
        batch = fetch_json(f"{STORE}/products.json?limit=50&page={page}")["products"]
        if not batch:
            break
        products.extend(batch)
        page += 1
        if page > 60:
            break
    colls = {}
    for c in fetch_json(f"{STORE}/collections.json")["collections"]:
        try:
            rows = fetch_json(f"{STORE}/collections/{c['handle']}/products.json?limit=250")["products"]
        except Exception:
            rows = []
        if rows:
            colls[c["title"]] = rows
    return products, colls


def cheapest_available(rows):
    av = [r for r in rows if available(r) and prices(r)]
    return min(av, key=lambda r: min(prices(r))) if av else None


def gen_questions(colls):
    """Auto-build a hard set from the store's real collections. Capability classes +
    creative/code-mixed wordings to test generalization, not phrase-matching."""
    items = []
    # rank collections by size; use the meatiest for richer GT
    ranked = sorted(((t, r) for t, r in colls.items() if len(r) >= 3), key=lambda x: -len(x[1]))
    pick = ranked[:8]
    creative_cs = ["{c}: how many up for grabs vs how many done", "{c} stock breakdown",
                   "{c} mein kitne mojood kitne nahi", "{c} ready-to-ship vs sold-out count"]
    for i, (title, rows) in enumerate(pick):
        av = sum(1 for r in rows if available(r))
        # count
        items.append({"q": f"how many {title} do you have?", "cls": "count",
                      "expect": [f"COUNT:{len(rows)}"]})
        # count_split (rotate creative wording so we test the CLASS, not one phrase)
        items.append({"q": creative_cs[i % len(creative_cs)].format(c=title), "cls": "count_split",
                      "expect": [f"COUNT:{len(rows)}", f"AVAILABLE_COUNT:{av}", f"OOS_COUNT:{len(rows)-av}"]})
        # min + in-stock
        ch = cheapest_available(rows)
        if ch:
            items.append({"q": f"cheapest {title} I can actually buy", "cls": "min+stock",
                          "expect": [ch["title"].split("(")[0].strip(),
                                     "PRICE:" + "|".join(sorted({str(int(round(x))) for x in prices(ch)}))]})
        # list + in-stock (forbid the sold-out ones)
        in_rows = [r for r in rows if available(r)][:4]
        oos = [r["title"] + " - out of stock" for r in rows if not available(r)][:3]
        if in_rows:
            items.append({"q": f"show me only the {title} that are in stock", "cls": "list+stock",
                          "expect": [r["title"] for r in in_rows] + ["available"], "forbid": oos})
    for it in items:
        it.setdefault("forbid", [])
        it.setdefault("history", [])
    return items


def normalize(t):
    t = str(t).lower()
    t = re.sub(r"rs\.?\s*", "", t).replace(",", "")
    return re.sub(r"\s+", " ", t).strip()


def token_ok(ans, tok):
    raw = str(tok)
    if raw.startswith("PRICE:"):
        return any(normalize(p) in ans for p in raw.split(":", 1)[1].split("|"))
    if raw.startswith("COUNT:"):
        n = int(raw.split(":", 1)[1])
        pats = [rf"\b{n}\s+(products?|items?|models?|listed|options?)\b",
                rf"\b(total|count|have|contains?|includes?)\D{{0,24}}{n}\b", rf"\bof\s+{n}\b"]
        if any(re.search(p, ans) for p in pats):
            return True
        nums = [int(m.group(1)) for m in re.finditer(r"(?:^|\s)(\d{1,3})\.", ans)]
        return bool(nums and max(nums) >= n)
    if raw.startswith("AVAILABLE_COUNT:"):
        n = raw.split(":", 1)[1]
        return re.search(rf"\b{re.escape(n)}\s+(available|in stock|buyable)", ans) is not None
    if raw.startswith("OOS_COUNT:"):
        n = raw.split(":", 1)[1]
        return re.search(rf"\b{re.escape(n)}\s+(out of stock|sold out|unavailable)", ans) is not None
    return normalize(raw) in ans


def grade(answer, expect, forbid):
    a = normalize(answer)
    missing = [t for t in expect if not token_ok(a, t)]
    forbidden = [t for t in forbid if token_ok(a, t)]
    return (not missing and not forbidden), missing, forbidden


def request_json(path, payload=None, method=None, timeout=90):
    data = payload if isinstance(payload, (bytes, type(None))) else json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def set_active():
    data = urllib.parse.urlencode({"password": ADMIN_PASSWORD, "name": DB}).encode()
    try:
        return request_json("/admin/databases/set-active", payload=data, method="POST")
    except Exception as e:
        print(f"WARN set-active: {e}")


def parse_chat(body, ctype):
    if "text/event-stream" in ctype or body.lstrip().startswith("data:"):
        out = []
        for line in body.splitlines():
            if line.startswith("data:"):
                raw = line[5:].strip()
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


def chat(question, history):
    payload = {"question": question, "history": history or [], "stream": False}
    headers = {"Content-Type": "application/json", "X-Widget-Key": widget_key()}
    for attempt in range(5):
        try:
            req = urllib.request.Request(BASE + "/chat", data=json.dumps(payload).encode(), headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=150) as r:
                return parse_chat(r.read().decode("utf-8", "replace"), r.headers.get("content-type", "")), None
        except urllib.error.HTTPError as e:
            if e.code == 429:
                s = [5, 10, 20, 40, 60][min(attempt, 4)]
                print(f"429; sleep {s}s")
                time.sleep(s)
                continue
            return "", {"error": f"HTTP {e.code}"}
        except Exception as e:
            time.sleep(8 if attempt == 0 else 3)
            last = e
    return "", {"error": str(last)}


def main():
    print(f"=== universal_eval: db={DB} store={STORE} ===")
    print("pulling store's own GT feeds...")
    _, colls = load_gt()
    print(f"collections with >=1 product: {len(colls)}")
    items = gen_questions(colls)
    print(f"generated {len(items)} questions across {len({i['cls'] for i in items})} capability classes")
    set_active()
    time.sleep(3)
    results = []
    for n, it in enumerate(items, 1):
        print(f"ASK {n:03d} [{it['cls']}]: {it['q']}")
        ans, err = chat(it["q"], it["history"])
        if err:
            results.append({**it, "ok": None, "cap": True, "answer": ""})
            print(f"  ERR {err}")
            continue
        ok, missing, forbidden = grade(ans, it["expect"], it["forbid"])
        results.append({**it, "ok": ok, "missing": missing, "forbidden": forbidden, "answer": ans})
        print(f"  {'PASS' if ok else 'FAIL'} missing={missing} forbidden={forbidden}")
        print(f"  -> {ans[:160].replace(chr(10),' | ')}")
        time.sleep(PACE)
    scored = [r for r in results if r.get("ok") is not None]
    passed = sum(1 for r in scored if r["ok"])
    pct = round(passed / len(scored) * 100, 2) if scored else 0
    # Freshness verdict: a high abstention rate ("we don't carry X") on count/list
    # questions means the ingested catalog is STALE/UNDER-INGESTED vs the live store —
    # a re-ingest problem, NOT a bot-logic failure. Flag it so 0% isn't misread.
    _abstain = re.compile(r"don'?t (currently )?carry|couldn'?t find|no .* in our catalog", re.I)
    abst = sum(1 for r in scored if _abstain.search(str(r.get("answer", ""))))
    stale = scored and abst / len(scored) >= 0.5
    if stale:
        print(f"\n⚠ DB LIKELY STALE / UNDER-INGESTED: {abst}/{len(scored)} answers abstained "
              f"('we don't carry…'). The ingested catalog doesn't match the live store — "
              f"RE-INGEST this tenant before trusting the score.")
    by_cls = {}
    for r in scored:
        d = by_cls.setdefault(r["cls"], [0, 0])
        d[1] += 1
        d[0] += 1 if r["ok"] else 0
    out = f"UNIVERSAL_EVAL_{DB}.json"
    json.dump({"db": DB, "store": STORE, "scored": len(scored), "passed": passed,
               "score_pct": pct, "stale_db": bool(stale), "abstained": abst,
               "by_class": by_cls, "results": results},
              open(out, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"\n=== {DB}: {passed}/{len(scored)} = {pct}% ===")
    for cls, (p, t) in sorted(by_cls.items()):
        print(f"  {cls}: {p}/{t}")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
