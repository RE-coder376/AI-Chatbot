"""Run Codex round-2 GT (product_round2_25q_ground_truth.json) against live Modal
and grade per kind. Faithful re-test: 25 Q/DB (5 easy + 5 medium + 15 hard incl.
more_expensive_difference and order_low_to_high). Usage: python mverify/quiz_round2.py [db ...]"""
import json, re, sys, time, urllib.request, urllib.error

GT = r"C:\Users\User\Documents\Codex\2026-05-06\hi\product_round2_25q_ground_truth.json"
SERVE = "https://re-coder376--ai-chatbot-serve.modal.run"

def http(url, data=None, headers=None, timeout=120):
    last = None
    for a in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers or {}), timeout=timeout) as r:
                return r.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            last = e
            if e.code == 429:
                time.sleep(20 + a * 10); continue
            raise
        except Exception as e:
            last = e; time.sleep(5 + a * 5)
    raise last

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

def has_money(ans, target):
    return any(abs(v - target) <= 0.5 for v in nums(ans))

def short(name, n=22):
    return name.lower()[:n]

NEG = re.compile(r"\b(no|don'?t|do not|cannot|can'?t|couldn'?t find|not (?:currently )?(?:carry|have|stock|sell|listed|available)|isn'?t)\b", re.I)

def grade(q, ans):
    k = q["kind"]; e = q["expected"]
    a = ans.lower()
    if k == "exact_price":
        return has_money(ans, e["product"]["price"])
    if k == "exists_price":
        return has_money(ans, e["product"]["price"]) and not NEG.match(ans.strip()[:6])
    if k in ("cheaper_difference", "more_expensive_difference"):
        who = e.get("cheaper") or e.get("more_expensive")
        return short(who) in a and has_money(ans, e["difference"])
    if k in ("order_three", "order_low_to_high"):
        idx = [a.find(short(p["name"])) for p in e["ordered"]]
        return all(i >= 0 for i in idx) and idx == sorted(idx)
    if k == "basket_total":
        return has_money(ans, e["total"])
    return False

def run(db, qs):
    key = key_for(db)
    print(f"\n{'='*78}\n{db}: {len(qs)} Q | key={'ok' if key else 'MISSING'}\n{'='*78}")
    by = {}; pe = 0
    for q in qs:
        ans = ask(key, q["question"])
        ok = grade(q, ans)
        diff = q["difficulty"]; by.setdefault(diff, [0, 0]); by[diff][1] += 1; by[diff][0] += ok
        pe += ok
        tag = "PASS" if ok else "FAIL"
        print(f"{q['id']} [{tag}] ({q['kind']}) {q['question'][:78]}", flush=True)
        if not ok:
            print(f"     ans: {ans[:200].encode('ascii','replace').decode()}")
        time.sleep(5)
    sb = " ".join(f"{d}={by[d][0]}/{by[d][1]}" for d in ("easy", "medium", "hard") if d in by)
    print(f">>> {db}: {pe}/{len(qs)}  ({sb})")
    return pe, len(qs)

if __name__ == "__main__":
    data = json.load(open(GT, encoding="utf-8"))["databases"]
    only = sys.argv[1:] or list(data)
    t = n = 0
    for db in only:
        if db not in data:
            continue
        a, b = run(db, data[db]["questions"]); t += a; n += b
    print(f"\n\n######## ROUND-2 GRAND TOTAL: {t}/{n} ########")
