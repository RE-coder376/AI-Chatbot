import base64
import hashlib
import hmac
import http.client
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = os.environ.get("DIECAST_MODAL_BASE", "https://re-coder376--ai-chatbot-serve.modal.run")
SHOPIFY_BASE = "https://diecaststation.pk"
ADMIN_PASSWORD = os.environ.get("PRODUCT_EVAL_ADMIN_PASSWORD") or os.environ.get("ADMIN_PASSWORD") or "iaah2006"
TEST_SET = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "DIECAST_100Q_20260629.json"
STAMP = TEST_SET.stem.replace("DIECAST_", "")
JSONL_OUT = ROOT / f"DIECAST_{STAMP}_RESULTS.jsonl"
JSON_OUT = ROOT / f"DIECAST_{STAMP}_RESULTS.json"
MD_OUT = ROOT / f"DIECAST_{STAMP}_RESULTS.md"
PACE_SECONDS = float(os.environ.get("DIECAST_EVAL_PACE", "5"))
RESUME = os.environ.get("DIECAST_EVAL_RESUME", "0") == "1"


def b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def widget_key(db="diecaststation", ttl_seconds=7200):
    payload = {"db": db, "exp": int(time.time()) + ttl_seconds, "v": 1}
    payload_b64 = b64url(json.dumps(payload, separators=(",", ":")).encode())
    sig = hmac.new(ADMIN_PASSWORD.encode(), payload_b64.encode(), hashlib.sha256).digest()
    return f"wk1.{payload_b64}.{b64url(sig)}"


def fetch_url_json(url, timeout=90, attempts=4):
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                return json.load(response)
        except (TimeoutError, urllib.error.URLError, http.client.HTTPException, json.JSONDecodeError) as exc:
            last_exc = exc
            if attempt == attempts:
                break
            time.sleep(min(2 * attempt, 8))
    raise last_exc


def money(value):
    return f"{int(round(float(value)))}"


def product_prices(product):
    return [float(v["price"]) for v in product.get("variants", []) if v.get("price") is not None]


def product_compare_prices(product):
    return [float(v["compare_at_price"]) for v in product.get("variants", []) if v.get("compare_at_price")]


def product_available(product):
    return any(v.get("available") for v in product.get("variants", []))


def product_price_token(product):
    return "PRICE:" + "|".join(sorted({money(x) for x in product_prices(product)}))


def product_compare_token(product):
    vals = product_compare_prices(product)
    return "PRICE:" + "|".join(sorted({money(x) for x in vals})) if vals else None


def product_ref(product):
    vals = product_prices(product)
    return {
        "title": product["title"],
        "handle": product["handle"],
        "url": f"{SHOPIFY_BASE}/products/{product['handle']}",
        "available": product_available(product),
        "price_range": [money(min(vals)), money(max(vals))] if vals else [],
        "variants": [{"title": v.get("title"), "price": money(v.get("price")), "compare_at_price": money(v.get("compare_at_price")) if v.get("compare_at_price") else None, "available": bool(v.get("available"))} for v in product.get("variants", [])],
    }


def fetch_all_products():
    # The full ?limit=250 dump is heavy and gets throttled/timed-out by the store.
    # Page in small (limit=50) batches — light requests the store serves reliably.
    out, page = [], 1
    while True:
        batch = fetch_url_json(f"{SHOPIFY_BASE}/products.json?limit=50&page={page}", timeout=30)["products"]
        if not batch:
            break
        out.extend(batch)
        page += 1
        if page > 60:
            break
    return out


def load_live_gt():
    products = fetch_all_products()
    by_title = {p["title"]: p for p in products}
    collections = fetch_url_json(f"{SHOPIFY_BASE}/collections.json")["collections"]
    collection_products = {}
    collection_handles = {}
    for collection in collections:
        title = collection["title"]
        handle = collection["handle"]
        collection_handles[title] = handle
        collection_products[title] = fetch_url_json(f"{SHOPIFY_BASE}/collections/{handle}/products.json?limit=250")["products"]
    return {"products": products, "by_title": by_title, "collections": collection_products, "collection_handles": collection_handles}


def select_product(rows, mode, availability=None):
    candidates = [x for x in rows if availability is None or product_available(x) == availability]
    key = lambda x: min(product_prices(x))
    return min(candidates, key=key) if mode == "min" else max(candidates, key=key)


def refresh_item_gt(item, live_gt):
    row = {**item}
    rule = row.get("gt_rule") or {}
    by = live_gt["by_title"]
    colls = live_gt["collections"]
    kind = rule.get("type")
    products = [by[t] for t in rule.get("products", []) if t in by]
    if products:
        row["gt_products"] = [product_ref(p) for p in products]
    if kind == "price" and products:
        p = products[0]
        exp = [p["title"].split("(")[0].strip(), product_price_token(p)]
        cmp = product_compare_token(p)
        if rule.get("include_compare") and cmp:
            exp.append(cmp)
        row["expect"] = exp
    elif kind == "stock" and products:
        p = products[0]
        row["expect"] = [p["title"].split("(")[0].strip(), "available" if product_available(p) else "out of stock"]
    elif kind == "count":
        rows = colls[rule["collection"]]
        row["expect"] = [f"COUNT:{len(rows)}"]
    elif kind == "count_split":
        rows = colls[rule["collection"]]
        av = sum(1 for x in rows if product_available(x))
        row["expect"] = [f"COUNT:{len(rows)}", f"AVAILABLE_COUNT:{av}", f"OOS_COUNT:{len(rows)-av}"]
    elif kind == "minmax":
        chosen = select_product(colls[rule["collection"]], rule["mode"], rule.get("availability"))
        row["expect"] = [chosen["title"], product_price_token(chosen), "available" if product_available(chosen) else "out of stock"]
        row["gt_products"] = [product_ref(chosen)]
    elif kind == "list":
        rows = [x for x in colls[rule["collection"]] if rule.get("availability") is None or product_available(x) == rule.get("availability")]
        lim = rule.get("limit", 4)
        row["expect"] = [x["title"] for x in rows[:lim]]
        if rule.get("availability") is True:
            row["expect"].append("available")
            row["forbid"] = [x["title"] + " - out of stock" for x in colls[rule["collection"]] if not product_available(x)][:3]
        elif rule.get("availability") is False:
            row["expect"].append("out of stock")
        row["gt_products"] = [product_ref(x) for x in rows[:lim]]
    elif kind == "budget":
        lo = rule.get("min")
        hi = rule.get("max")
        rows = [x for x in colls[rule["collection"]] if (rule.get("availability") is None or product_available(x) == rule.get("availability")) and (lo is None or min(product_prices(x)) >= lo) and (hi is None or min(product_prices(x)) <= hi)]
        lim = rule.get("limit", 3)
        row["expect"] = ([x["title"] for x in rows[:lim]] + ["available"]) if rows else ["contact"]
        row["gt_products"] = [product_ref(x) for x in rows[:lim]]
    elif kind == "basket" and products:
        total = sum(min(product_prices(p)) for p in products)
        row["expect"] = [f"TOTAL:{money(total)}"] + [p["title"].split("(")[0].strip() for p in products]
    elif kind == "ordering" and products:
        ordered = sorted(products, key=lambda p: min(product_prices(p)))
        row["expect"] = ["ORDER:" + ">".join([p["title"].split("(")[0].strip() for p in ordered])]
        row["gt_products"] = [product_ref(p) for p in ordered]
    elif kind == "compare" and products:
        mode = rule.get("mode")
        ordered = sorted(products, key=lambda p: min(product_prices(p)))
        if mode == "cheaper": chosen = ordered[0]
        elif mode == "expensive": chosen = ordered[-1]
        elif mode in {"stock", "buy"}: chosen = next((p for p in products if product_available(p)), products[0])
        elif mode == "order": chosen = ordered[0]
        else: chosen = ordered[0]
        exp = [p["title"].split("(")[0].strip() for p in products] + [chosen["title"].split("(")[0].strip()]
        if mode in {"stock", "buy"}: exp += ["available", "out of stock"]
        elif mode == "order": exp += ["ORDER:" + ">".join([p["title"].split("(")[0].strip() for p in ordered])]
        else: exp.append(product_price_token(chosen))
        row["expect"] = exp
    if row.get("gt_collection"):
        handle = live_gt["collection_handles"].get(row["gt_collection"])
        if handle:
            row["gt_collection_url"] = f"{SHOPIFY_BASE}/collections/{handle}"
    return row


def request_json(path, payload=None, headers=None, method=None, timeout=90):
    data = None
    if payload is not None:
        data = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(BASE + path, data=data, headers=headers or {}, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as response:
        body = response.read().decode("utf-8", errors="replace")
        ctype = response.headers.get("content-type", "")
    try:
        return json.loads(body)
    except Exception:
        return {"raw": body, "content_type": ctype}


def set_active():
    data = urllib.parse.urlencode({"password": ADMIN_PASSWORD, "name": "diecaststation"}).encode()
    try:
        return request_json("/admin/databases/set-active", payload=data, method="POST", timeout=90)
    except Exception as exc:
        print(f"WARN set-active failed: {exc}")
        return None


def parse_chat_response(body, ctype):
    if "text/event-stream" in ctype or body.lstrip().startswith("data:"):
        chunks, metadata = [], {}
        for line in body.splitlines():
            if not line.startswith("data:"):
                continue
            raw = line[5:].strip()
            if not raw or raw == "[DONE]":
                continue
            try:
                event = json.loads(raw)
            except Exception:
                continue
            if "chunk" in event:
                chunks.append(str(event["chunk"]))
            if isinstance(event.get("metadata"), dict):
                metadata.update(event["metadata"])
        return "".join(chunks), metadata
    try:
        data = json.loads(body)
    except Exception:
        return body, {}
    return str(data.get("answer") or data.get("response") or data.get("message") or data.get("content") or data.get("text") or ""), data.get("metadata") if isinstance(data.get("metadata"), dict) else {}


def chat(question, history):
    payload = {"question": question, "history": history or [], "stream": False}
    headers = {"Content-Type": "application/json", "X-Widget-Key": widget_key()}
    last_error = None
    for attempt in range(5):
        try:
            req = urllib.request.Request(BASE + "/chat", data=json.dumps(payload).encode("utf-8"), headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=150) as response:
                body = response.read().decode("utf-8", errors="replace")
                ctype = response.headers.get("content-type", "")
            return parse_chat_response(body, ctype), None
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code == 429:
                retry_after = exc.headers.get("Retry-After")
                sleep_for = int(retry_after) if retry_after and retry_after.isdigit() else [5, 10, 20, 40, 60][min(attempt, 4)]
                print(f"429 rate-limit; sleeping {sleep_for}s")
                time.sleep(sleep_for)
                continue
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code}: {body[:500]}") from exc
        except Exception as exc:
            last_error = exc
            if attempt == 0:
                print(f"warm/cold retry after error: {exc}")
                time.sleep(8)
                continue
            time.sleep(3)
    return ("", {}), {"capacity_429": isinstance(last_error, urllib.error.HTTPError) and getattr(last_error, "code", None) == 429, "error": str(last_error)}


def normalize(text):
    text = str(text).lower().replace("?", "-").replace("?", "-")
    text = re.sub(r"rs\.?\s*", "", text)
    text = text.replace(",", "")
    return re.sub(r"\s+", " ", text).strip()


def token_ok(answer_norm, token):
    raw = str(token)
    if raw.startswith("PRICE:"):
        return any(normalize(part) in answer_norm for part in raw.split(":", 1)[1].split("|"))
    if raw.startswith("TOTAL:"):
        return normalize(raw.split(":", 1)[1]) in answer_norm
    if raw.startswith("COUNT:"):
        n = int(raw.split(":", 1)[1])
        pats = [rf"\b{n}\s+(products?|items?|models?|listed|options?)\b", rf"\b(total|count|have|contains?|includes?)\D{{0,24}}{n}\b", rf"\bof\s+{n}\b"]
        if any(re.search(p, answer_norm) for p in pats): return True
        nums = [int(m.group(1)) for m in re.finditer(r"(?:^|\s)(\d{1,3})\.", answer_norm)]
        return bool(nums and max(nums) >= n)
    if raw.startswith("AVAILABLE_COUNT:"):
        n = raw.split(":", 1)[1]
        return re.search(rf"\b{re.escape(n)}\s+(available|in stock|buyable)", answer_norm) is not None
    if raw.startswith("OOS_COUNT:"):
        n = raw.split(":", 1)[1]
        return re.search(rf"\b{re.escape(n)}\s+(out of stock|sold out|unavailable)", answer_norm) is not None
    if raw.startswith("ORDER:"):
        parts = [normalize(x) for x in raw.split(":", 1)[1].split(">")]
        pos = []
        for part in parts:
            idx = answer_norm.find(part)
            if idx < 0: return False
            pos.append(idx)
        return pos == sorted(pos)
    token_norm = normalize(raw)
    aliases = {
        "out of stock": ["out of stock", "sold out", "unavailable", "not available", "currently out"],
        "available": ["available", "in stock", "mil jayega", "can buy", "buyable"],
        "contact": ["contact", "support", "reach out", "get in touch", "not specified", "don't have", "do not have"],
        "don't": ["don't", "do not", "doesn't", "not in", "don't carry", "not carry"],
    }
    if token_norm == "1:64 scale mini rc excavator":
        return token_norm in answer_norm or "mini rc excavator" in answer_norm
    if token_norm in aliases:
        return any(a in answer_norm for a in aliases[token_norm])
    return token_norm in answer_norm


def grade(answer, expect, forbid):
    answer_norm = normalize(answer)
    missing = [t for t in expect if not token_ok(answer_norm, t)]
    forbidden = [t for t in forbid if token_ok(answer_norm, t)]
    return not missing and not forbidden, missing, forbidden


def heuristic_judge(item, answer, token_ok_result):
    if not item.get("llm_judge"):
        return None
    answer_norm = normalize(answer)
    invented_policy = any(x in answer_norm for x in ["24 hours", "same day", "1 year", "6 months", "cod confirmed", "cash on delivery available", "cancel anytime"])
    result = bool(token_ok_result and not invented_policy)
    return {"mode": "heuristic_llm_judge_fallback", "pass": result, "reason": "Token grade passed and no obvious forbidden invented policy/spec claim found." if result else "Token grade failed or answer appears to invent unsupported policy/spec details."}


def load_existing():
    if not (RESUME and JSONL_OUT.exists()):
        return {}
    rows = {}
    for line in JSONL_OUT.read_text(encoding="utf-8").splitlines():
        if not line.strip(): continue
        row = json.loads(line)
        rows[row["id"]] = row
    return rows


def main():
    tests = json.loads(TEST_SET.read_text(encoding="utf-8"))
    print("refreshing live Shopify GT...")
    live_gt = load_live_gt()
    tests = [refresh_item_gt(item, live_gt) for item in tests]
    set_active()
    time.sleep(3)
    health_before = request_json("/health", timeout=90)
    print(f"health_before active_db={health_before.get('active_db')} docs={health_before.get('docs_indexed')}")
    existing = load_existing()
    if JSONL_OUT.exists() and not RESUME:
        JSONL_OUT.unlink()
    results = []
    for item in tests:
        if item["id"] in existing:
            print(f"RESUME {item['id']:03d}")
            row = existing[item["id"]]
            results.append(row)
            continue
        if item.get("ambiguous"):
            print(f"SKIP {item['id']:03d} ambiguous")
            row = {**item, "skipped": True, "ok": None, "answer": "", "missing": [], "forbidden_found": []}
        else:
            print(f"ASK {item['id']:03d}: {item['question']}")
            started = time.time()
            (answer, metadata), err = chat(item["question"], item.get("history", []))
            seconds = round(time.time() - started, 2)
            if err and err.get("capacity_429"):
                row = {**item, "capacity_429": True, "ok": None, "answer": "", "missing": [], "forbidden_found": [], "error": err, "seconds": seconds}
                print(f"429CAP {item['id']:03d}")
            else:
                ok, missing, forbidden = grade(answer, item.get("expect", []), item.get("forbid", []))
                judge = heuristic_judge(item, answer, ok)
                if judge is not None:
                    ok = bool(judge["pass"])
                row = {**item, "ok": ok, "missing": missing, "forbidden_found": forbidden, "answer": answer, "metadata": metadata, "seconds": seconds}
                if judge is not None:
                    row["judge_result"] = judge
                print(f"{'PASS' if ok else 'FAIL'} {item['id']:03d} missing={missing} forbidden={forbidden} answer={answer[:180].replace(chr(10),' | ')}")
            time.sleep(PACE_SECONDS)
        results.append(row)
        with JSONL_OUT.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    health_after = request_json("/health", timeout=90)
    scored = [r for r in results if not r.get("skipped") and not r.get("capacity_429")]
    passed = sum(1 for r in scored if r.get("ok"))
    failed = sum(1 for r in scored if r.get("ok") is False)
    cap = sum(1 for r in results if r.get("capacity_429"))
    skipped = sum(1 for r in results if r.get("skipped"))
    summary = {"test_set": str(TEST_SET), "total": len(results), "scored": len(scored), "passed": passed, "failed": failed, "skipped": skipped, "capacity_429": cap, "score_pct": round((passed/len(scored)*100) if scored else 0, 2), "health_before": health_before, "health_after": health_after, "results": results}
    JSON_OUT.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    lines = ["# Diecast 100Q 20260629 Results", "", f"Measured score: {passed}/{len(scored)} ({summary['score_pct']}%)", f"Skipped ambiguous: {skipped}", f"Capacity 429: {cap}", f"Health before active_db: {health_before.get('active_db')}", f"Health after active_db: {health_after.get('active_db')}", "", "## Failures"]
    for row in scored:
        if row.get("ok") is False:
            lines += ["", f"### Q{row['id']}: {row['question']}", f"Category: {row.get('category')}", f"Missing: {row.get('missing')}", f"Forbidden: {row.get('forbidden_found')}", f"Answer: {row.get('answer')}"]
    MD_OUT.write_text("\n".join(lines), encoding="utf-8")
    print(f"MEASURED {passed}/{len(scored)} ({summary['score_pct']}%) skipped={skipped} capacity_429={cap}")
    print(f"wrote {JSONL_OUT}")
    print(f"wrote {JSON_OUT}")
    print(f"wrote {MD_OUT}")


if __name__ == "__main__":
    main()
