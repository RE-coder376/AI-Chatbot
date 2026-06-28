"""Universal catalog intent+entity extractor (the "router").

The deterministic regex parser in catalog_query handles the common phrasings
exactly. But natural language is unbounded — a client can ask for a comparison,
a total, or a lookup in infinitely many ways ("what do these two set me back",
"tally that up", "which is the pricier one"). Regex can never enumerate that.

This module adds a SINGLE structured LLM call that turns ANY question into a
normalized plan {op, products, constraints}. It is used ONLY as a fallback when
the deterministic parser declines, so it can never regress a case the regex
already handles, and it never computes prices or math itself — execution stays
fully deterministic in catalog_query. On any failure it returns None (the caller
then falls back to ordinary RAG), so it can never produce a wrong answer.
"""
from __future__ import annotations

import json
import os
import re

_KINDS = {"lookup", "exists", "count", "cheapest", "priciest",
          "filter", "compare", "order", "basket", "browse", "other"}

_SYS = (
    "You convert a customer's question about an online store's product catalog into "
    "a STRICT JSON plan. You do NOT answer, price, or compute anything — you only "
    "classify the intent and copy out the product names the customer literally wrote.\n\n"
    "Return ONLY a JSON object, no prose, with EXACTLY these keys:\n"
    '{"kind": one of '
    '["lookup","exists","count","cheapest","priciest","filter","compare","order","basket","browse","other"],'
    ' "products": [exact product-name strings the customer mentioned, verbatim],'
    ' "compare_dir": "cheaper" | "more" | null,'
    ' "order_dir": "asc" | "desc" | null,'
    ' "price_min": number | null, "price_max": number | null,'
    ' "in_stock": true | false,'
    ' "stock_query": "in" | "out" | "both" | null,'
    ' "category": string | null}\n\n'
    "The question may be in English, Roman Urdu/Hindi, or a mix — understand the MEANING "
    "regardless of language and fill the SAME English JSON. Examples of intent words: "
    "'kitny/kitna ka hai'=price, 'hai?/mil jayega/milega/stock mein hai'=availability, "
    "'sasta/kam'=cheaper, 'mehnga/zyada'=more expensive, 'konsa/kaunsa/kis'=which, "
    "'aur/ya'=and/or, 'se kam'=under, 'dono/available aur sold out'=both states, "
    "'chahiye/order kar sakta hun'=wants to buy.\n"
    "stock_query: set 'in' if they ask for IN-STOCK/available only, 'out' if they ask for "
    "SOLD-OUT/unavailable only, 'both' if they ask for available AND sold out together, "
    "else null. For a single product's availability question use kind='lookup' with "
    "stock_query='in' (the engine reports its real status either way).\n"
    "Definitions:\n"
    "- lookup: price/availability of ONE named product.\n"
    "- exists: whether ONE named product is carried.\n"
    "- compare: which of TWO named products is cheaper/more expensive and the gap. "
    "compare_dir='more' if it asks which is more expensive/pricier/costs more, else 'cheaper'.\n"
    "- order: rank several named products by price. order_dir='asc' for lowest-to-highest, 'desc' otherwise.\n"
    "- basket: total/combined cost of buying several named products.\n"
    "- count/cheapest/priciest/filter: aggregates over a CATEGORY (not specific named products).\n"
    "- browse: open-ended 'what do you sell'. other: anything not about the catalog.\n"
    "Copy product names exactly as written, including punctuation. Output JSON only."
)

_FEWSHOT = [
    ('A customer buys one "Baby Piano Fitness Rack Gym Mat" plus one "Kids Foldable Scooty". '
     'What combined amount should they pay? Include both item prices.',
     {"kind": "basket", "products": ["Baby Piano Fitness Rack Gym Mat", "Kids Foldable Scooty"],
      "compare_dir": None, "order_dir": None, "price_min": None, "price_max": None,
      "in_stock": False, "stock_query": None, "category": None}),
    ('Which is the pricier one, "Ultra Male" or "Sultan Attar", and by how much?',
     {"kind": "compare", "products": ["Ultra Male", "Sultan Attar"], "compare_dir": "more",
      "order_dir": None, "price_min": None, "price_max": None, "in_stock": False,
      "stock_query": None, "category": None}),
    # Roman-Urdu price lookup ("kitny ka hai" = how much is it)
    ('hey bro bmw m8 1:24 kitny ka hai?',
     {"kind": "lookup", "products": ["bmw m8 1:24"], "compare_dir": None, "order_dir": None,
      "price_min": None, "price_max": None, "in_stock": False, "stock_query": None, "category": None}),
    # Roman-Urdu availability of one product ("hai?/mil jayega" = is it available)
    ('mini rc pocket drone mil jayega?',
     {"kind": "lookup", "products": ["mini rc pocket drone"], "compare_dir": None, "order_dir": None,
      "price_min": None, "price_max": None, "in_stock": False, "stock_query": "in", "category": None}),
    # Roman-Urdu comparison ("sasta konsa" = which is cheaper)
    ('bmw m8 aur bmw m4 mein sasta konsa hai?',
     {"kind": "compare", "products": ["bmw m8", "bmw m4"], "compare_dir": "cheaper", "order_dir": None,
      "price_min": None, "price_max": None, "in_stock": False, "stock_query": None, "category": None}),
    # Roman-Urdu both-states summary ("available aur sold out dono" = both)
    ('action figures mein available aur sold out dono batao',
     {"kind": "filter", "products": [], "compare_dir": None, "order_dir": None,
      "price_min": None, "price_max": None, "in_stock": False, "stock_query": "both",
      "category": "action figures"}),
    ('how much for the "Diecast Kawasaki Ninja H2R"?',
     {"kind": "lookup", "products": ["Diecast Kawasaki Ninja H2R"], "compare_dir": None,
      "order_dir": None, "price_min": None, "price_max": None, "in_stock": False,
      "stock_query": None, "category": None}),
]


def _coerce(obj: dict) -> dict | None:
    """Validate + normalize the model's JSON into a safe plan, or None if unusable."""
    if not isinstance(obj, dict):
        return None
    kind = str(obj.get("kind") or "").strip().lower()
    if kind not in _KINDS:
        return None
    products = obj.get("products") or []
    if not isinstance(products, list):
        return None
    products = [re.sub(r"\s+", " ", str(p)).strip() for p in products if str(p).strip()]
    cdir = obj.get("compare_dir")
    cdir = cdir if cdir in ("cheaper", "more") else None
    odir = obj.get("order_dir")
    odir = odir if odir in ("asc", "desc") else None

    def _n(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None
    sq = obj.get("stock_query")
    sq = sq if sq in ("in", "out", "both") else None
    return {
        "kind": kind,
        "products": products[:6],
        "compare_dir": cdir,
        "order_dir": odir,
        "price_min": _n(obj.get("price_min")),
        "price_max": _n(obj.get("price_max")),
        "in_stock": bool(obj.get("in_stock")) or sq == "in",
        "stock_query": sq,
        "category": (str(obj.get("category")).strip() or None) if obj.get("category") else None,
    }


def _extract_json(text: str) -> dict | None:
    if not text:
        return None
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:
        return None


def _plan(kind, names, cdir=None, odir=None, in_stock=False, stock_query=None):
    return {"kind": kind, "products": names[:6], "compare_dir": cdir, "order_dir": odir,
            "price_min": None, "price_max": None, "in_stock": in_stock,
            "stock_query": stock_query, "category": None}


def heuristic_plan(q: str) -> dict | None:
    """Deterministic backstop used when the LLM is unavailable (all free keys cooled
    down) or returns nothing. Extracts quoted product names + rule-based intent so a
    structured product question (price/exists/compare/order/basket) still answers
    from the catalog with ZERO LLM dependency. Conservative: needs a quoted name."""
    try:
        ql = (q or "").lower()
        names = [re.sub(r"\s+", " ", n).strip()
                 for n in re.findall(r'["“”]([^"“”]{3,})["“”]', q or "") if n.strip()]
        if not names:
            return None
        if len(names) >= 2 and re.search(
                r"cheaper|more\s+expensive|pricier|dearer|which\s+(?:one\s+)?(?:is|costs?)|"
                r"costs?\s+(?:more|less)|difference\s+between|price\s+gap|by\s+how\s+much|"
                r"\bvs\.?\b|versus", ql):
            cdir = "more" if re.search(r"more\s+expensive|costs?\s+more|pricier|dearer", ql) else "cheaper"
            return _plan("compare", names, cdir=cdir)
        if len(names) >= 2 and re.search(r"\b(order|rank|sort|arrange)\b", ql):
            odir = "asc" if re.search(r"low(?:est)?\s+to\s+high|cheap(?:est)?\s+first|ascending|least\s+to\s+most", ql) else "desc"
            return _plan("order", names, odir=odir)
        if len(names) >= 2 and re.search(
                r"\b(total|combined|altogether|sum|together|buy|buys|buying|bought|plus|tally|both|pay|spend|owe)\b", ql):
            return _plan("basket", names)
        if re.search(r"\b(got|carry|carries|stock|stocked|have|having|sell|sells|offer|"
                     r"looking\s+for|searching\s+for|in\s+stock|available)\b", ql):
            return _plan("exists", names[:1])
        return _plan("lookup", names[:1])
    except Exception:
        return None


def extract_plan(q: str, llm=None, model_override=None) -> dict | None:
    """One structured LLM call → normalized plan, or None on any failure.
    `llm` must be a LangChain-style object with .invoke([...]); when omitted we
    pull a fresh key-rotated LLM. `model_override` (or the ROUTER_MODEL env var)
    swaps the Groq router model — the failing class is multilingual category
    understanding, so the router can run a stronger multilingual model (e.g.
    qwen/qwen3-32b) while synthesis stays on the fast default. Never raises."""
    try:
        q = (q or "").strip()
        if not q or len(q) > 600:
            return None
        msgs = [{"role": "system", "content": _SYS}]
        for ex_q, ex_a in _FEWSHOT:
            msgs.append({"role": "user", "content": ex_q})
            msgs.append({"role": "assistant", "content": json.dumps(ex_a, ensure_ascii=False)})
        msgs.append({"role": "user", "content": q})
        _mo = str(model_override or os.getenv("ROUTER_MODEL", "") or "").strip() or None
        # Try a couple of key-rotated LLMs so a single cooled-down provider doesn't
        # drop the call; caller still has heuristic_plan as a zero-LLM backstop.
        attempts = [llm] if llm is not None else []
        if not attempts:
            from services.llm_keys import get_fresh_llm
            for _ in range(2):
                got = get_fresh_llm(model_override=_mo)
                if got is None:
                    break
                attempts.append(got)
        for cand in attempts:
            try:
                resp = cand.invoke(msgs)
                text = getattr(resp, "content", None) or (resp if isinstance(resp, str) else str(resp))
                plan = _coerce(_extract_json(text))
                if plan:
                    return plan
            except Exception:
                continue
        return None
    except Exception:
        return None
