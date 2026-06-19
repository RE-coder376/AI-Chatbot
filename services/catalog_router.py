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
    ' "category": string | null}\n\n'
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
      "in_stock": False, "category": None}),
    ('Which is the pricier one, "Ultra Male" or "Sultan Attar", and by how much?',
     {"kind": "compare", "products": ["Ultra Male", "Sultan Attar"], "compare_dir": "more",
      "order_dir": None, "price_min": None, "price_max": None, "in_stock": False, "category": None}),
    ('tally these up for me: "Pen A", "Pen B", "Pen C"',
     {"kind": "basket", "products": ["Pen A", "Pen B", "Pen C"], "compare_dir": None,
      "order_dir": None, "price_min": None, "price_max": None, "in_stock": False, "category": None}),
    ('how much for the "Diecast Kawasaki Ninja H2R"?',
     {"kind": "lookup", "products": ["Diecast Kawasaki Ninja H2R"], "compare_dir": None,
      "order_dir": None, "price_min": None, "price_max": None, "in_stock": False, "category": None}),
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
    return {
        "kind": kind,
        "products": products[:6],
        "compare_dir": cdir,
        "order_dir": odir,
        "price_min": _n(obj.get("price_min")),
        "price_max": _n(obj.get("price_max")),
        "in_stock": bool(obj.get("in_stock")),
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


def extract_plan(q: str, llm=None) -> dict | None:
    """One structured LLM call → normalized plan, or None on any failure.
    `llm` must be a LangChain-style object with .invoke([...]); when omitted we
    pull a fresh key-rotated LLM. Never raises."""
    try:
        q = (q or "").strip()
        if not q or len(q) > 600:
            return None
        if llm is None:
            from services.llm_keys import get_fresh_llm
            llm = get_fresh_llm()
        if llm is None:
            return None
        msgs = [{"role": "system", "content": _SYS}]
        for ex_q, ex_a in _FEWSHOT:
            msgs.append({"role": "user", "content": ex_q})
            msgs.append({"role": "assistant", "content": json.dumps(ex_a, ensure_ascii=False)})
        msgs.append({"role": "user", "content": q})
        resp = llm.invoke(msgs)
        text = getattr(resp, "content", None) or (resp if isinstance(resp, str) else str(resp))
        return _coerce(_extract_json(text))
    except Exception:
        return None
