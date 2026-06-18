"""Universal structured-catalog query engine.

A product catalog is a TABLE. "How many sharpeners", "cheapest gel pen",
"products under 500", "do you sell backpacks", "list kuromi items" are all the
SAME operation — filter rows, then aggregate — differing only in the aggregation
and the wording of the reply. So instead of a separate hand-written answerer per
question shape (the count/bounds/rank/absence pile this replaces), we parse the
question into an orthogonal (filter, aggregation) spec and execute it
deterministically against the product rows. New question shapes that combine the
same dimensions (e.g. "cheapest in-stock gel pen between 100 and 200") work for
free — no new code.

Per-tenant vocabulary (what counts as an accessory/refill that should not inflate
a count, e.g. "Stapler Pin", ink "Cartridge") stays in the DB config
(`count_exclude_terms`), never hardcoded here — the engine is domain-agnostic. A
term is only excluded when the user did not explicitly ask for it.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# Universal question scaffolding — these words are never product-category anchors.
_STOP = {
    "how", "many", "number", "count", "of", "do", "does", "did", "you", "your", "yours", "we",
    "our", "ours", "they", "them", "the", "a", "an", "store", "shop", "site", "sell", "sells",
    "sold", "selling", "stock", "stocked", "carry", "carries", "carried", "have", "has", "had",
    "offer", "offers", "offered", "got", "keep", "keeps", "any", "some", "are", "is", "was",
    "were", "there", "here", "available", "availability", "in", "on", "at", "for", "with", "and",
    "or", "to", "us", "me", "my", "mine", "it", "its", "this", "that", "these", "those", "all",
    "products", "product", "items", "item", "different", "types", "type", "kinds", "kind",
    "total", "catalog", "catalogue", "inventory", "range", "selection", "what", "whats", "which",
    "show", "list", "tell", "give", "display", "price", "prices", "priced", "pricing", "cost",
    "costs", "costing", "much", "cheap", "cheaper", "cheapest", "lowest", "least", "expensive",
    "priciest", "costliest", "dearest", "highest", "most", "affordable", "budget", "under",
    "below", "less", "than", "within", "max", "maximum", "minimum", "min", "over", "above",
    "more", "greater", "between", "up", "starting", "rs", "pkr", "rupees", "rupee", "dollar",
    "dollars", "currently", "entire", "whole", "absolute", "absolutely", "overall", "anything",
    "everything", "sale", "best", "top", "good", "quality", "options", "option", "can", "could",
    "would", "buy", "get", "from", "want", "need", "needed", "looking", "find", "please", "name",
}

_NUM = r"(?:rs\.?\s*|pkr\s*|[\$£€])?\s*([\d][\d,]*(?:\.\d{1,2})?)"


def _num(s: str):
    try:
        return float(str(s).replace(",", ""))
    except (TypeError, ValueError):
        return None


@dataclass
class Row:
    title: str
    price: float | None
    availability: str
    source: str
    hay: str  # lowercased title + categories + body head, for anchor matching
    currency: str = "Rs."  # detected per-row; engine output stays currency-agnostic
    cats: str = ""  # structured categories only (product_type + tags) for tag-precise counting


@dataclass
class Spec:
    agg: str  # count | exists | min | max | list | none
    anchors: list = field(default_factory=list)
    price_min: float | None = None
    price_max: float | None = None
    in_stock: bool = False
    structured: bool = True


def _variants(a: str) -> set[str]:
    """Singular<->plural so 'pens' matches 'pen'. (rc is handled in _hit.)"""
    vs = {a}
    if a.endswith("s") and len(a) > 3:
        vs.add(a[:-1])
    elif len(a) > 3:
        vs.add(a + "s")
    return vs


def _hit(row: Row, anchors: list[str]) -> bool:
    """ALL anchors must appear in the row (a 'gel pen' query needs both words)."""
    if not anchors:
        return True
    h = row.hay
    for a in anchors:
        if a == "rc":
            if re.search(r"\brc\b", h) or "remote control" in h or (
                re.search(r"\bremote\b", h) and re.search(r"\bcontrol", h)):
                continue
            return False
        if not any(re.search(rf"\b{re.escape(v)}\b", h) for v in _variants(a)):
            return False
    return True


def _cats_hit(row: Row, anchors: list[str]) -> bool:
    """Like _hit but against the STRUCTURED categories field only (product_type +
    tags), never the title or body. A count of 'claws' should be the products
    tagged as claws — not every title that happens to contain the substring."""
    c = row.cats
    if not anchors or not c:
        return False
    for a in anchors:
        if a == "rc":
            if re.search(r"\brc\b", c) or "remote control" in c:
                continue
            return False
        if not any(re.search(rf"\b{re.escape(v)}\b", c) for v in _variants(a)):
            return False
    return True


def _anchor_set(anchors: list[str]) -> set[str]:
    s: set[str] = set()
    for a in anchors:
        s |= _variants(a)
    return s


def _title_cover(title: str, anchor_set: set[str]) -> float:
    """How much of THIS product's title the query names. Catches a query that adds
    descriptor words beyond the stored (canonical) title — e.g. asking for
    'Pop N Play - Quick Push Pop Game' when the row title is just 'Pop N Play'.
    The strict all-anchor _hit fails there (extra words absent from the title);
    title-coverage resolves it by measuring the other direction."""
    tt = [t for t in re.findall(r"[a-z0-9]+", (title or "").lower()) if len(t) > 1]
    if len(tt) < 2:
        return 0.0
    return sum(1 for t in tt if t in anchor_set) / len(tt)


def parse(q: str) -> Spec:
    """Extract orthogonal dimensions (aggregation + price bounds + stock + anchors)
    from the question. Each dimension is detected independently, so any
    combination composes — that is what makes the engine universal."""
    ql = (q or "").lower()

    pmin = pmax = None
    m = re.search(r"between\s+" + _NUM + r"\s*(?:and|to|-|–)\s*" + _NUM, ql)
    if m:
        a, b = _num(m.group(1)), _num(m.group(2))
        if a is not None and b is not None:
            pmin, pmax = min(a, b), max(a, b)
    if pmax is None:
        m = re.search(r"(?:under|below|less\s+than|within|max(?:imum)?|budget(?:\s+of)?|up\s+to|cheaper\s+than|no\s+more\s+than)\s+" + _NUM, ql)
        if m:
            pmax = _num(m.group(1))
    if pmin is None:
        m = re.search(r"(?:over|above|more\s+than|greater\s+than|at\s+least|starting\s+(?:at|from)|priced\s+from)\s+" + _NUM, ql)
        if m:
            pmin = _num(m.group(1))

    in_stock = bool(re.search(r"\b(in[\s-]?stock|in\s+stock|available)\b", ql)) and not re.search(r"\bout\s+of\s+stock\b", ql)

    count_q = bool(re.search(r"\b(how\s+many|number\s+of|count\s+of|count)\b", ql))
    exist_q = bool(re.search(
        r"\bdo(?:es)?\s+(?:you|we|they|the\s+store)\b.*\b(?:sell|stock|carry|carries|have|has|offer|got|keep)\b"
        r"|\b(?:are|is)\s+there\s+(?:any|some)\b"
        r"|\bdo\s+you\s+(?:sell|stock|carry|have|offer|got)\b", ql))
    cheapest = bool(re.search(r"\b(cheapest|lowest[\s-]+priced?|least\s+expensive|most\s+affordable|lowest\s+cost)\b", ql))
    priciest = bool(re.search(r"\b(most\s+expensive|highest[\s-]+priced?|priciest|costliest|most\s+costly|dearest|highest\s+cost)\b", ql))
    list_q = bool(re.search(r"\b(show|list|which|what'?s?|give|display|products?|items?|sell|have|available)\b", ql))
    # Single-product price/availability lookup ("price of X", "how much is X",
    # "is X available"). Routed to the list path so the full-catalog scan (with the
    # title-coverage fallback) resolves the EXACT named product instead of the
    # similarity top-N returning same-prefix siblings. Bare price queries with no
    # product fall through the no-anchor guard below to ordinary RAG.
    priceof_q = bool(re.search(r"\b(price|prices|priced|pricing|cost|costs|costing|how\s+much|rate|rates)\b", ql))

    if count_q:
        agg = "count"
    elif cheapest:
        agg = "min"
    elif priciest:
        agg = "max"
    elif pmax is not None or pmin is not None:
        # A price bound makes it a bounds/list query even when phrased "do you
        # have any X under 50" — the incidental "do you have" must not route it to
        # the (subject-less) existence branch.
        agg = "list"
    elif exist_q:
        agg = "exists"
    elif list_q:
        agg = "list"
    elif priceof_q:
        agg = "list"
    else:
        return Spec("none", structured=False)

    anchors = [w for w in re.findall(r"[a-zA-Z]{2,}", ql) if w not in _STOP][:6]

    # A bare "list / what products do you sell" with no category and no price
    # bound is an open-ended browse — leave that to normal RAG, don't dump a
    # price-sorted slice of the whole catalog.
    if agg == "list" and not anchors and pmin is None and pmax is None:
        return Spec("none", structured=False)
    # Likewise a bare existence question with no subject is not answerable here.
    if agg == "exists" and not anchors:
        return Spec("none", structured=False)

    return Spec(agg, anchors, pmin, pmax, in_stock)


def load_rows(db, limit: int = 12000) -> list[Row]:
    """One full scan of the product rows — deduped by normalized title. Prices come
    from structured metadata or a labeled 'Price:' line only (never a body-text
    scan, which manufactures phantom prices from model numbers)."""
    try:
        total = min(int(db._collection.count() or 0), limit)
        raw = db._collection.get(limit=total, include=["documents", "metadatas"])
    except Exception:
        return []
    rows: list[Row] = []
    seen: set[str] = set()
    for doc, meta in zip(raw.get("documents", []) or [], raw.get("metadatas", []) or []):
        meta = meta or {}
        doc = str(doc or "")
        if not doc:
            continue
        src = str(meta.get("source") or meta.get("source_canonical") or "")
        src_l = src.lower()
        kind = str(meta.get("chunk_kind") or "").lower()
        ctype = str(meta.get("content_type") or "").lower()
        # Structured-shape signal: a positive `price` meta + a product title carrier
        # (name/title meta or a "Product:" line). Catches per-product chunks tagged
        # with a category/listing URL and no chunk_kind (webscraper.io-style ingests)
        # that the URL/kind tests miss. Listing/nav chunks lack a single price meta.
        try:
            _has_price_meta = meta.get("price") is not None and float(meta.get("price")) > 0
        except (TypeError, ValueError):
            _has_price_meta = False
        _has_title_meta = any(str(meta.get(k) or "").strip() for k in ("name", "product_title", "canonical_product_title"))
        _has_prod_line = bool(re.search(r"(?im)^product:\s*\S", doc))
        is_prod = (
            bool(re.search(r"/(?:products?|items?)(?:/|$|#)", src_l))
            or (kind == "product" and ctype == "product" and "/collections/" not in src_l)
            or (_has_price_meta and (_has_title_meta or _has_prod_line) and "/collections/" not in src_l)
        )
        if not is_prod:
            continue
        title = ""
        # `name` (webscraper.io-style) and a leading "Product:" doc line are the
        # universal title carriers when a crawl/ingest didn't set *_product_title.
        for k in ("canonical_product_title", "product_title", "page_title", "name"):
            t = str(meta.get(k) or "").strip()
            if t:
                title = re.sub(r"\s+", " ", t).strip(" -:|")
                break
        if not title:
            m = re.search(r"(?im)^product:\s*([^\n]{3,120})", doc)
            if m:
                title = re.sub(r"\s+", " ", m.group(1)).strip(" -:|")
        if not title or len(title) < 4:
            continue
        tl = title.lower()
        if tl in {"fun", "home", "shop", "catalog", "catalogue", "products", "collection", "page"}:
            continue
        if len(re.findall(r"[a-zA-Z0-9]+", title)) <= 1 and not re.search(r"\d", title):
            continue
        key = re.sub(r"[^a-z0-9]+", " ", tl).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        price = None
        try:
            mv = meta.get("price")
            if mv is not None and float(mv) > 0:
                price = float(mv)
        except (TypeError, ValueError):
            pass
        if price is None:
            m = re.search(r"(?im)^price:\s*(?:rs\.?|pkr|[\$£€])?\s*([\d,]+(?:\.\d{1,2})?)", doc)
            if m:
                v = _num(m.group(1))
                price = v if (v and v > 0) else None
        # Currency follows the data, not a hardcoded "Rs." — read the symbol off the
        # labeled Price: line (then any body symbol) so $/£/€ stores render correctly.
        cur = "Rs."
        cm = re.search(r"(?im)^price:\s*(Rs\.?|PKR|[\$£€])", doc) or re.search(r"([\$£€])\s*[\d]", doc)
        if cm:
            sym = cm.group(1)
            cur = sym if sym in ("$", "£", "€") else "Rs."
        avail = str(meta.get("availability") or "").strip().lower()
        if not avail:
            avail = "out of stock" if re.search(r"(?i)\b(out of stock|sold out|currently unavailable)\b", doc) else "available"
        # Membership matches title + curated categories only — NOT the body
        # description. A prose mention ("pairs well with our sharpener") must not
        # count an unrelated product into the "sharpener" category.
        cats_l = str(meta.get("categories") or "").lower()
        hay = " ".join([tl, cats_l])
        rows.append(Row(title, price, avail, src, hay, cur, cats_l))
    return rows


def _excl_re(cfg: dict | None, anchors: list[str]):
    aset = set(anchors) | {a.rstrip("s") for a in anchors}
    terms = [str(t).lower().strip() for t in ((cfg or {}).get("count_exclude_terms") or [])
             if str(t).lower().strip() and str(t).lower().strip().rstrip("s") not in aset]
    if not terms:
        return None
    return re.compile(r"\b(?:" + "|".join(re.escape(t) for t in terms) + r")\b", re.I)


def _dedup(xs):
    return list(dict.fromkeys(x for x in xs if x))


def _price_s(p: float, cur: str = "Rs.") -> str:
    body = f"{p:,.0f}" if float(p).is_integer() else f"{p:,.2f}"
    return f"{cur}{body}"


# DB-type gate thresholds — a product catalog is a corpus where product rows are a
# MEANINGFUL SHARE of the chunks, not a handful of accidental matches inside a docs
# corpus. A docs/RAG DB (e.g. agentfactory: 7.5k doc chunks) must never trip the
# catalog engine just because a lesson contains a "$9" example or a chapter-ingest
# mislabeled sections as products.
_MIN_CATALOG_ROWS = 5        # absolute floor of distinct product rows
_MIN_CATALOG_FRAC = 0.10     # product rows must be >=10% of all chunks
_DOCS_DBTYPES = {"docs", "documentation", "document", "text", "knowledge", "kb",
                 "rag", "article", "articles", "blog", "wiki", "manual", "guide"}
_PRODUCT_DBTYPES = {"product", "products", "catalog", "catalogue", "store", "shop",
                    "ecommerce", "e-commerce", "retail", "inventory"}


def is_catalog_db(db, rows: list, cfg: dict | None = None) -> bool:
    """Is this DB a product catalog (a TABLE of priced items) or a text/docs corpus?

    The catalog engine only owns the former. Order of authority: explicit config
    flag → docs/product type keyword → docs_path_hints (a docs-site marker) →
    corpus-share heuristic. Returning False sends the question to ordinary RAG/LLM.
    """
    cfg = cfg or {}
    flag = cfg.get("is_product_db")
    if flag is None:
        flag = cfg.get("product_db")
    if flag is True:
        return True
    if flag is False:
        return False
    db_type = str(cfg.get("db_type") or cfg.get("mode") or cfg.get("catalog_mode") or "").lower().strip()
    if db_type in _DOCS_DBTYPES:
        return False
    if db_type in _PRODUCT_DBTYPES:
        return True
    # A configured docs-site (per-language doc paths) is a documentation corpus.
    if cfg.get("docs_path_hints"):
        return False
    # Heuristic: product rows must be both numerous AND a real share of the corpus.
    n = len(rows)
    if n < _MIN_CATALOG_ROWS:
        return False
    try:
        total = int(db._collection.count() or 0)
    except Exception:
        total = 0
    if total > 0 and (n / total) < _MIN_CATALOG_FRAC:
        return False
    return True


def answer_catalog_query(q: str, db, cfg: dict | None = None, max_list: int = 12) -> tuple[str | None, list[str]]:
    """Single front-door for structured catalog questions. Returns (text, sources),
    or (None, []) when the question is not a structured-catalog question (caller
    then falls back to ordinary retrieval/LLM)."""
    try:
        if not q or db is None:
            return None, []
        spec = parse(q)
        if not spec.structured:
            return None, []
        rows = load_rows(db)
        if not rows:
            return None, []
        # DB-type gate: only a real product catalog gets the structured engine; a
        # text/docs corpus (with stray priced rows) falls through to RAG/LLM.
        if not is_catalog_db(db, rows, cfg):
            return None, []
        excl = _excl_re(cfg, spec.anchors)

        _base = [r for r in rows if _hit(r, spec.anchors)]
        if not _base and spec.anchors:
            # Strict all-anchor match found nothing — fall back to title-coverage so
            # a fully-named product still resolves. Only triggers when strict yields
            # 0, so a non-empty count can never be inflated.
            _aset = _anchor_set(spec.anchors)
            _cov = [(_title_cover(r.title, _aset), r) for r in rows]
            _base = [r for c, r in sorted(_cov, key=lambda x: -x[0]) if c >= 0.7]
        sel = []
        for r in _base:
            if excl and excl.search(r.title):
                continue
            # Only exclude clearly out-of-stock rows. Availability strings vary by
            # source ("available", "in stock", ""), so treat anything not explicitly
            # out-of-stock as in stock rather than demanding an exact "available".
            if spec.in_stock and re.search(r"out\s+of\s+stock|sold\s+out|unavailable|not\s+available", r.availability):
                continue
            sel.append(r)
        # Price bounds apply to every aggregation when present (count of X under Y,
        # cheapest X over Y, …) and require a known price.
        if spec.price_min is not None or spec.price_max is not None:
            sel = [r for r in sel if r.price is not None
                   and (spec.price_min is None or r.price >= spec.price_min)
                   and (spec.price_max is None or r.price <= spec.price_max)]

        label = " ".join(spec.anchors) if spec.anchors else "products"

        if spec.agg in ("count", "exists"):
            # Count precision: in a tag-bearing catalog, count only rows whose
            # STRUCTURED categories match the anchor (GT/attribute semantics).
            # A free title-substring match over-counts ("claw" leaks into unrelated
            # titles). Falls back to the title/hay match when the catalog has no
            # tags or no row is tagged with the anchor (untagged/HTML-crawl DBs).
            if spec.agg == "count" and spec.anchors:
                _tagged = sum(1 for r in rows if r.cats.strip())
                if rows and (_tagged / len(rows)) >= 0.5:
                    _tag_sel = [r for r in sel if _cats_hit(r, spec.anchors)]
                    if _tag_sel:
                        sel = _tag_sel
            n = len(sel)
            if n == 0:
                if spec.agg == "exists":
                    return (f"No — we don't currently carry any {label}. I couldn't find any "
                            f"in our catalog. Is there something else I can help you find?"), []
                return (f"No — we don't currently carry any {label}. Is there something else "
                        f"I can help you find?"), []
            # Show the price alongside each item when known — answers the common
            # "do you have X and its price" multi-intent in one shot.
            def _il(r):
                return f"- {r.title} — {_price_s(r.price, r.currency)}" if r.price is not None else f"- {r.title}"
            body = "\n".join(_il(r) for r in sel[:max_list])
            more = f"\n…and {n - max_list} more." if n > max_list else ""
            if spec.agg == "count":
                head = (f"We have {n} {label} product{'s' if n != 1 else ''} in our catalog:"
                        if spec.anchors else f"We have {n} products in our catalog:")
            else:
                head = f"Yes — we carry {label}. We have {n} option{'s' if n != 1 else ''}:"
            return f"{head}\n{body}{more}", _dedup(r.source for r in sel)[:max_list]

        # min / max / list — all need a price.
        priced = [r for r in sel if r.price is not None]
        if not priced:
            if spec.agg == "list" and sel:
                body = "\n".join(f"- {r.title}" for r in sel[:max_list])
                return f"Here are matching products from the catalog:\n{body}", _dedup(r.source for r in sel)[:max_list]
            return None, []
        priced.sort(key=lambda r: (-r.price if spec.agg == "max" else r.price, r.title.lower()))
        n_show = max_list if spec.agg == "list" else 5
        items = priced[:n_show]
        lines = ["Here are matching products from the catalog:"]
        for i, r in enumerate(items, 1):
            lines.append(f"{i}. {r.title} - {_price_s(r.price, r.currency)} - {r.availability}")
        return "\n".join(lines), _dedup(r.source for r in items)[:max_list]
    except Exception:
        return None, []
