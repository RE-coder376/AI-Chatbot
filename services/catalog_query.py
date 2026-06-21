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
    # price/value filler — never product-name anchors (else "what's the going RATE
    # for X" makes going/rate junk anchors that crowd out X's distinctive words)
    "going", "rate", "rates", "run", "runs", "damage", "charge", "charges", "charged",
    "worth", "value", "asking", "ask", "pay", "paying", "spend", "deal", "going-rate",
    # query scaffolding for category / name-substring / list filters — never anchors
    # ("most expensive in the Models CATEGORY", "products WHOSE name CONTAINS scale",
    # "include EVERY EXACT price"). Without these the engine matches rows against
    # 'category'/'contains'/'exact' and finds nothing → false "no such category".
    "category", "categories", "categorized", "categorised", "section", "department",
    "whose", "contains", "contain", "containing", "include", "includes", "including",
    "included", "exact", "exactly", "every", "each", "named", "calling", "called",
    "answer", "yes", "no",  # "...carry X? answer yes or no" — keep X, drop the rest
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
    strict_min: bool = False  # "strictly above/over X" → exclude p == X
    strict_max: bool = False  # "strictly below/under X" → exclude p == X
    title_only: bool = False  # "whose product NAME contains X" → match title, not body


def _variants(a: str) -> set[str]:
    """Singular<->plural so 'pens'~'pen' and 'battery'~'batteries'. Covers the
    -y/-ies and -s/-es forms (the bare -s rule missed 'batteries', 'boxes').
    (rc is handled in _hit.)"""
    vs = {a}
    if len(a) <= 3:
        return vs
    if a.endswith("ies"):
        vs.add(a[:-3] + "y")          # batteries -> battery
    elif a.endswith("y"):
        vs.add(a[:-1] + "ies")        # battery -> batteries
    if a.endswith("es") and len(a) > 4:
        vs.add(a[:-2])                # boxes -> box
    if a.endswith("s"):
        vs.add(a[:-1])                # pens -> pen
    else:
        vs.add(a + "s")               # pen -> pens
        vs.add(a + "es")              # box -> boxes
    return vs


def _hit(row: Row, anchors: list[str], title_only: bool = False) -> bool:
    """ALL anchors must appear in the row (a 'gel pen' query needs both words).
    title_only restricts the match to the product title — a "whose name contains
    X" query must not match a product whose body/description merely mentions X
    (e.g. a swim ring whose description cross-sells a 'vest')."""
    if not anchors:
        return True
    h = row.title.lower() if title_only else row.hay
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

    # Comparisons ("compare A and B", "X vs Y") are NOT a single structured
    # aggregation — they need the retrieval fan-out (which matches each named
    # product in both name directions) + LLM synthesis. Hand them off; otherwise a
    # "compare the price of A and B" would be caught by the price-of list path and
    # return whichever product's title the query happens to fully name.
    if (re.search(r"\b(compare|compared|comparison|versus|vs\.?|difference\s+between)\b", ql)
            and re.search(r"\b(and|or|vs\.?|versus|than)\b", ql)):
        return Spec("none", structured=False)

    # Advisory / recommendation intent ("which toy is best for a 2 year old",
    # "recommend a gift for a toddler", "what's good for sensitive skin") is a
    # SUBJECTIVE judgement, not a table filter — let RAG/LLM answer. Without this
    # the structured path fires on incidental tokens ("2 year" matched a mic's
    # "2 Year warranty") and dumps an irrelevant product as a confident answer.
    # Excludes the structured superlatives (cheapest/most expensive) — those have
    # their own price words and never carry these advisory cues.
    if re.search(
        r"\b(recommend\w*|suggest\w*|advise|advice)\b"
        # "best/good/ideal … for <recipient/use>" — recipient list avoids catching
        # the structured "best PRICE for this pen" (→ stays a price lookup).
        r"|\b(?:best|good|ideal|suitable|appropriate|perfect|great|right)\s+(?:\w+\s+){0,2}?for\s+"
        r"(?:a\b|an\b|my\b|your\b|kids?|children|child|toddler|baby|babies|boys?|girls?|teens?|"
        r"men|women|him|her|sensitive|oily|dry|daily|everyday|beginners?|gift|travel|school|office)"
        r"|\bgift\s+for\b|\bfor\s+(?:a|an|my)\s+\d+[\s-]*(?:year|yr|month|mo)s?[\s-]*old\b"
        r"|\bwhich\s+(?:one\s+)?(?:should|would|do)\s+(?:i|you)\b", ql):
        return Spec("none", structured=False)

    # Shipping / delivery / returns / payment / order questions are POLICY prose,
    # not product-catalog lookups — but "how much is delivery" trips the price-of
    # detector and "shipping charges" trips the list path, dumping a random product.
    # Hand to normal RAG so it answers from the policy page (or says it doesn't have
    # the info) rather than returning an unrelated product.
    # NB: "return" alone is excluded — the catalog verb ("return every available
    # item under 500") must not read as a returns-policy question; only plural
    # "returns", "return policy", or "return an item/order" count as policy.
    if re.search(r"\b(shipping|delivery|deliver(?:ed|ing)?|postage|courier|dispatch|"
                 r"returns\b|return\s+polic(?:y|ies)|return\s+(?:an?\s+)?(?:item|product|order|purchase)s?\b|"
                 r"refunds?|warranty|guarantee|"
                 r"payment\s+method|cod|cash\s+on\s+delivery|installments?|"
                 r"track(?:ing)?\s+(?:my\s+)?order|order\s+status|cancel(?:lation|\s+(?:my\s+)?order)?)\b", ql):
        return Spec("none", structured=False)

    # "which/what brands do you carry", "list the categories" = ENUMERATION of
    # brands/categories, not a product lookup — "do you carry" trips the existence
    # branch and would dump products as if they were brands. Hand to RAG so it
    # answers from the store's brands/category page.
    if re.search(r"\b(which|what|list|name|tell\s+me\s+(?:the|your))\b[\w\s]*\b"
                 r"(brands?|designers?|makes?|labels?|categor(?:y|ies)|collections?)\b", ql):
        return Spec("none", structured=False)

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

    # Exclusive bounds: "strictly below/under" and "more/greater than", "below",
    # "under", "above", "over" exclude the boundary value. Inclusive operators
    # ("up to", "no more than", "at most", "at least", "within", "max") keep it.
    _strict = bool(re.search(r"\bstrict(?:ly)?\b", ql))
    strict_max = pmax is not None and (_strict or bool(
        re.search(r"\b(below|under|less\s+than|cheaper\s+than|fewer\s+than)\b", ql)))
    strict_min = pmin is not None and (_strict or bool(
        re.search(r"\b(above|over|more\s+than|greater\s+than)\b", ql)))

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

    # Anchors include digit tokens (a model/size/style number disambiguates a
    # specific SKU — "Spiral Notebook Style 43" must not match every style, "Paint
    # 500ml" / "Set of 48" pin the variant). Drop numbers already consumed as price
    # bounds so "products under 500" doesn't anchor on 500.
    _price_tokens = set()
    for _v in (pmin, pmax):
        if _v is not None:
            _price_tokens.add(f"{_v:.0f}")
            _price_tokens.add(str(_v))
            # Thousands-separated prices ("Rs.3,025") tokenize to a digit fragment
            # ("025") that survived the bare integer filter and leaked as an anchor.
            for _part in f"{_v:,.0f}".split(","):
                _price_tokens.add(_part)
    # A quoted span is the reliable discriminator: the user (and the hard quiz)
    # quote the exact category / name-substring being asked about ("Ball Point"
    # category, name contains "fresh"). Anchor on the quoted content so question
    # scaffolding ("Using the full current catalog, count all items in …") can
    # never leak into the filter — the stop-word treadmill never catches every
    # phrasing ("using"/"full"/"current"/"across"/"enumerate"/…). Inside explicit
    # quotes we trust the user and skip _STOP (drop only price-bound fragments).
    # No quotes → fall back to stop-word subtraction over the whole question.
    _quoted = re.findall(r'["“”]([^"“”]{1,80})["“”]', q or "")
    if _quoted:
        _qtext = " ".join(_quoted).lower()
        anchors = [w for w in re.findall(r"[a-z0-9]{2,}", _qtext)
                   if w not in _price_tokens][:8]
    else:
        anchors = [w for w in re.findall(r"[a-z0-9]{2,}", ql)
                   if w not in _STOP and w not in _price_tokens][:8]

    # A bare "list / what products do you sell" with no category and no price
    # bound is an open-ended browse — leave that to normal RAG, don't dump a
    # price-sorted slice of the whole catalog.
    if agg == "list" and not anchors and pmin is None and pmax is None:
        return Spec("none", structured=False)
    # Likewise a bare existence question with no subject is not answerable here.
    if agg == "exists" and not anchors:
        return Spec("none", structured=False)

    # "whose product name contains/includes X", "named X", "titled X" → the user
    # is FILTERING on the title, so don't let a body mention of X pull in an
    # unrelated product. Must require a filter verb after "name": the bare phrase
    # "include each complete product name and exact price" is an OUTPUT instruction
    # (it appears in every list template, including category queries) and must NOT
    # flip a category query to title matching. (Plain category queries keep body
    # matching + category-tag precision.)
    title_only = bool(re.search(
        r"\b(?:whose\s+)?(?:product\s+)?name\s+(?:contain\w*|includ\w*|has\b|having\b|with\b|matching\b|"
        r"that\s+(?:contain\w*|includ\w*|has\b))"
        r"|\bnamed\b|\btitled?\b|\bcalled\b", ql))

    return Spec(agg, anchors, pmin, pmax, in_stock,
                strict_min=strict_min, strict_max=strict_max, title_only=title_only)


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
    seen: dict[str, Row] = {}
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
        if not key:
            continue
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
        row = Row(title, price, avail, src, hay, cur, cats_l)
        # Dedup by normalized title, but PREFER the priced chunk: a catalog that was
        # both crawled and catalog-ingested has two chunks per product (a crawl chunk
        # often without a structured price + a priced ingest chunk). Keeping whichever
        # came first dropped the price for products whose unpriced chunk sorted first
        # (then compare/basket couldn't use them). Upgrade in place when a later
        # duplicate carries a price the kept one lacks; also fill missing categories.
        prev = seen.get(key)
        if prev is None:
            seen[key] = row
            rows.append(row)
        elif prev.price is None and price is not None:
            prev.price = price
            prev.currency = cur
            if avail and not re.search(r"out\s+of\s+stock|sold\s+out", prev.availability):
                prev.availability = avail
            if not prev.cats and cats_l:
                prev.cats = cats_l
                prev.hay = " ".join([prev.title.lower(), cats_l])
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


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


_NAME_BOUNDARY = re.compile(r'["“”]\s*(?:and|with|or|versus|vs\.?|plus|,)\s*["“”]', re.I)


def _extract_names(q: str) -> list[str]:
    """Pull the individual product names a multi-product question references.
    Quoted names are the reliable signal (the hard quiz quotes each product, and
    real users naming a specific product tend to quote/capitalise it). Falls back
    to splitting on connectors only when there are no quotes."""
    # A product name can itself contain quotes (inch marks: a pool sized
    # 9'8"X6'3"), which defeats naive "([^"]+)" pairing and mis-splits the names
    # → a basket resolves the wrong product. Split instead on the QUOTE-delimited
    # connector BETWEEN names ('" and "', '", "'): inner inch-quotes never form
    # that boundary, so each name survives intact.
    if _NAME_BOUNDARY.search(q):
        tmp = _NAME_BOUNDARY.sub("\x00", q)
        m = re.search(r'["“”](.+)["“”]', tmp, re.S)  # first quote … last quote
        if m:
            names = [n.strip(" .\"'“”") for n in m.group(1).split("\x00")]
            names = [n for n in names if len(n) >= 3]
            if len(names) >= 2:
                return names[:5]
    names = [n.strip() for n in re.findall(r'[\"“”]([^\"“”]{3,})[\"“”]', q) if n.strip()]
    if len(names) >= 2:
        return names[:5]
    seg = q.split(":", 1)[1] if ":" in q else q
    parts = re.split(r"\s+(?:versus|vs\.?|and|or)\s+|,\s*", seg)
    parts = [re.sub(r"[?.\"]+$", "", p).strip(" .\"") for p in parts]
    parts = [p for p in parts if len(p) >= 4 and len(p.split()) >= 2]
    return parts[:5] if len(parts) >= 2 else names


_CMP_RE = re.compile(
    r"\bcheaper\b|\bpricier\b|\bdearer\b|\bmore\s+expensive\b|\bless\s+expensive\b"
    r"|\bdifference\s+between\b|\bby\s+how\s+much\b|\bhow\s+much\s+(?:more|less|cheaper)\b"
    r"|\bwhich\s+(?:one\s+)?(?:is|costs?)\b|\bversus\b|\bvs\.?\b|\bcosts?\s+(?:less|more)\b", re.I)
_ORDER_RE = re.compile(r"\b(order|rank|sort|arrange|list\s+these)\b", re.I)
_PRICE_DIM_RE = re.compile(
    r"\b(expensive|cheap(?:er|est)?|price[ds]?|cost(?:s|ly)?|least|most|"
    r"low(?:est)?\s+to\s+high(?:est)?|high(?:est)?\s+to\s+low(?:est)?|"
    r"ascending|descending|increasing|decreasing)\b", re.I)
# Basket = "buy/total/combined" of several named items. Broad on purpose: customers
# phrase it many ways ("total cost of buying X and Y", "buys one X plus one Y, what
# combined amount should they pay", "X together with Y"). Compare/order are detected
# first and win, so these verbs can't hijack a comparison/ranking. NOT 'cost' alone
# (that word appears in "which costs more" comparisons).
_BASKET_RE = re.compile(
    r"\b(total|sum|altogether|combined|basket|plus|together|spend|spends|owe|"
    r"purchas\w*|buy|buys|buying|bought|pay|pays|paying)\b", re.I)
# Ascending = cheapest-first. Catch every natural phrasing: a cheap/low pole word
# appearing before an expensive/high pole word across a "to" ("cheapest to most
# expensive", "low to high", "least to most expensive"), plus the explicit single
# forms. "most expensive to cheapest" can't match (poles reversed) → descending.
_ASC_RE = re.compile(
    r"\b(?:cheap(?:est)?|low(?:est)?|least|lower)\b[^.?!]*?\bto\b[^.?!]*?"
    r"\b(?:most|expensive|high(?:est)?|pric\w*|dear\w*|cost\w*)"
    r"|\b(?:cheap(?:est)?|low(?:est)?)\s+first\b"
    r"|\bleast\s+to\s+most\b|\blow(?:est)?\s+to\s+high(?:est)?\b"
    r"|\b(?:ascending|increasing)\b", re.I)
# Descending = priciest-first, stated explicitly. Used to disambiguate the order
# op when no ascending cue is present (default stays descending either way).
_DESC_RE = re.compile(
    r"\b(?:most\s+expensive|priciest|dearest|highest|expensive)\b[^.?!]*?\bto\b[^.?!]*?"
    r"\b(?:cheap\w*|low\w*|least)"
    r"|\b(?:most\s+expensive|priciest|dearest|highest)\s+first\b"
    r"|\bhigh(?:est)?\s+to\s+low(?:est)?\b|\b(?:descending|decreasing)\b", re.I)


def _parse_multi(q: str) -> dict | None:
    """Detect a multi-product reasoning question (compare / order / basket) and the
    product names it spans. Returns None for everything else."""
    ql = (q or "").lower()
    is_order = bool(_ORDER_RE.search(ql) and _PRICE_DIM_RE.search(ql))
    is_cmp = bool(_CMP_RE.search(ql))
    is_basket = bool(_BASKET_RE.search(ql))
    if not (is_order or is_cmp or is_basket):
        return None
    names = _extract_names(q)
    if len(names) < 2:
        return None
    # order and compare have distinct, specific verbs; basket is the remaining
    # multi-product intent. Compare wins over basket so "which costs more" is never
    # summed.
    op = "order" if is_order else ("compare" if is_cmp else "basket")
    # Compare direction: "which costs more / more expensive" names the dearer item;
    # default ("cheaper / costs less") names the cheaper. Either way both prices and
    # the exact gap are stated, so the answer carries the fact regardless.
    cmp_more = bool(re.search(r"\bmore\s+expensive\b|\bcosts?\s+more\b|\bpricier\b|\bdearer\b|\bhigher\s+price", ql))
    return {"op": op, "names": names, "asc": bool(_ASC_RE.search(ql)), "cmp_more": cmp_more}


def _raw(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _tie_break_key(r: Row, raw_name: str):
    """Deterministic ranking among rows that resolve EQUALLY well to a name, so a
    duplicate/near-duplicate listing never gets picked at random. Prefer the row
    whose raw title (punctuation+case) matches the query — e.g. a query naming
    'Pool ( 34" X 10" )' picks that exact listing over the 'Pool 34" x 10"' twin —
    then an in-stock row, then the closest title length, then a stable URL."""
    raw = _raw(r.title)
    in_stock = 1 if re.search(r"out\s+of\s+stock|sold\s+out|unavailable|not\s+available", r.availability or "") else 0
    return (0 if raw == _raw(raw_name) else 1, in_stock, abs(len(raw) - len(_raw(raw_name))), r.source or "")


def _resolve_detail(name: str, rows: list[Row]) -> tuple[Row | None, list[Row]]:
    """Resolve ONE named product. Returns (chosen_row, equally-good candidates).
    Exact normalized-title matches win; otherwise a bidirectional coverage match
    with a high floor so same-category siblings are rejected. When several rows
    tie, _tie_break_key chooses deterministically and `candidates` exposes the
    tie so the caller can clarify a genuine ambiguity (identical listings)."""
    key = _norm(name)
    qtoks = [t for t in key.split() if len(t) > 1]
    if not qtoks:
        return None, []
    qset: set[str] = set()
    for t in qtoks:
        qset |= _variants(t)
    exact = [r for r in rows if _norm(r.title) == key]
    if exact:
        best = min(exact, key=lambda r: _tie_break_key(r, name))
        return best, exact
    best = None
    best_key = (-1.0, -1.0, -1e9)
    tied: list[Row] = []
    for r in rows:
        rtoks = [t for t in _norm(r.title).split() if len(t) > 1]
        if not rtoks:
            continue
        rset: set[str] = set()
        for t in rtoks:
            rset |= _variants(t)
        cover = sum(1 for t in rtoks if t in qset) / len(rtoks)    # stored title named by query
        qcover = sum(1 for t in qtoks if t in rset) / len(qtoks)   # query named by stored title
        if not (cover >= 0.85 or qcover >= 0.85) or (cover + qcover) / 2 < 0.5:
            continue
        sk = (max(cover, qcover), min(cover, qcover), -abs(len(rtoks) - len(qtoks)))
        if sk > best_key:
            best_key, best, tied = sk, r, [r]
        elif sk == best_key:
            tied.append(r)
    if best is not None and len(tied) > 1:
        best = min(tied, key=lambda r: _tie_break_key(r, name))
    return best, (tied if best is not None else [])


def _resolve_one(name: str, rows: list[Row]) -> Row | None:
    return _resolve_detail(name, rows)[0]


def _ambiguous_alts(name: str, rows: list[Row]) -> list[Row]:
    """Return the price-distinct candidates when a name resolves to TWO listings
    that the tie-break cannot separate (same rank key) — a genuine ambiguity the
    bot must surface rather than guess between (e.g. two identical product
    listings at different prices). Empty when resolution is unambiguous."""
    best, cands = _resolve_detail(name, rows)
    if best is None or len(cands) < 2:
        return []
    # Compare on the MEANINGFUL rank fields (raw title, stock, length), not the
    # URL tiebreaker — two identical listings differ only by URL, so including it
    # would make them look distinguishable and suppress the clarification.
    bk = _tie_break_key(best, name)[:-1]
    top = [c for c in cands if _tie_break_key(c, name)[:-1] == bk and c.price is not None]
    if len({c.price for c in top}) > 1:
        return top
    return []


def _answer_multi(mp: dict, rows: list[Row]) -> tuple[str, list[str]] | None:
    """Resolve every named product, then compute the comparison / ordering / total.
    Returns None (→ RAG) unless EVERY named product resolves to a priced row, so a
    partial resolution never produces a confidently-wrong half-answer."""
    found = []
    for name in mp["names"]:
        # Genuine ambiguity: the name matches two indistinguishable listings at
        # different prices. Don't guess one into a basket/comparison — surface the
        # choice so the answer is never confidently wrong.
        alts = _ambiguous_alts(name, rows)
        if len(alts) > 1:
            opts = "; ".join(f"{_price_s(a.price, a.currency)}" for a in sorted(alts, key=lambda r: r.price))
            return (f'There are {len(alts)} listings for "{name}" at different prices ({opts}). '
                    f"Which one did you mean?"), _dedup(a.source for a in alts)
        r = _resolve_one(name, rows)
        if r is None or r.price is None:
            return None
        found.append(r)
    # Dedup by product URL: a name containing embedded quotes (e.g. a pool sized
    # 9'8"X6'3") mis-splits into fragments that resolve to the SAME row, which
    # would otherwise double-count it in the basket total. Genuine multi-product
    # questions name distinct products → distinct sources, so this is lossless.
    _seen, _uniq = set(), []
    for r in found:
        k = r.source or r.title
        if k in _seen:
            continue
        _seen.add(k)
        _uniq.append(r)
    found = _uniq
    if len(found) < 2:
        return None
    cur = found[0].currency
    srcs = _dedup(r.source for r in found)
    op = mp["op"]
    if op == "basket":
        total = sum(r.price for r in found)
        lines = ["Based on our catalog:"]
        lines += [f"- {r.title}: {_price_s(r.price, r.currency)}" for r in found]
        lines.append(f"\nTotal cost: {_price_s(total, cur)}")
        return "\n".join(lines), srcs
    if op == "order":
        ordered = sorted(found, key=lambda r: (r.price if mp["asc"] else -r.price, r.title.lower()))
        head = "From least to most expensive:" if mp["asc"] else "From most to least expensive:"
        lines = [head] + [f"{i}. {r.title} — {_price_s(r.price, r.currency)}" for i, r in enumerate(ordered, 1)]
        return "\n".join(lines), _dedup(r.source for r in ordered)
    # compare: state both prices and the exact difference; name the product the
    # question asked about (dearer for "which costs more", else the cheaper).
    pair = sorted(found, key=lambda r: r.price)
    cheaper, dearer = pair[0], pair[-1]
    diff = dearer.price - cheaper.price
    verdict = (f"{dearer.title} is more expensive by {_price_s(diff, cur)}."
               if mp.get("cmp_more")
               else f"{cheaper.title} is cheaper by {_price_s(diff, cur)}.")
    txt = (f"- {cheaper.title}: {_price_s(cheaper.price, cheaper.currency)}\n"
           f"- {dearer.title}: {_price_s(dearer.price, dearer.currency)}\n\n{verdict}")
    return txt, srcs


# Coarse "is this plausibly a product question?" gate for the LLM net. Broad on
# purpose (a quoted span or any price/buy/compare signal); the LLM does the precise
# classification. Keeps the extra call off pure policy/greeting questions.
_PRODUCTISH = re.compile(
    r'["“”]|\b(price[ds]?|cost(?:s|ly)?|how\s+much|cheap(?:er|est)?|expensive|afford|'
    r'total|combined|altogether|buy|buys|buying|purchas\w*|pay|spend|stock|stocked|'
    r'carry|carries|available|availability|sell|sells|which|compare|versus|vs\.?|'
    r'than|set\s+of|pack\s+of|do\s+you\s+have|in\s+stock)\b', re.I)


def _anchors_from(text: str) -> list:
    return [w for w in re.findall(r"[a-z0-9]{2,}", (text or "").lower()) if w not in _STOP][:8]


def _plan_to_specs(plan: dict) -> tuple[dict | None, "Spec | None"]:
    """Map an LLM-extracted plan onto the engine's existing (mp | Spec) execution.
    Returns (mp, spec) with at most one non-None. None,None = not executable here."""
    kind = plan.get("kind")
    names = [n for n in (plan.get("products") or []) if n]
    if kind in ("compare", "order", "basket"):
        if len(names) < 2:
            return None, None
        return {"op": kind, "names": names[:6],
                "asc": plan.get("order_dir") == "asc",
                "cmp_more": plan.get("compare_dir") == "more"}, None
    pmin, pmax, in_stock = plan.get("price_min"), plan.get("price_max"), bool(plan.get("in_stock"))
    if kind == "count":
        return None, Spec("count", _anchors_from(plan.get("category") or (names[0] if names else "")), pmin, pmax, in_stock)
    if kind == "cheapest":
        return None, Spec("min", _anchors_from(plan.get("category") or ""), pmin, pmax, in_stock)
    if kind == "priciest":
        return None, Spec("max", _anchors_from(plan.get("category") or ""), pmin, pmax, in_stock)
    if kind == "exists":
        if not names:
            return None, None
        return None, Spec("exists", _anchors_from(names[0]), pmin, pmax, in_stock)
    if kind in ("lookup", "filter"):
        anch = _anchors_from(names[0] if names else (plan.get("category") or ""))
        if not anch and pmin is None and pmax is None:
            return None, None
        return None, Spec("list", anch, pmin, pmax, in_stock)
    return None, None


def answer_catalog_query(q: str, db, cfg: dict | None = None, max_list: int = 12) -> tuple[str | None, list[str]]:
    """Single front-door for structured catalog questions. Returns (text, sources),
    or (None, []) when the question is not a structured-catalog question (caller
    then falls back to ordinary retrieval/LLM).

    Deterministic regex parser runs first (exact, no LLM). If it declines AND the DB
    is a real catalog, ONE structured LLM extraction (the router) is tried as a net,
    so unusual phrasings still resolve — but execution stays fully deterministic, so
    a number is never invented; an unresolved entity falls through to RAG."""
    try:
        if not q or db is None:
            return None, []
        spec = parse(q)
        mp = _parse_multi(q)
        rows = None
        if not spec.structured and not mp:
            # Deterministic router declined. Fire the LLM extractor only when the
            # question is plausibly about products (coarse signal — the LLM still does
            # the precise classification), so policy/greeting questions don't pay for
            # an extra call.
            if not _PRODUCTISH.search(q):
                return None, []
            rows = load_rows(db)
            if not rows or not is_catalog_db(db, rows, cfg):
                return None, []
            try:
                from services.catalog_router import extract_plan, heuristic_plan
                # LLM extractor first; deterministic backstop if the LLM is unavailable
                # so structured product questions answer even during an LLM outage.
                plan = extract_plan(q) or heuristic_plan(q)
            except Exception:
                plan = None
            if not plan or plan.get("kind") in (None, "browse", "other"):
                return None, []
            mp, spec = _plan_to_specs(plan)
            if mp is None and (spec is None or not getattr(spec, "structured", False)):
                return None, []
        if rows is None:
            rows = load_rows(db)
            if not rows:
                return None, []
            # DB-type gate: only a real product catalog gets the structured engine; a
            # text/docs corpus (with stray priced rows) falls through to RAG/LLM.
            if not is_catalog_db(db, rows, cfg):
                return None, []
        # Multi-product reasoning (compare / order / basket): fan out to resolve each
        # named product, then compute. Owns the question outright — a correct answer
        # or hand to RAG; never a single-product dump masquerading as a comparison.
        if mp:
            return _answer_multi(mp, rows) or (None, [])
        return _execute_spec(spec, rows, cfg, max_list)
    except Exception:
        return None, []


def _execute_spec(spec: "Spec", rows: list[Row], cfg: dict | None, max_list: int) -> tuple[str | None, list[str]]:
    """Deterministic execution of a (filter + aggregation) spec against the rows."""
    try:
        excl = _excl_re(cfg, spec.anchors)

        _base = [r for r in rows if _hit(r, spec.anchors, spec.title_only)]
        if not _base and spec.anchors:
            # Strict all-anchor match found nothing — fall back to title-coverage so
            # a fully-named product still resolves. Only triggers when strict yields
            # 0, so a non-empty count can never be inflated.
            # The fallback's real job is the SUPERSET case: the query names the whole
            # stored title plus extra descriptors (coverage→1.0, e.g. "Pop N Play -
            # Quick Push Pop Game" → row "Pop N Play"). A loose 0.7 floor admitted
            # same-category SIBLINGS sharing only generic words ("Goldfish Fountain
            # Pen Single" for a query naming a DIFFERENT fountain pen) → a
            # confidently-wrong product. Require near-full coverage AND keep only the
            # best-covered rows, so an exact superset wins outright and a query for an
            # absent product yields nothing (→ graceful RAG/IDK, not a wrong sibling).
            _aset = _anchor_set(spec.anchors)
            _ranked = sorted(((_title_cover(r.title, _aset), r) for r in rows), key=lambda x: -x[0])
            _top = _ranked[0][0] if _ranked else 0.0
            _base = [r for c, r in _ranked if c >= 0.85 and c >= _top - 1e-9]
        sel = []
        _oos = []  # matched the query but excluded only by the in-stock filter
        for r in _base:
            if excl and excl.search(r.title):
                continue
            # Only exclude clearly out-of-stock rows. Availability strings vary by
            # source ("available", "in stock", ""), so treat anything not explicitly
            # out-of-stock as in stock rather than demanding an exact "available".
            if spec.in_stock and re.search(r"out\s+of\s+stock|sold\s+out|unavailable|not\s+available", r.availability):
                _oos.append(r)
                continue
            sel.append(r)
        # Price bounds apply to every aggregation when present (count of X under Y,
        # cheapest X over Y, …) and require a known price.
        if spec.price_min is not None or spec.price_max is not None:
            def _in_bounds(p):
                if p is None:
                    return False
                if spec.price_min is not None:
                    if (p <= spec.price_min) if spec.strict_min else (p < spec.price_min):
                        return False
                if spec.price_max is not None:
                    if (p >= spec.price_max) if spec.strict_max else (p > spec.price_max):
                        return False
                return True
            sel = [r for r in sel if _in_bounds(r.price)]

        # Category precision (shared by count/min/max/list): in a tag-bearing catalog,
        # narrow to rows whose STRUCTURED categories match the anchor (GT/attribute
        # semantics) so "most expensive in the Models category" / "how many in Nautica"
        # rank the real category, not every title that happens to contain the word.
        # SKIPPED for name-substring queries (title_only): "every item whose NAME
        # contains monochrome" must keep ALL title matches — narrowing to the rows
        # that happen to also carry a coincidental "monochrome" category tag (e.g.
        # one marker set) silently drops the products the user actually asked for.
        # Also skipped for untagged/HTML-crawl DBs (tagged ratio < 50%).
        if spec.anchors and not spec.title_only and spec.agg in ("count", "min", "max", "list"):
            _tagged = sum(1 for r in rows if r.cats.strip())
            if rows and (_tagged / len(rows)) >= 0.5:
                _tag_sel = [r for r in sel if _cats_hit(r, spec.anchors)]
                if _tag_sel:
                    sel = _tag_sel

        label = " ".join(spec.anchors) if spec.anchors else "products"

        if spec.agg in ("count", "exists"):
            n = len(sel)
            if n == 0:
                # Found it, but the "in stock" filter removed it → it IS carried, just
                # out of stock. Don't mislead the customer into thinking we never have it.
                if spec.in_stock and _oos:
                    o = _oos[0]
                    pr = f" (normally {_price_s(o.price, o.currency)})" if o.price is not None else ""
                    return (f"We do carry {o.title}{pr}, but it's currently out of stock. "
                            f"Would you like me to suggest similar in-stock options?"), _dedup(r.source for r in _oos)[:max_list]
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
