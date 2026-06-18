"""
services/safety.py — Pure text/regex helpers extracted from app.py.

No shared mutable app state. Safe to import from anywhere.
"""

from __future__ import annotations

import re
import urllib.parse
from collections import Counter


_STOP_WORDS = {
    "what","how","why","when","where","who","which","is","are","was","were",
    "do","does","did","can","could","would","should","will","the","a","an",
    "in","on","at","to","for","of","and","or","tell","me","about","explain",
    "describe","give","show","please","i","you","we","they","it","this","that",
}

# Used by _strip_source_leaks()
_SOURCE_LEAK_PATTERNS = [
    r"(?i)based on (the |our |this )?(business context|knowledge base|kb|provided context|context provided)[,.]?\s*",
    r"(?i)according to (the )?(knowledge base|business context|kb|agentfactory knowledge base|provided (information|context|data))[,.]?\s*",
    r"(?i)the (knowledge base|business context|kb) (says?|states?|indicates?|shows?|mentions?|notes?)[,:]?\s*",
    r"(?i)from (the )?(knowledge base|business context|kb|provided context)[,:]?\s*",
    r"(?i)in (the )?(knowledge base|business context)[,:]?\s*",
    r"(?i)as (per|stated in|mentioned in) (the )?(knowledge base|business context|kb|context)[,:]?\s*",
    r"(?i)the (provided )?(context|kb|knowledge base) (provided )?(indicates?|shows?|states?|says?)[,:]?\s*",
    r"(?i)BUSINESS CONTEXT:.*?(?=\n\n|\Z)",  # raw section dump
    r"(?i)KNOWLEDGE BASE:.*?(?=\n\n|\Z)",    # raw section dump
]

_URL_PATTERN = re.compile(r'https?://[^\s)\]"\'<>,]+|www\.[^\s)\]"\'<>,]+', re.IGNORECASE)

_PRICE_QUERY_RE = re.compile(r'\b(price|cost|how much|rate|pkr|rs\.?)\b', re.I)
_PRICE_VAL_RE = re.compile(r'(?:Sale price|Regular price|Price:)\s*Rs\.?\s*([\d,]+(?:\.\d+)?)\s*PKR', re.I)


def _deterministic_price_answer(q: str, context: str) -> str | None:
    """Return a direct price answer from context without LLM. Returns None if not applicable."""
    if not _PRICE_QUERY_RE.search(q):
        return None
    prices = _PRICE_VAL_RE.findall(context)
    if not prices:
        return None
    # Prefer sale price (appears after "Sale price"), otherwise first price found
    sale_matches = re.findall(r'Sale price\s*Rs\.?\s*([\d,]+(?:\.\d+)?)\s*PKR', context, re.I)
    price_str = f"Rs. {sale_matches[0]} PKR" if sale_matches else f"Rs. {prices[0]} PKR"
    if len(sale_matches) >= 1 and len(prices) >= 1 and prices[0] != sale_matches[0]:
        price_str = f"Rs. {sale_matches[0]} PKR (regular Rs. {prices[0]} PKR)"
    return f"The price is {price_str}."


def _strip_source_leaks(text: str, kb_context: str = "") -> str:
    for pat in _SOURCE_LEAK_PATTERNS:
        text = re.sub(pat, "", text)
    # Strip invented URLs — only allow URLs that appear verbatim in KB context
    if kb_context:
        kb_urls = set(_URL_PATTERN.findall(kb_context))

        def _url_filter(m):
            url = m.group(0).rstrip(".,;)")
            return url if url in kb_urls else ""

        text = _URL_PATTERN.sub(_url_filter, text)
    # Capitalise first letter if it got stripped
    text = text.strip()
    if text and text[0].islower():
        text = text[0].upper() + text[1:]
    return text


_CONCEPT_MAP = {
    "curriculum": ["topics", "syllabus", "modules", "what will I learn", "chapters", "subjects", "course content", "roadmap", "outline"],
    "price": ["cost", "fees", "charges", "subscription", "pricing", "payment", "how much", "rate card", "pay", "buy"],
    "contact": ["email", "phone", "whatsapp", "address", "reach out", "support", "help", "connect"],
    "owner": ["founder", "ceo", "team", "who made", "creator", "management", "leadership"],
    "location": ["office", "where", "city", "country", "map", "headquarters"],
    # Product/spec concepts for e-commerce DBs
    "gpu": ["GeForce", "GTX", "graphics", "NVIDIA", "AMD", "Radeon", "1050", "1060", "1070", "1080"],
    "gaming": ["ROG", "STRIX", "Nitro", "Legion", "GTX", "GeForce", "gaming laptop", "dedicated GPU"],
    "ram": ["memory", "8GB", "16GB", "4GB", "DDR", "GB RAM"],
    "storage": ["SSD", "HDD", "hard drive", "solid state", "128GB", "256GB", "512GB", "1TB"],
    "laptop": ["notebook", "ThinkPad", "VivoBook", "Aspire", "MacBook", "Inspiron", "ZenBook"],
    "tablet": ["android tablet", "iPad", "IdeaTab", "Iconia", "Galaxy Tab"],
    "phone": ["smartphone", "Xperia", "iPhone", "Nokia", "Samsung Galaxy", "touch phone"],
}


def expand_query(q: str) -> list:
    """Return [original_query, keyword_query, semantic_expansions] for conceptual understanding."""
    q_lower = q.lower()
    expanded = [q]

    # 1. Basic Keyword extraction
    words = [w.strip("?.,!") for w in q_lower.split()]
    keywords = [w for w in words if w not in _STOP_WORDS and len(w) > 2]
    kw_query = " ".join(keywords)
    if kw_query and kw_query != q_lower and len(keywords) >= 2:
        expanded.append(kw_query)

    # 2. Universal Concept Expansion (Semantic understanding)
    for concept, synonyms in _CONCEPT_MAP.items():
        if concept in q_lower or any(s in q_lower for s in synonyms):
            # If the user mentioned the concept or a synonym, add all other synonyms to the search
            expanded.extend(synonyms[:5])  # Take top 5 to keep search efficient

    return list(dict.fromkeys(expanded))  # Unique items only


_INVISIBLE_CHARS = re.compile(r'[\u200b\u200c\u200d\ufeff\u00ad\u2060]')


_SKIP_LINK_RE = re.compile(r'(?i)\bskip to (?:main )?(?:content|navigation|nav)\b')


def _clean_text(text: str) -> str:
    """Strip invisible Unicode chars + the universal "skip to main content" nav
    link. The skip-link is an accessibility anchor present on most CMS/docs pages
    (Docusaurus etc.); left in, it dominates short title chunks and gets quoted as
    if it were content (e.g. "co-authors include Factory Skip to main content")."""
    return _SKIP_LINK_RE.sub(" ", _INVISIBLE_CHARS.sub("", text))


_PRODUCT_PRICE_CAPTURE_RE = re.compile(
    r'(?i)\b(?:rs\.?\s*|pkr\s*|\$\s*)([\d,]+(?:\.\d{1,2})?)\b'
)
_PRODUCT_PRICE_LINE_RE = re.compile(
    r'(?i)\b(?:price|sale price|regular price)\s*:\s*(rs\.?\s*|pkr\s*|\$\s*)?([\d,]+(?:\.\d{1,2})?)'
)
_PRODUCT_AVAIL_RE = re.compile(
    r'(?i)\b(in stock|out of stock|sold out|available|unavailable|pre[- ]?order)\b'
)

_STRUCTURAL_URL_RE = re.compile(
    r'(?i)(?:/|#)(?:login|sign-?in|register|signup|account|profile|progress|dashboard|flashcards?|quiz(?:zes)?|exercise|exercises|leaderboard|help|support|site-navigation|toc)(?:/|$|#)'
)
_STRUCTURAL_TEXT_RE = re.compile(
    r'(?i)\b(sign in|log in|register|my account|your profile|dashboard|leaderboard|flashcards?|quiz(?:zes)?|exercise|progress|continue learning|toggle theme)\b'
)

_STORE_BOILERPLATE_PATTERNS = [
    re.compile(r'(?is)\byou may also like\b.*?(?=(?:\bproduct\b|\bdescription\b|\breviews?\b|\Z))'),
    re.compile(r'(?is)\brecently viewed\b.*?(?=(?:\bproduct\b|\bdescription\b|\breviews?\b|\Z))'),
    re.compile(r'(?is)\bcustomers also bought\b.*?(?=(?:\bproduct\b|\bdescription\b|\breviews?\b|\Z))'),
    re.compile(r'(?is)\byour cart\b.*?(?=(?:\bcheckout\b|\bcontinue shopping\b|\Z))'),
    re.compile(r'(?is)\bsubtotal\b.*?(?=(?:\bcheckout\b|\bcontinue shopping\b|\Z))'),
    re.compile(r'(?is)\badd to wishlist\b'),
    re.compile(r'(?is)\b(?:decrease|increase)\s+quantity\s+for\b[^\n\.!]*'),
    re.compile(r'(?is)\bshipping calculated at checkout\b\.?'),
    re.compile(r'(?is)\bfree shipping on orders?\s+(?:over|above)\b[^\n\.!]*?(?:/-|\bpkr\b|\d)[^\n\.!]*'),
    re.compile(r'(?is)\buse code\s+[A-Z0-9_-]+\s+for\s+\d+% off\b'),
]

_CONTAMINATION_HINTS_RE = re.compile(
    r'(?i)\b(designer wear|premium fashion|men(?:\'s)? fashion|women(?:\'s)? fashion|apparel|clothing collection|footwear|shoes|handbags?)\b'
)

_GENERIC_SECTION_SPLIT_RE = re.compile(
    r'(?m)^(?=(?:Q:|Question:|How |What |Why |When |Where |Who |Can |Do |Is |Are |Does |Should ))',
    re.I,
)

_POLICY_URL_RE = re.compile(r'(?i)(?:/|#)(?:privacy|refund|return|shipping|delivery|warranty|policy|policies|terms)(?:/|$|#)')
_CATEGORY_URL_RE = re.compile(r'(?i)(?:/|#)(?:collections?|categories?|category|shop)(?:/|$|#)')
_ARTICLE_URL_RE = re.compile(r'(?i)(?:/|#)(?:blog|blogs|article|articles|lesson|lessons|chapter|chapters|docs|guide|guides|resources?)(?:/|$|#)')
_POLICY_TEXT_RE = re.compile(r'(?i)\b(refund policy|return policy|shipping policy|privacy policy|terms of service|warranty policy|delivery policy)\b')
_CATEGORY_TEXT_RE = re.compile(r'(?i)\b(sort by|filter|showing \d+|product type|collections?|categories?)\b')
_BOILERPLATE_SIGNAL_RE = re.compile(
    r'(?i)\b(you may also like|recently viewed|customers also bought|your cart|subtotal|checkout|wishlist|free shipping|use code|all rights reserved|privacy policy|terms of service)\b'
)
_NAV_CONTROL_RE = re.compile(
    r'(?i)\b(home|menu|search|login|sign in|register|profile|dashboard|toggle theme|wishlist|cart|checkout|compare|share)\b'
)


def _canonical_product_title(text: str) -> str:
    text = re.sub(r'(?i)^product:\s*', "", text or "").strip()
    text = re.sub(r"\s+", " ", text)
    # Strip variant tails ("Shirt - Color Red") but only past the title head:
    # "A Piece of Sky" must not be truncated to "A" because 'piece' ~ 'pieces?'.
    _m = re.search(r'(?i)\b(?:size|color|colour|variant|pack of|pcs?|pieces?)\b.*$', text)
    if _m and _m.start() >= 12 and len(text[:_m.start()].strip(" -,:")) >= 4:
        text = text[:_m.start()].strip(" -,:")
    return text


def _dedupe_repeated_lines(text: str) -> str:
    raw = text or ""
    if not raw.strip():
        return raw
    blocks = [blk for blk in re.split(r"\n{2,}", raw) if blk.strip()]
    if len(blocks) <= 1:
        lines = [ln.strip() for ln in re.split(r"[\r\n]+", raw) if ln.strip()]
        if not lines:
            return raw
        counts = Counter(lines)
        kept = []
        for ln in lines:
            if counts[ln] >= 3:
                if ln not in kept:
                    kept.append(ln)
                continue
            kept.append(ln)
        return "\n".join(kept)

    out_blocks = []
    for blk in blocks:
        lines = [ln.strip() for ln in re.split(r"[\r\n]+", blk) if ln.strip()]
        if not lines:
            continue
        counts = Counter(lines)
        kept = []
        for ln in lines:
            if counts[ln] >= 3:
                if ln not in kept:
                    kept.append(ln)
                continue
            kept.append(ln)
        out_blocks.append("\n".join(kept))
    return "\n\n".join(out_blocks) if out_blocks else raw


_SHOPIFY_PDP_PREAMBLE_RE = re.compile(r'(?is)^.*?\bskip to product information\b')
_MEDIA_MODAL_RE = re.compile(r'(?i)\bopen media\s+\d+\s+in modal\b')
_SHOPIFY_VENDOR_PLACEHOLDER_RE = re.compile(r'(?i)\bmy store\b')


def _strip_storefront_boilerplate(text: str) -> str:
    cleaned = text or ""
    # Shopify PDPs (Dawn-based themes) flatten the entire header nav + promo bar
    # ahead of the product. The "Skip to product information" skip-link is the
    # theme's universal main-content anchor — everything before it is chrome, so
    # cutting up to it isolates the real product title/price. Only fires when the
    # marker exists (non-Shopify text is untouched). Without this, flattened nav
    # text makes the title a menu fragment and "FREE SHIPPING above Rs.X" the price.
    if _SHOPIFY_PDP_PREAMBLE_RE.search(cleaned):
        cleaned = _SHOPIFY_PDP_PREAMBLE_RE.sub(" ", cleaned, count=1)
        cleaned = _MEDIA_MODAL_RE.sub(" ", cleaned)
        cleaned = _SHOPIFY_VENDOR_PLACEHOLDER_RE.sub(" ", cleaned)
    cleaned = _dedupe_repeated_lines(cleaned)
    for pat in _STORE_BOILERPLATE_PATTERNS:
        cleaned = pat.sub(" ", cleaned)
    cleaned = re.sub(
        r"(?im)^(?:checkout|wishlist|compare|share|follow us|all rights reserved|privacy policy|terms of service)\s*$",
        " ",
        cleaned,
    )
    cleaned = re.sub(r"(?i)\b(?:add to cart|buy now)\b(?:\s+\b(?:add to cart|buy now)\b)+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return _clean_text(cleaned).strip()


def _trusted_content_metrics(text: str) -> dict:
    body = _clean_text(text or "")
    # Count sentence-like units rather than raw punctuation marks so list-heavy
    # pages do not look "empty" just because they omit periods.
    sentence_like = [
        s.strip()
        for s in re.split(r"(?<=[.!?])\s+|[\r\n]+", body)
        if len(s.strip()) >= 20
    ]
    bullet_like = [
        s.strip()
        for s in re.findall(r"(?m)^\s*(?:[-*•]|\d{1,2}[.)])\s+(.+)$", body)
        if len(s.strip()) >= 20
    ]
    sentence_count = max(len(sentence_like), len(bullet_like))
    prose_chars = len(body)
    paragraphs = [p.strip() for p in re.split(r"[\r\n]+", body) if p.strip()]
    nav_hits = len(_NAV_CONTROL_RE.findall(body))
    policy_hits = len(_POLICY_TEXT_RE.findall(body))
    category_hits = len(_CATEGORY_TEXT_RE.findall(body))
    return {
        "sentence_count": sentence_count,
        "prose_chars": prose_chars,
        "paragraph_count": len(paragraphs),
        "nav_hits": nav_hits,
        "policy_hits": policy_hits,
        "category_hits": category_hits,
    }


def _looks_structural_page(url: str, text: str) -> bool:
    source = (url or "").lower()
    body = _clean_text(text or "")
    # /docs/ pages always contain real educational content — never structural
    if re.search(r'/docs/', source):
        return False
    if _STRUCTURAL_URL_RE.search(source):
        return True
    metrics = _trusted_content_metrics(body)
    if metrics["prose_chars"] < 120:
        return False
    if _STRUCTURAL_TEXT_RE.search(body):
        # Count only pure UI-shell indicators — chapter/lesson/module are legitimate educational content words
        # and appear in overview pages that ARE the content we want to retrieve.
        topic_hits = len(re.findall(r"(?i)\b(flashcard|quiz(?:zes?)?|exercise)\b", body))
        if metrics["sentence_count"] <= 2 or topic_hits >= 4:
            return True
    if metrics["nav_hits"] >= 8 and metrics["sentence_count"] <= 2:
        return True
    return False


_NEVER_PRODUCT_SEGMENT_RE = re.compile(
    r"(?:/|^)(?:checkout|cart|basket|account|login|register|sign-?in|sign-?up|wishlist|search|"
    r"blogs?|news|pages?|policies|privacy(?:-policy)?|terms(?:-and-conditions)?|contact(?:-us)?|"
    r"about(?:-us)?|faqs?|help|support|track(?:ing)?|order-status)(?:/|$|[?#])", re.I)


def _looks_like_product_page(url: str, text: str) -> bool:
    source = (url or "").lower()
    body = text or ""
    # Explicit product-detail URL segment is the strongest signal and wins first.
    if re.search(r"/(?:products?|items?)(?:/|$|#)", source):
        return True
    # Structural disqualifiers — universal across storefronts, checked BEFORE any
    # content heuristic. Site-wide chrome ("Free Shipping over 5000PKR" + the word
    # "product") makes the content test pass on every page of a store, so pages
    # that structurally cannot be a product DETAIL page must never reach it:
    # the homepage/root, checkout/cart/account flows, blogs, static info pages,
    # and machine files (.md/.txt/.xml/.json).
    try:
        _path = urllib.parse.urlparse(source).path or ""
    except Exception:
        _path = source
    if _path.rstrip("/") == "":  # site root is a storefront, never a single product
        return False
    if re.search(r"\.(?:md|txt|xml|json)$", _path):
        return False
    if _NEVER_PRODUCT_SEGMENT_RE.search(_path):
        return False
    # A listing URL (/collections/, /category/) without a product segment is a
    # catalog page no matter how product-ish its body looks (Shopify listings
    # contain prices + the word "product" on every card).
    if re.search(r"(?:/|#)(?:collections?|categor(?:y|ies)|shop|browse|listing)(?:/|$|#)", source):
        return False
    if re.search(r"(?i)\b@type\b.*\bproduct\b", body):
        return True
    if _PRODUCT_PRICE_CAPTURE_RE.search(body) and re.search(r"(?i)\b(add to cart|sku|availability|brand|product|variant)\b", body):
        return True
    return False


def _looks_like_catalog_page(url: str, text: str) -> bool:
    """Return True for listing/category pages that contain multiple product cards."""
    source = (url or "").lower()
    body = text or ""
    # If this is already a product detail page, do not downgrade it to a catalog/listing page.
    # This guard exists so detail pages like /product/104 stay product-shaped end to end.
    if _looks_like_product_page(source, body):
        return False
    if re.search(r"(?i)(?:/|#)(?:catalog|category|categories|collection|collections|shop|store|listing|browse|products)(?:/|$|#)", source):
        return True
    price_hits = len(_PRODUCT_PRICE_CAPTURE_RE.findall(body)) + len(_PRODUCT_PRICE_LINE_RE.findall(body))
    if price_hits >= 2:
        cardish = len(re.findall(r"(?is)\b(?:add to cart|reviews?|select color|full specs|product details)\b", body))
        if cardish >= 2 or re.search(r"(?is)\b(?:\d+\s+items|\d+\s+products|top items being scraped|category)\b", body):
            return True
    return False


def _is_urdu_script(text: str) -> bool:
    """Check if string contains Urdu/Arabic characters."""
    return any("\u0600" <= char <= "\u06FF" for char in text or "")

