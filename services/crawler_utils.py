"""
services/crawler_utils.py — Page classification, chunking, and product metadata helpers.
No shared mutable app state. Safe to import from anywhere.
"""

from __future__ import annotations

import json
import re
from typing import Any

from services.safety import (
    _clean_text,
    _looks_structural_page,
    _looks_like_product_page,
    _strip_storefront_boilerplate,
    _dedupe_repeated_lines,
    _canonical_product_title,
    _trusted_content_metrics,
    _BOILERPLATE_SIGNAL_RE,
    _GENERIC_SECTION_SPLIT_RE,
    _CONTAMINATION_HINTS_RE,
    _POLICY_URL_RE,
    _POLICY_TEXT_RE,
    _CATEGORY_URL_RE,
    _ARTICLE_URL_RE,
    _PRODUCT_PRICE_LINE_RE,
    _PRODUCT_PRICE_CAPTURE_RE,
    _PRODUCT_AVAIL_RE,
)


_product_db_cache: dict[str, bool] = {}  # db_name → is_product_db (stable per collection)


def _check_is_product_db(db, db_name: str = "") -> bool:
    """Return True if the DB collection has product metadata (price/ram_gb/gpu_vram_gb).
    Result cached per db_name so the sample query runs at most once per DB."""
    if db_name and db_name in _product_db_cache:
        return _product_db_cache[db_name]
    result = False
    if db:
        try:
            sample = db._collection.get(limit=1, include=["metadatas"])
            if sample and sample.get("metadatas") and sample["metadatas"][0]:
                m = sample["metadatas"][0]
                result = m.get("price") is not None or m.get("ram_gb") is not None or m.get("gpu_vram_gb") is not None
        except Exception:
            pass
    if db_name:
        _product_db_cache[db_name] = result
    return result


_PRODUCT_QUERY_STOP = {
    "what", "is", "the", "price", "pricing", "cost", "of", "for", "a", "an", "item", "product",
    "products", "much", "how", "does", "do", "you", "have", "tell", "me", "about", "details",
}


def _product_query_rerank_score(question: str, doc) -> float:
    source = str(((getattr(doc, "metadata", None) or {}).get("source")) or "")
    text = str(getattr(doc, "page_content", "") or "")
    if not source and not text:
        return 0.0
    q_tokens = {
        t for t in re.findall(r"[a-z0-9]+", (question or "").lower())
        if len(t) >= 2 and t not in _PRODUCT_QUERY_STOP
    }
    source_tokens = set(re.findall(r"[a-z0-9]+", source.lower().replace("-", " ").replace("_", " ")))
    head_tokens = set(re.findall(r"[a-z0-9]+", text[:500].lower()))
    combined_tokens = source_tokens | head_tokens
    overlap = len(q_tokens & combined_tokens)
    slug_overlap = len(q_tokens & source_tokens)
    head_overlap = len(q_tokens & head_tokens)
    q_numbers = set(re.findall(r"\b\d+\b", question or ""))
    doc_numbers = set(re.findall(r"\b\d+\b", f"{source} {text[:500]}"))
    numbers_hit = len(q_numbers & doc_numbers)
    normalized_q = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (question or "").lower())).strip()
    normalized_doc = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", f"{source} {text[:500]}".lower())).strip()
    score = overlap + (slug_overlap * 3.0) + (head_overlap * 1.25) + (numbers_hit * 2.0)
    if normalized_q and normalized_q in normalized_doc:
        score += 8.0
    if re.search(r"\b(price|pricing|cost)\b", question or "", re.I) and re.search(r"(?i)\b(?:rs\.?|pkr|\$)\s*[\d,]+", text[:500]):
        score += 1.0
    return score


def _extract_product_summary(text: str, url: str, title_hint: str = "") -> dict:
    body = _strip_storefront_boilerplate(text or "")
    title = ""
    used_structured_fields = []
    body_fallback_used = False
    title_match = re.search(r'(?i)\bname:\s*([^\n\.]{4,180})', body)
    if title_match:
        title = title_match.group(1).strip(" -:")
        used_structured_fields.append("name")
    if not title:
        title = (title_hint or "").strip()
        if title:
            used_structured_fields.append("title_hint")
    if not title:
        slug = (url or "").rstrip("/").split("/")[-1]
        title = re.sub(r'[-_]+', ' ', slug).strip().title()
    price_label = ""
    price_num = None
    pm = _PRODUCT_PRICE_LINE_RE.search(body) or _PRODUCT_PRICE_CAPTURE_RE.search(body)
    if pm:
        if pm.re is _PRODUCT_PRICE_LINE_RE:
            currency = (pm.group(1) or "").strip() or "Rs."
            digits = pm.group(2)
        else:
            whole = pm.group(0)
            digits = pm.group(1)
            currency = whole.replace(digits, "").strip() or "Rs."
        price_label = f"{currency}{digits}"
        used_structured_fields.append("price")
        try:
            price_num = float(digits.replace(",", ""))
        except Exception:
            price_num = None
    avail = ""
    am = _PRODUCT_AVAIL_RE.search(body)
    if am:
        avail = am.group(1).strip()
        used_structured_fields.append("availability")
    desc = ""
    dm = re.search(r'(?i)\bdescription:\s*([^\n]{20,600})', body)
    if dm:
        desc = dm.group(1).strip()
        used_structured_fields.append("description")
    if not desc:
        body_fallback_used = True
        sentences = re.split(r'(?<=[.!?])\s+', body)
        useful = []
        for sent in sentences:
            s = sent.strip()
            if len(s) < 25:
                continue
            if re.search(r'(?i)\b(add to cart|wishlist|recently viewed|you may also like|customers also bought|checkout|subtotal)\b', s):
                continue
            useful.append(s)
            if len(" ".join(useful)) >= 420:
                break
        desc = " ".join(useful[:3]).strip()
    contaminated = bool(_CONTAMINATION_HINTS_RE.search(body))
    return {
        "title": title.strip(),
        "price_label": price_label.strip(),
        "price_num": price_num,
        "availability": avail,
        "description": desc[:900],
        "contaminated": contaminated,
        "used_structured_fields": list(dict.fromkeys(used_structured_fields)),
        "body_fallback_used": body_fallback_used,
    }


def _classify_page_type(url: str, cleaned: str, product_like: bool, structural: bool) -> str:
    source = (url or "").lower()
    if product_like:
        return "product"
    if structural:
        return "structural"
    if _POLICY_URL_RE.search(source) or _POLICY_TEXT_RE.search(cleaned):
        return "policy"
    if len(_GENERIC_SECTION_SPLIT_RE.split(cleaned)) >= 3:
        return "faq"
    # Outcomes/learning-goals marker override (universal):
    # Many docs index pages look "category/structural" but still contain the
    # canonical "By completing..., you will:" goals list. Never classify those
    # as category-only content.
    if re.search(r"(?i)\b(by completing|by the end of this (?:chapter|lesson)|you will be able to|learning outcomes|objectives|goals)\b", cleaned or ""):
        return "article"
    metrics = _trusted_content_metrics(cleaned)
    if _CATEGORY_URL_RE.search(source) or (metrics["category_hits"] >= 2 and metrics["sentence_count"] <= 2):
        return "category"
    if _ARTICLE_URL_RE.search(source) or (
        metrics["prose_chars"] > 300 and metrics["sentence_count"] >= 2 and metrics["nav_hits"] <= max(2, metrics["sentence_count"])
    ):
        return "article"
    return "unknown"


def _quality_score(
    cleaned: str,
    *,
    page_type: str,
    used_structured_fields: list[str] | None = None,
    body_fallback_used: bool = False,
    structural: bool = False,
    contaminated: bool = False,
    had_boilerplate: bool = False,
) -> float:
    score = 0.0
    used_structured_fields = used_structured_fields or []
    metrics = _trusted_content_metrics(cleaned)
    if used_structured_fields:
        score += 0.4
    if any(f in used_structured_fields for f in ("price", "availability", "description")):
        score += 0.2
    if metrics["prose_chars"] > 200:
        score += 0.2
    if not had_boilerplate:
        score += 0.2
    if body_fallback_used:
        score -= 0.1
    if structural or contaminated:
        score = min(score, 0.2)
    if page_type == "unknown":
        score = min(score, 0.49)
    return round(max(0.0, min(1.0, score)), 2)


def _page_classifier_confidence(
    cleaned: str,
    *,
    page_type: str,
    product_like: bool = False,
    structural: bool = False,
    used_structured_fields: list[str] | None = None,
    body_fallback_used: bool = False,
) -> float:
    used_structured_fields = used_structured_fields or []
    metrics = _trusted_content_metrics(cleaned)
    if page_type == "product":
        conf = 0.55
        if product_like:
            conf += 0.15
        conf += min(0.2, 0.05 * len(used_structured_fields))
        if body_fallback_used:
            conf -= 0.1
        return round(max(0.2, min(0.98, conf)), 2)
    if page_type == "structural":
        conf = 0.6 if structural else 0.45
        if metrics["nav_hits"] >= 8:
            conf += 0.15
        return round(max(0.2, min(0.98, conf)), 2)
    if page_type == "faq":
        conf = 0.7 if len(_GENERIC_SECTION_SPLIT_RE.split(cleaned)) >= 3 else 0.5
        return round(max(0.2, min(0.98, conf)), 2)
    if page_type == "policy":
        conf = 0.75 if metrics["policy_hits"] else 0.55
        return round(max(0.2, min(0.98, conf)), 2)
    if page_type == "category":
        conf = 0.65 if metrics["category_hits"] else 0.5
        return round(max(0.2, min(0.98, conf)), 2)
    if page_type == "article":
        conf = 0.55
        if metrics["prose_chars"] > 300:
            conf += 0.15
        if metrics["sentence_count"] >= 3:
            conf += 0.1
        return round(max(0.2, min(0.98, conf)), 2)
    return 0.35


def _prepare_crawl_page(text: str, url: str, title_hint: str = "") -> tuple[str, dict]:
    raw = _clean_text(text or "")
    product_like = _looks_like_product_page(url, raw)
    cleaned = _strip_storefront_boilerplate(raw) if product_like else _dedupe_repeated_lines(raw)
    cleaned = _clean_text(cleaned)
    had_boilerplate = cleaned != raw and bool(_BOILERPLATE_SIGNAL_RE.search(raw))
    structural = _looks_structural_page(url, cleaned)
    page_type = _classify_page_type(url, cleaned, product_like, structural)
    meta = {
        "structural": structural,
        "page_type": page_type,
        "contaminated": False,
        "used_structured_fields": [],
        "body_fallback_used": False,
        "had_boilerplate": had_boilerplate,
        "extraction_mode": "body_text",
    }
    if product_like:
        product = _extract_product_summary(cleaned, url, title_hint=title_hint)
        meta["product"] = product
        meta["contaminated"] = bool(product.get("contaminated"))
        meta["used_structured_fields"] = list(product.get("used_structured_fields") or [])
        meta["body_fallback_used"] = bool(product.get("body_fallback_used"))
        meta["extraction_mode"] = "structured_product" if meta["used_structured_fields"] else "body_text"
    meta["quality_score"] = _quality_score(
        cleaned,
        page_type=meta["page_type"],
        used_structured_fields=meta.get("used_structured_fields") or [],
        body_fallback_used=bool(meta.get("body_fallback_used")),
        structural=bool(meta.get("structural")),
        contaminated=bool(meta.get("contaminated")),
        had_boilerplate=bool(meta.get("had_boilerplate")),
    )
    meta["page_classifier_confidence"] = _page_classifier_confidence(
        cleaned,
        page_type=meta["page_type"],
        product_like=product_like,
        structural=bool(meta.get("structural")),
        used_structured_fields=meta.get("used_structured_fields") or [],
        body_fallback_used=bool(meta.get("body_fallback_used")),
    )
    quarantine_reason = ""
    retrieve_eligible = True
    if meta["page_type"] in {"structural", "category"}:
        retrieve_eligible = False
        quarantine_reason = meta["page_type"]
    elif meta.get("contaminated"):
        retrieve_eligible = False
        quarantine_reason = "contaminated"
    elif meta["page_type"] == "product" and meta.get("body_fallback_used") and len(meta.get("used_structured_fields") or []) < 2:
        retrieve_eligible = False
        meta["quality_score"] = min(float(meta.get("quality_score") or 0.0), 0.35)
        quarantine_reason = "weak_product_fallback"
    elif meta["page_type"] in {"product", "unknown"} and float(meta.get("quality_score") or 0.0) < 0.5:
        # Article/faq/policy pages have no structured fields so their score
        # is capped at 0.4 by design — don't quarantine them on score alone.
        retrieve_eligible = False
        quarantine_reason = "low_quality"
    meta["retrieve_eligible"] = retrieve_eligible
    meta["quarantine_reason"] = quarantine_reason
    return cleaned.strip(), meta


def _chroma_safe_metadata(metadata: dict | None) -> dict:
    """Chroma metadata values must be scalar; encode richer crawl fields compactly."""
    safe = {}
    for key, value in (metadata or {}).items():
        if value is None:
            continue
        if isinstance(value, (str, int, float, bool)):
            safe[key] = value
        elif isinstance(value, (list, tuple, set)):
            values = [str(v) for v in value if v is not None and str(v) != ""]
            if values:
                safe[key] = ", ".join(values)
        elif isinstance(value, dict):
            try:
                safe[key] = json.dumps(value, ensure_ascii=True, sort_keys=True)
            except Exception:
                safe[key] = str(value)
        else:
            safe[key] = str(value)
    return safe


def _sanitize_docs_for_chroma(docs: list) -> list:
    for doc in docs or []:
        doc.metadata = _chroma_safe_metadata(getattr(doc, "metadata", None) or {})
    return docs


def _merge_variant_docs(docs: list) -> list:
    if not docs:
        return docs
    grouped = {}
    for doc in docs:
        meta = getattr(doc, "metadata", None) or {}
        title = _canonical_product_title(meta.get("product_title") or "")
        if not title:
            grouped[id(doc)] = [doc]
            continue
        price_key = str(meta.get("price") or "").strip()
        key = (title.lower(), price_key)
        grouped.setdefault(key, []).append(doc)
    merged = []
    for group in grouped.values():
        if len(group) == 1:
            merged.extend(group)
            continue
        base = group[0]
        variants = []
        for doc in group:
            text = getattr(doc, "page_content", "") or ""
            mm = re.findall(r'(?i)\b(?:color|colour|size|variant|pack(?: of)?|piece(?:s)?)\b[^\n,;]*', text)
            variants.extend(v.strip() for v in mm if v.strip())
        uniq_variants = list(dict.fromkeys(variants))
        if uniq_variants:
            base.page_content = f"{base.page_content}\nVariants: {'; '.join(uniq_variants[:8])}".strip()
            base.metadata["variant_count"] = len(uniq_variants)
        base.metadata["dedup_applied"] = len(group) > 1
        merged.append(base)
    return merged


# ── Product spec extraction regexes (used by smart chunker) ──────────────────
_PROD_PRICE_RE = re.compile(r'(?i)(?:\$|rs\.?\s*|pkr\s*)(\d[\d,]*\.?\d*)')
_PROD_SPEC_RE = re.compile(r'\b(?:processor|cpu|ram|memory|storage|ssd|hdd|gpu|graphics|display|battery|os|android|windows|linux|screen)\b', re.I)
_PROD_SPLIT_RE = re.compile(r'(?i)(?:\$|rs\.?\s*|pkr\s*)(\d[\d,]*\.?\d*)\s+([A-Z][A-Za-z0-9 \(\)\-\.]+?(?:,[^\$]{10,400}?))(?=\s*(?:\$|rs\.?\s*|pkr\s*)|\s*\Z)', re.S)
_FAQ_SPLIT_RE = re.compile(r'(?m)^(?=(?:Q:|Question:|How |What |Why |When |Where |Who |Can |Do |Is |Are |Does |Should ))', re.I)


def _smart_chunk_page(text: str, url: str, chunk_size: int = 400, chunk_step: int = 320, page_meta: dict | None = None) -> list:
    from langchain_core.documents import Document
    """
    Smart page chunker. Three modes, tried in order:
      1. Product page  — $PRICE + spec keywords → one Document per product, price metadata
      2. FAQ page      — Q&A or heading sections → one Document per section
      3. Generic       — existing word-based sliding window (unchanged fallback)
    Returns list[Document]. Never raises.
    """
    # Important: preserve newline structure for list-ish pages (outcomes, policies, release notes, etc.).
    # _clean_text() is intentionally aggressive and tends to flatten whitespace, which destroys bullet boundaries.
    raw_text = (text or "")
    raw_text = raw_text.replace("\r\n", "\n").replace("\r", "\n")
    raw_text = re.sub(r"[ \t]+", " ", raw_text)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)
    clean = _clean_text(raw_text)
    docs = []
    page_meta = page_meta or {}
    _base_meta = {"source": url}
    if page_meta.get("structural"):
        _base_meta["structural"] = True
    if page_meta.get("contaminated"):
        _base_meta["contaminated"] = True
    if page_meta.get("page_type"):
        _base_meta["content_type"] = page_meta["page_type"]
    if "quality_score" in page_meta:
        _base_meta["quality_score"] = page_meta.get("quality_score")
    if "page_classifier_confidence" in page_meta:
        _base_meta["page_classifier_confidence"] = page_meta.get("page_classifier_confidence")
    if page_meta.get("used_structured_fields"):
        _base_meta["used_structured_fields"] = list(page_meta.get("used_structured_fields") or [])
    if "body_fallback_used" in page_meta:
        _base_meta["body_fallback_used"] = bool(page_meta.get("body_fallback_used"))
    if "retrieve_eligible" in page_meta:
        _base_meta["retrieve_eligible"] = bool(page_meta.get("retrieve_eligible"))
    if page_meta.get("quarantine_reason"):
        _base_meta["quarantine_reason"] = page_meta.get("quarantine_reason")
    if page_meta.get("extraction_mode"):
        _base_meta["extraction_mode"] = page_meta.get("extraction_mode")
    _base_meta["dedup_applied"] = False

    product_meta = page_meta.get("product") or {}
    if page_meta.get("page_type") == "product" and product_meta.get("title") and (product_meta.get("price_label") or product_meta.get("description")):
        lines = [f"Product: {product_meta['title']}"]
        if product_meta.get("price_label"):
            lines.append(f"Price: {product_meta['price_label']}")
        if product_meta.get("availability"):
            lines.append(f"Availability: {product_meta['availability']}")
        if product_meta.get("description"):
            lines.append(f"Description: {product_meta['description']}")
            lines.append(f"Full specs: {product_meta['title']}, {product_meta['description']}")
        _prod_text = "\n".join(lines).strip()
        _prod_meta = dict(_base_meta)
        _prod_meta["product_title"] = product_meta["title"]
        if product_meta.get("price_num") is not None:
            _prod_meta["price"] = product_meta["price_num"]
        if product_meta.get("availability"):
            _prod_meta["availability"] = product_meta["availability"]
        _prod_meta["used_structured_fields"] = list(product_meta.get("used_structured_fields") or _base_meta.get("used_structured_fields") or [])
        _prod_meta["body_fallback_used"] = bool(product_meta.get("body_fallback_used"))
        _prod_meta.update(_extract_product_metadata(_prod_text))
        docs.append(Document(page_content=_prod_text, metadata=_prod_meta))
        return _merge_variant_docs(docs)

    # ── Mode 1: product page ──────────────────────────────────────────────────
    if len(_PROD_PRICE_RE.findall(clean)) >= 1 and _PROD_SPEC_RE.search(clean):
        products = []
        for m in _PROD_SPLIT_RE.finditer(clean):
            price_str = m.group(1).replace(',', '')
            try:
                price_num = float(price_str)
            except:
                continue
            raw = m.group(2).strip()
            comma_idx = raw.find(',')
            name = raw[:comma_idx].strip() if comma_idx > 0 else raw
            specs = raw[comma_idx + 1:].strip() if comma_idx > 0 else ''
            # Strip breadcrumb prefix "Dell Inspiron... Dell Inspiron 15"
            name = re.sub(r'^[^\s]+(?:\s+[^\s]+){0,3}\.{2,}\s+', '', name).strip()
            name = re.sub(r'\s+reviews?\s*$', '', name, flags=re.I).strip()
            if not name or len(name) < 3:
                continue
            products.append((price_num, f"${price_str}", name, specs))

        if len(products) >= 1:
            for price_num, price_label, name, specs in products:
                lines = [f"Product: {name}", f"Price: {price_label}"]
                # ── Attribute normalization: inject user-vocabulary tags ──────
                _attr_tags = []
                if re.search(r'geforce|gtx|rtx|radeon\s+r[579x]|radeon\s+rx', specs, re.I):
                    _attr_tags.append("gaming laptop dedicated GPU")
                if re.search(r'\btouch\b', specs, re.I) or re.search(r'\btouch\b', name, re.I):
                    _attr_tags.append("touchscreen display")
                if re.search(r'2\s*in\s*1|360|yoga|spin\b', name, re.I):
                    _attr_tags.append("convertible 2-in-1 laptop")
                if re.search(r'\bssd\b', specs, re.I):
                    _attr_tags.append("fast SSD storage")
                if re.search(r'windows', specs, re.I):
                    _attr_tags.append("Windows laptop")
                if re.search(r'android', specs, re.I):
                    _attr_tags.append("Android device")
                if _attr_tags:
                    lines.append("Features: " + ", ".join(_attr_tags))
                for part in [s.strip() for s in specs.split(',')]:
                    pl = part.lower()
                    if re.search(r'geforce|nvidia|radeon|amd\s+r|gtx|rtx|mx\d', pl):
                        lines.append(f"GPU: {part}")
                    elif re.search(r'\d+\s*gb(?:\s+ram)?$|\bddr\b', pl):
                        lines.append(f"RAM: {part}")
                    elif re.search(r'\d+\s*(?:gb|tb)\s+(?:ssd|hdd|emmc)|\d+\s*tb\b', pl):
                        lines.append(f"Storage: {part}")
                    elif re.search(r'core\s+i\d|celeron|pentium|ryzen|athlon|snapdragon', pl):
                        lines.append(f"Processor: {part}")
                    elif re.search(r'windows|linux|dos|macos|android|chrome\s*os', pl):
                        lines.append(f"OS: {part}")
                    elif re.search(r'\d+\.?\d*\"\s*(?:hd|fhd|uhd|ips|touch)?|(?:hd|fhd|uhd|ips)\s+display', pl):
                        lines.append(f"Display: {part}")
                if specs:
                    lines.append(f"Full specs: {name}, {specs}")
                _prod_text = "\n".join(lines)
                _prod_meta = dict(_base_meta)
                _prod_meta.update({"price": price_num, "product_title": name})
                _prod_meta.update(_extract_product_metadata(_prod_text))
                docs.append(Document(page_content=_prod_text, metadata=_prod_meta))
            if docs:
                return _merge_variant_docs(docs)

    # ── Mode 2: FAQ / section page ────────────────────────────────────────────
    sections = _FAQ_SPLIT_RE.split(clean)
    if len(sections) >= 3:
        for sec in sections:
            sec = sec.strip()
            if len(sec) > 40:
                docs.append(Document(page_content=sec[:2000], metadata=dict(_base_meta)))
        if docs:
            return docs

    # ── Mode 2.5: bullet/list page ────────────────────────────────────────────
    # Preserve newline structure for outcomes/checklists/release notes. Generic
    # word-splitting destroys list boundaries and makes retrieval unreliable.
    try:
        # Use raw_text lines (not `clean`) so we don't lose bullets/numbering.
        _raw_lines = [ln.strip() for ln in raw_text.split("\n") if ln and ln.strip()]
        _bullet_re = re.compile(r"^(?:[-*•]|\d{1,2}[.)])\s+")
        _bullet_lines = [ln for ln in _raw_lines if _bullet_re.match(ln)]
        if len(_bullet_lines) >= 3:
            buf = []
            buf_chars = 0
            for ln in _raw_lines:
                if len(ln) > 500:
                    continue
                if buf and (buf_chars + len(ln) + 1 > 2000):
                    docs.append(Document(page_content="\n".join(buf).strip(), metadata=dict(_base_meta)))
                    buf, buf_chars = [], 0
                buf.append(ln)
                buf_chars += len(ln) + 1
            if buf:
                docs.append(Document(page_content="\n".join(buf).strip(), metadata=dict(_base_meta)))
            if docs:
                return docs
    except Exception:
        pass

    # ── Mode 3: generic word-split (existing behaviour — unchanged) ───────────
    words = clean.split()
    for j in range(0, max(1, len(words)), chunk_step):
        chunk = " ".join(words[j:j + chunk_size])
        if len(chunk) > 20:
            docs.append(Document(page_content=chunk, metadata=dict(_base_meta)))
        if j + chunk_size >= len(words):
            break
    return docs


def _extract_product_metadata(text: str) -> dict:
    """Parse product-catalog chunk text into structured ChromaDB metadata fields."""
    meta = {}
    pm = re.search(r'Price:\s*(?:\$|rs\.?\s*|pkr\s*)?([\d,]+\.?\d*)', text, re.I)
    if pm:
        try:
            meta['price'] = float(pm.group(1).replace(',', ''))
        except:
            pass
    rm = re.search(r'RAM:\s*(\d+)\s*GB', text, re.I)
    if rm:
        try:
            meta['ram_gb'] = int(rm.group(1))
        except:
            pass
    gm = re.search(r'GPU:[^\n]*?(\d+)\s*GB', text, re.I)
    if gm:
        try:
            meta['gpu_vram_gb'] = int(gm.group(1)); meta['has_gpu'] = 1
        except:
            pass
    else:
        meta['has_gpu'] = 0
    meta['has_touch'] = 1 if re.search(r'Display:[^\n]*\bTouch\b', text) else 0
    meta['is_convertible'] = 1 if re.search(r'\bconvertible\b', text, re.I) else 0
    meta['has_ssd'] = 1 if re.search(r'\bSSD\b', text) else 0
    return meta


def _enrich_docs_metadata(docs: list) -> list:
    """Auto-detect product catalog chunks and enrich with structured metadata."""
    for doc in docs:
        if re.search(r'^Product:\s+\S', doc.page_content, re.M):
            extracted = _extract_product_metadata(doc.page_content)
            if doc.metadata:
                doc.metadata.update(extracted)
            else:
                doc.metadata = extracted
    return docs
