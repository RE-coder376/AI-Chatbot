"""
services/crawler_utils.py â€” Page classification, chunking, and product metadata helpers.
No shared mutable app state. Safe to import from anywhere.
"""

from __future__ import annotations

import hashlib
import json
import re
import urllib.parse
from datetime import datetime, timezone
from typing import Any

from services.safety import (
    _clean_text,
    _looks_structural_page,
    _looks_like_product_page,
    _looks_like_catalog_page,
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
    _PROD_PRICE_CAPTURE_RE,
    _PRODUCT_AVAIL_RE,
)


_product_db_cache: dict[str, bool] = {}  # db_name â†’ is_product_db (stable per collection)


# Stable per-page URL identity used for dedupe/replace across crawls.
def _canonical_source_url(url: str) -> str:
    try:
        u = (url or "").strip()
        if not u:
            return ""
        p = urllib.parse.urlparse(u)
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = re.sub(r"/+", "/", p.path or "/")
        if path != "/":
            path = path.rstrip("/")
        return f"{scheme}://{netloc}{path}"
    except Exception:
        return (url or "").strip()


def _looks_generic_title(title: str) -> bool:
    cand = _canonical_product_title(title or "").strip(" -:|")
    if not cand:
        return True
    if len(cand) < 4:
        return True
    if "|" in cand:
        return True
    if re.search(r"(?i)\b(web scraper test sites|all rights reserved|privacy policy|terms of service|home\s*\|\s*[^|]+)$", cand):
        return True
    low = cand.lower()
    if re.search(
        r"\b(?:web scraper|cloud scraper)\b.*\b(?:extension|pricing|marketplace|learn|documentation|video tutorials|test sites|forum|install|login|company|about us|contact|privacy policy|media kit|resources|blog|screenshots|status)\b",
        low,
    ):
        return True
    if re.fullmatch(
        r"(?:web scraper|cloud scraper|test sites|forum|documentation|video tutorials|pricing|marketplace|learn|install|login|about us|contact us)(?:\s*[-|]\s*.*)?",
        low,
    ):
        return True
    if re.fullmatch(r"[\W_]+", cand):
        return True
    words = re.findall(r"[A-Za-z0-9]+", cand)
    if len(words) <= 1 and re.match(r"(?i)^(?:black|white|blue|grey|gray|silver|gold|red|green|pink|purple|yellow|orange|brown|beige|navy|teal|lavender|maroon|violet|golden|transparent|clear|unknown|default|variant|color|colour)$", cand):
        return True
    return False


def _derive_page_title(title_hint: str, cleaned: str, product: dict | None = None) -> str:
    candidates: list[str] = []
    body = cleaned or ""
    for pat in (
        r"(?m)^\s*(?:##|###)\s+(.+?)\s*$",
        r"(?i)\b(?:name|title|product)\s*:\s*([^\n\.]{4,180})",
        r"(?m)^(?!https?://)([A-Z][^\n]{4,120})$",
    ):
        for mm in re.finditer(pat, body):
            cand = _canonical_product_title((mm.group(1) or "").strip())
            if cand and cand not in candidates:
                candidates.append(cand)
                break

    for cand in [title_hint or "", (product or {}).get("title") or "", (product or {}).get("canonical_title") or ""]:
        cand = _canonical_product_title(str(cand or "")).strip(" -:|")
        if cand and cand not in candidates:
            candidates.append(cand)

    non_generic = [c for c in candidates if not _looks_generic_title(c)]
    if non_generic:
        candidates = non_generic + [c for c in candidates if c not in non_generic]
    return candidates[0] if candidates else ""


def _check_is_product_db(db, db_name: str = "") -> bool:
    """Return True if the DB collection looks like a product/catalog DB.
    Result cached per db_name so the sample query runs at most once per DB."""
    if db_name and db_name in _product_db_cache:
        return _product_db_cache[db_name]
    result = False
    if db:
        try:
            sample = db._collection.get(limit=24, include=["documents", "metadatas"])
            docs = sample.get("documents") or []
            metas = sample.get("metadatas") or []
            for i, m in enumerate(metas):
                m = m or {}
                text = str(docs[i] if i < len(docs) else "" or "")
                source = str(m.get("source") or "").lower()
                if m.get("price") is not None or m.get("ram_gb") is not None or m.get("gpu_vram_gb") is not None:
                    result = True
                    break
                if re.search(r"/(?:products?|items?)(?:/|$|#)", source) or "/collections/" in source:
                    result = True
                    break
                tl = text.lower()
                if ("rs." in tl or "pkr" in tl or "$" in tl) and ("add to cart" in tl or "shopping cart" in tl):
                    result = True
                    break
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
    used_structured_fields: list[str] = []
    body_fallback_used = False

    def _canonicalize_title(candidate: str) -> str:
        return _canonical_product_title(candidate or "").strip(" -:|")

    def _is_generic_title(candidate: str) -> bool:
        cand = _canonicalize_title(candidate)
        if not cand:
            return True
        if len(cand) < 4:
            return True
        if "|" in cand or "web scraper test sites" in cand.lower():
            return True
        if re.fullmatch(r"[\W_]+", cand):
            return True
        words = re.findall(r"[A-Za-z0-9]+", cand)
        if len(words) <= 1 and re.match(r"(?i)^(?:black|white|blue|grey|gray|silver|gold|red|green|pink|purple|yellow|orange|brown|beige|navy|teal|lavender|maroon|violet|golden|transparent|clear|unknown|default|variant|color|colour)$", cand):
            return True
        return False

    def _dedupe_repeated_phrase(text: str) -> str:
        toks = [t for t in re.split(r"\s+", (text or "").strip()) if t]
        if len(toks) >= 4 and len(toks) % 2 == 0:
            half = len(toks) // 2
            if toks[:half] == toks[half:]:
                return " ".join(toks[:half]).strip(" -:|")
        return (text or "").strip(" -:|")

    def _score_title(candidate: str) -> tuple[int, int, int]:
        cand = _canonicalize_title(candidate)
        if not cand:
            return (0, 0, 0, -10)
        words = re.findall(r"[A-Za-z0-9]+", cand)
        generic = 1 if _is_generic_title(cand) else 0
        has_modelish_token = 1 if re.search(r"(?:\d|[A-Z]{2,}\d|\d+[A-Za-z][A-Za-z0-9\-]*)", cand) else 0
        penalty = 0
        if generic:
            penalty += 3
        if len(words) < 2:
            penalty += 1
        # Prefer non-generic product names first; among those, prefer titles
        # with model-like tokens and then the richer titles.
        return (1 - generic, has_modelish_token, len(words), len(cand), -penalty)

    title_candidates: list[str] = []

    def _push_title(candidate: str, label: str) -> None:
        cand = _canonicalize_title(candidate)
        if not cand:
            return
        if cand not in title_candidates:
            title_candidates.append(cand)
        if label:
            used_structured_fields.append(label)

    def _title_from_body_line(line: str) -> str:
        raw = re.sub(r"\s+", " ", (line or "")).strip(" -:|")
        if not raw:
            return ""
        raw = re.sub(r'(?i)^(?:product|name|title|model)\s*:\s*', "", raw).strip(" -:|")
        # Prefer the model-like phrase immediately after a price marker.
        for pm in re.finditer(r'(?i)(?:\$|rs\.?|pkr)\s*[\d,]+(?:\.\d{1,2})?\s+(.{4,120})', raw):
            tail = pm.group(1).strip(" -:|")
            tail = re.split(
                r'(?i)\s+(?:hdd:|ssd:|ram:|processor:|display:|os:|availability:|reviews?|'
                r'add to cart|wishlist|toggle navigation|cloud scraper|pricing|marketplace|'
                r'learn documentation|video tutorials|test sites|forum|contact us|copyright|description:)\b',
                tail,
            )[0].strip(" -:|")
            tail = tail.split(",")[0].strip(" -:|")
            tail = _dedupe_repeated_phrase(tail)
            tail_candidates = []
            if tail:
                tail_candidates.append(_canonicalize_title(tail))
            tail_spans = re.findall(
                r'([A-Z][A-Za-z0-9&\'"()\-]+(?:\s+[A-Z0-9][A-Za-z0-9&\'"()\-]+){1,8})',
                tail,
            )
            tail_candidates.extend(s.strip(" -:|") for s in tail_spans if s and len(s.strip()) <= 90)
            tail_candidates = [s for s in tail_candidates if s and len(s) <= 120]
            tail_candidates = [s for s in tail_candidates if not _is_generic_title(s)]
            cand = max(tail_candidates, key=_score_title) if tail_candidates else _canonicalize_title(tail)
            if cand and not _is_generic_title(cand):
                return cand

        # Strip obvious spec / boilerplate suffixes from generic lines.
        raw = re.split(
            r'(?i)\s+(?:hdd:|ssd:|ram:|processor:|display:|os:|availability:|reviews?|'
            r'add to cart|wishlist|toggle navigation|cloud scraper|pricing|marketplace|'
            r'learn documentation|video tutorials|test sites|forum|contact us|copyright|description:)\b',
            raw,
        )[0].strip(" -:|")
        raw = raw.split(",")[0].strip(" -:|")
        # On long mixed lines, prefer the longest capitalized product-like span.
        spans = re.findall(
            r'([A-Z][A-Za-z0-9&\'"()\-]+(?:\s+[A-Z0-9][A-Za-z0-9&\'"()\-]+){1,8})',
            raw,
        )
        spans = [s.strip(" -:|") for s in spans if s and len(s.strip()) <= 90]
        spans = [s for s in spans if not _is_generic_title(s)]
        if spans:
            return max(spans, key=_score_title)
        return _canonicalize_title(raw).strip(" -:|")

    def _price_tail_title(text: str) -> str:
        raw = re.sub(r"\s+", " ", (text or "")).strip(" -:|")
        if not raw:
            return ""
        raw = re.sub(r'(?i)^(?:product|name|title|model)\s*:\s*', "", raw).strip(" -:|")
        for pm in re.finditer(r'(?i)(?:\$|rs\.?|pkr)\s*[\d,]+(?:\.\d{1,2})?\s+(.{4,120})', raw):
            tail = pm.group(1).strip(" -:|")
            tail = re.split(
                r'(?i)\s+(?:hdd:|ssd:|ram:|processor:|display:|os:|availability:|reviews?|'
                r'add to cart|wishlist|toggle navigation|cloud scraper|pricing|marketplace|'
                r'learn documentation|video tutorials|test sites|forum|contact us|copyright|description:)\b',
                tail,
            )[0].strip(" -:|")
            tail = tail.split(",")[0].strip(" -:|")
            tail = _dedupe_repeated_phrase(tail)
            tail_candidates = []
            if tail:
                tail_candidates.append(_canonicalize_title(tail))
            tail_spans = re.findall(
                r'([A-Z][A-Za-z0-9&\'"()\-]+(?:\s+[A-Z0-9][A-Za-z0-9&\'"()\-]+){1,8})',
                tail,
            )
            tail_candidates.extend(s.strip(" -:|") for s in tail_spans if s and len(s.strip()) <= 90)
            tail_candidates = [s for s in tail_candidates if s and len(s) <= 120]
            tail_candidates = [s for s in tail_candidates if not _is_generic_title(s)]
            if tail_candidates:
                cand = max(tail_candidates, key=_score_title)
                if cand and not _is_generic_title(cand):
                    return cand
        return ""

    # Prefer a body-derived title when the page title is generic or boilerplate.
    price_title = _price_tail_title(body)
    if price_title:
        _push_title(price_title, "price_title")
    body_lines = [re.sub(r"\s+", " ", ln).strip(" -:|") for ln in re.split(r"[\r\n]+", body) if len(ln.strip()) >= 4]
    for ln in body_lines[:180]:
        cand = _title_from_body_line(ln)
        if not cand or len(cand) > 120:
            continue
        if _is_generic_title(cand):
            continue
        if re.search(r'(?i)\b(?:add to cart|wishlist|reviews?|toggle navigation|cloud scraper|pricing|marketplace|learn documentation|video tutorials|test sites|forum|privacy policy|terms of service|all rights reserved)\b', cand):
            continue
        if re.search(r'(?i)^\s*(?:rs\.?|pkr|\$)\s*[\d,]+', cand):
            continue
        _push_title(cand, "body_title")
        break

    title_match = re.search(r'(?i)\bname:\s*([^\n\.]{4,180})', body)
    if title_match:
        _push_title(title_match.group(1), "name")
    if title_hint:
        _push_title(title_hint, "title_hint")
    slug = (url or "").rstrip("/").split("/")[-1]
    _push_title(re.sub(r'[-_]+', ' ', slug).strip().title(), "slug")

    # If the best title is still generic, try to salvage a better one from the
    # early body lines before falling back to the weak variant.
    if title_candidates:
        title = max(title_candidates, key=_score_title)
        if _is_generic_title(title):
            body_candidates: list[str] = []
            for pat in (
                r'(?i)\bfull specs?\s*:\s*([^\n]+)',
                r'(?i)\bdescription\s*:\s*([^\n]+)',
            ):
                mm = re.search(pat, body)
                if not mm:
                    continue
                cand = _canonicalize_title((mm.group(1) or "").split(",")[0])
                if not cand or len(cand) > 120:
                    continue
                if _is_generic_title(cand):
                    continue
                if re.search(r'(?i)\b(?:price|availability|add to cart|wishlist|reviews?)\b', cand):
                    continue
                if re.search(r'(?i)\b(?:rs\.?|pkr|\$)\s*[\d,]+', cand):
                    continue
                body_candidates.append(cand)
            for ln in body_lines[:220]:
                cand = _title_from_body_line(ln)
                if not cand or len(cand) > 120:
                    continue
                if _is_generic_title(cand):
                    continue
                body_candidates.append(cand)
            for cand in body_candidates:
                _push_title(cand, "body_title")
            non_generic_titles = [cand for cand in title_candidates if not _is_generic_title(cand)]
            if non_generic_titles:
                title = max(non_generic_titles, key=_score_title)
            else:
                title = max(title_candidates, key=_score_title)
    else:
        title = ""
    # On storefront pages, a valid price-tail title is the strongest signal and
    # should win over generic site chrome or breadcrumb noise.
    if price_title and not _is_generic_title(price_title):
        title = price_title

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
    canonical_title = _canonicalize_title(title)
    return {
        "title": title.strip(),
        "canonical_title": canonical_title,
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
    body = cleaned or ""
    metrics = _trusted_content_metrics(body)
    if _looks_like_catalog_page(source, body):
        return "catalog"
    if product_like:
        return "product"
    if structural:
        return "structural"
    # Outcomes/learning-goals marker override (universal):
    # Many docs index pages look "category/structural" but still contain the
    # canonical "By completing..., you will:" goals list. Never classify those
    # as category-only content.
    if re.search(r"(?i)\b(by completing|by the end of this (?:chapter|lesson)|you will be able to|learning outcomes|objectives|goals)\b", body):
        return "article"
    if len(_GENERIC_SECTION_SPLIT_RE.split(body)) >= 3:
        return "faq"
    if _CATEGORY_URL_RE.search(source) or (metrics["category_hits"] >= 2 and metrics["sentence_count"] <= 3):
        return "category"
    if _ARTICLE_URL_RE.search(source) or (
        metrics["prose_chars"] > 300 and metrics["sentence_count"] >= 2 and metrics["nav_hits"] <= max(2, metrics["sentence_count"])
    ):
        return "article"
    if _POLICY_URL_RE.search(source):
        return "policy"
    if _POLICY_TEXT_RE.search(body) and metrics["policy_hits"] >= 2 and metrics["prose_chars"] < 1400 and metrics["sentence_count"] <= 8:
        return "policy"
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
    if page_type == "catalog":
        conf = 0.68 if metrics["category_hits"] or metrics["prose_chars"] > 300 else 0.52
        if metrics["sentence_count"] >= 3:
            conf += 0.08
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
    catalog_like = _looks_like_catalog_page(url, raw)
    product_like = False if catalog_like else _looks_like_product_page(url, raw)
    cleaned = _strip_storefront_boilerplate(raw) if product_like else _dedupe_repeated_lines(raw)
    cleaned = _clean_text(cleaned)
    had_boilerplate = cleaned != raw and bool(_BOILERPLATE_SIGNAL_RE.search(raw))
    structural = _looks_structural_page(url, cleaned)
    page_type = _classify_page_type(url, cleaned, product_like, structural)
    now_iso = datetime.now(timezone.utc).isoformat()
    content_hash = hashlib.sha256(cleaned.encode("utf-8", errors="ignore")).hexdigest() if cleaned else ""
    meta = {
        "structural": structural,
        "page_type": page_type,
        "contaminated": False,
        "used_structured_fields": [],
        "body_fallback_used": False,
        "had_boilerplate": had_boilerplate,
        "extraction_mode": "body_text",
        "crawled_at": now_iso,
        "last_verified_at": now_iso,
        "source_status": "live",
        "content_hash": content_hash,
    }
    meta["page_title"] = _derive_page_title(title_hint, cleaned)
    meta["catalog_listing"] = False
    if catalog_like:
        meta["catalog_listing"] = True
        meta["page_type"] = "catalog"
        meta["page_title"] = _derive_page_title(title_hint, cleaned)
    if product_like:
        product = _extract_product_summary(cleaned, url, title_hint=title_hint)
        meta["product"] = product
        meta["contaminated"] = bool(product.get("contaminated"))
        meta["used_structured_fields"] = list(product.get("used_structured_fields") or [])
        meta["body_fallback_used"] = bool(product.get("body_fallback_used"))
        meta["extraction_mode"] = "structured_product" if meta["used_structured_fields"] else "body_text"
        if product.get("title") and (product.get("price_label") or product.get("description") or meta["used_structured_fields"]):
            meta["page_type"] = "product"
            meta["page_title"] = _derive_page_title(title_hint, cleaned, product)
    elif meta["page_type"] in {"policy", "unknown", "category", "article"}:
        # Product/catalog pages often carry policy/footer boilerplate that can
        # overwhelm the classifier. If structured product signals are present,
        # promote them even when the URL path itself is generic.
        product = _extract_product_summary(cleaned, url, title_hint=title_hint)
        multiple_price_hits = len(_PROD_PRICE_CAPTURE_RE.findall(cleaned)) + len(_PRODUCT_PRICE_LINE_RE.findall(cleaned))
        if product.get("title") and multiple_price_hits >= 2:
            meta["catalog_listing"] = True
            meta["catalog_item_count"] = multiple_price_hits
            meta["page_type"] = "catalog"
            meta["page_title"] = _derive_page_title(title_hint, cleaned, product)
        elif product.get("title") and (product.get("price_label") or product.get("description") or product.get("used_structured_fields")):
            meta["product"] = product
            meta["page_type"] = "product"
            meta["contaminated"] = bool(product.get("contaminated"))
            meta["used_structured_fields"] = list(product.get("used_structured_fields") or [])
            meta["body_fallback_used"] = bool(product.get("body_fallback_used"))
            meta["extraction_mode"] = "structured_product" if meta["used_structured_fields"] else "body_text"
            meta["page_title"] = _derive_page_title(title_hint, cleaned, product)
        else:
            meta["page_title"] = _derive_page_title(title_hint, cleaned)
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
        # Large structural/category pages are often the canonical overview pages
        # for doc sets. Keep them searchable when they have real body content.
        if len(cleaned.split()) > 150:
            retrieve_eligible = True
            quarantine_reason = ""
        else:
            retrieve_eligible = False
            quarantine_reason = meta["page_type"]
    elif meta.get("contaminated"):
        retrieve_eligible = False
        quarantine_reason = "contaminated"
    elif meta["page_type"] == "product" and meta.get("body_fallback_used") and len(meta.get("used_structured_fields") or []) < 2:
        retrieve_eligible = False
        meta["quality_score"] = min(float(meta.get("quality_score") or 0.0), 0.35)
        quarantine_reason = "weak_product_fallback"
    elif meta["page_type"] in {"product", "catalog", "unknown"} and float(meta.get("quality_score") or 0.0) < 0.5:
        # Article/faq/policy pages have no structured fields so their score
        # is capped at 0.4 by design â€” don't quarantine them on score alone.
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


# â”€â”€ Product spec extraction regexes (used by smart chunker) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_PROD_PRICE_RE = re.compile(r'(?i)(?:\$|rs\.?\s*|pkr\s*)(\d[\d,]*\.?\d*)')
_PROD_SPEC_RE = re.compile(r'\b(?:processor|cpu|ram|memory|storage|ssd|hdd|gpu|graphics|display|battery|os|android|windows|linux|screen)\b', re.I)
_PROD_SPLIT_RE = re.compile(r'(?i)(?:\$|rs\.?\s*|pkr\s*)(\d[\d,]*\.?\d*)\s+([A-Z][A-Za-z0-9 \(\)\-\.]+?(?:,[^\$]{10,400}?))(?=\s*(?:\$|rs\.?\s*|pkr\s*)|\s*\Z)', re.S)
_FAQ_SPLIT_RE = re.compile(r'(?m)^(?=(?:Q:|Question:|How |What |Why |When |Where |Who |Can |Do |Is |Are |Does |Should ))', re.I)


def _smart_chunk_page(text: str, url: str, chunk_size: int = 400, chunk_step: int = 320, page_meta: dict | None = None) -> list:
    from langchain_core.documents import Document
    """
    Smart page chunker. Three modes, tried in order:
      1. Product page  â€” $PRICE + spec keywords â†’ one Document per product, price metadata
      2. FAQ page      â€” Q&A or heading sections â†’ one Document per section
      3. Generic       â€” existing word-based sliding window (unchanged fallback)
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
    _canon_source = _canonical_source_url(str((page_meta or {}).get("source_canonical") or url or ""))
    _base_meta = {"source": (_canon_source or url), "source_canonical": (_canon_source or str(url or ""))}
    for _k in ("crawled_at", "last_verified_at", "source_status", "content_hash"):
        if page_meta.get(_k) is not None:
            _base_meta[_k] = page_meta.get(_k)
    if page_meta.get("structural"):
        _base_meta["structural"] = True
    if page_meta.get("contaminated"):
        _base_meta["contaminated"] = True
    if page_meta.get("page_type"):
        _base_meta["content_type"] = page_meta["page_type"]
    if page_meta.get("page_title"):
        # Helps disambiguate chapter/part/policy pages with similar boilerplate phrases.
        _base_meta["page_title"] = str(page_meta.get("page_title") or "")[:200]
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

    def _finalize_docs(out_docs: list) -> list:
        for _idx, _doc in enumerate(out_docs or []):
            _m = dict(getattr(_doc, "metadata", None) or {})
            _m["chunk_index"] = int(_idx)
            _m["section_id"] = str(_m.get("section_id") or f"s{_idx}")
            if not _m.get("chunk_kind"):
                sid = str(_m.get("section_id") or "").lower()
                if sid.startswith("product_"):
                    _m["chunk_kind"] = "product"
                elif sid.startswith("catalog_"):
                    _m["chunk_kind"] = "catalog"
                elif sid.startswith("category_"):
                    _m["chunk_kind"] = "category"
                elif sid.startswith("faq_"):
                    _m["chunk_kind"] = "faq"
                elif sid.startswith("head_"):
                    _m["chunk_kind"] = "heading"
                elif sid.startswith("para_"):
                    _m["chunk_kind"] = "paragraph"
                elif sid.startswith("table_"):
                    _m["chunk_kind"] = "tabular"
                elif sid.startswith("list_"):
                    _m["chunk_kind"] = "list"
                else:
                    _m["chunk_kind"] = "generic"
            _sample = str(getattr(_doc, "page_content", "") or "")
            _m["chunk_hash"] = hashlib.sha256(_sample.encode("utf-8", errors="ignore")).hexdigest()
            _doc.metadata = _m
        return out_docs

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
        _prod_meta["canonical_product_title"] = product_meta.get("canonical_title") or _canonical_product_title(product_meta["title"])
        _prod_meta["page_title"] = product_meta.get("title") or _base_meta.get("page_title") or ""
        if product_meta.get("price_num") is not None:
            _prod_meta["price"] = product_meta["price_num"]
        if product_meta.get("availability"):
            _prod_meta["availability"] = product_meta["availability"]
        _prod_meta["used_structured_fields"] = list(product_meta.get("used_structured_fields") or _base_meta.get("used_structured_fields") or [])
        _prod_meta["body_fallback_used"] = bool(product_meta.get("body_fallback_used"))
        _prod_meta.update(_extract_product_metadata(_prod_text))
        _prod_meta["section_id"] = "product_0"
        _prod_meta["chunk_kind"] = "product"
        docs.append(Document(page_content=_prod_text, metadata=_prod_meta))
        return _finalize_docs(_merge_variant_docs(docs))

    # Mode 1b: catalog/listing page â€” split repeated product cards into per-item chunks.
    if page_meta.get("catalog_listing") or len(_PROD_PRICE_RE.findall(clean)) >= 2:
        products = []
        for m in _PROD_SPLIT_RE.finditer(clean):
            price_str = m.group(1).replace(',', '')
            try:
                price_num = float(price_str)
            except Exception:
                continue
            raw = m.group(2).strip()
            comma_idx = raw.find(',')
            name = raw[:comma_idx].strip() if comma_idx > 0 else raw
            specs = raw[comma_idx + 1:].strip() if comma_idx > 0 else ''
            name = re.sub(r'^[^\s]+(?:\s+[^\s]+){0,3}\.{2,}\s+', '', name).strip()
            name = re.sub(r'\s+reviews?\s*$', '', name, flags=re.I).strip()
            if not name or len(name) < 3:
                continue
            products.append((price_num, f"${price_str}", name, specs))

        if products:
            for price_num, price_label, name, specs in products:
                lines = [f"Product: {name}", f"Price: {price_label}"]
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
                _card_text = "\n".join(lines).strip()
                _card_meta = dict(_base_meta)
                _card_meta["product_title"] = name
                _card_meta["canonical_product_title"] = _canonical_product_title(name)
                _card_meta["page_title"] = _base_meta.get("page_title") or name
                _card_meta["price"] = price_num
                _card_meta["section_id"] = f"catalog_{len(docs)}"
                _card_meta["chunk_kind"] = "catalog"
                _card_meta["catalog_listing"] = True
                _card_meta.update(_extract_product_metadata(_card_text))
                docs.append(Document(page_content=_card_text, metadata=_card_meta))
            if docs:
                return _finalize_docs(_merge_variant_docs(docs))

        if len(clean.split()) > 80:
            _cat_meta = dict(_base_meta)
            _cat_meta["section_id"] = "category_0"
            _cat_meta["chunk_kind"] = "category"
            _cat_meta["catalog_listing"] = True
            _cat_meta["page_title"] = _base_meta.get("page_title") or _derive_page_title(title_hint, clean)
            docs.append(Document(page_content=clean[:5000], metadata=_cat_meta))
            return _finalize_docs(docs)

    # â”€â”€ Mode 1: product page â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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
                # â”€â”€ Attribute normalization: inject user-vocabulary tags â”€â”€â”€â”€â”€â”€
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
                _prod_meta["canonical_product_title"] = _canonical_product_title(name)
                _prod_meta.update(_extract_product_metadata(_prod_text))
                _prod_meta["section_id"] = f"product_{len(docs)}"
                docs.append(Document(page_content=_prod_text, metadata=_prod_meta))
            if docs:
                return _finalize_docs(_merge_variant_docs(docs))

    # Mode 2: FAQ / section page
    sections = _FAQ_SPLIT_RE.split(clean)
    if len(sections) >= 3:
        for sec in sections:
            sec = sec.strip()
            if len(sec) > 40:
                _m2 = dict(_base_meta)
                _m2["section_id"] = f"faq_{len(docs)}"
                docs.append(Document(page_content=sec[:2000], metadata=_m2))
        if docs:
            return _finalize_docs(docs)

    # Mode 2b: heading-aware page
    try:
        _heading_hits = list(re.finditer(r"(?m)^(?:##|###)\s+", raw_text))
        if _heading_hits:
            _starts = [m.start() for m in _heading_hits] + [len(raw_text)]

            _pending_small_segments: list[tuple[str, str | None]] = []

            def _emit_docs_from_text(seg_text: str, heading: str | None):
                seg_text = (seg_text or "").strip()
                if not seg_text:
                    return
                if heading and not re.match(r"(?m)^\s*(?:##|###)\s+", seg_text):
                    seg_text = f"## {heading}\n\n{seg_text}"
                if len(seg_text) <= 1600:
                    _meta = dict(_base_meta)
                    _meta["section_id"] = f"head_{len(docs)}"
                    _meta["chunk_kind"] = "heading"
                    if heading:
                        _meta["heading"] = heading
                    docs.append(Document(page_content=seg_text, metadata=_meta))
                    return

                # Large sections: split on paragraphs / sentence boundaries and
                # keep a small overlap so answers spanning a boundary are not lost.
                blocks = [p.strip() for p in re.split(r"\n{2,}|(?<=[.?!])\s{2,}", seg_text) if p.strip()]
                if len(blocks) <= 1:
                    blocks = [seg_text[i:i + 1200] for i in range(0, len(seg_text), 1200)]
                packed = []
                buf = []
                buf_chars = 0
                for blk in blocks:
                    if buf and (buf_chars + len(blk) + 2 > 1400):
                        packed.append("\n\n".join(buf).strip())
                        tail = packed[-1][-180:].strip()
                        buf = [tail] if tail else []
                        buf_chars = len(tail) + (2 if tail else 0)
                    buf.append(blk)
                    buf_chars += len(blk) + 2
                if buf:
                    packed.append("\n\n".join(buf).strip())

                for idx, piece in enumerate(packed):
                    if not piece:
                        continue
                    if idx and packed[idx - 1]:
                        prev_tail = packed[idx - 1][-120:].strip()
                        if prev_tail and not piece.startswith(prev_tail):
                            piece = f"{prev_tail}\n\n{piece}"
                    _meta = dict(_base_meta)
                    _meta["section_id"] = f"head_{len(docs)}"
                    _meta["chunk_kind"] = "heading"
                    if heading:
                        _meta["heading"] = heading
                    docs.append(Document(page_content=piece, metadata=_meta))

            def _flush_small_segments():
                nonlocal _pending_small_segments
                if not _pending_small_segments:
                    return
                merged_parts = []
                for seg_text, seg_heading in _pending_small_segments:
                    seg_text = (seg_text or "").strip()
                    if not seg_text:
                        continue
                    if seg_heading and not re.match(r"(?m)^\s*(?:##|###)\s+", seg_text):
                        seg_text = f"## {seg_heading}\n\n{seg_text}"
                    merged_parts.append(seg_text)
                merged = "\n\n".join(merged_parts).strip()
                _pending_small_segments = []
                if merged:
                    _emit_docs_from_text(merged, None)

            for idx, start in enumerate(_starts[:-1]):
                seg = raw_text[start:_starts[idx + 1]].strip()
                hm = re.match(r"^(?:##|###)\s+(.+?)\s*(?=\Z|(?:##|###)\s+)", seg, re.S)
                heading = hm.group(1).strip() if hm else None
                if len(seg) <= 280:
                    _pending_small_segments.append((seg, heading))
                    continue
                _flush_small_segments()
                _emit_docs_from_text(seg, heading)
            _flush_small_segments()
            if docs:
                return _finalize_docs(docs)
    except Exception:
        pass

    # Mode 2c: paragraph-aware grouping
    try:
        _paras = [p.strip() for p in re.split(r"\n{2,}", raw_text) if p and p.strip()]
        if len(_paras) >= 2:
            _seed_heading = str((page_meta or {}).get("page_title") or "").strip()
            if _seed_heading and re.search(r"(?i)(web scraper test sites|all rights reserved|privacy policy|terms of service|home\s*\|\s*[^|]+)$", _seed_heading):
                _seed_heading = ""
            _last_heading = _seed_heading
            _buf: list[str] = []
            _chars = 0
            _last_emitted: str | None = None

            def _emit_para_chunk(chunk_text: str):
                nonlocal _last_emitted
                chunk_text = (chunk_text or "").strip()
                if not chunk_text:
                    return
                if _last_emitted:
                    tail = _last_emitted[-120:].strip()
                    if tail and not chunk_text.startswith(tail):
                        chunk_text = f"{tail}\n\n{chunk_text}"
                _last_emitted = chunk_text
                _meta = dict(_base_meta)
                _meta["section_id"] = f"para_{len(docs)}"
                _meta["chunk_kind"] = "paragraph"
                if _last_heading:
                    _meta["heading"] = _last_heading
                docs.append(Document(page_content=chunk_text, metadata=_meta))

            def _flush_para_buf():
                nonlocal _buf, _chars
                if not _buf:
                    return
                _chunk = "\n\n".join(_buf).strip()
                if not _chunk:
                    _buf, _chars = [], 0
                    return
                if _last_heading and not re.match(r"(?m)^\s*(?:##|###)\s+", _chunk):
                    _chunk = f"## {_last_heading}\n\n{_chunk}"
                _emit_para_chunk(_chunk)
                _buf, _chars = [], 0
            for para in _paras:
                _hm = re.match(r"^\s*(?:##|###)\s+(.+?)\s*$", para)
                if _hm:
                    _flush_para_buf()
                    _last_heading = _hm.group(1).strip()
                    continue
                _para_txt = para
                if _buf and (_chars + len(_para_txt) + 2 > 1600):
                    _flush_para_buf()
                _buf.append(_para_txt)
                _chars += len(_para_txt) + 2
            _flush_para_buf()
            if docs:
                return _finalize_docs(docs)
    except Exception:
        pass

    # Mode 2d: tabular / key-value / code-ish page
    try:
        _lines = [ln.rstrip() for ln in raw_text.splitlines()]
        _nonempty_lines = [ln.strip() for ln in _lines if ln.strip()]

        def _is_rowish(ln: str) -> bool:
            s = ln.strip()
            if not s:
                return False
            if re.match(r"^\s*(?:##|###)\s+", s):
                return False
            if s.startswith(("```", "~~~")):
                return True
            if "\t" in s or " | " in s:
                return True
            if re.match(r"^[A-Za-z][A-Za-z0-9 _/\-]{1,40}\s*:\s+\S", s):
                return True
            if re.match(r"^[A-Za-z][A-Za-z0-9 _/\-]{1,40}\s*=\s*\S", s):
                return True
            if re.match(r"^(?:[-*•]|\d{1,2}[.)])\s+\S", s):
                return True
            if re.match(r"^\s{2,}\S", ln):
                return True
            return False

        _row_lines = [ln for ln in _nonempty_lines if _is_rowish(ln)]
        if len(_row_lines) >= 4 and len(_row_lines) >= max(4, int(len(_nonempty_lines) * 0.45)):
            _last_heading = ""
            _buf: list[str] = []
            _chars = 0

            def _flush_row_buf():
                nonlocal _buf, _chars
                if not _buf:
                    return
                _chunk = "\n".join(_buf).strip()
                if _last_heading and not re.match(r"(?m)^\s*(?:##|###)\s+", _chunk):
                    _chunk = f"## {_last_heading}\n\n{_chunk}"
                _meta = dict(_base_meta)
                _meta["section_id"] = f"table_{len(docs)}"
                _meta["chunk_kind"] = "tabular"
                if _last_heading:
                    _meta["heading"] = _last_heading
                docs.append(Document(page_content=_chunk[:2400], metadata=_meta))
                _buf, _chars = [], 0

            for ln in _lines:
                s = ln.strip()
                if not s:
                    continue
                hm = re.match(r"^\s*(?:##|###)\s+(.+?)\s*$", s)
                if hm:
                    _flush_row_buf()
                    _last_heading = hm.group(1).strip()
                    continue
                if not _is_rowish(ln):
                    continue
                if _buf and (_chars + len(s) + 1 > 1600):
                    _flush_row_buf()
                _buf.append(s)
                _chars += len(s) + 1
            _flush_row_buf()
            if docs:
                return _finalize_docs(docs)
    except Exception:
        pass

    # Mode 2.5: bullet/list page
    try:
        _raw_lines = [ln.strip() for ln in raw_text.split("\n") if ln and ln.strip()]
        _bullet_re = re.compile(r"^(?:[-*•]|\d{1,2}[.)])\s+")
        _bullet_lines = [ln for ln in _raw_lines if _bullet_re.match(ln)]
        if len(_bullet_lines) >= 3:
            buf = []
            buf_chars = 0
            for ln in _raw_lines:
                if len(ln) > 500:
                    continue
                if not _bullet_re.match(ln):
                    continue
                if buf and (buf_chars + len(ln) + 1 > 2000):
                    _m3 = dict(_base_meta)
                    _m3["section_id"] = f"list_{len(docs)}"
                    _m3["chunk_kind"] = "list"
                    docs.append(Document(page_content="\n".join(buf).strip(), metadata=_m3))
                    buf, buf_chars = [], 0
                buf.append(ln)
                buf_chars += len(ln) + 1
            if buf:
                _m4 = dict(_base_meta)
                _m4["section_id"] = f"list_{len(docs)}"
                _m4["chunk_kind"] = "list"
                docs.append(Document(page_content="\n".join(buf).strip(), metadata=_m4))
            if docs:
                return _finalize_docs(docs)
    except Exception:
        pass

    # Mode 3: generic word-split (existing behaviour — unchanged)
    words = clean.split()
    for j in range(0, max(1, len(words)), chunk_step):
        chunk = " ".join(words[j:j + chunk_size])
        if len(chunk) > 20:
            _m5 = dict(_base_meta)
            _m5["section_id"] = f"generic_{len(docs)}"
            docs.append(Document(page_content=chunk, metadata=_m5))
        if j + chunk_size >= len(words):
            break
    return _finalize_docs(docs)


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
