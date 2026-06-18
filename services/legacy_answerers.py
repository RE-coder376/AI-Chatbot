"""Kept-aside deterministic per-question-shape product answerers.

Superseded by the universal structured-catalog engine
(``services.catalog_query.answer_catalog_query``), which is the LEAD structured
path. These remain wired in app.py ONLY as fallbacks for catalogs that lack the
structured product metadata the engine needs. Moved out of app.py 2026-06-14 to
keep the hot path lean; preserved verbatim so they can be re-promoted or removed
once the engine is validated across every DB (incl. webscraper.io/$ stores).

Shapes:
  _deterministic_product_bounds_answer  — "products under Rs.X" (metadata scan)
  _deterministic_count_answer           — "how many X" / "do you sell X" (+absence)
  _deterministic_product_catalog_answer — cheapest/priciest/list from kb_context
"""
import re


def _extract_product_name_phrase(q: str) -> str:
    # Late import: app imports this module at load; this resolves at call time.
    from app import _extract_product_name_phrase as _impl
    return _impl(q)


def _deterministic_product_bounds_answer(q: str, db, max_items: int = 8) -> tuple[str | None, list[str]]:
    """Answer product price-cap queries directly from Chroma metadata.

    Retrieval already has a bounds scan, but widget-key tenant requests can still
    reach the sparse-KB path without those docs becoming the final answer. This
    deterministic pass makes "products under Rs.X" independent of LLM context
    ordering and uses structured product metadata where available.
    """
    try:
        if not q or db is None:
            return None, []
        ql = (q or "").lower()
        pm = re.search(r"(?:under|below|less\s+than|max(?:imum)?|budget\s+of|within)\s+(?:rs\.?\s*|pkr\s*)?[\$£€]?([\d,]+)", ql, re.I)
        if not pm:
            return None, []
        try:
            max_price = float(pm.group(1).replace(",", ""))
        except Exception:
            return None, []
        if max_price <= 0:
            return None, []
        if not re.search(r"\b(products?|items?|sell|have|available|availability|under|below|less\s+than|within|budget|price|prices|cost)\b", ql, re.I):
            return None, []

        stop = {
            "show", "list", "which", "what", "price", "prices", "cost", "available", "availability",
            "stock", "under", "below", "sell", "sells", "products", "product", "items", "item",
            "have", "with", "best", "top", "affordable", "currently", "school", "baby", "kids",
            "toy", "toys", "cars", "are", "rs", "pkr", "than", "less", "within", "maximum",
            "max", "rupees", "do", "does", "you", "your", "we", "our", "in", "on", "at", "of",
            "for", "and", "or", "the", "a", "an", "there", "store", "shop", "catalog", "catalogue",
        }
        phrase = _extract_product_name_phrase(q or "") or ql
        anchors = [w for w in re.findall(r"[a-zA-Z]{2,}", phrase.lower()) if w not in stop][:8]

        def _anchor_hits(hay: str) -> int:
            if not anchors:
                return 0
            hits = 0
            for a in anchors:
                h = hay.lower()
                if a == "rc":
                    if re.search(r"\brc\b", h) or "remote control" in h or (re.search(r"\bremote\b", h) and re.search(r"\bcontrol(?:led)?\b", h)):
                        hits += 1
                    continue
                variants = {a}
                if a.endswith("s") and len(a) > 3:
                    variants.add(a[:-1])
                elif len(a) > 3:
                    variants.add(a + "s")
                if any(re.search(rf"\b{re.escape(v)}\b", h) for v in variants):
                    hits += 1
            return hits

        def _first_price(doc: str, meta: dict) -> float | None:
            try:
                mv = meta.get("price")
                if mv is not None and float(mv) > 0:
                    return float(mv)
            except Exception:
                pass
            m = re.search(r"(?im)^price:\s*(?:rs\.?|pkr|\$|£|€)?\s*([\d,]+(?:\.\d{1,2})?)", doc or "")
            if m:
                try:
                    val = float(m.group(1).replace(",", ""))
                    if val > 0:
                        return val
                except Exception:
                    pass
            # Do not scan arbitrary body text here: model names/descriptions such
            # as "DJI RS 3" are exactly how sub-Rs.10 phantom products were born.
            return None

        def _title_from(doc: str, meta: dict) -> str:
            for key in ("canonical_product_title", "product_title", "page_title"):
                t = str(meta.get(key) or "").strip()
                if t:
                    return re.sub(r"\s+", " ", t).strip(" -:|")
            m = re.search(r"(?im)^product:\s*([^\n]{3,120})", doc or "")
            if m:
                return re.sub(r"\s+", " ", m.group(1)).strip(" -:|")
            return ""

        try:
            total = min(int(db._collection.count() or 0), 12000)
            raw = db._collection.get(limit=total, include=["documents", "metadatas"])
        except Exception:
            return None, []

        seen_titles: set[str] = set()
        candidates: list[tuple[float, float, str, str, str]] = []
        for doc, meta in zip(raw.get("documents", []) or [], raw.get("metadatas", []) or []):
            meta = meta or {}
            doc = str(doc or "")
            if not doc:
                continue
            source = str(meta.get("source") or meta.get("source_canonical") or "")
            src_l = source.lower()
            kind = str(meta.get("chunk_kind") or "").lower()
            ctype = str(meta.get("content_type") or "").lower()
            title = _title_from(doc, meta)
            title_l = title.lower()
            if not title or title_l in {"fun", "home", "shop", "catalog", "catalogue", "products", "collection"}:
                continue
            if len(re.findall(r"[a-zA-Z0-9]+", title)) <= 1 and not re.search(r"\d", title):
                continue
            is_productish = (
                bool(re.search(r"/(?:products?|items?)(?:/|$|#)", src_l))
                or (kind == "product" and ctype == "product" and "/collections/" not in src_l)
            )
            if not is_productish:
                continue
            if "sitemap" in src_l and not re.search(r"/(?:products?|items?)(?:/|$|#)", src_l):
                continue
            price = _first_price(doc, meta)
            if price is None or price <= 0 or price > max_price:
                continue
            hay = " ".join([title, source, str(meta.get("categories") or ""), doc[:1200]])
            hits = _anchor_hits(hay)
            if anchors and hits <= 0:
                continue
            key = re.sub(r"[^a-z0-9]+", " ", title_l).strip()
            if not key or key in seen_titles:
                continue
            seen_titles.add(key)
            score = (hits * 20.0) + (10.0 if kind == "product" else 4.0 if kind == "catalog" else 0.0)
            # With category+cap queries, users usually want the strongest matching
            # products near the cap; with pure cap queries, cheapest-first is clearer.
            if anchors:
                score += max(0.0, 8.0 - abs(price - max_price) / max(1.0, max_price / 4.0))
            availability = str(meta.get("availability") or "").strip()
            if not availability:
                availability = "out of stock" if re.search(r"(?i)\b(out of stock|sold out|currently unavailable)\b", doc) else "available"
            candidates.append((score, price, title, availability, source))

        if not candidates:
            return None, []
        if anchors:
            candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)
        else:
            candidates.sort(key=lambda x: (x[1], x[2].lower()))
        items = candidates[:max_items]
        lines = ["Here are matching products from the catalog:"]
        sources: list[str] = []
        for idx, (_, price, title, availability, source) in enumerate(items, 1):
            price_s = f"Rs.{price:,.0f}" if float(price).is_integer() else f"Rs.{price:,.2f}"
            lines.append(f"{idx}. {title} - {price_s} - {availability}")
            if source:
                sources.append(source)
        return "\n".join(lines), list(dict.fromkeys(sources))[:max_items]
    except Exception:
        return None, []


def _deterministic_count_answer(q: str, db, cfg: dict | None = None, max_list: int = 12) -> tuple[str | None, list[str]]:
    """Answer 'how many X' and 'do you sell/carry X' (incl. absence) from a full
    catalog scan. Counts are impossible from top-k context, and similarity never
    surfaces a 'no results' page — so absence reads as wrong items without this.
    Runs on the tenant DB before the LLM, so it bypasses the sparse-KB guard.

    Accessory/refill words that inflate a category count ("Stapler Pin", ink
    "Cartridge") are NOT hardcoded here — they live per-DB in config
    `count_exclude_terms`, so the engine stays domain-agnostic. A term is only
    excluded when the user did not ask for it (so 'how many stapler pins' works)."""
    try:
        if not q or db is None:
            return None, []
        ql = (q or "").lower()
        # Price/rank/bounds phrasings belong to the bounds/catalog answerers.
        if re.search(r"\b(price|prices|cost|how\s+much|cheap(?:est)?|expensive|priciest|under|below|less\s+than|within|budget|over|above|between)\b", ql):
            return None, []
        _count_q = bool(re.search(r"\b(how\s+many|number\s+of|count\s+of)\b", ql))
        _exist_q = bool(re.search(
            r"\bdo(?:es)?\s+(?:you|we|they|the\s+store)\b.*\b(?:sell|stock|carry|carries|have|has|offer|got|keep)\b"
            r"|\b(?:are|is)\s+there\s+(?:any|some)\b|\bany\b.*\b(?:available|in\s+stock)\b"
            r"|\bdo\s+you\s+(?:sell|stock|carry|have|offer)\b", ql))
        if not (_count_q or _exist_q):
            return None, []
        stop = {
            "how", "many", "number", "count", "of", "do", "does", "you", "your", "we", "our",
            "they", "the", "store", "sell", "sells", "stock", "carry", "carries", "have", "has",
            "offer", "offers", "got", "keep", "any", "some", "are", "is", "there", "available",
            "in", "products", "product", "items", "item", "different", "types", "type", "kinds",
            "kind", "total", "catalog", "catalogue", "shop", "and", "or", "for", "with", "rs",
            "pkr", "currently", "stocked", "range", "selection", "what", "which", "sale", "your",
        }
        # Anchors come straight from the question — _extract_product_name_phrase is
        # tuned for "price of X" exact-name lookups and returns a trailing phrase
        # ("do you have") for count/existence questions, leaving zero anchors.
        anchors = [w for w in re.findall(r"[a-zA-Z]{3,}", ql) if w not in stop][:4]
        if not anchors:
            return None, []
        # Per-DB accessory/refill terms (domain vocab, not engine logic). Skip a
        # term if the user actually asked for it (anchor), so it stays a no-op for
        # "how many stapler pins".
        _anchor_set = set(anchors) | {a.rstrip("s") for a in anchors}
        _excl = [str(t).lower().strip() for t in ((cfg or {}).get("count_exclude_terms") or [])
                 if str(t).lower().strip() and str(t).lower().strip().rstrip("s") not in _anchor_set]
        _excl_re = re.compile(r"\b(?:" + "|".join(re.escape(t) for t in _excl) + r")\b", re.I) if _excl else None

        def _hit(hay: str) -> bool:
            h = hay.lower()
            for a in anchors:  # ALL anchors must hit (a "gel pen" query needs both)
                variants = {a}
                if a.endswith("s") and len(a) > 3:
                    variants.add(a[:-1])
                elif len(a) > 3:
                    variants.add(a + "s")
                if not any(re.search(rf"\b{re.escape(v)}\b", h) for v in variants):
                    return False
            return True

        try:
            total = min(int(db._collection.count() or 0), 12000)
            raw = db._collection.get(limit=total, include=["documents", "metadatas"])
        except Exception:
            return None, []
        seen: dict[str, tuple[str, str]] = {}
        for doc, meta in zip(raw.get("documents", []) or [], raw.get("metadatas", []) or []):
            meta = meta or {}
            if not str(doc or ""):
                continue
            src_l = str(meta.get("source") or meta.get("source_canonical") or "").lower()
            kind = str(meta.get("chunk_kind") or "").lower()
            ctype = str(meta.get("content_type") or "").lower()
            is_productish = (
                bool(re.search(r"/(?:products?|items?)(?:/|$|#)", src_l))
                or (kind == "product" and ctype == "product" and "/collections/" not in src_l)
            )
            if not is_productish:
                continue
            title = ""
            for key in ("canonical_product_title", "product_title", "page_title"):
                t = str(meta.get(key) or "").strip()
                if t:
                    title = re.sub(r"\s+", " ", t).strip(" -:|")
                    break
            if not title or len(title) < 4:
                continue
            if _excl_re and _excl_re.search(title):
                continue  # accessory/refill (per-DB count_exclude_terms), not the product
            if not _hit(" ".join([title, str(meta.get("categories") or "")])):
                continue
            norm = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
            if norm and norm not in seen:
                seen[norm] = (title, str(meta.get("source") or ""))
        label = " ".join(anchors)
        n = len(seen)
        if n == 0:
            if _exist_q:
                return (f"No — we don't currently carry any {label}. I couldn't find any "
                        f"{label} in our catalog. Is there something else I can help you find?"), []
            return (f"We don't currently have any {label} listed in our catalog."), []
        rows = list(seen.values())
        body = "\n".join(f"- {t}" for t, _ in rows[:max_list])
        more = f"\n…and {n - max_list} more." if n > max_list else ""
        if _count_q:
            head = f"We have {n} {label} product{'s' if n != 1 else ''} in our catalog:"
        else:
            head = f"Yes — we carry {label}. We have {n} option{'s' if n != 1 else ''}:"
        srcs = list(dict.fromkeys(s for _, s in rows if s))[:max_list]
        return f"{head}\n{body}{more}", srcs
    except Exception:
        return None, []


def _deterministic_product_catalog_answer(q: str, kb_context: str, max_items: int = 5) -> str | None:
    """Build a concise product answer directly from retrieved catalog text."""
    try:
        if not q or not kb_context:
            return None
        ql = q.lower()
        # A comparison ("compare A and B", "X vs Y") must reach the LLM to
        # synthesize a side-by-side answer over BOTH products; this single-best
        # catalog list would drop one. Step aside so the comparison fan-out +
        # LLM path (which already places both products in context) handles it.
        if (re.search(r"\b(compare|compared|comparison|versus|vs\.?|difference\s+between)\b", ql)
                and re.search(r"\b(and|or|vs\.?|versus|than)\b", ql)):
            return None
        rank_mode = None
        if re.search(r"\b(cheapest|lowest\s+price|lowest\s+priced|least\s+expensive|most\s+affordable|budget)\b", ql, re.I):
            rank_mode = "asc"
        elif re.search(r"\b(most\s+expensive|highest\s+price|highest\s+priced|priciest|costliest|most\s+costly)\b", ql, re.I):
            rank_mode = "desc"
        if not re.search(
            r"\b(show|list|which|what|price|prices|cost|available|availability|stock|under|below|sell|products?|toys?|pens?|notebooks?|cars?)\b",
            ql,
            re.I,
        ):
            return None
        max_price = None
        pm = re.search(r"(?:under|below|less than|max(?:imum)?|within)\s+(?:rs\.?\s*)?([\d,]+)", ql, re.I)
        if pm:
            try:
                max_price = float(pm.group(1).replace(",", ""))
            except Exception:
                max_price = None
        stop = {
            "show", "list", "which", "what", "price", "prices", "cost", "available", "availability",
            "stock", "under", "below", "sell", "products", "product", "toys", "have", "with",
            "best", "top", "affordable", "currently", "school", "baby", "kids", "toy", "cars",
            "are", "rs", "pkr", "than", "less", "within", "maximum", "max", "rupees"
        }
        rank_stop = stop | {"cheapest", "lowest", "least", "expensive", "highest", "priced", "priciest", "costliest", "most", "among", "listed", "laptops", "laptop",
                            "is", "the", "you", "your", "yours", "our", "ours", "a", "an", "in", "on", "at",
                            "do", "does", "we", "me", "my", "it", "its", "of", "or", "to", "us", "and", "for",
                            "can", "buy", "get", "from", "there", "here", "store", "shop", "site", "whats",
                            # Catalog-scope words, not category anchors: "most expensive
                            # product in your ENTIRE store" must rank the whole catalog,
                            # not docs that happen to contain the word "entire".
                            "entire", "whole", "absolute", "absolutely", "overall", "anything",
                            "everything", "item", "items", "carry", "offer", "offers", "offered",
                            "catalog", "catalogue", "inventory", "range", "selection", "second"}
        _prod_phrase = _extract_product_name_phrase(q or "")
        anchors = [w for w in re.findall(r"[a-zA-Z]{2,}", (_prod_phrase or ql)) if w not in (rank_stop if rank_mode else stop)][:10]
        # Combined intent (category word + price cap, e.g. "RC products under
        # Rs.5000"): this parser's context may lack the category's items entirely
        # (proven: RC chunks absent). Step aside — the LLM path reads the
        # retrieval bounds-scan docs and answers these correctly (verified on N7).
        if max_price is not None and anchors and not rank_mode:
            return None
        exact_phrase = re.sub(r"\s{2,}", " ", (_prod_phrase or "").strip(" -:|")).lower()
        exact_tokens = [w for w in re.findall(r"[a-zA-Z0-9]{3,}", exact_phrase) if w not in stop]
        docs = [d.strip() for d in re.split(r"\n\s*\n", kb_context or "") if d.strip()]
        candidates = []
        nav_bad = ("about us", "faq", "contact us", "privacy policy", "terms and conditions", "return & exchange")
        for doc in docs:
            dl = doc.lower()
            if not ("rs." in dl or "rs " in dl):
                continue
            if not ("add to cart" in dl or "shopping cart" in dl or "sale price" in dl or "regular price" in dl
                    or "price:" in dl or ("product:" in dl and "rs." in dl)):
                continue
            if any(b in dl[:180] for b in nav_bad) and "add to cart" not in dl:
                continue
            title = ""
            m = re.search(r"^\s*([^.\n]{4,90}?)(?:\s*&ndash;|\s+-\s+|\s+–\s+)", doc)
            if m:
                title = m.group(1).strip()
            if not title:
                m = re.search(r"Shopping Cart\s+([^.\n]{4,90}?)\s+Rs\.?", doc, re.I)
                if m:
                    title = m.group(1).strip()
            if not title:
                m = re.search(r"([A-Z][A-Za-z0-9 '&/().-]{4,90}?)\s+Rs\.?\s*[\d,]+", doc)
                if m:
                    title = m.group(1).strip()
            if not title:
                m = re.search(r"(?i)^product:\s*([^\n]{4,90})", doc, re.M)
                if m:
                    title = m.group(1).strip()
            title = re.sub(r"\s+", " ", title).strip(" -:|")
            title = re.sub(r"(?i)\s*(babyfy|stationery studio|our company)$", "", title).strip(" -:|")
            if len(title) < 3 or title.lower() in {"home page", "faq", "about us", "contact us"}:
                continue
            title_l = title.lower()
            anchor_hits = 0
            exact_anchor_hit = False
            if rank_mode:
                # Rank by price, but a category word in the query ("most expensive
                # backpack") must still gate candidates — without it the global
                # extremes (Rs.981,000 pools) hijack every category ranking. Doc
                # text now carries crawl-graph "Category: …" lines, so match
                # anchors against title + text; drop docs that miss every anchor.
                if anchors:
                    # ALL anchors must hit: "cheapest gel pen" with any-match lets
                    # every ballpoint through via the bare "pen" anchor.
                    _rk_miss = False
                    for a in anchors[:4]:
                        variants = {a}
                        if a.endswith("s") and len(a) > 3:
                            variants.add(a[:-1])
                        elif len(a) > 3:
                            variants.add(a + "s")
                        if a == "rc":
                            variants.update({"remote", "control"})
                        if not any(re.search(rf"\b{re.escape(v)}\b", title_l) or re.search(rf"\b{re.escape(v)}\b", dl) for v in variants):
                            _rk_miss = True
                            break
                    if _rk_miss:
                        continue
            elif exact_phrase:
                title_norm = re.sub(r"[^a-z0-9]+", " ", title_l).strip()
                phrase_norm = re.sub(r"[^a-z0-9]+", " ", exact_phrase).strip()
                if phrase_norm and phrase_norm not in title_norm:
                    # Exact-name queries must not silently drift to sibling products.
                    continue
            elif anchors:
                for a in anchors:
                    variants = {a}
                    if a.endswith("s") and len(a) > 3:
                        variants.add(a[:-1])
                    if a == "rc":
                        variants.update({"remote", "control"})
                    for v in variants:
                        if re.search(rf"\b{re.escape(v)}\b", title_l):
                            anchor_hits += 1
                            exact_anchor_hit = True
                            break
                if anchor_hits <= 0:
                    continue
            prices = []
            # The canonical "Price: Rs.X" line is authoritative when present —
            # body scans hit model numbers ("Erasers 4865" → Rs.4,865, the \b
            # keeps "...ers 4865" from matching) and the <50 junk floor would
            # skip genuinely cheap items (Rs.20 ruler, Rs.40 erasers).
            _label_m = re.search(r"(?im)^price:\s*(?:rs\.?|pkr)\s*([\d,]+(?:\.\d{1,2})?)", doc)
            if _label_m:
                try:
                    _lv = float(_label_m.group(1).replace(",", ""))
                    if _lv > 0:
                        prices.append(_lv)
                except Exception:
                    pass
            if not prices:
                for raw in re.findall(r"\bRs\.?\s*([\d,]+(?:\.\d{1,2})?)", doc, re.I):
                    try:
                        val = float(raw.replace(",", ""))
                    except Exception:
                        continue
                    if val < 50:
                        continue
                    prices.append(val)
            if not prices:
                continue
            price = min(prices)
            if max_price is not None and price > max_price:
                continue
            availability = "sold out" if re.search(r"(?i)\b(sold out|currently unavailable|out of stock)\b", doc) else "available"
            note = ""
            desc = re.search(r"Description\s+(.{40,180})", doc, re.I | re.S)
            if desc:
                note = re.sub(r"\s+", " ", desc.group(1)).strip()
            score = 0.0
            if rank_mode:
                # Lower prices win for "cheapest", higher prices win for "most expensive".
                score += (100000.0 - price) if rank_mode == "asc" else price
            elif anchors:
                score += anchor_hits * 5.0
                if exact_anchor_hit:
                    score += 4.0
                if any(re.search(rf"\b{re.escape(a)}\b", dl[:1200]) for a in anchors[:4]):
                    score += 1.5
            if max_price is not None:
                # Prefer lower prices when the user asked for a cap.
                score += max(0.0, 6.0 - abs(price - max_price) / max(1.0, max_price / 3.0))
            if availability == "available":
                score += 0.5
            if note:
                score += 0.25
            candidates.append((score, title, price, availability, note))
        if not candidates:
            return None
        if rank_mode:
            candidates.sort(key=lambda x: x[2], reverse=(rank_mode == "desc"))
        else:
            candidates.sort(key=lambda x: (x[0], -x[2]), reverse=True)
        items = candidates[:max_items]
        lines = ["Here are matching products from the catalog:"]
        for idx, (_, title, price, availability, note) in enumerate(items, 1):
            price_s = f"Rs.{price:,.0f}" if float(price).is_integer() else f"Rs.{price:,.2f}"
            line = f"{idx}. {title} - {price_s} - {availability}"
            if note:
                line += f". {note[:140]}"
            lines.append(line)
        return "\n".join(lines)
    except Exception:
        return None
