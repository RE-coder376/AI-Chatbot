"""
services/retrieval.py ? Vector/BM25/keyword retrieval pipeline and live API fallback.
Accesses _bm25_cache via module-level state (moved here from app.py).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import re
from pathlib import Path

from langchain_core.messages import HumanMessage, SystemMessage

from services.config import DATABASES_DIR, ACTIVE_DB_FILE
from services.safety import _STRUCTURAL_URL_RE, _clean_text, _is_urdu_script, expand_query
from services.llm_keys import get_fresh_llm
from services.crawler_utils import (
    _check_is_product_db,
    _product_query_rerank_score,
    _enrich_docs_metadata,
)

logger = logging.getLogger(__name__)

# Outcomes/learning intent (universal)
_OUTCOMES_INTENT_RE = re.compile(r"\b(what\s+will\s+i\s+learn|what\s+will\s+we\s+learn|learning\s+outcomes?|objectives?|goals?|by\s+the\s+end|you\s+will\s+be\s+able\s+to)\b", re.I)


# Heuristic reranker (universal): improves reliability for chapter/part/title questions across DBs.
# Used only when DB is not a product catalog (product DBs use different ranking rules).
_NAV_HINTS = (
    'on this page', 'copy as markdown', 'site index', '#site-index', '#site-navigation',
    'ctrl+', 'toggle theme', 'skip to main content'
)


def _extract_title_phrase(q: str) -> str:
    try:
        m = re.search(r"\b(?:chapter|part|section|lesson|unit)\s*\d+\s*[:\-]\s*([^\n]{6,200})", q or '', re.I)
        if m:
            return m.group(1).strip().strip('"\'')
    except Exception:
        pass
    return ''


def _meaningful_keywords(q: str) -> list[str]:
    ql = (q or '').lower()
    # Keep longer tokens; drop generic question glue.
    stop = {
        'what','which','who','when','where','why','how','the','a','an','and','or','to','of','in','on','for','with','by','from',
        'according','book','chapter','part','section','lesson','unit','end','able','learn','goals','objective','objectives','outcomes'
    }
    words = [w for w in re.findall(r"[a-zA-Z]{4,}", ql) if w not in stop]
    # Prefer longer/more specific words.
    words = sorted(set(words), key=len, reverse=True)
    return words[:10]


def _heuristic_rerank_score(doc, q: str, title_phrase: str) -> float:
    try:
        meta = (getattr(doc, 'metadata', None) or {})
        src = str(meta.get('source') or '')
        body = str(getattr(doc, 'page_content', '') or '')
        sl = src.lower()
        bl = body.lower()

        score = 0.0

        # Penalize navigation / index heavy chunks.
        if any(h in sl for h in ('#site-index', '#site-navigation')):
            score -= 8.0
        if any(h in bl for h in _NAV_HINTS):
            score -= 4.0
        if (bl.count('http://') + bl.count('https://')) >= 3:
            score -= 6.0

        # Reward keyword overlap.
        kws = _meaningful_keywords(q)
        hit = sum(1 for k in kws if k in bl)
        score += hit * 2.0
        url_hit = sum(1 for k in kws[:6] if k in sl)
        score += url_hit * 1.5

        # If a title phrase exists, prioritize docs that contain it.
        if title_phrase:
            tpl = title_phrase.lower()
            if tpl in bl:
                score += 10.0
            else:
                # Partial token overlap with title phrase.
                tks = [w for w in re.findall(r"[a-zA-Z]{4,}", tpl)][:8]
                t_hit = sum(1 for t in tks if t in bl)
                score += t_hit * 2.5
                t_url_hit = sum(1 for t in tks[:6] if t in sl)
                score += t_url_hit * 1.0

        # Reward outcome-marker docs when the question is asking for goals/learning/outcomes.
        if _OUTCOMES_INTENT_RE.search(q or ''):
            if re.search(r"\b(by the end|you will be able to|learning outcomes|objectives|goals)\b", body, re.I):
                score += 6.0

        return score
    except Exception:
        return 0.0

_api_resp_cache: dict = {}      # url → (text, expiry) — Jikan response cache (10 min TTL)

_entity_cache: dict = {}        # question_lower → extracted entity string (LRU, max 500)

_bm25_cache: dict = {}  # db_name → {index, docs, metas, num_docs}

_BM25_CACHE_MAX = 10   # max DB indices in memory (each can be 50-100MB for large DBs)

_API_CACHE_MAX = 200            # max entries before LRU eviction

_ENTITY_CACHE_MAX = 500

def _cache_insert(url: str, raw, expiry: float) -> None:
    """Insert into _api_resp_cache; evict expired then oldest if over limit."""
    if len(_api_resp_cache) >= _API_CACHE_MAX:
        now_t = time.time()
        for k in [k for k, (_, e) in list(_api_resp_cache.items()) if now_t >= e]:
            del _api_resp_cache[k]
        if len(_api_resp_cache) >= _API_CACHE_MAX:
            oldest = min(_api_resp_cache, key=lambda k: _api_resp_cache[k][1])
            del _api_resp_cache[oldest]
    _api_resp_cache[url] = (raw, expiry)

async def _extract_search_entity(q: str) -> str:
    """Use a fast LLM call to extract the anime/character/manga title from a natural-language question.
    Runs in parallel with ChromaDB retrieval — adds 0 net latency.
    Results are cached so the same question never calls the LLM twice."""
    key = q.lower().strip()
    if key in _entity_cache:
        return _entity_cache[key]
    try:
        llm = get_fresh_llm()
        if not llm:
            return q
        # Override max_tokens to 20 — title extraction needs only a few tokens
        try: llm.max_tokens = 30
        except Exception: pass
        result = await asyncio.wait_for(llm.ainvoke([
            {"role": "system", "content":
                "Extract the search term from the anime/manga question. "
                "If the question is about a CHARACTER (person/villain/hero), return the CHARACTER NAME. "
                "If the question is about an ANIME/MANGA TITLE, return the TITLE. "
                "If comparing TWO titles, return both separated by | with nothing else. "
                "If about a genre/category/type (no specific title), return just the genre keywords. "
                "No punctuation, no explanation, no full sentences. "
                "Examples: "
                "'tell me about uchiha madara' → 'uchiha madara' | "
                "'who is gojo satoru' → 'gojo satoru' | "
                "'tell me about levi ackerman' → 'levi ackerman' | "
                "'who is the main character of oshi no ko' → 'oshi no ko' | "
                "'how many episodes does bleach TYBW have' → 'bleach thousand year blood war' | "
                "'is attack on titan worth watching' → 'attack on titan' | "
                "'jujutsu kaisen or attack on titan rating' → 'jujutsu kaisen|attack on titan' | "
                "'compare naruto and one piece' → 'naruto|one piece' | "
                "'what are the best isekai anime' → 'isekai anime' | "
                "'recommend some mecha anime' → 'mecha anime'"},
            {"role": "user", "content": q}
        ]), timeout=10)
        entity = (result.content if hasattr(result, "content") else str(result)).strip().strip('"\'')
        # Sanity check: if LLM hallucinated something way longer, fall back
        if not entity or len(entity) > len(q) + 20:
            entity = q
        # LRU eviction
        if len(_entity_cache) >= _ENTITY_CACHE_MAX:
            oldest = next(iter(_entity_cache))
            del _entity_cache[oldest]
        _entity_cache[key] = entity
        return entity
    except Exception:
        return q

def _retrieval_visible_doc(doc) -> bool:
    meta = (getattr(doc, "metadata", None) or {})
    source = str(meta.get("source") or "")
    # Structural exclusion: use URL pattern (reliable) rather than stale metadata flags
    # (metadata was written by older crawler versions that had false-positive structural
    # detection on Docusaurus/educational pages — "skip to main content" nav landmark).
    if _STRUCTURAL_URL_RE.search(source) or "#site-navigation" in source:
        return False
    if meta.get("contaminated"):
        return False
    if meta.get("content_type") == "category":
        return False
    if meta.get("retrieve_eligible") is False:
        # "structural" quarantine reason = stale false-positive — don't exclude.
        # All other reasons (low_quality, weak_product_fallback, contaminated) still apply.
        qr = meta.get("quarantine_reason") or ""
        if qr == "structural":
            pass  # was a false positive — let it through
        else:
            return False
    try:
        if (
            meta.get("quality_score") is not None
            and meta.get("page_type", "unknown") in {"product"}
            and float(meta.get("quality_score") or 0.0) < 0.5
        ):
            return False
    except Exception:
        pass
    # Drop corrupted/binary chunks — if <70% of first 200 chars are printable, skip.
    try:
        text = getattr(doc, "page_content", None) or ""
        sample = text[:200]
        if len(sample) > 20:
            printable_ratio = sum(1 for c in sample if c.isprintable()) / len(sample)
            if printable_ratio < 0.70:
                return False
    except Exception:
        pass
    return True

_ROLE_TERMS = {"ceo", "cto", "coo", "cfo", "cpo", "vp", "founder", "author", "director",
               "president", "chairman", "head", "lead", "chief"}

async def _smart_search_expand(q: str, history: list = None) -> tuple:
    """
    Combined HyDE + MultiQuery + History-Aware in one LLM call.
    History-Aware: if query is ambiguous (short/pronouns) and history exists,
    the LLM resolves it using prior context before generating HyDE + variants.
    Returns (hyde_text: str, variants: list[str])
    """
    try:
        llm = get_fresh_llm()
        if not llm:
            return "", []

        # Build history context string (last 2 turns only — keep tokens low)
        hist_ctx = ""
        if history:
            recent = history[-2:] if len(history) >= 2 else history
            hist_ctx = "\n".join(
                f"{'Customer' if m.get('role')=='user' else 'Bot'}: {m.get('content','')[:120]}"
                for m in recent
            )

        system_msg = (
            "You are a product search assistant helping retrieve items from a product catalog.\n"
            + (f"Recent conversation:\n{hist_ctx}\n\n" if hist_ctx else "")
            + "Given the customer's latest query, output EXACTLY 3 lines:\n"
            "LINE 1 (HyDE): A 2-sentence hypothetical product listing with exact spec terms and "
            "technical keywords that would match the customer's need. Use catalog language "
            "(e.g. 'Touch display', 'GeForce GTX', 'Windows 10 Home', 'fast SSD storage').\n"
            "LINE 2: Alternative phrasing of the query using different words/synonyms.\n"
            "LINE 3: Another alternative phrasing from a different angle.\n"
            "Output ONLY these 3 lines. No labels, no preamble."
        )
        res = await asyncio.wait_for(llm.ainvoke([
            SystemMessage(content=system_msg),
            HumanMessage(content=q)
        ]), timeout=8)
        lines = [l.strip() for l in res.content.strip().split("\n") if l.strip()]
        hyde_text = lines[0] if lines else ""
        variants  = [l for l in lines[1:3] if l and l.lower() != q.lower()]
        logger.info(f"[SmartSearch] hyde={hyde_text[:80]} | variants={variants}")
        return hyde_text, variants
    except Exception as e:
        logger.warning(f"[SmartSearch] expand failed: {e}")
        return "", []

def _keyword_rescue(q: str, db, seen: set, k: int = 5) -> list:
    """Find chunks that exactly contain technical terms (acronyms, proper nouns) missing from vector results."""
    words = q.split()
    _RESCUE_STOPWORDS = {
        'what','which','who','when','where','why','how','chapter','part','section','lesson','unit',
        'according','book','the','a','an','and','or','to','of','in','on','for','with','by','end',
        'from','about','learn','goals','objective','objectives','outcomes','able'
    }
    # Extract: (1) uppercase acronyms, (2) capitalized proper nouns, (3) role titles
    # Extract: uppercase acronyms + capitalized proper nouns, but avoid generic question words.
    technical = []
    for w in words:
        ww = w.strip("?.,!\"'")
        if not ww or len(ww) < 4:
            continue
        lw = ww.lower()
        if lw in _RESCUE_STOPWORDS:
            continue
        if (ww.isupper() and len(ww) >= 2) or (len(ww) > 1 and ww[0].isupper() and not ww.isupper()):
            technical.append(ww)
    technical = sorted(set(technical), key=len, reverse=True)
    # Also add uppercase form of role titles found in query
    q_lower = q.lower()
    for role in _ROLE_TERMS:
        if role in q_lower:
            technical.append(role.upper())  # Search for "CEO" when query has "ceo"
    rescue_docs = []
    from langchain_core.documents import Document
    for term in technical[:3]:
        try:
            raw = db._collection.get(where_document={"$contains": term.lower()}, limit=k * 2)
            for doc_text, meta in zip(raw.get("documents", []), raw.get("metadatas", [])):
                key = doc_text[:100]
                if key not in seen and term.lower() in (doc_text or "").lower():
                    seen.add(key)
                    rescue_docs.append(Document(page_content=doc_text, metadata=meta or {}))
        except Exception:
            pass
    return rescue_docs

def _get_bm25_index(db, db_name: str):
    """Build or return cached BM25 index. Auto-invalidates when chunk count changes."""
    try:
        from rank_bm25 import BM25Okapi
        cached = _bm25_cache.get(db_name)
        total = db._collection.count()
        # Only rebuild if chunk count changed by >50 — avoids full 131MB re-read on every auto-crawl write
        if cached and abs(cached["num_docs"] - total) < 50:
            return cached
        # Load all docs and build index
        all_data = db._collection.get(limit=total + 100, include=["documents", "metadatas"])
        docs = all_data.get("documents") or []
        metas = all_data.get("metadatas") or [{}] * len(docs)
        if not docs:
            return None
        def _tokenize_bm25(text):
            tokens = text.lower().split()
            bigrams = [f"{tokens[i]}_{tokens[i+1]}" for i in range(len(tokens) - 1)]
            return tokens + bigrams
        tokenized = [_tokenize_bm25(d) for d in docs]
        index = BM25Okapi(tokenized)
        entry = {"index": index, "docs": docs, "metas": metas, "num_docs": len(docs)}
        if len(_bm25_cache) >= _BM25_CACHE_MAX:
            _bm25_cache.pop(next(iter(_bm25_cache)), None)
        _bm25_cache[db_name] = entry
        logger.info(f"[BM25] Index built for '{db_name}': {len(docs)} docs")
        return entry
    except Exception as e:
        logger.warning(f"[BM25] Build failed for '{db_name}': {e}")
        return None

def _bm25_search(q: str, db, db_name: str, k: int = 5):
    """BM25 keyword search — finds lexically-matching chunks that vector search misses.
    Works for any website: structural queries, product names, exact terms, navigation content."""
    try:
        from langchain_core.documents import Document
        # Fast-path: if cache is empty, skip rather than trigger a 20-40s blocking build.
        # _prewarm_bm25() at startup populates the cache in the background.
        # Queries before pre-warm completes fall back to vector-only (fast); afterwards hybrid.
        entry = _bm25_cache.get(db_name)
        if not entry:
            return []
        # Validate cache is still fresh (chunk count check only if already cached)
        entry = _get_bm25_index(db, db_name)
        if not entry:
            return []
        q_tokens = q.lower().split()
        q_bigrams = [f"{q_tokens[i]}_{q_tokens[i+1]}" for i in range(len(q_tokens) - 1)]
        scores = entry["index"].get_scores(q_tokens + q_bigrams)
        top_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:k]
        return [
            Document(page_content=entry["docs"][i], metadata=entry["metas"][i] or {})
            for i in top_idx if scores[i] > 0.1
        ]
    except Exception as e:
        logger.warning(f"[BM25] Search failed: {e}")
        return []

async def _translate_query_for_search(q: str) -> str:
    """Fast translation of Urdu script to English keywords for retrieval."""
    try:
        llm = get_fresh_llm()
        if not llm: return q
        res = await asyncio.wait_for(llm.ainvoke([
            SystemMessage(content="Extract the core technical concepts and product names from this Urdu query and translate them to English keywords for database search. Only return the English keywords, separated by spaces. Example: 'لائیو کٹ کیا ہے' -> 'LiveKit'"),
            HumanMessage(content=q)
        ]), timeout=10)
        return res.content.strip()
    except:
        return q

async def async_intent_aware_expansion(q: str) -> list:
    """Use LLM to understand intent and generate search-friendly variations."""
    q_lower = q.lower()
    # Skip LLM for very short queries to save tokens/time
    if len(q_lower.split()) < 2:
        return [q]
        
    try:
        llm = get_fresh_llm()
        if not llm: return [q]
        
        res = await asyncio.wait_for(llm.ainvoke([
            SystemMessage(content=(
                "You are a search query optimizer. Given a user query, identify the core intent and "
                "return 2-3 specific search phrases that would help find the answer in a knowledge base. "
                "Include synonyms and related technical terms. "
                "Return ONLY the phrases separated by '|'. No preamble. "
                "Example: 'whats the curriculam' -> 'course syllabus|learning modules|main topics'"
            )),
            HumanMessage(content=q)
        ]), timeout=8)
        
        variations = [v.strip() for v in res.content.split("|") if v.strip()]
        unique_vars = [v for v in variations if v.lower() != q_lower]
        return [q] + unique_vars[:3]
    except Exception as e:
        logger.error(f"Intent expansion failed: {e}")
        return [q]

async def retrieve_context(q: str, db, k: int = 25, fast: bool = False, expansion_task=None, history: list = None) -> tuple:
    """Multilingual Retrieval: Handles English and Urdu in the same vector space.
    Returns (context_text, doc_count, sources) so callers can cite sources.
    fast=True skips the LLM expansion step (used for sub-queries in multi-part decomposition)."""
    try:
        _cnt = db._collection.count() if db else 0
        logger.info(f"[RETRIEVE] db={'set' if db else 'None'}, chunks={_cnt}, q={q[:60]}")
        # Small product catalog: retrieve everything for complete recall.
        # Prevents non-deterministic answers caused by vector search missing products.
        # 500 products × ~200 chars = ~100K chars max — capped by context budget downstream.
        if _cnt > 0 and _cnt <= 500:
            k = _cnt
    except Exception as _le:
        logger.warning(f"[RETRIEVE] db count failed: {_le}")
    # If this DB has live API sources, skip LLM expansion — the API data is the freshness source
    if not fast:
        try:
            _adb = getattr(db, '_db_name', '') or (ACTIVE_DB_FILE.read_text(encoding="utf-8").strip() if ACTIVE_DB_FILE.exists() else "")
            if _adb and json.loads((DATABASES_DIR / _adb / "config.json").read_text()).get("api_sources"):
                fast = True
        except Exception:
            pass

    # 1. Start with hardcoded concept expansion (fast)
    search_queries = expand_query(q)

    _title_phrase = _extract_title_phrase(q)
    _is_outcomes_intent = bool(_OUTCOMES_INTENT_RE.search(q))
    if _title_phrase and all(_title_phrase.lower() != str(sq).lower() for sq in search_queries):
        search_queries.insert(0, _title_phrase)

    # Outcomes-marker-first retrieval: add explicit markers as query variants so we pull the actual
    # learning outcomes list instead of generic book/TOC pages.
    if _is_outcomes_intent:
        for _mq in (
            (_title_phrase + ' by the end of this chapter you will be able to') if _title_phrase else 'by the end of this chapter you will be able to',
            (_title_phrase + ' learning outcomes') if _title_phrase else 'learning outcomes',
            (_title_phrase + ' objectives') if _title_phrase else 'objectives',
            (_title_phrase + ' goals') if _title_phrase else 'goals',
        ):
            if _mq and all(_mq.lower() != str(sq).lower() for sq in search_queries):
                search_queries.append(_mq)

    # Cap variants to keep latency bounded
    search_queries = search_queries[:6]

    # Extra query signal: if the user asks about 'Chapter N: Title' or 'Part N: Title',
    # add the title portion as an additional retrieval query. This dramatically improves
    # retrieval for book/curriculum KBs where 'chapter/part' is too generic.
    try:
        _m_title = re.search(r"\b(?:chapter|part)\s*\d+\s*[:\-]\s*([^\n]{6,200})", q, re.I)
        if _m_title:
            _title_q = _m_title.group(1).strip().strip("\"'")
            if _title_q and all(_title_q.lower() != str(sq).lower() for sq in search_queries):
                search_queries.insert(0, _title_q)
                search_queries = search_queries[:4]
    except Exception:
        pass

    # ── smart_search feature: load features for this DB ───────────────────────
    _ss_db_name = getattr(db, '_db_name', '') or ""
    _features: set = set()
    if _ss_db_name:
        try:
            _fcfg = json.loads((DATABASES_DIR / _ss_db_name / "config.json").read_text(encoding="utf-8"))
            _features = set(_fcfg.get("features", []))
        except Exception:
            pass

    _ss_task = None
    if "smart_search" in _features:
        # Fire combined HyDE + MultiQuery in parallel — passes history for context-aware HyDE
        _ss_task = asyncio.create_task(_smart_search_expand(q, history=history))

    # 2. Add LLM-based intent expansion — skip for api DBs and smart_search DBs (_smart_search_expand replaces it)
    if not fast and "smart_search" not in _features:
        if expansion_task is not None:
            try:
                intent_vars = await asyncio.wait_for(asyncio.shield(expansion_task), timeout=6)
            except (asyncio.TimeoutError, Exception):
                intent_vars = [q]
        else:
            intent_vars = await async_intent_aware_expansion(q)
        for v in intent_vars:
            if v not in search_queries:
                search_queries.append(v)
        search_queries = search_queries[:2]

    # Await smart_search expand result and merge
    if _ss_task is not None:
        try:
            _ss_result = await asyncio.wait_for(asyncio.shield(_ss_task), timeout=8)
            _hyde_text, _variants = _ss_result if isinstance(_ss_result, tuple) else ("", [])
            # HyDE text goes second (after original query — highest priority)
            if _hyde_text and _hyde_text not in search_queries:
                search_queries.insert(1, _hyde_text)
            # MultiQuery variants appended after
            for _v in _variants:
                if _v and _v not in search_queries:
                    search_queries.append(_v)
        except Exception as _sse:
            logger.warning(f"[SmartSearch] merge failed: {_sse}")
        search_queries = search_queries[:4]  # original + hyde + 2 variants

    logger.debug(f"Expanded queries: {search_queries}")
    
    # 3. Handle Urdu script
    if _is_urdu_script(q):
        translated = await _translate_query_for_search(q)
        if translated and translated != q and translated not in search_queries:
            search_queries.append(translated)
            logger.info(f"Cross-lingual search added: {translated}")

    # ── Pre-check: skip ALL ChromaDB ops for keyword-matched Jikan queries ──────
    # If any API source keyword matches and the query is not a detail request,
    # skip keyword rescue + similarity search — Jikan live data is the sole source.
    _skip_chromadb = False
    try:
        _adb2 = getattr(db, '_db_name', '') or (ACTIVE_DB_FILE.read_text(encoding="utf-8").strip() if ACTIVE_DB_FILE.exists() else "")
        if _adb2:
            _adb2_srcs = json.loads((DATABASES_DIR / _adb2 / "config.json").read_text()).get("api_sources", [])
            _ql2 = q.lower()
            _needs_detail = re.compile(
                r'\b(tell me about|explain|describe|synopsis|plot|story|review|compare|versus|vs|'
                r'character|voice actor|who is|who are|detail)\b', re.I)
            if not _needs_detail.search(_ql2):
                for _src2 in _adb2_srcs:
                    _kw2 = [k.lower().strip() for k in _src2.get("keywords", []) if k.strip()]
                    if _kw2 and any(kw in _ql2 for kw in _kw2):
                        _skip_chromadb = True
                        break
    except Exception:
        pass

    # Keyword rescue FIRST — exact matches; skipped if Jikan fast path or no DB
    rescue_seen: set = set()
    rescue_results = []
    if not _skip_chromadb and db is not None:
        for rescue_q in [q] + search_queries:
            for r in _keyword_rescue(rescue_q, db, rescue_seen):
                rescue_results.append(r)

    # Outcomes rescue (universal): when user asks learning outcomes/goals, directly pull chunks likely to
    # contain outcomes lists and then filter by the title phrase tokens in URL/body.
    if _is_outcomes_intent and db is not None:
        try:
            from langchain_core.documents import Document
            _tpl = (_title_phrase or '').lower()
            _tks = [w for w in re.findall(r'[a-zA-Z]{4,}', _tpl)][:10]
            # "agent(s)" is too generic to be a reliable anchor token across most KBs.
            _tks = [t for t in _tks if t not in {"agent", "agents"}]
            _pc = re.search(r"\b(part|chapter)\s+(\d{1,3})\b", q, re.I)
            _pc_anchor = (f"{_pc.group(1).lower()} {_pc.group(2)}" if _pc else "")
            # Broad net first ("you will" is common), then we filter hard by title tokens.
            # This avoids DB-specific hardcoding while still reliably surfacing outcome bullets.
            # 1) Direct anchor phrase (highest precision when present).
            # This is universal (not DB-specific): if the user asked "Part 10" and a chunk contains
            # "By completing Part 10", grab it first.
            if _pc_anchor:
                _anchor_phrase = f"by completing {_pc_anchor}"
                for _needle in (_anchor_phrase, _anchor_phrase.title()):
                    try:
                        raw2 = db._collection.get(where_document={"$contains": _needle}, limit=80)
                        for doc_text, meta in zip(raw2.get('documents', []), raw2.get('metadatas', [])):
                            if doc_text:
                                rescue_seen.add(doc_text[:100])
                                rescue_results.append(Document(page_content=doc_text, metadata=meta or {}))
                    except Exception:
                        pass

            # 2) Broad net ("you will" is common), then filter hard by anchor tokens.
            raw = db._collection.get(where_document={"$contains": "you will"}, limit=250)
            for doc_text, meta in zip(raw.get('documents', []), raw.get('metadatas', [])):
                if not doc_text:
                    continue
                src = str((meta or {}).get('source') or '').lower()
                body = str(doc_text).lower()
                # Only keep chunks that look like an outcomes list.
                if not re.search(r"(?i)\b(by (the end of|completing|after completing|upon completing)\b|you will be able to\b|you will\s*:)", doc_text):
                    continue
                # If the query names a specific Part/Chapter, require that exact anchor to appear somewhere
                # in the chunk or its source URL (prevents extracting the wrong outcomes list).
                if _pc_anchor and (_pc_anchor not in body) and (_pc_anchor not in src):
                    continue
                if _tks:
                    # Require at least 2 title tokens in either URL or body
                    hit = sum(1 for t in _tks if (t in src) or (t in body))
                    if hit < 2:
                        continue
                key = doc_text[:100]
                if key not in rescue_seen:
                    rescue_seen.add(key)
                    rescue_results.append(Document(page_content=doc_text, metadata=meta or {}))
        except Exception:
            pass

    # Fire entity extraction in parallel with ChromaDB search — zero net latency
    # Skip if fast-path (keyword-matched Jikan) — avoids wasted LLM TCP connection
    if _skip_chromadb:
        async def _noop_entity(): return q
        _entity_task = asyncio.create_task(_noop_entity())
    else:
        _entity_task = asyncio.create_task(_extract_search_entity(q))

    seen = set()  # used only by _has_product_meta price filter below
    _bm25_raw: list = []
    _vector_raw: list = []

    # ── Product catalog metadata filter ──────────────────────────────────────
    # Detects structured product chunks (has price metadata) and builds a
    # combined ChromaDB WHERE filter from query constraints — done in Python,
    # NOT by the LLM. This is the industry-standard approach for product RAG.
    _combined_filter = None
    _max_price = None
    _has_product_meta = False
    try:
        _sample = db._collection.get(limit=1, include=["metadatas"]) if db else None
        if _sample and _sample.get("metadatas") and _sample["metadatas"][0].get("price") is not None:
            _has_product_meta = True
    except Exception as _pfe:
        logger.warning(f"[META-FILTER] Sample check failed: {_pfe}")

    if _has_product_meta:
        _meta_conds = []
        _required_flags = {}   # boolean fields required post-retrieval
        _ram_min_req = None    # RAM minimum for post-filter
        # Price
        _pm = re.search(r'(?:under|below|less\s+than|max(?:imum)?|budget\s+of|within)\s+\$?([\d,]+)', q, re.I)
        if _pm:
            try:
                _max_price = float(_pm.group(1).replace(',', ''))
                _meta_conds.append({"price": {"$lte": _max_price}})
            except: pass
        # RAM minimum
        _rm = re.search(r'(?:at\s+least\s+|minimum\s+)?(\d+)\s*gb\s+(?:ram|memory)', q, re.I)
        if _rm:
            try:
                _ram_min_req = int(_rm.group(1))
                _meta_conds.append({"ram_gb": {"$gte": _ram_min_req}})
            except: pass
        # Touchscreen
        if re.search(r'\btouch(?:screen)?\b', q, re.I):
            _meta_conds.append({"has_touch": {"$eq": 1}})
            _required_flags['has_touch'] = 1
        # Convertible/flip/tablet
        if re.search(r'\b(flip|convertible|2.in.1|tablet\s+mode|handwriting|stylus)\b', q, re.I):
            _meta_conds.append({"is_convertible": {"$eq": 1}})
            _required_flags['is_convertible'] = 1
        # SSD — filter positively or negatively based on query intent
        _ssd_negated = re.search(r"(?:no|without|don.t\s+(?:need|want)|not\s+(?:need|want))\s+(?:an?\s+)?ssd", q, re.I)
        if re.search(r'\bssd\b', q, re.I):
            if _ssd_negated:
                # User explicitly doesn't want SSD — filter for HDD-only products
                _meta_conds.append({"has_ssd": {"$eq": 0}})
                _required_flags['has_ssd'] = 0
            else:
                _meta_conds.append({"has_ssd": {"$eq": 1}})
                _required_flags['has_ssd'] = 1
        # Gaming (dedicated GPU)
        if re.search(r'\bgaming\b', q, re.I):
            _meta_conds.append({"has_gpu": {"$eq": 1}})
            _required_flags['has_gpu'] = 1
        if _meta_conds:
            _combined_filter = _meta_conds[0] if len(_meta_conds) == 1 else {"$and": _meta_conds}
            k = max(k, 60)
            logger.info(f"[META-FILTER] {_combined_filter}")

    policy_results: list = []
    if not _skip_chromadb and db is not None:
        loop = asyncio.get_running_loop()
        # Run vector searches + BM25 ALL IN PARALLEL
        # smart_search: threshold on original query only — HyDE/variants bypass it.
        # Reason: original query may use colloquial language (low sim) while HyDE is
        # LLM-generated catalog text (trusted) — filtering HyDE drops semantically
        # correct results (e.g. convertible laptops when user says "flip/tablet").
        _score_threshold = 0.15 if "smart_search" in _features else 0.0
        if _score_threshold > 0:
            def _scored_search(q_=None, f=None):
                pairs = db.similarity_search_with_relevance_scores(q_, k=k, filter=f)
                return [doc for doc, score in pairs if score >= _score_threshold]
            _search_tasks = []
            for _sq_idx, _sq in enumerate(search_queries):
                if _sq_idx == 0:  # original query — apply threshold
                    _search_tasks.append(loop.run_in_executor(None, lambda q=_clean_text(_sq), f=_combined_filter: _scored_search(q, f)))
                else:  # HyDE + variants — no threshold (LLM-generated catalog text, trusted)
                    _search_tasks.append(loop.run_in_executor(None, lambda q=_clean_text(_sq), f=_combined_filter: db.similarity_search(q, k=k, filter=f)))
        else:
            _search_tasks = [
                loop.run_in_executor(None, lambda q=_clean_text(query), f=_combined_filter: db.similarity_search(q, k=k, filter=f))
                for query in search_queries
            ]
        # BM25 hybrid: use stored _db_name attr (reliable), fallback to path parse
        _bm25_db_name = getattr(db, '_db_name', None) or ""
        if not _bm25_db_name:
            try:
                _bm25_db_name = Path(db._persist_directory).name.split("_", 1)[-1]
            except Exception:
                pass
        _bm25_task = loop.run_in_executor(None, lambda: _bm25_search(q, db, _bm25_db_name, k=k))
        # Policy injection: secondary search for shipping/discount/return docs when query is policy-related.
        # Runs in parallel — zero latency overhead. Prepended to context so never crowded out by product chunks.
        _POLICY_Q_RE = re.compile(
            r'\b(ship|deliver|return|refund|exchang|policy|policies|discount|bulk|warranty|repair|'
            r'track|cod|cash.?on.?delivery|free.?deliver|minimum.?order|charges?|fees?|international)\b',
            re.I
        )
        _policy_task = None
        _outcomes_title_task = None
        _outcomes_title_task = None
        # Fire policy search when: (a) query explicitly mentions policy terms, OR
        # (b) DB is a product/retail catalog (_has_product_meta) — covers "Deli Punch shipping?"
        # type queries where the user asks about a product but the answer lives in a policy chunk.
        if _POLICY_Q_RE.search(q) or _has_product_meta:
            _policy_task = loop.run_in_executor(
                None, lambda: db.similarity_search(
                    "shipping delivery return policy discount rules refund charges", k=5
                )
            )
            logger.debug(f"[POLICY-INJECT] Triggered (product_meta={_has_product_meta}) for: {q[:60]}")
        if _is_outcomes_intent:
            _oq = (_title_phrase or "").strip()
            if _oq:
                _outcomes_title_task = loop.run_in_executor(
                    None, lambda: db.similarity_search(_oq, k=8)
                )
        try:
            _gather_tasks = (
                [*_search_tasks, _bm25_task]
                + ([_policy_task] if _policy_task else [])
                + ([_outcomes_title_task] if _outcomes_title_task else [])
            )
            _all_results = await asyncio.wait_for(
                asyncio.gather(*_gather_tasks, return_exceptions=True),
                timeout=35
            )
        except asyncio.TimeoutError:
            logger.warning("ChromaDB parallel search timed out")
            _all_results = []
        # Split results: last slot is policy_task (if fired), rest are main search results
        _n_main = len(_search_tasks) + 1  # search tasks + bm25
        _main_results = _all_results[:_n_main]
        _policy_results_raw = _all_results[_n_main] if _policy_task and len(_all_results) > _n_main else []
        policy_results = []
        if not isinstance(_policy_results_raw, Exception):
            for _pr in _policy_results_raw:
                _pk = _pr.page_content[:100]
                if _pk not in seen:
                    seen.add(_pk)
                    policy_results.append(_pr)
        # Combine in priority order: BM25 exact matches first, then rescue, then vector.
        # Dedup in order so highest-priority group wins. This ensures the BM25 best
        # match (e.g. "Marabu Chalky Chic") appears at position 0 in the context,
        # not buried after 20+ generic same-brand docs from rescue.
        _bm25_idx = len(_search_tasks)  # BM25 is last in _gather_tasks
        _bm25_result = _main_results[_bm25_idx] if len(_main_results) > _bm25_idx else []
        _bm25_raw = [] if isinstance(_bm25_result, Exception) else _bm25_result
        _vector_raw = []
        _vector_exc_count = 0
        _vector_exc_sample = None
        for _vi, _vr in enumerate(_main_results):
            if _vi == _bm25_idx:
                continue
            if isinstance(_vr, Exception):
                _vector_exc_count += 1
                if _vector_exc_sample is None:
                    _vector_exc_sample = _vr
                continue
            _vector_raw.extend(_vr)
        if _vector_exc_count:
            logger.error(f"Vector retrieval error(s): {_vector_exc_count} task(s) failed; sample={type(_vector_exc_sample).__name__}: {_vector_exc_sample}")
        if isinstance(_bm25_result, Exception):
            logger.error(f"BM25 retrieval error: {_bm25_result}")

    # ── Product catalog: build combined list in priority order ────────────────
    # policy → BM25 (lexical exact match) → rescue (term-contains) → vector
    _comb_seen: set = set()
    _combined_before_cap: list = []
    for _grp in (policy_results, _bm25_raw, rescue_results, _vector_raw):
        for _r in _grp:
            if not _retrieval_visible_doc(_r):
                continue
            _k = _r.page_content[:100]
            if _k in _comb_seen:
                continue
            # Post-filter: drop chunks exceeding price constraint
            if _max_price is not None:
                _cp = _r.metadata.get("price") if _r.metadata else None
                if _cp is not None and _cp > _max_price:
                    continue
            _comb_seen.add(_k)
            _combined_before_cap.append(_r)
    

    # Heuristic rerank for non-product DBs: improves chapter/part/title lookup accuracy.
    if not _has_product_meta and _combined_before_cap:
        _title_phrase = _extract_title_phrase(q)
        _combined_before_cap.sort(key=lambda d: _heuristic_rerank_score(d, q, _title_phrase), reverse=True)
        # outcomes-marker bubble: if user asks learning outcomes/goals, put the explicit outcomes chunks first.
        if _is_outcomes_intent:
            def _has_outcome_marker(d):
                try:
                    b = str(getattr(d,'page_content','') or '').lower()
                    return (
                        ('you will be able to' in b)
                        or ('by the end' in b)
                        or ('by completing' in b)
                        or ('learning outcomes' in b)
                        or ('objectives' in b)
                        or ('goals:' in b)
                    )
                except Exception:
                    return False
            _combined_before_cap.sort(key=lambda d: (1 if _has_outcome_marker(d) else 0, _heuristic_rerank_score(d, q, _title_phrase)), reverse=True)
        # title_phrase filter: if query names a specific Chapter/Part title, prefer docs that actually mention it.
        if _title_phrase:
            _tpl = _title_phrase.lower()
            _tks = [w for w in re.findall(r'[a-zA-Z]{4,}', _tpl)][:8]
            def _title_match(d):
                try:
                    b = str(getattr(d,'page_content','') or '').lower()
                    s = str((getattr(d,'metadata',None) or {}).get('source') or '').lower()
                    if _tpl in b or _tpl in s:
                        return True
                    if not _tks:
                        return False
                    hit = sum(1 for t in _tks if t in b)
                    return hit >= 2
                except Exception:
                    return False
            _filtered = [d for d in _combined_before_cap if _title_match(d)]
            if len(_filtered) >= 6:
                _combined_before_cap = _filtered
    if _has_product_meta and (_required_flags or _ram_min_req is not None or _max_price is not None):
        _post_filtered = []
        for r in _combined_before_cap:
            _rm2 = r.metadata or {}
            # Boolean flags: e.g. is_convertible=1, has_ssd=1
            if any(_rm2.get(_fk) != _fv for _fk, _fv in _required_flags.items()):
                continue
            # RAM minimum
            if _ram_min_req is not None:
                _rval = _rm2.get('ram_gb')
                if _rval is not None and _rval < _ram_min_req:
                    continue
            # Price max
            if _max_price is not None:
                _pval = _rm2.get('price')
                if _pval is not None and _pval > _max_price:
                    continue
            _post_filtered.append(r)
        top = _post_filtered[:40]
        logger.info(f"[META-FILTER] post-filter: {len(top)} chunks remain")
    else:
        top = _combined_before_cap[:40]

    # ── Product catalog: dedup + GPU ranking ─────────────────────────────────
    if _has_product_meta:
        # Dedup: same product+price should appear only once
        _dedup_seen, _deduped = set(), []
        for r in top:
            _pk = (str(r.metadata.get('price', '')) + r.page_content[:60]) if r.metadata else r.page_content[:80]
            if _pk not in _dedup_seen:
                _dedup_seen.add(_pk)
                _deduped.append(r)
        top = _deduped
        # For "best gaming / highest VRAM" queries: sort by GPU VRAM descending in Python
        if re.search(r'\bbest\b.*\bgaming\b|\bhighest\b.*\b(?:gpu|vram)\b|\bmost\s+powerful\b', q, re.I):
            top.sort(key=lambda r: r.metadata.get('gpu_vram_gb', 0) if r.metadata else 0, reverse=True)
    # Product-title rerank: exact product pages should outrank same-brand neighbors for
    # pricing/detail questions like "Pack Of 12" even on DBs whose chunks are URL/text-only.
    _has_productish_sources = any(
        "/products/" in str(((getattr(r, "metadata", None) or {}).get("source")) or "").lower()
        for r in top[:20]
    )
    if _has_productish_sources and re.search(r"\b(price|pricing|cost|how much|sku|size|color|pack|piece|pieces)\b", q, re.I):
        top.sort(key=lambda r: _product_query_rerank_score(q, r), reverse=True)
    sources = []
    for r in top:
        src = r.metadata.get("source", "")
        # Validate scheme — only allow http/https to prevent javascript: injection
        if src and src not in sources:
            try:
                from urllib.parse import urlparse as _urlparse
                scheme = _urlparse(src).scheme
                if scheme in ("http", "https"):
                    sources.append(src)
            except:
                pass
    context = "\n\n".join([_clean_text(r.page_content) for r in top])

    # ── Live API fallback: trigger if no results OR db has api_sources ────
    _live_filler = re.compile(
        r"^(tell me about|what is|explain|who is|describe|give me info on|i want to know about|can you tell me|info on|details about)\s+"
        r"|^(the\s+)?"
        r"(main character|protagonist|antagonist|main protagonist|lead character|hero|heroine|villain|cast|characters|voice actor|va|seiyuu)"
        r"\s+(of|in|from|for)\s+",
        re.I)
    try:
        import urllib.parse as _up, httpx as _hx
        active_db_name = getattr(db, '_db_name', '') or (ACTIVE_DB_FILE.read_text(encoding="utf-8").strip() if ACTIVE_DB_FILE.exists() else "")
        if active_db_name:
            db_cfg_file = DATABASES_DIR / active_db_name / "config.json"
            if db_cfg_file.exists():
                db_cfg = json.loads(db_cfg_file.read_text(encoding="utf-8-sig"))
                api_sources = db_cfg.get("api_sources", [])
                _airing_kw = {"airing","season","current","now","this week","today","schedule","simulcast","new anime"}
                # Always hit live API for any question — DB is checked first (RAG context),
                # live API supplements with fresh data for ALL query types
                _needs_live = True
                if api_sources and _needs_live:
                    ql = q.lower()
                    # ── Pass 1: collect keyword-matched sources (no entity extraction needed) ──
                    to_fetch = []
                    _search_sources_no_kw = []  # generic search sources (no keywords, need entity)
                    _search_sources_kw = []     # search sources WITH keyword match (need entity)
                    for src in api_sources:
                        api_url = src.get("url", "")
                        if not api_url: continue
                        is_ranking = "{q}" not in api_url
                        cfg_kw = [k.lower().strip() for k in src.get("keywords", []) if k.strip()]
                        if cfg_kw:
                            if not any(kw in ql for kw in cfg_kw):
                                continue
                            # Keyword match found
                            if is_ranking:
                                to_fetch.append((src, api_url, is_ranking))
                            else:
                                _search_sources_kw.append(src)
                        else:
                            if is_ranking:
                                continue  # Ranking sources require explicit keywords
                            else:
                                _search_sources_no_kw.append(src)  # Generic search (always fires)

                    # ── Entity extraction — only await if search sources will actually fire ──
                    # Generic search sources are skipped when keyword-matched sources already provide data
                    _need_entity = bool(_search_sources_kw) or (bool(_search_sources_no_kw) and not to_fetch and not _search_sources_kw)
                    clean_q = q  # default: raw query (used if entity skipped)
                    if _need_entity:
                        clean_q = await _entity_task
                        clean_q = re.sub(r'\bS(\d+)\b', lambda m: f"season {m.group(1)}", clean_q, flags=re.I)
                        for src in (_search_sources_kw + (_search_sources_no_kw if not to_fetch else [])):
                            api_url = src.get("url", "")
                            entities = [e.strip() for e in clean_q.split("|") if e.strip()] or [clean_q]
                            for entity in entities:
                                search_url = api_url.replace("{q}", _up.quote_plus(entity))
                                to_fetch.append((src, search_url, False))
                    elif not _entity_task.done():
                        _entity_task.cancel()  # Not needed — cancel to free resources

                    # Detect year-ranking queries: "best anime of 2025", "top 2024 anime" etc.
                    q_words = set(w.lower() for w in re.findall(r'\w{3,}', clean_q))
                    _year_rank_kw = re.compile(r'\b(best|top|highest|greatest|popular|rated)\b', re.I)
                    _year_match = re.search(r'\b(20\d{2})\b', q)
                    if _year_match and _year_rank_kw.search(q):
                        yr = _year_match.group(1)
                        year_url = f"https://api.jikan.moe/v4/anime?start_date={yr}-01-01&end_date={yr}-12-31&order_by=score&sort=desc&limit=5"
                        try:
                            async with _hx.AsyncClient(timeout=5) as client:
                                yr_resp = await client.get(year_url, headers={"User-Agent": "Mozilla/5.0"})
                            if yr_resp.status_code == 200:
                                yr_items = yr_resp.json().get("data", [])[:5]
                                yr_text = "\n\n".join(_flatten_to_text(i) for i in yr_items if _flatten_to_text(i).strip())
                                if yr_text.strip():
                                    context = f"[Top anime of {yr} from Jikan]\n{yr_text}" + ("\n\n" + context if context else "")
                                    if year_url not in sources: sources.insert(0, year_url)
                                    logger.info(f"[LIVE FALLBACK] Year query {yr}: {q[:50]}")
                        except Exception as _ye:
                            logger.warning(f"[LIVE FALLBACK] Year query failed: {_ye}")

                    async def _fetch_one(src, search_url, is_ranking, client):
                        hdrs = {"User-Agent": "Mozilla/5.0"}
                        if src.get("api_key"): hdrs["Authorization"] = f"Bearer {src['api_key']}"
                        try:
                            # Static ranking sources (no {q}) cached 30 min; search sources 10 min
                            _ttl = 1800 if is_ranking else 600
                            _now = time.time()
                            if search_url in _api_resp_cache:
                                cached_raw, cached_exp = _api_resp_cache[search_url]
                                if _now < cached_exp:
                                    raw = cached_raw
                                    logger.info(f"[LIVE FALLBACK] Cache hit: {src['name']}")
                                else:
                                    del _api_resp_cache[search_url]
                                    resp = await client.get(search_url, headers=hdrs)
                                    if resp.status_code != 200: return None
                                    raw = resp.json()
                                    _cache_insert(search_url, raw, _now + _ttl)
                            else:
                                resp = await client.get(search_url, headers=hdrs)
                                if resp.status_code != 200: return None
                                raw = resp.json()
                                _cache_insert(search_url, raw, _now + _ttl)
                            obj = raw
                            if src.get("json_path"):
                                for key in src["json_path"].split("."):
                                    if isinstance(obj, dict): obj = obj.get(key, obj)
                            elif isinstance(raw, dict):
                                for v in raw.values():
                                    if isinstance(v, list) and v: obj = v; break
                            items = (obj if isinstance(obj, list) else [obj])[:(150 if is_ranking else 5)]
                            # Relevance check only for search queries (not ranking lists)
                            # Trust the API's own search relevance — no client-side filtering needed
                            # Ranking sources: compact one-liner with rank field
                            # Search sources: compact one-liner with score/rank/genres (much smaller than full flatten)
                            _fmt = _flatten_ranking_item if is_ranking else _flatten_search_item
                            live_text = "\n".join(_fmt(i) for i in items if _fmt(i).strip())
                            if not live_text.strip(): return None
                            return (src, search_url, items, live_text)
                        except Exception as _e:
                            logger.warning(f"[LIVE FALLBACK] {src['name']} failed: {_e}")
                            return None

                    if to_fetch:
                        async with _hx.AsyncClient(timeout=10) as client:
                            fetch_results = await asyncio.gather(
                                *[_fetch_one(s, u, r, client) for s, u, r in to_fetch]
                            )
                        for result in fetch_results:
                            if result is None: continue
                            src, search_url, items, live_text = result
                            try:
                                from langchain_core.documents import Document as _Doc
                                live_docs = [_Doc(page_content=_flatten_to_text(i).strip(),
                                             metadata={"source": search_url, "api_name": src["name"]})
                                             for i in items if len(_flatten_to_text(i).strip()) > 20]
                                # Only cache API responses in DB for crawl-based DBs.
                                # API-only DBs (no crawl_url) should not accumulate stale chunks.
                                _db_has_crawl = bool(db_cfg.get("crawl_url", ""))
                                if live_docs and db and _db_has_crawl:
                                    loop = asyncio.get_running_loop()
                                    loop.run_in_executor(None, lambda d=live_docs: db.add_documents(d))
                            except Exception as e:
                                logger.debug(f"[LIVE FALLBACK] Doc save failed: {e}")
                            live_block = f"[Live data from {src['name']}]\n{live_text}"
                            context = live_block + ("\n\n" + context if context else "")
                            if search_url not in sources: sources.insert(0, search_url)
                            logger.info(f"[LIVE FALLBACK] Used API '{src['name']}' for query: {q[:50]}")
    except Exception as _e:
        logger.warning(f"[LIVE FALLBACK] outer error: {_e}")
    finally:
        # Clean up entity task if it wasn't awaited (no api_sources path)
        if not _entity_task.done():
            _entity_task.cancel()

    return context, len(top), sources[:5]

def _fast_format_jikan(context: str, q: str) -> str | None:
    """Return a formatted response directly from Jikan live data, skipping the LLM.
    Returns None if LLM is still needed (specific anime info, comparisons, etc.)."""
    if "[Live data from" not in context:
        return None

    q_lower = q.lower()
    # Only fast-format clearly structured queries; fall back for detailed info requests
    _needs_llm = re.compile(
        r'\b(tell me about|explain|describe|synopsis|plot|story|review|opinion|compare|versus|vs|'
        r'recommend to me|should i watch|worth watching|better than|character|voice actor|'
        r'who is|who are|what is the (story|plot|summary)|detail)\b', re.I)
    if _needs_llm.search(q_lower):
        return None

    # Parse live data sections
    sections: list[tuple[str, list[str]]] = []
    cur_src = ""
    cur_items: list[str] = []
    for line in context.split("\n"):
        if line.startswith("[Live data from "):
            if cur_src and cur_items:
                sections.append((cur_src, cur_items[:]))
            cur_src = line[len("[Live data from "):-1]
            cur_items = []
        elif line.strip() and cur_src:
            cur_items.append(line.strip())
    if cur_src and cur_items:
        sections.append((cur_src, cur_items[:]))
    if not sections:
        return None

    rank_pos = _RANK_POS_RE.search(q_lower)
    parts: list[str] = []

    for src, items in sections:
        sl = src.lower()

        # ── Genre sources (Isekai / Mecha / Romance etc.) ────────────────────
        if "genre" in sl:
            genre = src.split("Genre")[-1].strip() if "Genre" in src else "anime"
            parts.append(f"Here are the top-rated **{genre}** anime on MyAnimeList:\n")
            for i, item in enumerate(items[:10], 1):
                title = item.split(" | ")[0].strip()
                score = next((p.replace("Score:", "").strip() for p in item.split(" | ") if "Score:" in p), "")
                parts.append(f"{i}. {title}" + (f" — Score: {score}" if score else "") + "\n")

        # ── Airing Now ───────────────────────────────────────────────────────
        elif "airing now" in sl:
            parts.append("Anime currently airing this season:\n")
            for i, item in enumerate(items[:10], 1):
                title = item.split(" | ")[0].strip()
                score = next((p.replace("Score:", "").strip() for p in item.split(" | ") if "Score:" in p), "")
                parts.append(f"{i}. {title}" + (f" — Score: {score}" if score else "") + "\n")

        # ── Upcoming ─────────────────────────────────────────────────────────
        elif "upcoming" in sl:
            parts.append("Upcoming anime next season:\n")
            for i, item in enumerate(items[:8], 1):
                title = item.split(" | ")[0].strip()
                parts.append(f"{i}. {title}\n")

        # ── Top Rated pages (ranking position queries) ───────────────────────
        elif "top rated p" in sl or "top airing" in sl or "most popular" in sl:
            page_offset = 75 if "p4" in sl else 50 if "p3" in sl else 25 if "p2" in sl else 0
            if rank_pos and ("top rated p" in sl):
                pos = int(rank_pos.group(1))
                local_idx = pos - page_offset - 1
                if 0 <= local_idx < len(items):
                    item = items[local_idx]
                    title = item.split(": ", 1)[-1].split(" | ")[0] if ": " in item else item.split(" | ")[0]
                    score = next((p.replace("Score:", "").strip() for p in item.split(" | ") if "Score:" in p), "")
                    return f"The **{pos}th highest rated** anime on MAL is: **{title}**" + (f" (Score: {score})" if score else "") + "."
            else:
                label = "Top rated" if "top rated" in sl else ("Top airing" if "top airing" in sl else "Most popular")
                parts.append(f"{label} anime on MAL:\n")
                for i, item in enumerate(items[:10], 1):
                    title = item.split(": ", 1)[-1].split(" | ")[0] if ": " in item else item.split(" | ")[0]
                    score = next((p.replace("Score:", "").strip() for p in item.split(" | ") if "Score:" in p), "")
                    parts.append(f"{i}. {title}" + (f" — Score: {score}" if score else "") + "\n")

        # ── Manga top list ────────────────────────────────────────────────────
        elif "top manga" in sl:
            parts.append("Top manga on MAL:\n")
            for i, item in enumerate(items[:10], 1):
                title = item.split(": ", 1)[-1].split(" | ")[0] if ": " in item else item.split(" | ")[0]
                score = next((p.replace("Score:", "").strip() for p in item.split(" | ") if "Score:" in p), "")
                parts.append(f"{i}. {title}" + (f" — Score: {score}" if score else "") + "\n")

        # ── Search results — skip (other structured sections may still render)
        elif "search" in sl:
            continue  # Skip search sections; LLM only if no other sections produced output

    return "".join(parts).strip() or None


# NOTE: retrieve_context uses _flatten_to_text for live API fallback formatting. In app.py this
# helper lives elsewhere; we keep an identical copy here to keep retrieval self-contained.
def _flatten_to_text(obj, depth=0) -> str:
    """Recursively flatten dict/list to readable key: value lines."""
    lines = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, (dict, list)):
                lines.append(f"{'  '*depth}{k}:")
                lines.append(_flatten_to_text(v, depth+1))
            elif v is not None and str(v).strip():
                lines.append(f"{'  '*depth}{k}: {v}")
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, (dict, list)):
                lines.append(_flatten_to_text(item, depth))
            elif item is not None and str(item).strip():
                lines.append(f"{'  '*depth}{item}")
    else:
        lines.append(str(obj))
    return "\n".join(lines)
