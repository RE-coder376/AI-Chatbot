import argparse
import hashlib
import json
import logging
import os
import re
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import requests
from evals.llm_judge import JudgeVerdict, derive_fallback_verdict, judge_answer
from services.config import get_config

EVALS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = EVALS_DIR.parent
DATABASES_DIR = PROJECT_ROOT / "databases"
logger = logging.getLogger(__name__)


IDK_SIGS = (
    "i don't have",
    "i do not have",
    "don't have specific details",
    "don't have enough information",
    "i'm not sure",
    "i am not sure",
    "i cannot find",
    "i can't find",
    "no information",
    "not available",
    "not in my knowledge base",
    "not in the knowledge base",
)

REFUSE_SIGS = (
    "i can't help with that",
    "i cannot help with that",
    "outside that scope",
    "outside my scope",
    "i specialize in helping with",
    "i can only help with",
    "i only help with",
    "i'm only here to help with",
)

GENERIC_OOS_PATTERNS = (
    re.compile(r"\bweather\b", re.I),
    re.compile(r"\bcapital of\b", re.I),
    re.compile(r"\b2\s*[\+\-x\*\/]\s*2\b", re.I),
    re.compile(r"\bceo'?s favorite color\b", re.I),
    re.compile(r"\bhack\b", re.I),
    re.compile(r"\bscrape twitter\b", re.I),
    re.compile(r"\bfifa world cup\b", re.I),
    re.compile(r"\bchocolate cake\b", re.I),
    re.compile(r"\bintegral of\b", re.I),
    re.compile(r"\bsystem prompt\b", re.I),
    re.compile(r"\bignore (all|your|previous) instructions\b", re.I),
    re.compile(r"\bdrop table\b", re.I),
    re.compile(r"\bgaming laptop\b", re.I),
    re.compile(r"\btouchscreen\b", re.I),
    re.compile(r"\bfirst one you mentioned\b", re.I),
    re.compile(r"\bfrom now on your name\b", re.I),
    re.compile(r"\bwhat is your name\b", re.I),
)

STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "how",
    "i", "in", "is", "it", "me", "my", "of", "on", "or", "our", "that",
    "the", "this", "to", "us", "we", "what", "when", "where", "which", "who",
    "why", "with", "your",
}

CODEISH_PATTERNS = (
    re.compile(r"[{}<>\\]"),
    re.compile(r"\bprint\s*\("),
    re.compile(r"\bdef\s+\w+\s*\("),
    re.compile(r"\bclass\s+\w+"),
    re.compile(r"\bimport\s+\w+"),
    re.compile(r"\breturn\s+"),
)

INSTRUCTIONAL_PATTERNS = (
    re.compile(r"\bwhat you are learning\b", re.I),
    re.compile(r"\btry with ai\b", re.I),
    re.compile(r"\bhere is my data\b", re.I),
    re.compile(r"\bmeeting notes\b", re.I),
    re.compile(r"\buse these codes\b", re.I),
    re.compile(r"\bquality-check\b", re.I),
    re.compile(r"\blast verified\b", re.I),
    re.compile(r"\bdeliverable\b", re.I),
    re.compile(r"\bpolicy reference\b", re.I),
)

TOPIC_NOISE_PATTERNS = (
    re.compile(r"\bmib/s\b", re.I),
    re.compile(r"\bkib\b", re.I),
    re.compile(r"\bobjects:\b", re.I),
    re.compile(r"\bdone\b", re.I),
    re.compile(r"\bmkdir\b", re.I),
    re.compile(r"\bcd\s+~/", re.I),
    re.compile(r"\blearning-spec\b", re.I),
)


@dataclass
class EvalItem:
    q: str
    expect: str = "ANSWER"
    source: str = "analytics"
    difficulty: str = "medium"
    reference_answer: str = ""
    frequency: int = 0
    retrieve_doc_count: int = 0
    retrieve_context_length: int = 0
    retrieve_context_preview: str = ""
    retrieve_sources: list[str] = field(default_factory=list)
    selection_bucket: str = ""
    candidate_key: str = ""


class EvalAuthError(RuntimeError):
    pass


def _read_json(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _normalize_question(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _looks_like_oos_or_noise(question: str) -> bool:
    q = _normalize_question(question)
    if len(q) < 8:
        return True
    if len(q.split()) < 2:
        return True
    if len(q) > 180:
        return True
    if q.count("?") > 1:
        return True
    if sum(1 for ch in q if ch in ":;[]{}|") >= 4:
        return True
    if any(p.search(q) for p in CODEISH_PATTERNS):
        return True
    if any(p.search(q) for p in INSTRUCTIONAL_PATTERNS):
        return True
    return any(p.search(q) for p in GENERIC_OOS_PATTERNS)


def _difficulty_for_question(question: str) -> str:
    q = _normalize_question(question).lower()
    score = 0
    if len(q) >= 80:
        score += 1
    if re.search(r"\b(compare|difference|versus|vs\.?|under|at least|between|exact|specific)\b", q):
        score += 1
    if re.search(r"\b(and|or)\b", q) and len(q.split()) >= 10:
        score += 1
    if score >= 2:
        return "hard"
    if score == 1:
        return "medium"
    return "easy"


def _load_faq_items(db_name: str) -> list[EvalItem]:
    db_dir = DATABASES_DIR / db_name
    faqs = _read_json(db_dir / "faqs.json") or []
    items: list[EvalItem] = []
    scope_tokens = _tenant_scope_tokens(db_name)
    if not isinstance(faqs, list):
        return items
    for faq in faqs:
        if not isinstance(faq, dict):
            continue
        q = _normalize_question(faq.get("question", ""))
        a = _normalize_question(faq.get("answer", ""))
        if not q or not a or _looks_like_oos_or_noise(q):
            continue
        if not (
            _is_tenant_relevant_question(q, db_name, scope_tokens)
            or _is_tenant_relevant_text(a, db_name, scope_tokens)
        ):
            continue
        items.append(
            EvalItem(
                q=q,
                source="faq",
                difficulty=_difficulty_for_question(q),
                reference_answer=a,
                frequency=1000,
            )
        )
    return items


def _analytics_candidates_path(db_name: str) -> Path:
    db_dir = (DATABASES_DIR / db_name).resolve()
    analytics_path = (db_dir / "analytics.json").resolve()
    if analytics_path.parent != db_dir:
        raise RuntimeError(f"Unsafe analytics path resolved for '{db_name}': {analytics_path}")
    return analytics_path


def _tenant_scope_tokens(db_name: str) -> set[str]:
    cfg = get_config(db_name)
    scope_text = " ".join(
        [
            str(cfg.get("business_name") or ""),
            str(cfg.get("topics") or ""),
            str(cfg.get("business_description") or ""),
        ]
    )
    tokens = _token_set(scope_text)
    if tokens:
        return tokens
    # Fallback for DBs missing config.json: infer scope from stored source URLs
    # and the tenant name itself so eval generation stays tenant-local instead
    # of inheriting unrelated global/root config.
    inferred = set(_token_set(db_name.replace("_", " ").replace("-", " ")))
    inferred_counts: Counter[str] = Counter()
    try:
        rows = _load_document_rows(db_name)
        for row in rows[:100]:
            _, text, source = _unpack_doc_row(row)
            if not source:
                pass
            else:
                host = source.split("://", 1)[-1].split("/", 1)[0]
                host = host.replace("www.", "").replace(".", " ").replace("-", " ")
                inferred.update(_token_set(host))
            for tok in _token_set(text):
                if len(tok) < 4:
                    continue
                if tok in {
                    "this", "that", "with", "from", "your", "have", "will", "into", "about",
                    "chapter", "chapters", "course", "courses", "module", "modules", "lesson",
                    "lessons", "price", "pricing", "product", "products", "agent", "agents",
                }:
                    continue
                inferred_counts[tok] += 1
        for tok, _count in inferred_counts.most_common(10):
            inferred.add(tok)
            if len(inferred) >= 16:
                break
    except Exception:
        pass
    return inferred


def _is_tenant_relevant_question(question: str, db_name: str, scope_tokens: set[str] | None = None) -> bool:
    q_tokens = _token_set(question)
    if not q_tokens:
        return False
    scope_tokens = scope_tokens if scope_tokens is not None else _tenant_scope_tokens(db_name)
    if not scope_tokens:
        return True
    # Hard OOS signals: words that clearly belong to a different tenant's domain.
    # Only applied when the DB is NOT a retail/product DB (scope has no product tokens).
    scope_is_retail = bool(scope_tokens & {"toy", "toys", "baby", "kids", "product", "products",
                                           "shop", "store", "buy", "sell", "price", "prices"})
    if not scope_is_retail:
        hard_oos = {
            "fountain", "pen", "pens", "stationery", "ink", "pencil", "pencils",
            "laptop", "laptops", "phone", "phones", "gaming", "gpu", "touchscreen",
            "anime", "manga", "episode", "episodes", "airing",
        }
        if q_tokens & hard_oos:
            return False
    # Expand scope with simple singular/plural variants to catch "car" vs "cars" etc.
    scope_expanded = set(scope_tokens)
    for t in scope_tokens:
        if t.endswith("s") and len(t) > 3:
            scope_expanded.add(t[:-1])
        elif len(t) > 2:
            scope_expanded.add(t + "s")
    overlap = q_tokens & scope_expanded
    if overlap:
        return True
    # Generic business terms valid for any tenant (no edu gate)
    generic_ok = {
        "price", "pricing", "cost", "warranty", "return", "refund", "shipping",
        "delivery", "stock", "available", "availability", "sale", "discount",
        "course", "courses", "program", "programs", "curriculum",
        "enrollment", "enrolment", "team", "leader", "leadership", "support",
        "contact", "chapter", "chapters", "module", "modules", "agent", "agents",
        "education", "learning", "learn", "class", "classes", "subscription",
        "subscriptions", "plan", "plans", "cancel", "billing",
    }
    return bool(q_tokens & generic_ok)


def _is_tenant_relevant_text(text: str, db_name: str, scope_tokens: set[str] | None = None) -> bool:
    qn = _normalize_question(text)
    return bool(qn) and _is_tenant_relevant_question(qn, db_name, scope_tokens)


def _load_analytics_candidates(db_name: str) -> list[EvalItem]:
    analytics_path = _analytics_candidates_path(db_name)
    logger.info("[EVAL] Loading analytics candidates for '%s' from %s", db_name, analytics_path)
    analytics = _read_json(analytics_path) or {}
    questions = Counter()
    scope_tokens = _tenant_scope_tokens(db_name)
    offscope_filtered = 0

    q_counts = analytics.get("questions") or {}
    if isinstance(q_counts, dict):
        for q, count in q_counts.items():
            qn = _normalize_question(q)
            if not qn or not _looks_like_good_question(qn):
                continue
            if not _is_tenant_relevant_question(qn, db_name, scope_tokens):
                offscope_filtered += 1
                continue
            try:
                questions[qn] += int(count or 0)
            except Exception:
                questions[qn] += 1

    history = analytics.get("history") or []
    if isinstance(history, list):
        for entry in history:
            if not isinstance(entry, dict):
                continue
            qn = _normalize_question(entry.get("q", ""))
            if not qn or not _looks_like_good_question(qn):
                continue
            if not _is_tenant_relevant_question(qn, db_name, scope_tokens):
                offscope_filtered += 1
                continue
            questions[qn] += 1

    if offscope_filtered:
        logger.warning(
            "[EVAL] Filtered %s off-tenant analytics questions for '%s' from %s",
            offscope_filtered,
            db_name,
            analytics_path,
        )

    items = [
        EvalItem(
            q=q,
            source="analytics",
            difficulty=_difficulty_for_question(q),
            frequency=freq,
        )
        for q, freq in questions.most_common()
    ]
    return items


def _load_gap_candidates(db_name: str) -> list[EvalItem]:
    db_dir = DATABASES_DIR / db_name
    gaps = _read_json(db_dir / "knowledge_gaps.json") or []
    items: list[EvalItem] = []
    scope_tokens = _tenant_scope_tokens(db_name)
    if not isinstance(gaps, list):
        return items
    for gap in gaps:
        if not isinstance(gap, dict):
            continue
        q = _normalize_question(gap.get("question", ""))
        if not q or not _looks_like_good_question(q):
            continue
        if not _is_tenant_relevant_question(q, db_name, scope_tokens):
            continue
        items.append(
            EvalItem(
                q=q,
                source="knowledge_gap",
                difficulty=_difficulty_for_question(q),
                frequency=1,
            )
        )
    return items


def _load_document_rows_via_chroma(db_name: str) -> list[tuple[str, str, str]]:
    db_dir = DATABASES_DIR / db_name
    try:
        import chromadb

        client = chromadb.PersistentClient(path=str(db_dir))
        rows: list[tuple[str, str, str]] = []
        for collection in client.list_collections():
            try:
                payload = collection.get(include=["documents", "metadatas"])
            except Exception:
                continue
            ids = payload.get("ids") or []
            documents = payload.get("documents") or []
            metadatas = payload.get("metadatas") or []
            for row_id, doc, metadata in zip(ids, documents, metadatas):
                if isinstance(doc, str) and doc.strip():
                    source = ""
                    if isinstance(metadata, dict):
                        source = str(metadata.get("source") or "")
                    rows.append((str(row_id), doc, source))
        return rows
    except Exception:
        return []


def _load_document_rows_via_sqlite(db_name: str) -> list[tuple[str, str, str]]:
    db_path = DATABASES_DIR / db_name / "chroma.sqlite3"
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT docs.id, docs.string_value, COALESCE(src.string_value, '') "
            "FROM embedding_metadata docs "
            "LEFT JOIN embedding_metadata src ON src.id = docs.id AND src.key='source' "
            "WHERE docs.key='chroma:document' ORDER BY docs.id",
        ).fetchall()
        conn.close()
        return [(str(row_id), raw_text or "", source or "") for row_id, raw_text, source in rows]
    except Exception:
        return []


def _load_document_rows(db_name: str) -> list[tuple[str, str, str]]:
    # Try SQLite first (read-only, no file lock conflict with running server).
    # Fall back to ChromaDB client only if SQLite finds nothing.
    rows = _load_document_rows_via_sqlite(db_name)
    if rows:
        return rows
    return _load_document_rows_via_chroma(db_name)


def _unpack_doc_row(row) -> tuple[str, str, str]:
    if len(row) >= 3:
        row_id, text, source = row[0], row[1], row[2]
        return str(row_id), str(text or ""), str(source or "")
    row_id, text = row[0], row[1]
    return str(row_id), str(text or ""), ""


def _load_doc_qa_fallbacks(db_name: str, limit: int) -> list[EvalItem]:
    items: list[EvalItem] = []
    rows = _load_document_rows(db_name)
    scope_tokens = _tenant_scope_tokens(db_name)
    if not rows:
        return items

    for row in rows:
        _, text, _ = _unpack_doc_row(row)
        match = re.search(r"Q:\s*([^\n\r]{3,220}?)\s*A:\s*(.+)", text, re.S | re.I)
        if not match:
            continue
        q = _normalize_question(match.group(1))
        a = _normalize_question(match.group(2))
        if not q or not a or not _looks_like_good_question(q):
            continue
        if not (
            _is_tenant_relevant_question(q, db_name, scope_tokens)
            or _is_tenant_relevant_text(a, db_name, scope_tokens)
        ):
            continue
        items.append(
            EvalItem(
                q=q,
                expect=_infer_expectation_from_reference(a),
                source="embedded_qa",
                difficulty=_difficulty_for_question(q),
                reference_answer=a,
                frequency=5,
                candidate_key=f"embedded_qa::{q.lower()}",
            )
        )
        if len(items) >= limit:
            break
    return items


def _chunk_state_file(db_name: str) -> Path:
    return EVALS_DIR / "state" / f"{db_name}_rotation.json"


def _load_rotation_state(db_name: str) -> dict:
    path = _chunk_state_file(db_name)
    data = _read_json(path) or {}
    if not isinstance(data, dict):
        return {"cursor": 0}
    return {"cursor": int(data.get("cursor", 0) or 0)}


def _save_rotation_state(db_name: str, cursor: int) -> None:
    path = _chunk_state_file(db_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"cursor": int(cursor)}, indent=2), encoding="utf-8")


def _clean_chunk_text(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    cleaned = re.sub(r"Skip to main content", "", cleaned, flags=re.I)
    cleaned = re.sub(r"Toggle theme", "", cleaned, flags=re.I)
    cleaned = re.sub(r"Sign In Sign Up", "", cleaned, flags=re.I)
    cleaned = re.sub(r"Privacy\s+PANAVERSITY.*$", "", cleaned, flags=re.I)
    cleaned = re.sub(r"SITE NAVIGATION AND STRUCTURE:\s*", "", cleaned, flags=re.I)
    return re.sub(r"\s+", " ", cleaned).strip()


def _looks_like_instructional_chunk(text: str) -> bool:
    cleaned = _clean_chunk_text(text)
    if not cleaned:
        return True
    lower = cleaned.lower()
    if any(p.search(cleaned) for p in CODEISH_PATTERNS):
        return True
    if any(p.search(cleaned) for p in INSTRUCTIONAL_PATTERNS):
        return True
    if "```" in cleaned or "\"\"\"" in cleaned:
        return True
    if lower.count("chapter") >= 3 and lower.count(":") >= 3:
        return True
    return False


def _is_low_value_chunk(text: str) -> bool:
    cleaned = _clean_chunk_text(text)
    if len(cleaned.split()) < 20:
        return True
    if _looks_like_instructional_chunk(cleaned):
        return True
    junk_hits = sum(
        1
        for phrase in (
            "skip to main content",
            "sign in",
            "privacy",
            "all rights reserved",
            "leaderboard",
            "dashboard",
            "toggle theme",
        )
        if phrase in cleaned.lower()
    )
    return junk_hits >= 2


def _looks_like_good_topic(topic: str) -> bool:
    norm = _normalize_question(topic)
    if not norm or len(norm) < 6 or len(norm) > 90:
        return False
    words = norm.split()
    if len(words) < 2 or len(words) > 10:
        return False
    if any(p.search(norm) for p in CODEISH_PATTERNS):
        return False
    if any(p.search(norm) for p in INSTRUCTIONAL_PATTERNS):
        return False
    if any(p.search(norm) for p in TOPIC_NOISE_PATTERNS):
        return False
    if sum(1 for ch in norm if ch in ":;[]{}|") >= 3:
        return False
    if norm.lower().startswith(("what ", "how ", "why ", "here ", "this ", "that ", "they ", "you ")):
        return False
    return True


def _looks_like_good_question(question: str) -> bool:
    q = _normalize_question(question)
    if _looks_like_oos_or_noise(q):
        return False
    if len(q) > 160:
        return False
    if not (
        q.endswith("?")
        or q.lower().startswith(("what ", "how ", "why ", "when ", "where ", "who ", "which ", "can ", "does ", "is ", "are ", "should ", "summarize ", "compare "))
    ):
        return False
    return True


def _infer_expectation_from_reference(reference_text: str) -> str:
    text = (reference_text or "").lower()
    if any(sig in text for sig in IDK_SIGS):
        return "IDK"
    if "escalated this" in text or "couldn't find information" in text or "could not find information" in text:
        return "IDK"
    return "ANSWER"


def _topic_from_source(source: str) -> str:
    raw = (source or "").strip().rstrip("/")
    if not raw:
        return ""
    slug = raw.split("/")[-1]
    slug = re.sub(r"\.[a-z0-9]+$", "", slug, flags=re.I)
    slug = slug.replace("-", " ").replace("_", " ").strip()
    slug = re.sub(r"\s+", " ", slug)
    if slug.lower() in {"docs", "english", "leaderboard"}:
        return ""
    topic = " ".join(word.capitalize() if word.islower() else word for word in slug.split())
    return topic if _looks_like_good_topic(topic) else ""


def _extract_chunk_topic(text: str, source: str = "") -> str:
    cleaned = _clean_chunk_text(text)
    if not cleaned:
        return ""

    product_specs = re.search(r"Full specs:\s*([^,]{3,80})", cleaned, re.I)
    if product_specs:
        topic = _normalize_question(product_specs.group(1))
        if _looks_like_good_topic(topic):
            return topic

    product = re.search(r"Product:\s*([A-Za-z0-9/&+'\"().,\- ]{3,90}?)\s+Price:", cleaned, re.I)
    if product:
        topic = _normalize_question(product.group(1))
        if _looks_like_good_topic(topic) and topic.lower() not in {"black", "blue", "white", "silver", "gold"}:
            return topic

    chapter = re.search(
        r"(Chapter\s+\d+\s*:\s*(?:[A-Z][A-Za-z0-9/&,\-()']*\s*){1,8})",
        cleaned,
    )
    if chapter:
        topic = _normalize_question(chapter.group(1))
        return topic if _looks_like_good_topic(topic) else ""

    source_topic = _topic_from_source(source)
    if source_topic:
        return source_topic

    heading = re.search(r"([A-Z][A-Za-z0-9/&,\-()' ]{4,80})[:.]", cleaned)
    if heading:
        topic = _normalize_question(heading.group(1))
        words = topic.split()
        titleish = sum(1 for word in words if word[:1].isupper() or word.isupper())
        if _looks_like_good_topic(topic) and len(words) <= 8 and titleish / max(1, len(words)) >= 0.6:
            return topic

    return ""


def _split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+|\n+", _clean_chunk_text(text))
    sentences = []
    for part in parts:
        sent = _normalize_question(part).strip(" .")
        if len(sent) < 20:
            continue
        if _looks_like_instructional_chunk(sent):
            continue
        if re.search(r"\bprompt\s+\d+\b", sent, re.I):
            continue
        sentences.append(sent)
    return sentences


def _chunk_reference_answer(topic: str, chunk_text: str) -> str:
    topic_tokens = _token_set(topic)
    sentences = _split_sentences(chunk_text)
    if not sentences:
        return _clean_chunk_text(chunk_text)[:500]

    scored: list[tuple[float, str]] = []
    for idx, sentence in enumerate(sentences):
        sent_tokens = _token_set(sentence)
        overlap = len(topic_tokens & sent_tokens)
        score = overlap
        if re.search(r"\b(step|process|workflow|allows|requires|includes|means|used to|helps|lets you)\b", sentence, re.I):
            score += 0.75
        if idx == 0:
            score += 0.4
        scored.append((score, sentence))

    picked: list[str] = []
    seen = set()
    for _, sentence in sorted(scored, key=lambda x: (-x[0], sentences.index(x[1]))):
        key = sentence.lower()
        if key in seen:
            continue
        seen.add(key)
        picked.append(sentence)
        if len(" ".join(picked)) >= 420 or len(picked) >= 3:
            break

    return " ".join(picked)[:500] if picked else _clean_chunk_text(chunk_text)[:500]


def _make_chunk_question(topic: str, chunk_text: str) -> str:
    topic = _normalize_question(topic)
    if not topic:
        return ""
    text = _clean_chunk_text(chunk_text).lower()
    low_topic = topic.lower()
    reference = _chunk_reference_answer(topic, chunk_text)
    ref_lower = reference.lower()
    if re.search(r"\b(quiz|exercise|worksheet|assignment|prompt \d+|module \d+)\b", low_topic):
        return ""
    if re.search(r"\b(what you are learning|exercise \d+|prompt \d+|producer vs consumer)\b", text):
        return ""
    if re.search(r"\b(prerequisite|prerequisites|requirement|requirements)\b", ref_lower):
        question = f"What are the prerequisites for {topic}?"
    elif re.search(r"\b(price|pricing|cost|fee|fees)\b", ref_lower):
        question = f"What is the pricing for {topic}?"
    elif re.search(r"\b(duration|timeline|week|weeks|month|months|hours|complete|finish)\b", ref_lower):
        question = f"How long does {topic} take to complete?"
    elif re.search(r"\b(step 1|step 2|follow these steps|to create|to build|to add|to configure|to set up)\b", text):
        if re.search(r"\bchapter\s+\d+\b", low_topic, re.I):
            question = f"What is the process covered in {topic}?"
        elif len(topic.split()) <= 4:
            question = f"How does {topic} work?"
        else:
            question = f"What is the process for {topic}?"
    elif re.search(r"\bcompare|difference|versus|vs\.?\b", low_topic, re.I):
        question = f"What is the difference explained in {topic}?"
    elif re.search(r"\bchapter\s+\d+\b", low_topic, re.I):
        question = f"What does {topic} cover?"
    elif len(topic.split()) <= 4:
        question = f"What is {topic}?"
    elif re.search(r"\b(policy|workflow|system|framework|review|management|configuration)\b", low_topic):
        question = f"How does {topic} work?"
    else:
        question = f"What does {topic} cover?"
    return question if _looks_like_good_question(question) else ""


def _candidate_row_order(rows: list[tuple[str, str]]) -> list[tuple[str, str]]:
    return sorted(
        rows,
        key=lambda row: int(hashlib.sha1(str(row[0]).encode("utf-8")).hexdigest()[:12], 16),
    )


def _load_chunk_topic_candidates(db_name: str, limit: int) -> list[EvalItem]:
    rows = _load_document_rows(db_name)
    if not rows:
        return []

    items: list[EvalItem] = []
    seen_topics = set()
    max_candidates = max(limit * 12, 120)
    for row in _candidate_row_order(rows):
        row_id, text, source = _unpack_doc_row(row)
        if _is_low_value_chunk(text):
            continue
        topic = _extract_chunk_topic(text, source)
        if not topic:
            continue
        topic_key = topic.lower()
        if topic_key in seen_topics:
            continue
        seen_topics.add(topic_key)
        question = _make_chunk_question(topic, text)
        if not question:
            continue
        items.append(
            EvalItem(
                q=question,
                source="chunk_topic",
                difficulty=_difficulty_for_question(question),
                reference_answer=_chunk_reference_answer(topic, text),
                frequency=2,
                candidate_key=f"chunk::{row_id}::{topic_key}",
            )
        )
        if len(items) >= max_candidates:
            break
    return items


def _collect_seed_items(db_name: str, desired_count: int) -> list[EvalItem]:
    candidates: list[EvalItem] = []
    candidates.extend(_load_faq_items(db_name))
    candidates.extend(_load_analytics_candidates(db_name))
    candidates.extend(_load_gap_candidates(db_name))
    candidates.extend(_load_doc_qa_fallbacks(db_name, desired_count))
    candidates.extend(_load_chunk_topic_candidates(db_name, desired_count))

    deduped: list[EvalItem] = []
    seen = set()
    for item in candidates:
        key = item.q.lower()
        if key in seen:
            continue
        seen.add(key)
        if not item.candidate_key:
            item.candidate_key = f"{item.source}::{key}"
        deduped.append(item)
    return _trim_seed_pool(deduped, desired_count)


def _is_tenant_relevant_eval_question(question: str, db_name: str) -> bool:
    qn = _normalize_question(question)
    return bool(qn) and _looks_like_good_question(qn) and _is_tenant_relevant_question(qn, db_name)


def _trim_seed_pool(items: list[EvalItem], desired_count: int) -> list[EvalItem]:
    if not items:
        return []

    desired_count = max(1, desired_count)
    per_source_cap = {
        "faq": max(4, desired_count),
        "embedded_qa": max(6, desired_count),
        "analytics": max(10, desired_count * 2),
        "chunk_topic": max(12, desired_count * 2),
        "knowledge_gap": max(6, desired_count),
    }
    trimmed: list[EvalItem] = []
    for source in ("faq", "embedded_qa", "analytics", "chunk_topic", "knowledge_gap"):
        group = [item for item in items if item.source == source]
        group.sort(key=_source_priority)
        trimmed.extend(_select_diverse_items(group, per_source_cap.get(source, desired_count)))

    target = max(desired_count, min(max(desired_count * 2, 12), 30))
    trimmed.sort(key=_source_priority)
    selected = _select_diverse_items(trimmed, target)
    if len(selected) >= target:
        return selected

    seen = {item.q.lower() for item in selected}
    for item in trimmed:
        if item.q.lower() in seen:
            continue
        selected.append(item)
        seen.add(item.q.lower())
        if len(selected) >= target:
            break
    return selected


def _preflight_retrieve(base_url: str, password: str, item: EvalItem) -> EvalItem:
    try:
        res = requests.post(
            f"{base_url}/debug/retrieve",
            json={"password": password, "question": item.q},
            timeout=60,
        )
        if res.status_code in (401, 403):
            raise EvalAuthError(
                "Owner auth failed during eval preflight. Pass the owner password used for /debug/retrieve."
            )
        if res.status_code != 200:
            return item
        data = res.json() or {}
        item.retrieve_doc_count = int(data.get("doc_count") or 0)
        item.retrieve_context_length = int(data.get("context_length") or 0)
        item.retrieve_context_preview = str(data.get("context_preview") or "")
        item.retrieve_sources = list(data.get("sources") or [])[:5]
        return item
    except EvalAuthError:
        raise
    except Exception:
        return item


def _is_grounded(item: EvalItem) -> bool:
    # FAQs/embedded QA are pre-verified; analytics items need higher bar to avoid
    # navigation-only matches (e.g. chapter title in TOC != chapter content in KB).
    if item.source in {"faq", "embedded_qa"}:
        min_context = 80
        min_overlap = 0.08
    else:
        min_context = 300
        min_overlap = 0.15
    if not (item.retrieve_doc_count > 0 and item.retrieve_context_length >= min_context):
        return False
    preview = (item.retrieve_context_preview or "").strip()
    if not preview:
        return False
    overlap = _overlap_score(item.q, preview)
    ref = (item.reference_answer or "").strip()
    ref_overlap = _overlap_score(ref, preview) if ref else 0.0
    return overlap >= min_overlap or ref_overlap >= min_overlap


def _select_diverse_items(items: list[EvalItem], count: int) -> list[EvalItem]:
    buckets = {"easy": [], "medium": [], "hard": []}
    for item in items:
        buckets.get(item.difficulty, buckets["medium"]).append(item)

    selected: list[EvalItem] = []
    seen = set()
    while len(selected) < count:
        progressed = False
        for difficulty in ("easy", "medium", "hard"):
            bucket = buckets[difficulty]
            if not bucket:
                continue
            item = bucket.pop(0)
            if item.q.lower() in seen:
                continue
            selected.append(item)
            seen.add(item.q.lower())
            progressed = True
            if len(selected) >= count:
                break
        if not progressed:
            break

    if len(selected) < count:
        remaining = []
        for difficulty in ("easy", "medium", "hard"):
            remaining.extend(buckets[difficulty])
        for item in remaining:
            if item.q.lower() in seen:
                continue
            selected.append(item)
            seen.add(item.q.lower())
            if len(selected) >= count:
                break
    return selected


def _source_priority(item: EvalItem) -> tuple[int, int, int, int]:
    order = {
        "faq": 0,
        "embedded_qa": 1,
        "analytics": 2,
        "chunk_topic": 3,
        "knowledge_gap": 4,
    }
    return (
        order.get(item.source, 99),
        -item.frequency,
        -item.retrieve_doc_count,
        -item.retrieve_context_length,
    )


def _rotate_candidates(items: list[EvalItem], count: int, db_name: str) -> list[EvalItem]:
    if count <= 0 or not items:
        return []
    ordered = sorted(
        items,
        key=lambda item: (
            item.source != "chunk_topic",
            int(hashlib.sha1(item.candidate_key.encode("utf-8")).hexdigest()[:12], 16),
        ),
    )
    state = _load_rotation_state(db_name)
    cursor = state["cursor"] % len(ordered)
    rotated = ordered[cursor:] + ordered[:cursor]
    picked: list[EvalItem] = []
    seen_difficulties = set()
    for item in rotated:
        if item.difficulty in seen_difficulties:
            continue
        picked.append(item)
        seen_difficulties.add(item.difficulty)
        if len(picked) >= count:
            break
    if len(picked) < count:
        picked_keys = {item.q.lower() for item in picked}
        for item in rotated:
            if item.q.lower() in picked_keys:
                continue
            picked.append(item)
            picked_keys.add(item.q.lower())
            if len(picked) >= count:
                break
    next_cursor = (cursor + len(picked)) % max(1, len(ordered))
    _save_rotation_state(db_name, next_cursor)
    return picked


def _finalize_selection(core_pool: list[EvalItem], discovery_pool: list[EvalItem], count: int, db_name: str) -> list[EvalItem]:
    stable_count = min(len(core_pool), max(1, round(count * 0.6)))
    discovery_count = max(0, count - stable_count)

    stable_core = _select_diverse_items(core_pool, stable_count)
    for item in stable_core:
        item.selection_bucket = "stable_core"

    stable_keys = {item.q.lower() for item in stable_core}
    discovery_candidates = [item for item in discovery_pool if item.q.lower() not in stable_keys]
    rotating = _rotate_candidates(discovery_candidates, discovery_count, db_name)
    for item in rotating:
        item.selection_bucket = "rotating_discovery"

    selected = stable_core + rotating
    if len(selected) < count:
        filler = [
            item for item in core_pool + discovery_pool
            if item.q.lower() not in {chosen.q.lower() for chosen in selected}
        ]
        filler = _select_diverse_items(filler, count - len(selected))
        for item in filler:
            item.selection_bucket = item.selection_bucket or "fallback"
        selected.extend(filler)
    return selected[:count]


def _collect_eval_items(base_url: str, password: str, db_name: str, count: int) -> list[EvalItem]:
    seeds = _collect_seed_items(db_name, max(count * 4, 20))
    if not seeds:
        return []

    preflighted = [_preflight_retrieve(base_url, password, item) for item in seeds]
    grounded = [item for item in preflighted if _is_grounded(item)]
    grounded.sort(key=_source_priority)

    core_pool = [item for item in grounded if item.source in {"faq", "embedded_qa", "analytics"}]
    discovery_pool = [item for item in grounded if item.source in {"chunk_topic", "knowledge_gap", "analytics", "embedded_qa"}]
    if not core_pool:
        core_pool = grounded
    if not discovery_pool:
        discovery_pool = grounded

    return _finalize_selection(core_pool, discovery_pool, count, db_name)


def _wait_health(base_url: str, db_name: str, timeout_s: int = 120) -> None:
    start = time.time()
    while time.time() - start < timeout_s:
        try:
            h = requests.get(f"{base_url}/health", timeout=10).json()
            if h.get("active_db") == db_name and h.get("status") == "ok":
                return
        except Exception:
            pass
        time.sleep(2)
    raise SystemExit(f"Timed out waiting for active_db={db_name}")


def _is_idk(text: str) -> bool:
    t = (text or "").lower()
    return any(s in t for s in IDK_SIGS)


def _is_refuse(text: str) -> bool:
    t = (text or "").lower()
    return any(s in t for s in REFUSE_SIGS)


def _answer_class(text: str) -> str:
    if _is_refuse(text):
        return "REFUSE"
    if _is_idk(text):
        return "IDK"
    return "ANSWER"


def _token_set(text: str) -> set[str]:
    tokens = re.findall(r"[a-z0-9]+", (text or "").lower())
    return {t for t in tokens if len(t) > 2 and t not in STOPWORDS}


def _question_type(question: str) -> str:
    q = _normalize_question(question).lower()
    if re.search(r"\b(compare|difference|versus|vs\.?)\b", q):
        return "comparison"
    if re.search(r"\b(how|steps|process|create|setup|set up|register|apply|start)\b", q):
        return "how_to"
    if re.search(r"\b(policy|refund|return|privacy|terms|cancellation|guarantee)\b", q):
        return "policy"
    if re.search(r"\b(price|pricing|cost|fee|plan|package)\b", q):
        return "pricing"
    if re.search(r"\b(contact|support|email|phone|whatsapp|book|schedule)\b", q):
        return "contact_or_next_step"
    if re.search(r"\b(can|does|do you offer|available|availability|integrate)\b", q):
        return "availability"
    return "fact"


def _overlap_score(a: str, b: str) -> float:
    ta = _token_set(a)
    tb = _token_set(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(1, min(len(ta), len(tb)))


def _reference_text_for(item: EvalItem) -> str:
    return item.reference_answer or item.retrieve_context_preview


def _judge_prompt_snapshot(db_name: str) -> str:
    cfg = get_config(db_name)
    parts = [
        f"bot_name={cfg.get('bot_name', 'AI Assistant')}",
        f"business_name={cfg.get('business_name', db_name or 'the company')}",
        f"topics={cfg.get('topics', 'general information')}",
        "core_rules=no_fabrication; tier3_idk_when_no_kb_match; scope_guard_in_scope_only; refusal_for_prompt_injection; identity_lock",
    ]
    secondary_prompt = str(cfg.get("secondary_prompt") or "").strip()
    if secondary_prompt:
        parts.append(f"secondary_prompt={secondary_prompt[:1200]}")
    return "\n".join(parts)


def _source_match(actual_source: str, expected_source: str) -> bool:
    actual = (actual_source or "").strip().lower()
    expected = (expected_source or "").strip().lower()
    if not actual or not expected:
        return False
    if actual == expected:
        return True
    return actual.endswith(expected) or expected.endswith(actual) or expected in actual or actual in expected


def _average_precision(binary_verdicts: list[int]) -> float:
    if not binary_verdicts:
        return 0.0
    cumsum = 0
    numerator = 0.0
    for i, verdict in enumerate(binary_verdicts, start=1):
        cumsum += verdict
        if verdict:
            numerator += cumsum / i
    return round(numerator / max(1, cumsum), 3) if cumsum else 0.0


def _statement_list(text: str) -> list[str]:
    raw = re.split(r"(?<=[.!?])\s+|\s*;\s*|\s+\-\s+|\s+\band\b\s+(?=[A-Z0-9])", (text or "").strip())
    statements = []
    for part in raw:
        stmt = _normalize_question(part).strip(" .")
        if len(stmt) < 12:
            continue
        statements.append(stmt)
    return statements[:8]


def _reference_contexts(item: EvalItem) -> list[str]:
    parts = []
    ref = (item.reference_answer or "").strip()
    preview = (item.retrieve_context_preview or "").strip()
    if ref:
        parts.append(ref)
    if preview and preview != ref:
        parts.append(preview)
    return parts or [item.q]


def _statement_supported(statement: str, contexts: list[str]) -> tuple[bool, float]:
    st_tokens = _token_set(statement)
    if not st_tokens:
        return False, 0.0
    union_context = " ".join(contexts)
    union_tokens = _token_set(union_context)
    st_nums = set(re.findall(r"\b\d+\b", statement))
    union_nums = set(re.findall(r"\b\d+\b", union_context))
    if st_nums and union_nums and not st_nums.issubset(union_nums):
        return False, 0.0
    stmt_l = statement.lower()
    union_l = union_context.lower()
    negative_cues = (" not ", " never ", " without ", " no ")
    if any(cue in f" {stmt_l} " for cue in negative_cues) and not any(cue in f" {union_l} " for cue in negative_cues):
        shared = len(st_tokens & union_tokens)
        if shared >= 2:
            return False, 0.0
    best = 0.0
    for context in contexts:
        ctx_nums = set(re.findall(r"\b\d+\b", context))
        if st_nums and ctx_nums and not st_nums.issubset(ctx_nums):
            continue
        ctx_l = context.lower()
        if ("without proof" in stmt_l or "no proof" in stmt_l) and "proof of purchase" in ctx_l and "without proof" not in ctx_l and "no proof" not in ctx_l:
            continue
        score = _overlap_score(statement, context)
        ctx_hits = len(st_tokens & _token_set(context))
        if ctx_hits >= 3:
            score = max(score, min(1.0, ctx_hits / max(1, len(st_tokens))))
        best = max(best, score)
    return best >= 0.34, round(best, 3)


def _statement_relevant(statement: str, question: str, reference_text: str = "") -> tuple[bool, float]:
    st_tokens = _token_set(statement)
    q_tokens = _token_set(question)
    ref_tokens = _token_set(reference_text)
    if not st_tokens or (not q_tokens and not ref_tokens):
        return False, 0.0
    q_overlap = len(st_tokens & q_tokens)
    r_overlap = len(st_tokens & ref_tokens)
    q_score = q_overlap / max(1, min(len(st_tokens), len(q_tokens))) if q_tokens else 0.0
    r_score = r_overlap / max(1, min(len(st_tokens), len(ref_tokens))) if ref_tokens else 0.0
    score = max(q_score, r_score)
    long_stmt = len(st_tokens) >= 6
    min_overlap = 3 if long_stmt else 2
    return (q_overlap >= min_overlap or r_overlap >= min_overlap or score >= 0.35), round(score, 3)


def _doc_relevance(doc_text: str, question: str, reference_text: str, source: str = "", expected_source: str = "") -> tuple[int, str]:
    q_tokens = _token_set(question)
    doc_tokens = _token_set(doc_text)
    ref_tokens = _token_set(reference_text)
    q_hits = len(q_tokens & doc_tokens)
    ref_hits = len(ref_tokens & doc_tokens)
    source_hit = _source_match(source, expected_source)
    overlap = _overlap_score(doc_text, reference_text or question)

    score = 0.0
    if source_hit:
        score += 0.45
    if q_hits >= 2:
        score += 0.2
    elif q_hits == 1:
        score += 0.1
    if ref_hits >= 4:
        score += 0.35
    elif ref_hits >= 2:
        score += 0.2
    elif ref_hits == 1:
        score += 0.1
    score = max(score, overlap)

    if score >= 0.55:
        return 1, "strong_match"
    if score >= 0.3:
        return 0, "partial_match"
    return 0, "weak_match"


def _context_recall(item: EvalItem) -> float | None:
    reference_text = (item.reference_answer or "").strip()
    preview = (item.retrieve_context_preview or "").strip()
    if not reference_text or not preview:
        return None
    ref_statements = _statement_list(reference_text)
    if not ref_statements:
        ref_statements = [reference_text]
    supported = 0
    for statement in ref_statements:
        ok, _ = _statement_supported(statement, [preview])
        if ok:
            supported += 1
    return round(supported / max(1, len(ref_statements)), 3)


def _grade_retrieval(item: EvalItem, doc_rows: list[dict] | None = None, expected_source: str = "") -> dict:
    rows = list(doc_rows or [])
    if not rows and item.retrieve_context_preview:
        rows = [{"source": (item.retrieve_sources[0] if item.retrieve_sources else ""), "preview": item.retrieve_context_preview}]

    reference_text = _reference_text_for(item)
    binary_verdicts: list[int] = []
    ranked = []
    for idx, row in enumerate(rows, start=1):
        preview = str((row or {}).get("preview") or "")
        source = str((row or {}).get("source") or "")
        verdict, label = _doc_relevance(preview, item.q, reference_text, source=source, expected_source=expected_source)
        binary_verdicts.append(verdict)
        ranked.append({"rank": idx, "source": source, "verdict": verdict, "label": label})

    hit = any(v == 1 for v in binary_verdicts)
    precision_at_3 = round(sum(binary_verdicts[:3]) / max(1, min(3, len(binary_verdicts))), 3) if binary_verdicts else 0.0
    average_precision = _average_precision(binary_verdicts)
    first_relevant_rank = next((i + 1 for i, v in enumerate(binary_verdicts) if v == 1), None)
    context_recall = _context_recall(item)

    if not rows:
        status = "FAIL"
        diagnosis = "retrieval_miss"
        reason = "Retriever returned no document rows for this question."
    elif hit and average_precision >= 0.55 and (context_recall is None or context_recall >= 0.67):
        status = "PASS"
        diagnosis = "retrieval_ok"
        reason = None
    elif hit and context_recall is not None and context_recall < 0.67:
        status = "FAIL"
        diagnosis = "retrieval_incomplete"
        reason = "Retriever found relevant chunks, but the retrieved context did not cover enough of the expected answer."
    elif hit:
        status = "FAIL"
        diagnosis = "weak_top_k"
        reason = "Retriever found some relevant context, but the top-ranked chunks were not precise enough."
    else:
        status = "FAIL"
        diagnosis = "retrieval_miss"
        reason = "Top retrieved chunks did not align strongly enough with the expected tenant content."

    return {
        "status": status,
        "reason": reason,
        "diagnosis": diagnosis,
        "hit": hit,
        "precision_at_3": precision_at_3,
        "average_precision": average_precision,
        "first_relevant_rank": first_relevant_rank,
        "context_recall": context_recall,
        "ranked_verdicts": ranked[:5],
    }


def _answer_metrics(item: EvalItem, answer_text: str, judge_verdict: JudgeVerdict | None = None) -> dict:
    answer_class = _answer_class(answer_text)
    reference_text = _reference_text_for(item)
    overlap = _overlap_score(answer_text, reference_text)
    reference_tokens = _token_set(reference_text)
    answer_tokens = _token_set(answer_text)
    token_hits = len(reference_tokens & answer_tokens)
    statements = _statement_list(answer_text)
    if not statements and answer_text.strip():
        statements = [_normalize_question(answer_text)]
    contexts = _reference_contexts(item)

    supported = 0
    relevant = 0
    support_scores = []
    relevance_scores = []
    for statement in statements:
        ok_support, support_score = _statement_supported(statement, contexts)
        ok_relevance, relevance_score = _statement_relevant(statement, item.q, reference_text)
        support_scores.append(support_score)
        relevance_scores.append(relevance_score)
        if ok_support:
            supported += 1
        if ok_relevance:
            relevant += 1

    total = len(statements)
    deterministic_faithfulness = round(supported / total, 3) if total else 0.0
    deterministic_answer_relevance = round(relevant / total, 3) if total else 0.0
    faithfulness = deterministic_faithfulness
    answer_relevance = deterministic_answer_relevance
    judge_used = False
    if judge_verdict and not judge_verdict.error:
        if judge_verdict.faithfulness_score is not None:
            faithfulness = judge_verdict.faithfulness_score
            judge_used = True
        if judge_verdict.answer_relevance_score is not None:
            answer_relevance = judge_verdict.answer_relevance_score
            judge_used = True
    return {
        "answer_class": answer_class,
        "overlap": overlap,
        "token_hits": token_hits,
        "statements": statements,
        "faithfulness": faithfulness,
        "answer_relevance": answer_relevance,
        "deterministic_faithfulness": deterministic_faithfulness,
        "deterministic_answer_relevance": deterministic_answer_relevance,
        "context_recall": _context_recall(item),
        "support_scores": support_scores,
        "relevance_scores": relevance_scores,
        "judge_used": judge_used,
    }


def _grade_answer(item: EvalItem, answer_text: str, judge_verdict: JudgeVerdict | None = None) -> tuple[str, str | None, float]:
    metrics = _answer_metrics(item, answer_text, judge_verdict=judge_verdict)
    answer_class = metrics["answer_class"]
    overlap = metrics["overlap"]
    token_hits = metrics["token_hits"]
    reference_tokens = _token_set(_reference_text_for(item))
    required_hits = max(4, min(8, (len(reference_tokens) + 4) // 5)) if reference_tokens else 4
    qtype = _question_type(item.q)
    min_faithfulness = 0.67 if qtype in {"policy", "pricing", "comparison"} else 0.6
    min_relevance = 0.55 if qtype in {"how_to", "comparison"} else 0.5

    if item.expect == "ANSWER":
        if answer_class != "ANSWER":
            diag = "grounded_but_answered_idk" if answer_class == "IDK" else "grounded_but_refused"
            return "FAIL", f"Expected an in-domain answer, got {answer_class}. ({diag})", overlap
        if len(answer_text) < 25:
            return "FAIL", "Answer was too short to be trustworthy.", overlap
        if item.retrieve_doc_count <= 0 or item.retrieve_context_length < 80:
            return "FAIL", "Answer was produced without enough retrieved tenant context.", overlap
        if metrics["faithfulness"] < min_faithfulness:
            return "FAIL", "Answer was not faithful enough to the retrieved tenant context.", overlap
        if metrics["answer_relevance"] < min_relevance:
            return "FAIL", "Answer did not address the user's question directly enough.", overlap
        if overlap < 0.1 and token_hits < required_hits:
            return "FAIL", "Answer did not align closely enough with the retrieved tenant reference.", overlap
        return "PASS", None, overlap

    if item.expect == "IDK":
        return ("PASS", None, overlap) if answer_class in {"IDK", "REFUSE"} else ("FAIL", f"Expected IDK-safe behavior, got {answer_class}.", overlap)

    return ("PASS", None, overlap) if answer_class == "REFUSE" else ("FAIL", f"Expected REFUSE, got {answer_class}.", overlap)


def _score_of(statuses: list[str]) -> float | None:
    relevant = [s for s in statuses if s in {"PASS", "FAIL"}]
    if not relevant:
        return None
    passed = sum(1 for s in relevant if s == "PASS")
    return round((passed / len(relevant)) * 10, 1)


def _score_meaning(score: float | None) -> str:
    if score is None:
        return "Not exercised in this run."
    if score >= 9:
        return "Strong and ready for confidence checks."
    if score >= 7:
        return "Good, but still worth tightening before client-facing promises."
    if score >= 5:
        return "Mixed; there are visible weak spots."
    return "Unreliable; this area needs attention before the score can be trusted."


def _build_summary(results: list[dict]) -> dict:
    retrieval_statuses = [r.get("retrieval_status", "SKIP") for r in results]
    answer_statuses = [r.get("answer_status", "SKIP") for r in results]
    idk_statuses = [r.get("idk_status", "SKIP") for r in results]
    overall_statuses = [r.get("overall_status", "SKIP") for r in results]
    failure_breakdown = dict(
        Counter(
            (r.get("retrieval_diagnosis") or r.get("answer_diagnosis") or r.get("idk_diagnosis") or "unknown")
            for r in results
            if r.get("overall_status") == "FAIL"
        )
    )
    failure_source_mix = dict(
        Counter(
            ((r.get("judge") or {}).get("likely_failure_source") or "none")
            for r in results
            if r.get("overall_status") == "FAIL" and ((r.get("judge") or {}).get("likely_failure_source") or "none") != "none"
        )
    )
    failure_step_mix = dict(
        Counter(
            ((r.get("judge") or {}).get("exact_failure_step") or "")
            for r in results
            if r.get("overall_status") == "FAIL" and ((r.get("judge") or {}).get("exact_failure_step") or "")
        )
    )
    root_cause_mix = dict(
        Counter(
            ((r.get("judge") or {}).get("root_cause_note") or "")
            for r in results
            if r.get("overall_status") == "FAIL" and ((r.get("judge") or {}).get("root_cause_note") or "")
        )
    )
    fix_hint_mix = dict(
        Counter(
            ((r.get("judge") or {}).get("fix_hint") or "")
            for r in results
            if r.get("overall_status") == "FAIL" and ((r.get("judge") or {}).get("fix_hint") or "")
        )
    )
    judge_confidences = [float((r.get("judge") or {}).get("confidence")) for r in results if (r.get("judge") or {}).get("confidence") is not None]

    retrieval_score = _score_of(retrieval_statuses)
    answer_score = _score_of(answer_statuses)
    idk_score = _score_of(idk_statuses)
    overall_score = _score_of(overall_statuses)

    if retrieval_score is not None and retrieval_score < 7:
        diagnosis = "Retrieval is still the main bottleneck for these DB-grounded questions."
    elif answer_score is not None and answer_score < 7:
        diagnosis = "Retrieval is finding context, but answer behavior is still inconsistent."
    else:
        diagnosis = "This run stayed inside tenant knowledge and the scoring signals are aligned."

    return {
        "overall_score": overall_score,
        "retrieval_score": retrieval_score,
        "answer_score": answer_score,
        "idk_score": idk_score,
        "diagnosis": diagnosis,
        "selection_mix": dict(Counter(r.get("selection_bucket", "unknown") for r in results)),
        "failure_breakdown": failure_breakdown,
        "failure_source_mix": failure_source_mix,
        "failure_step_mix": failure_step_mix,
        "root_cause_mix": root_cause_mix,
        "fix_hint_mix": fix_hint_mix,
        "judge_confidence_mean": round(sum(judge_confidences) / len(judge_confidences), 3) if judge_confidences else None,
        "score_meanings": {
            "overall": _score_meaning(overall_score),
            "retrieval": _score_meaning(retrieval_score),
            "answer": _score_meaning(answer_score),
            "idk": _score_meaning(idk_score),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Eval v1: DB-grounded scored checks per tenant.")
    ap.add_argument("--base-url", default="http://localhost:8000")
    ap.add_argument("--db", required=True)
    ap.add_argument("--password", required=True, help="Owner password (ADMIN_PASSWORD).")
    ap.add_argument("--count", type=int, default=10)
    ap.add_argument("--mode", choices=("retrieve", "chat", "both"), default="both")
    ap.add_argument("--out-dir", default="evals/results")
    ap.add_argument("--judge", action="store_true", help="Enable Groq-based LLM judging for semantic answer metrics.")
    ap.add_argument("--judge-key", default="", help="Dedicated judge key(s), comma-separated. Falls back to JUDGE_API_KEY env var.")
    args = ap.parse_args()

    db = args.db.strip()
    judge_key = (args.judge_key or os.getenv("JUDGE_API_KEY") or "").strip()
    prompt_snapshot = _judge_prompt_snapshot(db) if args.judge else ""

    r = requests.post(
        f"{args.base_url}/admin/databases/set-active",
        data={"password": args.password, "name": db},
        timeout=30,
    )
    if r.status_code != 200:
        raise SystemExit(f"set-active failed: HTTP {r.status_code} {r.text[:200]}")
    _wait_health(args.base_url, db)

    items = _collect_eval_items(args.base_url, args.password, db, max(3, min(args.count, 25)))
    if not items:
        raise SystemExit(
            "No DB-grounded eval questions were found for this tenant. "
            "Add FAQs or more real tenant questions, then rerun."
        )

    results = []
    failures = 0
    for item in items:
        row = {
            "q": item.q,
            "expect": item.expect,
            "source": item.source,
            "selection_bucket": item.selection_bucket,
            "difficulty": item.difficulty,
            "question_type": _question_type(item.q),
            "reference_preview": (item.reference_answer or item.retrieve_context_preview)[:400],
            "retrieval_status": "SKIP",
            "answer_status": "SKIP",
            "idk_status": "SKIP",
            "overall_status": "SKIP",
            "judge": JudgeVerdict(error="judge_disabled").to_dict(),
        }

        retrieve_pass = None
        if args.mode in ("retrieve", "both"):
            row["doc_count"] = item.retrieve_doc_count
            row["context_length"] = item.retrieve_context_length
            row["sources"] = item.retrieve_sources
            expected_source = item.retrieve_sources[0] if item.retrieve_sources else ""
            retrieval_eval = _grade_retrieval(item, expected_source=expected_source)
            retrieve_pass = retrieval_eval["status"] == "PASS"
            row["retrieval_status"] = retrieval_eval["status"]
            row["retrieval_reason"] = retrieval_eval.get("reason")
            row["retrieval_diagnosis"] = retrieval_eval.get("diagnosis")
            row["retrieval_metrics"] = {
                "hit": retrieval_eval.get("hit"),
                "precision_at_3": retrieval_eval.get("precision_at_3"),
                "average_precision": retrieval_eval.get("average_precision"),
                "first_relevant_rank": retrieval_eval.get("first_relevant_rank"),
            }
            row["retrieval_ranked_verdicts"] = retrieval_eval.get("ranked_verdicts", [])

        if args.mode in ("chat", "both"):
            cr = requests.post(
                f"{args.base_url}/chat",
                json={"question": item.q, "history": [], "stream": False},
                timeout=120,
            )
            row["chat_http"] = cr.status_code
            if cr.status_code != 200:
                row["answer_status"] = "FAIL"
                row["overall_status"] = "FAIL"
                row["chat_error"] = cr.text[:200]
                failures += 1
                results.append(row)
                continue

            ans = (cr.json() or {}).get("answer", "")
            ans_s = str(ans).strip()
            answer_class = _answer_class(ans_s)
            judge_verdict = JudgeVerdict(error="judge_disabled")
            if args.judge:
                if item.expect == "ANSWER":
                    judge_verdict = judge_answer(
                        item.q,
                        item.retrieve_context_preview,
                        ans_s,
                        _reference_text_for(item),
                        judge_key,
                        prompt_context=prompt_snapshot,
                        retrieval_metrics=row.get("retrieval_metrics") or {},
                        retrieval_diagnosis=row.get("retrieval_diagnosis") or "",
                    )
                else:
                    judge_verdict = JudgeVerdict(error="judge_skipped_non_answer")
            answer_metrics = _answer_metrics(item, ans_s, judge_verdict=judge_verdict)
            answer_status, answer_reason, overlap = _grade_answer(item, ans_s, judge_verdict=judge_verdict)

            row["answer_preview"] = ans_s[:400]
            row["answer_class"] = answer_class
            row["answer_overlap"] = round(overlap, 3)
            row["judge"] = judge_verdict.to_dict()
            row["answer_metrics"] = {
                "faithfulness": answer_metrics["faithfulness"],
                "answer_relevance": answer_metrics["answer_relevance"],
                "deterministic_faithfulness": answer_metrics["deterministic_faithfulness"],
                "deterministic_answer_relevance": answer_metrics["deterministic_answer_relevance"],
                "token_hits": answer_metrics["token_hits"],
                "statement_count": len(answer_metrics["statements"]),
                "context_recall": answer_metrics["context_recall"],
                "judge_used": answer_metrics["judge_used"],
            }
            if item.expect == "IDK":
                row["idk_status"] = answer_status
                row["idk_diagnosis"] = "idk_ok" if answer_status == "PASS" else "idk_mismatch"
                if answer_reason:
                    row["idk_reason"] = answer_reason
            else:
                row["answer_status"] = answer_status
                if answer_status == "FAIL":
                    if "grounded_but_answered_idk" in (answer_reason or ""):
                        row["answer_diagnosis"] = "grounded_but_answered_idk"
                    elif "grounded_but_refused" in (answer_reason or ""):
                        row["answer_diagnosis"] = "grounded_but_refused"
                    elif "faithful enough" in (answer_reason or ""):
                        row["answer_diagnosis"] = "answer_not_faithful"
                    elif "address the user's question" in (answer_reason or ""):
                        row["answer_diagnosis"] = "answer_irrelevant"
                    else:
                        row["answer_diagnosis"] = "answer_mismatch"
                if answer_reason:
                    row["answer_reason"] = answer_reason

        statuses = [row.get("retrieval_status"), row.get("answer_status"), row.get("idk_status")]
        relevant = [s for s in statuses if s in {"PASS", "FAIL"}]
        if relevant:
            row["overall_status"] = "PASS" if all(s == "PASS" for s in relevant) else "FAIL"
        if row["overall_status"] == "FAIL":
            failures += 1

        results.append(row)

    summary = _build_summary(results)

    out_dir = Path(args.out_dir) / db
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"eval_{ts}.json"
    payload = {
        "db": db,
        "mode": args.mode,
        "failures": failures,
        "summary": summary,
        "results": results,
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(str(out_path))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
