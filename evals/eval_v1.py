import argparse
import hashlib
import json
import re
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import requests

EVALS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = EVALS_DIR.parent
DATABASES_DIR = PROJECT_ROOT / "databases"


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
)

STOPWORDS = {
    "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "how",
    "i", "in", "is", "it", "me", "my", "of", "on", "or", "our", "that",
    "the", "this", "to", "us", "we", "what", "when", "where", "which", "who",
    "why", "with", "your",
}


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
    if not isinstance(faqs, list):
        return items
    for faq in faqs:
        if not isinstance(faq, dict):
            continue
        q = _normalize_question(faq.get("question", ""))
        a = _normalize_question(faq.get("answer", ""))
        if not q or not a or _looks_like_oos_or_noise(q):
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


def _load_analytics_candidates(db_name: str) -> list[EvalItem]:
    db_dir = DATABASES_DIR / db_name
    analytics = _read_json(db_dir / "analytics.json") or {}
    questions = Counter()

    q_counts = analytics.get("questions") or {}
    if isinstance(q_counts, dict):
        for q, count in q_counts.items():
            qn = _normalize_question(q)
            if not qn or _looks_like_oos_or_noise(qn):
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
            if not qn or _looks_like_oos_or_noise(qn):
                continue
            questions[qn] += 1

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
    if not isinstance(gaps, list):
        return items
    for gap in gaps:
        if not isinstance(gap, dict):
            continue
        q = _normalize_question(gap.get("question", ""))
        if not q or _looks_like_oos_or_noise(q):
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


def _load_document_rows_via_chroma(db_name: str) -> list[tuple[str, str]]:
    db_dir = DATABASES_DIR / db_name
    try:
        import chromadb

        client = chromadb.PersistentClient(path=str(db_dir))
        rows: list[tuple[str, str]] = []
        for collection in client.list_collections():
            try:
                payload = collection.get(include=["documents"])
            except Exception:
                continue
            ids = payload.get("ids") or []
            documents = payload.get("documents") or []
            for row_id, doc in zip(ids, documents):
                if isinstance(doc, str) and doc.strip():
                    rows.append((str(row_id), doc))
        return rows
    except Exception:
        return []


def _load_document_rows_via_sqlite(db_name: str) -> list[tuple[str, str]]:
    db_path = DATABASES_DIR / db_name / "chroma.sqlite3"
    if not db_path.exists():
        return []
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT id, string_value FROM embedding_metadata "
            "WHERE key='chroma:document' ORDER BY id",
        ).fetchall()
        conn.close()
        return [(str(row_id), raw_text or "") for row_id, raw_text in rows]
    except Exception:
        return []


def _load_document_rows(db_name: str) -> list[tuple[str, str]]:
    rows = _load_document_rows_via_chroma(db_name)
    if rows:
        return rows
    return _load_document_rows_via_sqlite(db_name)


def _load_doc_qa_fallbacks(db_name: str, limit: int) -> list[EvalItem]:
    items: list[EvalItem] = []
    rows = _load_document_rows(db_name)
    if not rows:
        return items

    for _, text in rows:
        match = re.search(r"Q:\s*(.+?)\s*A:\s*(.+)", text, re.S | re.I)
        if not match:
            continue
        q = _normalize_question(match.group(1))
        a = _normalize_question(match.group(2))
        if not q or not a or _looks_like_oos_or_noise(q):
            continue
        items.append(
            EvalItem(
                q=q,
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


def _is_low_value_chunk(text: str) -> bool:
    cleaned = _clean_chunk_text(text)
    if len(cleaned.split()) < 20:
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


def _extract_chunk_topic(text: str) -> str:
    cleaned = _clean_chunk_text(text)
    if not cleaned:
        return ""

    chapter = re.search(r"(Chapter\s+\d+\s*:\s*[^.?!]{4,90})", cleaned, re.I)
    if chapter:
        return _normalize_question(chapter.group(1))

    heading = re.search(r"([A-Z][A-Za-z0-9/&,\-()' ]{8,90})[:.]", cleaned)
    if heading:
        topic = _normalize_question(heading.group(1))
        if len(topic.split()) >= 2:
            return topic

    words = cleaned.split()
    topic = " ".join(words[: min(10, len(words))]).strip(" ,.-:")
    return _normalize_question(topic)


def _make_chunk_question(topic: str, chunk_text: str) -> str:
    cleaned = _clean_chunk_text(chunk_text)
    topic = _normalize_question(topic)
    if not topic:
        return ""
    templates = [
        "What is {topic}?",
        "How does {topic} work?",
        "What should I know about {topic}?",
        "What are the key details about {topic}?",
        "Summarize {topic}.",
    ]
    if re.search(r"\bcompare|difference|versus|vs\.?\b", cleaned, re.I):
        templates = [
            "What is the difference explained in {topic}?",
            "How does {topic} compare the options it mentions?",
            "Summarize the comparison in {topic}.",
        ]
    elif re.search(r"\bprice|cost|shipping|return|refund|policy\b", cleaned, re.I):
        templates = [
            "What does {topic} say about pricing or policy?",
            "What are the important policy details in {topic}?",
            "Summarize the key customer-facing details in {topic}.",
        ]
    idx = int(hashlib.sha1(topic.lower().encode("utf-8")).hexdigest()[:8], 16) % len(templates)
    return templates[idx].format(topic=topic)


def _load_chunk_topic_candidates(db_name: str, limit: int) -> list[EvalItem]:
    rows = _load_document_rows(db_name)
    if not rows:
        return []

    items: list[EvalItem] = []
    seen_topics = set()
    for row_id, raw_text in rows:
        text = raw_text or ""
        if _is_low_value_chunk(text):
            continue
        topic = _extract_chunk_topic(text)
        if not topic:
            continue
        topic_key = topic.lower()
        if topic_key in seen_topics:
            continue
        seen_topics.add(topic_key)
        question = _make_chunk_question(topic, text)
        if not question or _looks_like_oos_or_noise(question):
            continue
        items.append(
            EvalItem(
                q=question,
                source="chunk_topic",
                difficulty=_difficulty_for_question(question),
                reference_answer=_clean_chunk_text(text)[:700],
                frequency=2,
                candidate_key=f"chunk::{row_id}::{topic_key}",
            )
        )
        if len(items) >= limit:
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
    return deduped


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
    min_context = 80 if item.source in {"faq", "embedded_qa"} else 120
    return item.retrieve_doc_count > 0 and item.retrieve_context_length >= min_context


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


def _overlap_score(a: str, b: str) -> float:
    ta = _token_set(a)
    tb = _token_set(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(1, min(len(ta), len(tb)))


def _reference_text_for(item: EvalItem) -> str:
    return item.reference_answer or item.retrieve_context_preview


def _grade_answer(item: EvalItem, answer_text: str) -> tuple[str, str | None, float]:
    answer_class = _answer_class(answer_text)
    overlap = _overlap_score(answer_text, _reference_text_for(item))
    reference_tokens = _token_set(_reference_text_for(item))
    answer_tokens = _token_set(answer_text)
    token_hits = len(reference_tokens & answer_tokens)
    required_hits = max(4, min(8, (len(reference_tokens) + 4) // 5)) if reference_tokens else 4

    if item.expect == "ANSWER":
        if answer_class != "ANSWER":
            return "FAIL", f"Expected an in-domain answer, got {answer_class}.", overlap
        if len(answer_text) < 25:
            return "FAIL", "Answer was too short to be trustworthy.", overlap
        if item.retrieve_doc_count <= 0 or item.retrieve_context_length < 80:
            return "FAIL", "Answer was produced without enough retrieved tenant context.", overlap
        if overlap < 0.12 or token_hits < required_hits:
            return "FAIL", "Answer did not align closely enough with the retrieved tenant reference.", overlap
        return "PASS", None, overlap

    if item.expect == "IDK":
        return ("PASS", None, overlap) if answer_class == "IDK" else ("FAIL", f"Expected IDK, got {answer_class}.", overlap)

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
    args = ap.parse_args()

    db = args.db.strip()

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
            "reference_preview": (item.reference_answer or item.retrieve_context_preview)[:400],
            "retrieval_status": "SKIP",
            "answer_status": "SKIP",
            "idk_status": "SKIP",
            "overall_status": "SKIP",
        }

        retrieve_pass = None
        if args.mode in ("retrieve", "both"):
            row["doc_count"] = item.retrieve_doc_count
            row["context_length"] = item.retrieve_context_length
            row["sources"] = item.retrieve_sources
            retrieve_pass = item.retrieve_doc_count > 0 and item.retrieve_context_length >= 120
            row["retrieval_status"] = "PASS" if retrieve_pass else "FAIL"
            if not retrieve_pass:
                row["retrieval_reason"] = "Retriever did not surface enough tenant context for this question."

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
            answer_status, answer_reason, overlap = _grade_answer(item, ans_s)

            row["answer_preview"] = ans_s[:400]
            row["answer_class"] = answer_class
            row["answer_overlap"] = round(overlap, 3)
            if item.expect == "IDK":
                row["idk_status"] = answer_status
                if answer_reason:
                    row["idk_reason"] = answer_reason
            else:
                row["answer_status"] = answer_status
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
