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

# Debug-only: helps confirm which extractor logic is running on HF.
_OUTCOMES_EXTRACTOR_VERSION = "2026-06-06.v23"


def _is_binary_chunk(text: str) -> bool:
    """Return True if a DB chunk contains binary/corrupted data (not readable text)."""
    if not text:
        return False
    sample = text[:200]
    non_printable = sum(
        1 for c in sample
        if ord(c) > 127 or (ord(c) < 32 and c not in "\n\r\t ")
    )
    return (non_printable / max(1, len(sample))) > 0.15

# Outcomes/learning intent (universal)
# Note: "principles/steps/rules" queries intentionally excluded — they go through LLM directly.
_OUTCOMES_INTENT_RE = re.compile(
    r"\b(what\s+will\s+i\s+learn|what\s+will\s+we\s+learn|learning\s+outcomes?|objectives?|goals?|"
    r"by\s+the\s+end|you\s+will\s+be\s+able\s+to|"
    r"what\s+(?:do|does|will|would)\s+[^.!?]{3,80}\s+cover|"
    r"what\s+(?:is|are)\s+covered|covers?\b)\b",
    re.I,
)

_BULLET_LINE_RE = re.compile(r"^\s*(?:[-*•]|\d{1,2}[.)])\s+(.+?)\s*$")

def is_outcomes_question(q: str) -> bool:
    return bool(_OUTCOMES_INTENT_RE.search(q or ""))

_INTERROGATIVE_START = {"what", "which", "why", "how", "when", "where", "who"}
_OUTCOME_VERB_HINTS = {
    "understand","build","ship","use","integrate","deliver","identify","structure","apply","prepare",
    "run","runs","evaluate","deploy","install","test","design","verify","retrofit","decide","learn","encode",
    "create","implement","configure","monitor","optimize","define","package",
    "map","capture","explore","master","analyze","develop","write","generate","complete",
    "starts","restarts","logs","accepts","executes","survives","recognizes","recognise","debug","debugs",
    "can","configure","move","read","set","direct","establish","manage","enable","perform",
    # v9: narrative-outcome verbs (lowercase 3rd-person / base forms for Q4-style chunks)
    "answers","uses","browses","speaks","operates","handles","connects","responds","have","has","get","gets",
}


def try_extract_outcomes_answer(q: str, context: str, debug: dict | None = None) -> str | None:
    """Deterministically extract the best contiguous bullet list from retrieved context.

    This is intentionally universal (works for any DB) and is only activated for
    outcomes-style questions (goals/objectives/learn/outcomes).
    """
    if not context or not is_outcomes_question(q):
        return None
    try:
        if debug is not None:
            debug["outcomes_extractor_version"] = _OUTCOMES_EXTRACTOR_VERSION
            debug["outcomes_extractor_path"] = ""
            debug["outcomes_extractor_marker_line"] = ""
            debug["outcomes_extractor_block_preview"] = ""
            debug["outcomes_extractor_marker_hits"] = []
        _GENERIC_GOALS_HEADINGS = (
            "lessons", "about the code in this chapter", "about this chapter", "before you begin",
            "exercises", "skill", "skills", "what you'll learn", "what you will learn",
            "what you will be able to do", "what you'll be able to do"
        )
        def _generic_heading_count(items: list[str]) -> int:
            c = 0
            for it in items or []:
                t = (it or "").strip().lower().strip(":")
                if t in _GENERIC_GOALS_HEADINGS:
                    c += 1
                    continue
                if any(t.startswith(h + ":") for h in _GENERIC_GOALS_HEADINGS):
                    c += 1
            return c
        def _valid_outcomes_items(items: list[str]) -> bool:
            if not (3 <= len(items) <= 25):
                return False
            # Strip "You can/should/will/are able to" prefix before checking verb starters
            # e.g. "You can evaluate a migration..." → "evaluate a migration..."
            _stripped = [re.sub(r"^you\s+(?:can|should|will|are\s+able\s+to)\s+", "", it, flags=re.I) for it in items]
            starters = [(_s.split()[:1][0].lower() if _s.split() else "") for _s in _stripped]
            verbish = sum(1 for s in starters if s in _OUTCOME_VERB_HINTS)
            verb_ratio = verbish / max(1.0, float(len(items)))
            # Reject prompt templates / nav / diagnostic checklists.
            bad_starts = ("prompt", "step", "previous", "next", "prerequisites", "authors", "company", "about")
            if sum(1 for it in items if it.strip().lower().startswith(bad_starts)) >= max(2, len(items) // 2):
                return False
            joined = " ".join(items).lower()
            # Reject operational checklists / shell commands / file scaffolding.
            if any(tok in joined for tok in ("kubectl", "readiness probe", "liveness probe", "exec probe", "agent-deployment-")):
                return False
            if re.search(r"\b(mkdir|cd\s+~/?|kubectl|pip\s+install|npm\s+install|git\s+clone|python\s+-m)\b", joined):
                return False
            if re.search(r"\b(?:learning-spec|readme)\.md\b|\b[-a-z0-9_\\/]+\.py\b|\b[-a-z0-9_\\/]+\.md\b", joined):
                return False
            longish = sum(1 for it in items if len(it) > 140 or it.count(".") >= 2 or it.count(":") >= 2)
            if longish >= max(2, len(items) // 2):
                return False
            if _generic_heading_count(items) >= max(2, len(items) // 2):
                return False
            # If the question names a specific chapter/part title, require overlap with that title
            # only when the retrieved context itself has no explicit chapter/part anchor.
            if title_tokens and (not _scope_anchor_present):
                hit = sum(1 for t in title_tokens[:6] if t and (t in joined))
                # Question-format items (Why?/How?) rephrase chapter content, not the title —
                # one matching title token is sufficient evidence they're from the right chapter.
                _is_q_items = sum(1 for it in items if it.rstrip().endswith("?") and
                                  (it.split()[:1][0].lower() if it.split() else "") in _INTERROGATIVE_START
                                  ) >= max(2, len(items) // 2)
                req = 1 if _is_q_items else (2 if len(title_tokens) >= 3 else 1)
                if hit < req and not (len(items) >= 4 and verb_ratio >= 0.60):
                    return False
                # Q-format items that passed the title-token check are valid:
                # they rephrase chapter content, so low verb_ratio is expected.
                if _is_q_items and len(items) >= 3 and _generic_heading_count(items) == 0:
                    return True
            # Learning outcomes should usually be verb-start imperatives, but
            # some exact chapter pages surface as clean topic lists under a strong
            # explicit marker. Accept those when we already have a scope anchor.
            if verb_ratio >= 0.45:
                return True
            if _scope_anchor_present and len(items) >= 3 and _generic_heading_count(items) == 0:
                _q_count = sum(1 for it in items if "?" in it)
                if _q_count == 0:
                    return True  # clean non-question items with scope anchor
                # Also accept majority-question items — "By chapter end, answer these questions:" format
                _q_starters = sum(1 for it in items if re.match(r"^(?:why|how|when|what|which|where)\b", it.strip().lower()))
                if _q_starters >= max(3, len(items) // 2):
                    return True
            return False

        def _valid_chapter_topics_items(items: list[str], strong_marker: bool = False) -> bool:
            # Used for sections like "compare it to what we'll learn in this chapter:" which often
            # lists topic nouns (not verb-imperatives). Still must be clean, non-operational content.
            if not (2 <= len(items) <= 25):
                return False
            joined = " ".join(items).lower()
            if re.search(r"https?://", joined):
                return False
            if re.search(r"\b(mkdir|cd\s+~/?|kubectl|pip\s+install|npm\s+install|git\s+clone|python\s+-m)\b", joined):
                return False
            if re.search(r"\b(?:learning-spec|readme)\.md\b|\b[-a-z0-9_\\/]+\.py\b|\b[-a-z0-9_\\/]+\.md\b", joined):
                return False
            # Keep them reasonably short and list-like.
            longish = sum(1 for it in items if len(it) > 140 or it.count(".") >= 2 or it.count(":") >= 2)
            if longish >= max(2, len(items) // 2):
                return False
            if _generic_heading_count(items) >= max(2, len(items) // 2):
                return False
            # For goals/outcomes queries, reject question-list blocks (common contamination source).
            qlike = sum(1 for it in items if "?" in it or ((it.split()[:1][0].lower() if it.split() else "") in _INTERROGATIVE_START))
            if qlike >= 1:
                return False
            if (not strong_marker) and title_tokens and (not _scope_anchor_present):
                hit = sum(1 for t in title_tokens[:6] if t and (t in joined))
                req = 2 if len(title_tokens) >= 3 else 1
                if hit < req and not (len(items) >= 4 and verb_ratio >= 0.60):
                    return False
            return True

        title_phrase = _extract_title_phrase(q or "")
        _q_lower = (q or "").lower()
        _q_is_chapter = "chapter" in _q_lower
        chapter_no = _extract_chapter_number(q or "")
        part_no = _extract_part_number(q or "")
        chapter_anchor = f"chapter {chapter_no}" if chapter_no is not None else ""
        part_anchor = f"part {part_no}" if part_no is not None else ""
        title_tokens = [w.lower() for w in re.findall(r"[a-zA-Z]{4,}", title_phrase)][:10] if title_phrase else []
        lines = [ln.rstrip() for ln in str(context).splitlines()]
        _ctx_lower = str(context).lower()
        _title_token_hits = sum(1 for t in title_tokens[:6] if t and t in _ctx_lower) if title_tokens else 0
        _scope_anchor_present = bool(
            (chapter_anchor and chapter_anchor in _ctx_lower) or
            (part_anchor and part_anchor in _ctx_lower) or
            (title_tokens and _title_token_hits >= (2 if len(title_tokens) >= 3 else 1))
        )
        if debug is not None:
            try:
                debug["outcomes_extractor_line_count"] = int(len(lines))
                _ctx_l = str(context).lower()
                _ctx_ln = _ctx_l.replace("\u2019", "'").replace("’", "'")
                debug["outcomes_extractor_has_well_learn_marker"] = bool("what we'll learn in this chapter" in _ctx_ln)
            except Exception:
                pass
        # Marker window: if the context explicitly contains an outcomes marker that matches
        # the user query (Part/Chapter), strongly prefer lists that appear right after it.
        _marker_ix: int | None = None
        try:
            ql = (q or "").lower()
            _wants_part = ("part" in ql) and (part_no is not None)
            _wants_ch = ("chapter" in ql) and (chapter_no is not None)
            for _mi, _ln in enumerate(lines[:800]):
                _ll = (_ln or "").lower()
                if not re.search(r"\b(by completing|by the end|you will be able to|you should be able to|by chapter end|when you finish this|learning outcomes|outcomes|objectives|goals|chapter contract|competencies)\b", _ll):
                    continue
                if _wants_part and (f"part {part_no}" not in _ll):
                    continue
                if _wants_ch and (f"chapter {chapter_no}" not in _ll):
                    continue
                if _wants_ch and ("by completing part" in _ll):
                    continue
                _marker_ix = _mi
                break
        except Exception:
            _marker_ix = None

        # Marker-first extraction (safer than the old "Goals ..." heuristic):
        # If we see an explicit outcomes marker line, extract short, verb-ish lines immediately after it.
        try:
            markers = ("by completing", "by the end", "you will be able to", "you should be able to",
                       "by chapter end", "when you finish this", "learning outcomes", "outcomes", "objectives", "goals",
                       "chapter contract", "competencies")
            for mi, ln in enumerate(lines[:900]):
                ll = (ln or "").lower()
                if not any(m in ll for m in markers):
                    continue
                # If the question names Chapter/Part number, require it to be present in the marker
                # line OR in the nearby context window (page heading a few lines above the marker).
                # Pure line-check was too strict: most pages have "Learning Outcomes:" without
                # repeating "Chapter N" on that same line.
                if chapter_no is not None and ("chapter" in _q_lower) and (f"chapter {chapter_no}" not in ll):
                    _nearby_ctx = " ".join(lines[max(0, mi - 15): mi + 3]).lower()
                    if f"chapter {chapter_no}" not in _nearby_ctx:
                        # Strong markers like "by the end of this chapter" are reliable enough
                        # with only 1 matching title token (the chunk simply omits the chapter number).
                        _strong_marker = _has_explicit_outcomes_marker(ll)
                        _tok_req = 1 if _strong_marker else 2
                        if not (title_tokens and sum(1 for t in title_tokens[:6] if t in _nearby_ctx) >= _tok_req):
                            continue
                if part_no is not None and ("part" in _q_lower) and (f"part {part_no}" not in ll):
                    _nearby_ctx = " ".join(lines[max(0, mi - 15): mi + 3]).lower()
                    if f"part {part_no}" not in _nearby_ctx:
                        if not (title_tokens and sum(1 for t in title_tokens[:6] if t in _nearby_ctx) >= 2):
                            continue
                if _q_is_chapter and ("by completing part" in ll):
                    continue
                cand = []
                # Inline extraction: goals on same line as marker (no trailing newlines).
                # Strategy A: colon-delimited ("By completing X, you will: goals...")
                # Strategy B: bare marker word to stop-phrase ("Goals Understand...Learn...")
                try:
                    _INLINE_STOPS = ("lesson progression", "chapter progression",
                                     "outcome & method", "outcome and method",
                                     "why this order", "prerequisites", "prerequisite",
                                     "what this part will teach", "seven chapters",
                                     "frameworks first", "direct speech",
                                     "this is a real", "not a toy example")
                    _IVSP = re.compile(
                        r'(?=\b(?:Understand|Map|Learn|Capture|Build|Ship|Use|Integrate|Deliver|'
                        r'Identify|Structure|Apply|Verify|Run|Runs|Retrofit|Install|Design|Validate|'
                        r'Create|Implement|Configure|Monitor|Optimize|Define|Package|Explore|'
                        r'Master|Develop|Write|Generate|Complete|'
                        r'Start|Starts|Restart|Restarts|Log|Logs|Accept|Accepts|Execute|Executes|'
                        r'Survive|Survives|Can|Answer|Answers|Browse|Browses|Speak|Speaks|'
                        r'Operate|Operates|Respond|Responds|Handle|Handles|Connect|Connects|'
                        r'Enable|Enables|Expose|Exposes|Move|Moves|Read|Reads|'
                        r'Set|Sets|Establish|Establishes|Manage|Manages|Perform|Performs)\b)'
                    )

                    def _split_goals(txt: str) -> list[str]:
                        tl = txt.lower()
                        stop = len(txt)
                        for _s in _INLINE_STOPS:
                            _sp = tl.find(_s)
                            if 0 < _sp < stop:
                                stop = _sp
                        seg = txt[:stop].strip()
                        if not seg:
                            return []
                        parts = [p.strip() for p in re.split(r'\.{3,}', seg) if p.strip()]
                        if len(parts) < 2:
                            parts = [p.strip(' :;-') for p in _IVSP.split(seg) if p.strip(' :;-')]
                        # Filter lesson-table TOC rows: "Build 1 The AI Employee Moment... 2"
                        # They start with verb+ordinal ("Build 1 X") or end with a dangling
                        # next-lesson number (" 3" at end-of-string) from table row splitting.
                        parts = [p for p in parts
                                 if not re.match(r"^[A-Za-z]\w*\s+\d+\s+\S", p)
                                 and not re.search(r"\s+\d{1,2}\s*$", p.rstrip())]
                        return [p for p in parts if 8 <= len(p) <= 260]

                    # Strategy A: colon-terminated marker phrase
                    for _mp in ("you will:", "able to:", "outcomes:", "objectives:"):
                        _pi = ll.find(_mp)
                        if _pi >= 0:
                            _res = _split_goals(ln[_pi + len(_mp):].strip())
                            if len(_res) >= 2:
                                cand = _res
                            break

                    # Strategy B: bare marker word, goals follow up to a stop phrase
                    if not cand:
                        for _mp in ("goals", "by completing", "by the end", "learning outcomes", "objectives"):
                            _pi = ll.find(_mp)
                            if _pi < 0:
                                continue
                            _tail = ln[_pi + len(_mp):].strip()
                            # Strip short colon-preamble ("By completing X, you will: ...")
                            _fc = _tail.lower().find(":")
                            if 0 <= _fc < 100:
                                _tail = _tail[_fc + 1:].strip()
                            # Advance to first outcome verb to drop any remaining preamble
                            _vm = _IVSP.search(_tail)
                            if _vm:
                                _tail = _tail[_vm.start():]
                            _res = _split_goals(_tail)
                            if len(_res) >= 2:
                                cand = _res
                                break
                    # Strategy C: question-format goals ("answer these N questions: Why...? How...?")
                    if not cand:
                        _is_q_anchor = any(ph in ll for ph in ("answer these", "answer the following", "answer each"))
                        if _is_q_anchor:
                            # find colon and split by "?"
                            _q_ci = ll.find(":")
                            _q_tail = ln[_q_ci + 1:].strip() if _q_ci >= 0 else ln
                            _q_items = [p.strip() + "?" for p in _q_tail.split("?") if len(p.strip()) > 10]
                            if len(_q_items) >= 3:
                                cand = _q_items[:10]
                except Exception:
                    pass
                for ln2 in lines[mi + 1: mi + 45]:
                    t = (ln2 or "").strip()
                    if not t:
                        if cand:
                            break
                        continue
                    if len(t) > 260:
                        continue
                    if _BULLET_LINE_RE.match(t):
                        cand.append(_BULLET_LINE_RE.match(t).group(1).strip())
                        continue
                    # Break immediately on any section-boundary line (prevents chapter
                    # progression / "Why this order" lines from being appended as goals).
                    if re.match(r"(?i)^(?:chapter|part|lesson|section|try with ai|prompt\s+\d+|safety note|prerequisites|authors|company|privacy|previous|next|why this order|outcome|capstone)\b", t):
                        break
                    # Verb-start lines begin a NEW goal item — check BEFORE continuation.
                    # This prevents "Ship with LiveKit Agents" from being merged into
                    # the previous goal's continuation text.
                    w0 = (t.split()[:1][0].lower() if t.split() else "")
                    if w0 in _OUTCOME_VERB_HINTS and ("http" not in t.lower()) and ("?" not in t):
                        cand.append(t.strip(" -•\t"))
                        continue
                    # Non-verb lines are continuations of the last goal item.
                    if cand and len(t) <= 180:
                        cand[-1] = (cand[-1].rstrip() + " " + t.strip(" -:•\t")).strip()
                        continue
                    # stop on obvious non-outcomes section headers (fixed: \b not \\b)
                    if re.search(r"(?i)\b(previous|next|prerequisites|authors|company|privacy|capstone|lesson progression|chapter progression|why this order)\b", t):
                        break
                if 3 <= len(cand) <= 18:
                    starters = [(it.split()[:1][0].lower() if it.split() else "") for it in cand]
                    verbish = sum(1 for s in starters if s in _OUTCOME_VERB_HINTS)
                    verb_ratio = verbish / max(1.0, float(len(cand)))
                    _bad_starts = ("prompt", "step", "previous", "next", "prerequisites", "authors", "company", "about")
                    _bs = sum(1 for it in cand if it.strip().lower().startswith(_bad_starts))
                    _long = sum(1 for it in cand if len(it) > 140 or it.count(".") >= 2 or it.count(":") >= 2)
                    if _bs >= max(2, len(cand) // 2):
                        continue
                    if _long >= max(2, len(cand) // 2):
                        continue
                    if verb_ratio < 0.45 and not (_marker_ix is not None and len(cand) >= 3 and _generic_heading_count(cand) == 0):
                        continue
                    if not _valid_outcomes_items(cand):
                        continue
                    if debug is not None:
                        debug["outcomes_extractor_path"] = "marker_following_lines"
                        debug["outcomes_extractor_marker_line"] = str(ln or "").strip()[:220]
                        debug["outcomes_extractor_block_preview"] = "\n".join(cand[:8])[:900]
                    return "Learning outcomes:\n\n" + "\n".join(f"- {it}" for it in cand)
        except Exception:
            pass

        # Chapter-topics extraction: some curricula express goals as a topic list after
        # "what we'll learn in this chapter". This is not verb-imperative, but is still a
        # direct chapter learning-goals list.
        try:
            topic_markers = (
                "what we'll learn in this chapter",
                "what we will learn in this chapter",
                "what you're learning",
                "what you are learning",
                "what this part will teach you",
            )
            for mi, ln in enumerate(lines[:900]):
                ll = (ln or "").lower()
                ll_norm = ll.replace("\u2019", "'").replace("’", "'")
                _is_topic_marker = any(m in ll_norm for m in topic_markers)
                _has_well_learn = ("what we'll learn in this chapter" in ll_norm)
                # Extra debug: if the full context contains the well-learn marker but we
                # never see it as a single line, it may be joined without newlines.
                if debug is not None and _has_well_learn and len(debug.get("outcomes_extractor_marker_hits") or []) < 6:
                    debug["outcomes_extractor_marker_hits"].append(f"well_learn_line@{mi}:{(ln or '').strip()[:180]}")
                if not _is_topic_marker:
                    continue
                # If the user explicitly asked about a specific Part, only accept the
                # part-scoped marker. This prevents "Try with AI" prompt-question lists
                # (which often sit near other topic markers) from being mis-extracted as
                # Part learning outcomes.
                if (part_no is not None) and ("part" in _q_lower) and ("what this part will teach you" not in ll_norm):
                    continue
                if debug is not None and len(debug.get("outcomes_extractor_marker_hits") or []) < 6:
                    debug["outcomes_extractor_marker_hits"].append(f"topic_marker@{mi}:{(ln or '').strip()[:180]}")
                items = []
                # Special-case: some curricula use a small "Chapter / Focus / Framework" table
                # under "What This Part Will Teach You". Convert it into bullets deterministically.
                if "what this part will teach you" in ll_norm:
                    try:
                        cur_ch = None
                        cur_focus = ""
                        cur_fw = ""
                        table_items = []
                        for ln2 in lines[mi + 1: mi + 140]:
                            t = (ln2 or "").strip()
                            if not t:
                                continue
                            # stop once we hit the prompt-template section
                            if re.search(r"(?i)\\b(try with ai|prompt\\s+\\d+|safety note|previous|next)\\b", t):
                                break
                            # Chapter numbers (e.g. 62)
                            if re.fullmatch(r"\\d{1,3}", t):
                                if cur_ch and cur_focus:
                                    table_items.append(
                                        f"Chapter {cur_ch}: {cur_focus}" + (f" ({cur_fw})" if cur_fw else "")
                                    )
                                cur_ch = t
                                cur_focus = ""
                                cur_fw = ""
                                continue
                            # Skip headers
                            if t.lower() in ("chapter", "focus", "framework"):
                                continue
                            if cur_ch and not cur_focus:
                                cur_focus = t
                                continue
                            if cur_ch and cur_focus and not cur_fw:
                                cur_fw = t
                                continue
                        if cur_ch and cur_focus:
                            table_items.append(
                                f"Chapter {cur_ch}: {cur_focus}" + (f" ({cur_fw})" if cur_fw else "")
                            )
                        table_items = [it for it in table_items if it and len(it) <= 260]
                        if len(table_items) >= 3:
                            if debug is not None:
                                debug["outcomes_extractor_path"] = "part_table_marker"
                                debug["outcomes_extractor_marker_line"] = str(ln or "").strip()[:220]
                                debug["outcomes_extractor_block_preview"] = "\n".join(table_items[:10])[:900]
                            return "Learning outcomes:\n\n" + "\n".join(f"- {it}" for it in table_items[:18])
                    except Exception:
                        pass
                for ln2 in lines[mi + 1: mi + 60]:
                    t = (ln2 or "").strip()
                    if not t:
                        if items:
                            break
                        continue
                    m2 = _BULLET_LINE_RE.match(t)
                    if m2:
                        items.append(m2.group(1).strip())
                        continue
                    if t.startswith("- "):
                        items.append(t[2:].strip())
                        continue
                    # stop at headings
                    if re.search(r"(?i)\\b(previous|next|safety note|prompt\\s+\\d+|prerequisites|authors|company|privacy)\\b", t):
                        break
                    # Once we've started collecting bullets, stop when we hit normal prose.
                    if items and len(t) >= 8:
                        break
                items = [it for it in items if it and len(it) <= 260]
                # For explicit chapter/part questions, require anchor in marker line or nearby list text.
                _scope_blob = (str(ln or "") + " " + " ".join(items)).lower()
                if chapter_anchor and ("chapter" in _q_lower) and (chapter_anchor not in _scope_blob):
                    continue
                if part_anchor and ("part" in _q_lower) and (part_anchor not in _scope_blob):
                    continue
                # This marker is inherently chapter-scoped, so don't require title-token overlap.
                # If the user question explicitly references a chapter/part (number + title),
                # and retrieval has already been title-constrained, topic-marker lists are
                # sufficiently scoped even if the bullet items don't repeat the title words.
                _strong = (
                    ("what we'll learn in this chapter" in ll_norm)
                    or ("what we will learn in this chapter" in ll_norm)
                    or _q_is_chapter
                    or ("part" in _q_lower)
                )
                # For explicit Chapter/Part questions, a topic-marker bullet list is already scoped.
                # Accept it when we have at least 2 bullets, even if the items don't overlap the title words.
                # Reject interrogative/checklist bullets for goals/outcomes prompts.
                _qlike_items = sum(
                    1 for it in items
                    if ("?" in it) or ((it.split()[:1][0].lower() if it.split() else "") in _INTERROGATIVE_START)
                )
                _generic_items = _generic_heading_count(items)
                if _strong and len(items) >= 2 and _qlike_items < max(2, len(items) // 2) and _generic_items < max(2, len(items) // 2):
                    if debug is not None:
                        debug["outcomes_extractor_path"] = "chapter_topics_marker"
                        debug["outcomes_extractor_marker_line"] = str(ln or "").strip()[:220]
                        debug["outcomes_extractor_block_preview"] = "\n".join(items[:10])[:900]
                    return "Learning outcomes:\n\n" + "\n".join(f"- {it}" for it in items[:18])
                if _valid_chapter_topics_items(items, strong_marker=_strong):
                    if debug is not None:
                        debug["outcomes_extractor_path"] = "chapter_topics_marker"
                        debug["outcomes_extractor_marker_line"] = str(ln or "").strip()[:220]
                        debug["outcomes_extractor_block_preview"] = "\\n".join(items[:10])[:900]
                    return "Learning outcomes:\n\n" + "\n".join(f"- {it}" for it in items[:18])
        except Exception:
            pass

        # If the marker exists in the full context but doesn't appear as a standalone line,
        # it may have been flattened by chunking. Fall back to a windowed regex extraction.
        try:
            if debug is not None and debug.get("outcomes_extractor_has_well_learn_marker") and not debug.get("outcomes_extractor_path"):
                _ctx_norm = str(context).replace("\u2019", "'").replace("’", "'")
                _m = re.search(r"(?i)what\\s+we(?:'|’)ll\\s+learn\\s+in\\s+this\\s+chapter\\s*:\\s*(.{0,2000})", _ctx_norm)
                if _m:
                    tail = _m.group(1)
                    # Extract bullet-ish lines (works even if the page was flattened; we search for "- " runs)
                    cand = []
                    for mm in re.finditer(r"(?m)^\\s*(?:[-*•]|\\d{1,2}[.)])\\s+(.{4,200})$", tail):
                        cand.append(mm.group(1).strip())
                        if len(cand) >= 18:
                            break
                    if _valid_chapter_topics_items(cand, strong_marker=True):
                        if debug is not None:
                            debug["outcomes_extractor_path"] = "chapter_topics_window_regex"
                            debug["outcomes_extractor_marker_line"] = "what we'll learn in this chapter:"
                            debug["outcomes_extractor_block_preview"] = "\n".join(cand[:10])[:900]
                        return "Learning outcomes:\n\n" + "\n".join(f"- {it}" for it in cand[:18])
        except Exception:
            pass
        best = None  # (score, start_idx, end_idx)
        i = 0
        while i < len(lines):
            m = _BULLET_LINE_RE.match(lines[i] or "")
            if not m:
                i += 1
                continue
            # Collect a contiguous block of bullet-ish lines with optional continuations.
            start = i
            items = []
            while i < len(lines):
                ln = lines[i]
                m2 = _BULLET_LINE_RE.match(ln or "")
                if m2:
                    items.append(m2.group(1).strip())
                    i += 1
                    continue
                # Allow a single indented continuation line for the previous bullet.
                if items and ln and (ln.startswith("  ") or ln.startswith("\t")) and len(ln.strip()) >= 6:
                    items[-1] = (items[-1] + " " + ln.strip()).strip()
                    i += 1
                    continue
                if items and ln and len(ln.strip()) <= 180 and not re.match(r"(?i)^(?:chapter|part|lesson|section|try with ai|prompt\s+\d+|safety note|prerequisites|authors|company|privacy|previous|next)\b", ln.strip()):
                    items[-1] = (items[-1].rstrip() + " " + ln.strip(" -:•\t")).strip()
                    i += 1
                    continue
                break
            end = i
            # Basic validity: list-like blocks only.
            items = [it for it in items if it and len(it) <= 260]
            if 3 <= len(items) <= 25:
                # Reject link directories / nav lists (common in structural docs).
                joined_raw = " ".join(items)
                if re.search(r"https?://", joined_raw, re.I):
                    continue
                # Reject UI/navigation control lists (universal).
                _jr_low = joined_raw.lower()
                _ui_hints = ("toggle theme", "toggle menu", "skip to main content", "copy as markdown", "sign in to ask", "sign in to access", "ctrl+", "ctrl k", "search...", "leaderboard")
                if any(h in _jr_low for h in _ui_hints):
                    continue
                # Reject question-list "outcomes" (universal).
                if any("?" in it for it in items):
                    continue
                _wh = ("what", "which", "why", "how", "when", "where", "who")
                if sum(1 for it in items if it.strip().lower().startswith(_wh)) > max(0, len(items) - 2):
                    continue
                # Reject "Title : URL" shapes even if url missing protocol.
                if sum(1 for it in items if re.search(r"\bwww\.\b|\.org\b|\.com\b|/docs/", it, re.I)) >= max(2, len(items) // 2):
                    continue

                # Reject "prompt templates" masquerading as outcomes (common in curriculum pages).
                # If most bullets start with "Prompt" / "Step" / "Previous" / "Next" it's not goals.
                _bad_starts = ("prompt", "step", "previous", "next", "prerequisites", "authors", "company", "about us")
                _bs = sum(1 for it in items if it.strip().lower().startswith(_bad_starts))
                if _bs >= max(2, len(items) // 2):
                    continue

                # Outcomes are typically concise imperatives. If most bullets are long multi-sentence blobs,
                # it's probably a chunk boundary artifact or an instruction block, not goals.
                _long = sum(1 for it in items if len(it) > 140 or it.count(".") >= 2 or it.count(":") >= 2)
                if _long >= max(2, len(items) // 2):
                    continue
                # If the user mentioned a title phrase, require non-trivial overlap so we don't
                # extract unrelated lists from other pages.
                if title_tokens:
                    jl = joined_raw.lower()
                    title_hit_min = 2 if len(title_tokens) >= 3 else 1
                    _th = sum(1 for t in title_tokens[:6] if t in jl)
                    if _th < title_hit_min:
                        continue

                # Universal scoring: prefer outcome-like lists; avoid reflection-question lists,
                # but do not hard-reject on '?' (some domains may phrase outcomes as questions).
                starters = [(it.split()[:1][0].lower() if it.split() else "") for it in items]
                q_marks = sum(1 for it in items if "?" in it)
                interrogative = sum(1 for s in starters if s in _INTERROGATIVE_START)
                verbish = sum(1 for s in starters if s in _OUTCOME_VERB_HINTS)
                title_hit = 0
                if title_tokens:
                    jl = joined_raw.lower()
                    title_hit = sum(1 for t in title_tokens[:6] if t in jl)
                n = float(len(items))
                question_ratio = q_marks / n
                inter_ratio = interrogative / n
                verb_ratio = verbish / n

                score = n
                score += (verbish * 2.5)
                score += (title_hit * 2.0)
                score -= (question_ratio * 8.0)
                score -= (inter_ratio * 6.0)

                # Minimum viability: needs either verb-ish starts or strong title overlap.
                if (verbish < 2) and (title_hit < 2):
                    continue
                # For goals/outcomes, we expect mostly verb-start bullets (Define/Structure/Prepare/Run...).
                if verb_ratio < 0.45:
                    continue
                # If it's mostly questions, it's almost certainly not learning outcomes.
                if question_ratio >= 0.5 or inter_ratio >= 0.5:
                    continue
                joined = " ".join(items).lower()
                # Reward "objective/outcome" verbs but do not require any fixed phrase.
                score += 4.0 if ("will" in joined or "able to" in joined) else 0.0
                score += 2.0 if ("understand" in joined or "build" in joined or "ship" in joined or "integrate" in joined) else 0.0
                # Prefer earlier blocks slightly (retrieval already sorts by relevance).
                score += max(0.0, 6.0 - (start / 120.0))
                # If there's an explicit matching marker, heavily reward the list nearest after it.
                if _marker_ix is not None:
                    if start >= _marker_ix and start <= (_marker_ix + 80):
                        score += 30.0
                    else:
                        score -= 10.0
                if (best is None) or (score > best[0]):
                    best = (score, start, end)
            # Continue scanning from end of this block.
        if not best:
            # Inline list fallback: sometimes crawled pages lose bullet newlines and
            # the list becomes one long line after a ":" (still deterministic to parse).
            flat = " ".join(ln.strip() for ln in lines if ln and ln.strip())
            title_ix = flat.lower().find(title_phrase.lower()) if title_phrase else -1

            def _split_inline_list(tail: str) -> list[str]:
                # Start candidates are Titlecase words followed by a lowercase "glue" word
                # (e.g., "Understand the", "Integrate real", "Ship with"). This avoids
                # splitting on proper nouns like "LiveKit Agents".
                starts = []
                for m in re.finditer(r"\b([A-Z][a-z]{2,})\b\s+([a-z]{2,}|to|with|for|the|a|an|in|on|of|when|vs)\b", tail):
                    starts.append(m.start(1))
                # De-dupe and ensure minimum spacing to avoid noisy splits.
                starts = sorted(set(starts))
                pruned = []
                for s in starts:
                    if not pruned or (s - pruned[-1]) >= 18:
                        pruned.append(s)
                if len(pruned) < 3:
                    return []
                items = []
                for a, b in zip(pruned, pruned[1:] + [len(tail)]):
                    seg = tail[a:b].strip(" \t-•;|")
                    if 10 <= len(seg) <= 260:
                        items.append(seg)
                return items

            def _trim_after_heading(items: list[str]) -> list[str]:
                """Stop the list if a section heading leaks in (common when lists are inline)."""
                out = []
                for it in items:
                    mm = re.search(r"\b(?:Lesson Progression|Chapter Progression|Capstone|Outcome\b|Method\b)\b", it, re.I)
                    if mm and mm.start() >= 10:
                        head = it[:mm.start()].strip(" -:;,.")
                        if head:
                            out.append(head)
                        break
                    out.append(it)
                return [x for x in out if x]

            best_inline = None  # (kind_prio, score, items)

            def _consider_tail(tail: str, proximity_pos: int | None = None, kind: str = "colon"):
                nonlocal best_inline
                if not tail or len(tail) < 60:
                    return
                if part_no is not None and proximity_pos is not None:
                    win = flat[max(0, proximity_pos - 420): proximity_pos + 850].lower()
                    if f"part {part_no}" not in win:
                        return
                    # Prefer canonical outcomes phrasing for Part-level goals to avoid
                    # picking random lesson checklists that merely mention "Part N".
                    if ("completing part" in win) and (f"completing part {part_no}" not in win):
                        return
                if title_tokens and proximity_pos is not None:
                    win2 = flat[max(0, proximity_pos - 420): proximity_pos + 850].lower()
                    hit = sum(1 for t in title_tokens[:6] if t in win2)
                    req = 3 if part_no is not None else 2
                    if hit < req:
                        return
                # Title phrases are not always repeated inside the goals list itself
                # (often the list is just verb-start items). So only require title-token
                # overlap when we can anchor by proximity to the title occurrence.
                if title_phrase and title_ix != -1 and proximity_pos is not None:
                    tks = [w.lower() for w in re.findall(r"[a-zA-Z]{4,}", title_phrase)][:8]
                    if tks and not any(t in tail.lower() for t in tks[:4]):
                        return
                items = _split_inline_list(tail)
                if not (3 <= len(items) <= 25):
                    return
                if re.search(r"https?://", " ".join(items), re.I):
                    return
                items = _trim_after_heading(items)
                if not (3 <= len(items) <= 25):
                    return
                # Inline lists are especially error-prone (page text can include prompts/checklists).
                # Require predominantly verb-start items to qualify as "learning outcomes".
                starters = [(it.split()[:1][0].lower() if it.split() else "") for it in items]
                verbish = sum(1 for s in starters if s in _OUTCOME_VERB_HINTS)
                verb_ratio = verbish / max(1.0, float(len(items)))
                if verb_ratio < 0.45:
                    return
                joined = " ".join(items).lower()
                if _q_is_chapter and re.search(r"\bby\s+completing\s+part\s+\d+\b", joined):
                    return
                score = float(len(items))
                score += 2.0 if ("will" in joined or "able to" in joined) else 0.0
                # Chapter queries: penalize lists that clearly describe Part-level goals.
                if _q_is_chapter and "part " in joined:
                    score -= 6.0
                if kind == "goals":
                    score += 4.0
                if proximity_pos is not None and title_ix != -1:
                    dist = abs(proximity_pos - title_ix)
                    score += max(0.0, 6.0 - (dist / 180.0))
                kind_prio = 1 if kind == "goals" else 0
                if (best_inline is None) or ((kind_prio, score) > (best_inline[0], best_inline[1])):
                    best_inline = (kind_prio, score, items)

            # Pass A: Scan ":" occurrences ("By completing X, you will: ...")
            for _m in re.finditer(r":", flat):
                cpos = _m.start()
                if cpos < 10:
                    continue
                if title_ix != -1 and (cpos < (title_ix - 200) or cpos > (title_ix + 600)):
                    continue
                tail = flat[cpos + 1: cpos + 1400].strip()
                _consider_tail(tail, proximity_pos=cpos, kind="colon")

            # Pass B: Handle "Goals ..." lead-in without a colon.
            for _m in re.finditer(r"\bGoals\b", flat, re.I):
                gpos = _m.end()
                if title_ix != -1 and (gpos < (title_ix - 400) or gpos > (title_ix + 900)):
                    continue
                tail = flat[gpos: gpos + 1400].strip(" :;-—\t")
                _consider_tail(tail, proximity_pos=gpos, kind="goals")
            if best_inline:
                _, __, items = best_inline
                # Merge obvious split fragments (e.g., "Ship with LiveKit" + "Agents ...").
                try:
                    glue_end = {"on","with","for","to","in","and","or","of","the","a","an","vs"}
                    merged = []
                    i = 0
                    while i < len(items):
                        cur = items[i].strip()
                        if i + 1 < len(items):
                            nxt = items[i + 1].strip()
                            cur_last = (cur.split()[-1].lower() if cur.split() else "")
                            if (cur_last in glue_end) and (len(nxt.split()) >= 3):
                                # Only merge if next is not tiny.
                                merged.append((cur + " " + nxt).strip())
                                i += 2
                                continue
                        merged.append(cur)
                        i += 1
                    items = [m for m in merged if m]
                except Exception:
                    pass
                if len(items) < 3:
                    return None
                if not _valid_outcomes_items(items):
                    return None
                if debug is not None:
                    debug["outcomes_extractor_path"] = "inline_list"
                    debug["outcomes_extractor_block_preview"] = "\n".join(items[:8])[:900]
                return "Learning outcomes:\n\n" + "\n".join(f"- {it}" for it in items)
            return None
        _, s, e = best
        out_items = []
        for ln in lines[s:e]:
            m3 = _BULLET_LINE_RE.match(ln or "")
            if m3:
                out_items.append(m3.group(1).strip())
            elif out_items and ln and (ln.startswith("  ") or ln.startswith("\t")):
                out_items[-1] = (out_items[-1] + " " + ln.strip()).strip()
        out_items = [it for it in out_items if it]
        if 3 <= len(out_items) <= 25 and _valid_outcomes_items(out_items):
            # Normalize to a clean bullet list (keep it universal; don't require a specific marker string).
            if debug is not None:
                debug["outcomes_extractor_path"] = "bullet_block"
                debug["outcomes_extractor_block_preview"] = "\n".join(out_items[:8])[:900]
            return "Learning outcomes:\n\n" + "\n".join(f"- {it}" for it in out_items)

        # Last-resort universal extractor:
        # harvest contiguous short verb-start lines even when explicit markers are missing
        # (some docs render goals as plain lines instead of list bullets).
        cand: list[str] = []
        for ln in lines[:1200]:
            t = (ln or "").strip()
            if not t:
                if len(cand) >= 4:
                    break
                continue
            if len(t) > 220:
                if len(cand) >= 4:
                    break
                continue
            if re.search(r"https?://", t, re.I):
                continue
            if re.search(r"(?i)\b(previous|next|chapter\s+quiz|try with ai|prompt\s+\d+|authors|company|privacy)\b", t):
                if len(cand) >= 4:
                    break
                continue
            w0 = (re.findall(r"[a-zA-Z]+", t.lower())[:1] or [""])[0]
            if w0 in _OUTCOME_VERB_HINTS:
                cand.append(t.strip(" -*\t"))
            elif len(cand) >= 4:
                break
        if 4 <= len(cand) <= 18 and _valid_outcomes_items(cand):
            if debug is not None:
                debug["outcomes_extractor_path"] = "verb_lines_fallback"
                debug["outcomes_extractor_block_preview"] = "\n".join(cand[:8])[:900]
            return "Learning outcomes:\n\n" + "\n".join(f"- {it}" for it in cand[:18])
        return None
    except Exception:
        return None


# Heuristic reranker (universal): improves reliability for chapter/part/title questions across DBs.
# Used only when DB is not a product catalog (product DBs use different ranking rules).
_NAV_HINTS = (
    'on this page', 'copy as markdown', 'site index', '#site-index', '#site-navigation',
    'ctrl+', 'toggle theme', 'skip to main content'
)

_PROMPT_TEMPLATE_HINTS = (
    'try with ai',
    'prompt 1:',
    'prompt 2:',
    'prompt 3:',
    'safety note',
    'reflect on your skill',
)


def _is_prompt_template_chunk(text: str) -> bool:
    """Heuristic: detect "prompt template" content blocks that frequently pollute retrieval.

    Universal: many doc sites include 'Try With AI' and numbered prompt scaffolds.
    Great for learning, but usually the wrong evidence for goals/outcomes/policy questions.
    """
    try:
        tl = (text or "").lower()
        if not tl:
            return False
        # Strong signals
        if ("try with ai" in tl) and (("prompt 1:" in tl) or ("prompt 2:" in tl)):
            return True
        # Multiple prompt markers is typically a template block
        prompt_hits = tl.count("prompt 1:") + tl.count("prompt 2:") + tl.count("prompt 3:")
        if prompt_hits >= 2:
            return True
        # Generic fallback
        return any(h in tl for h in _PROMPT_TEMPLATE_HINTS) and ("prompt" in tl)
    except Exception:
        return False


def _has_explicit_outcomes_marker(text: str) -> bool:
    try:
        tl = (text or "").lower()
        if not tl:
            return False
        return (
            ("learning outcomes" in tl)
            or ("what you will learn" in tl)
            or ("what you'll learn" in tl)
            or ("what we'll learn in this chapter" in tl)
            or ("what this part will teach you" in tl)
            or ("by the end of this chapter" in tl)
            or ("by completing" in tl)
            or ("you will be able to" in tl)
            or ("you should be able to" in tl)
            or ("by chapter end" in tl)
            or ("when you finish this" in tl)
        )
    except Exception:
        return False


def _extract_title_phrase(q: str) -> str:
    try:
        m = re.search(r"\b(?:chapter|part|section|lesson|unit)\s*\d+\s*[:\-]\s*([^\n]{6,200})", q or '', re.I)
        if m:
            phrase = m.group(1).strip().strip('"\'')
            # Strip common trailing glue that isn't part of the title.
            phrase = re.sub(r"\s+according\s+to\s+.*$", "", phrase, flags=re.I).strip()
            phrase = re.sub(r"\s+from\s+the\s+book\s*$", "", phrase, flags=re.I).strip()
            # If the user included the chapter/part prefix in the extracted title, drop it
            # so URL/title matching can land on the canonical page slug instead of the scoped prefix.
            phrase = re.sub(
                r"^(?:chapter|part|section|lesson|unit)\s*\d+[A-Za-z]?\s*[:\-]?\s*",
                "",
                phrase,
                flags=re.I,
            ).strip()
            return phrase
        # Pattern 1b: "Chapter/Part N[A-Za-z]? Title" — no colon/dash separator.
        # e.g. "Chapter 22 Linux Operations for Agent Deployment" → "Linux Operations for Agent Deployment"
        # e.g. "Chapter 21B Postgres as System of Record" → "Postgres as System of Record"
        m1b = re.search(r"\b(?:chapter|part|section|lesson|unit)\s+\d+[A-Za-z]?\s+(.+)", q or '', re.I)
        if m1b:
            phrase = m1b.group(1).strip().strip('"\'')
            phrase = re.sub(r"\s*\??\s*$", "", phrase).strip()
            phrase = re.sub(r"\s+according\s+to\s+.*$", "", phrase, flags=re.I).strip()
            phrase = re.sub(r"\s+from\s+the\s+book\s*$", "", phrase, flags=re.I).strip()
            if len(phrase) >= 5 and not re.match(r"^(what|why|how|which|who|when|where|are|is|do|does)\b", phrase, re.I):
                return phrase
        m2 = re.search(r"^\s*in\s+([^,\n]{6,200})\s*,\s*(?:what|why|how|which|who|when)\b", q or "", re.I)
        if m2:
            phrase = m2.group(1).strip().strip('"\'')
            phrase = re.sub(r"\s{2,}", " ", phrase).strip()
            return phrase
        # Handle "Title (N)" pattern: e.g. "Voice Foundations (109)" or "Chapter (79)"
        # Strip the parenthesised number; strip leading question/filler words.
        _m3_all = re.findall(
            r"\b([A-Z][A-Za-z][A-Za-z\s&:\-]{2,70})\s*\(\s*\d{1,4}\s*\)",
            q or "",
        )
        if _m3_all:
            phrase = _m3_all[-1].strip()
            _q_stops = {"what", "which", "how", "when", "who", "why", "where",
                        "do", "does", "is", "are", "the", "a", "an", "in", "of",
                        "for", "with", "from", "by", "at", "to", "give", "show",
                        "tell", "list", "explain", "describe", "cover", "covers"}
            parts = phrase.split()
            while parts and parts[0].lower() in _q_stops:
                parts = parts[1:]
            phrase = " ".join(parts).strip()
            if len(phrase) >= 4:
                return phrase
    except Exception:
        pass
    # Pattern 4: "[question] in [Title Case Phrase]?" — title at end, no chapter number.
    # e.g. "What are the four TTS modes in Give It a Voice?"
    m4 = re.search(
        r"\bin\s+([A-Z][A-Za-z\-']+(?:\s+(?:[A-Z][A-Za-z\-']+|[a-z]{1,4})){1,8})\s*\??$",
        q or "",
    )
    if m4:
        phrase = m4.group(1).strip().strip("\"'")
        phrase = re.sub(r"\s{2,}", " ", phrase).strip()
        if len(phrase) >= 4:
            return phrase
    # Pattern 5: "N principles/steps/rules of [Topic]" — extract the topic.
    # e.g. "What are the seven principles of General Agent Problem Solving" → "General Agent Problem Solving"
    try:
        m5 = re.search(
            r"\b(?:principles?|steps?|rules?|methods?|practices?|phases?|pillars?|concepts?|lessons?)\b[^.?]*\bof\s+(.+?)(?:\?|\s*$)",
            q or '', re.I
        )
        if m5:
            phrase = m5.group(1).strip().strip('"\'')
            phrase = re.sub(r"\s*\??$", "", phrase).strip()
            if (len(phrase) >= 6
                    and not re.match(r"^(chapter|part|section|lesson|unit)\b", phrase, re.I)
                    and not re.match(r"^(what|why|how|which|who|when|where|are|is|do|does)\b", phrase, re.I)):
                return phrase
    except Exception:
        pass
    return ''


def _slugish(text: str) -> str:
    try:
        return re.sub(r"[^a-z0-9]+", "-", (text or "").lower()).strip("-")
    except Exception:
        return ""


def _extract_chapter_number(q: str) -> int | None:
    try:
        # v11: [A-Za-z]? handles suffixed chapters like "Chapter 21B"
        m = re.search(r"\bchapter\s*(\d{1,4})[A-Za-z]?\b", q or "", re.I)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def _extract_part_number(q: str) -> int | None:
    try:
        m = re.search(r"\bpart\s*(\d{1,4})\b", q or "", re.I)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None

def _doc_matches_strict_scope(doc, q: str, title_phrase: str) -> bool:
    """Universal gate for scoped chapter/part queries.
    Keeps only chunks with strong number/title anchors in URL or text.
    """
    try:
        src = str(((getattr(doc, "metadata", None) or {}).get("source")) or "").lower()
        body = str(getattr(doc, "page_content", "") or "").lower()
        ch = _extract_chapter_number(q or "")
        pt = _extract_part_number(q or "")
        title_slug = _slugish(title_phrase or "")
        tks = [w.lower() for w in re.findall(r"[a-zA-Z0-9]{4,}", title_phrase or "")][:10]
        tks = [t for t in tks if t not in {"chapter", "part", "learning", "goals", "outcomes", "according", "book"}]
        scope_phrase = _extract_scope_phrase(q or "")
        focus = _extract_focus_phrase(q or "")
        scope_tks = [w.lower() for w in re.findall(r"[a-zA-Z0-9]{4,}", scope_phrase or "")][:8]
        focus_tks = [w.lower() for w in re.findall(r"[a-zA-Z0-9]{4,}", focus or "")][:8]
        scope_hit = any((st in src or st in body) for st in scope_tks) if scope_tks else False
        focus_hit = any((ft in src or ft in body) for ft in focus_tks) if focus_tks else False

        num_hit = False
        if ch is not None:
            if (f"chapter-{ch}" in src) or (f"chapter {ch}" in src) or (f"chapter {ch}" in body) or re.search(rf"\bch(?:apter)?\.?\s*{ch}\b", body):
                num_hit = True
        if pt is not None:
            if (f"part-{pt}" in src) or (f"part {pt}" in src) or (f"part {pt}" in body):
                num_hit = True

        phrase_hit = False
        if title_phrase:
            tpl = title_phrase.lower().strip()
            # Slug-in-URL is the strongest signal — check first
            if title_slug and title_slug in src:
                phrase_hit = True
            elif tpl and (tpl in src or tpl in body):
                phrase_hit = True
            elif tks:
                hits = {t for t in tks if (t in src or t in body)}
                # With a chapter/part anchor: 2 token hits sufficient.
                # Without (e.g. "TTS modes in Give It a Voice?"): require ALL title tokens
                # to hit so we don't false-positive on short/common tokens.
                required = 2 if (ch is not None or pt is not None) else len(tks)
                if len(hits) >= required:
                    phrase_hit = True
            elif title_slug and title_slug in body:
                phrase_hit = True

        # Section-style queries: "In <scope>, what ..." should require the scope phrase.
        if scope_tks or focus_tks:
            if scope_tks and not scope_hit:
                return False
            if focus_tks and not focus_hit:
                return False
            # If we have a section-style query, the scope/focus anchors are enough to treat it as matched.
            if ch is None and pt is None:
                return True

        if num_hit and (phrase_hit or not tks):
            return True
        if phrase_hit and (ch is None and pt is None):
            if (scope_tks and not scope_hit) or (focus_tks and not focus_hit):
                return False
            return True
        if phrase_hit and _has_explicit_outcomes_marker(body):
            return True
        if num_hit and _has_explicit_outcomes_marker(body):
            return True
    except Exception:
        pass
    return False


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


def _question_heading_anchors(q: str, title_phrase: str = "") -> list[str]:
    anchors: list[str] = []
    try:
        ql = (q or "").lower()
        ch = _extract_chapter_number(q or "")
        pt = _extract_part_number(q or "")
        if ch is not None:
            anchors.extend([f"chapter {ch}", f"chapter-{ch}"])
        if pt is not None:
            anchors.extend([f"part {pt}", f"part-{pt}"])
        if title_phrase:
            anchors.append(title_phrase.lower())
            for t in re.findall(r"[a-zA-Z]{4,}", title_phrase.lower())[:6]:
                anchors.append(t)
        for k in _meaningful_keywords(ql)[:6]:
            anchors.append(k)
    except Exception:
        pass
    # keep order, dedup
    out = []
    seen = set()
    for a in anchors:
        if a and a not in seen:
            seen.add(a)
            out.append(a)
    return out


def _extract_focus_phrase(q: str) -> str:
    try:
        qq = (q or "").strip()
        m = re.search(r"\bwhat\s+(?:is|does)\s+(.+?)(?:\?|$)", qq, re.I)
        if not m:
            return ""
        phrase = m.group(1).strip()
        phrase = re.sub(r"\b(?:used for|use for|do|does|mean|means|evaluate|test|simulate|trying to detect)\b.*$", "", phrase, flags=re.I).strip()
        phrase = re.sub(r"^[Tt]he\s+", "", phrase).strip()
        return phrase[:120]
    except Exception:
        return ""


def _extract_scope_phrase(q: str) -> str:
    try:
        qq = (q or "").strip()
        m = re.search(r"^\s*in\s+([^,\n]{6,200})\s*,\s*(?:what|why|how|which|who|when)\b", qq, re.I)
        if not m:
            return ""
        phrase = m.group(1).strip().strip('"\'')
        phrase = re.sub(r"\s{2,}", " ", phrase).strip()
        return phrase[:120]
    except Exception:
        return ""


def _evidence_rerank_score(doc, q: str, title_phrase: str) -> float:
    """Evidence-first score: exact title/heading/anchor hits before broad semantic hits."""
    try:
        meta = (getattr(doc, "metadata", None) or {})
        src = str(meta.get("source") or "")
        body = str(getattr(doc, "page_content", "") or "")
        sl = src.lower()
        bl = body.lower()
        score = 0.0
        anchors = _question_heading_anchors(q, title_phrase)
        if not anchors:
            return 0.0

        # Head section of each chunk often carries title/heading context.
        head = bl[:500]
        first_lines = "\n".join((body or "").splitlines()[:10]).lower()

        if title_phrase:
            tpl = title_phrase.lower()
            if tpl in bl:
                score += 20.0
            if tpl in sl:
                score += 16.0
            if tpl in head:
                score += 12.0
            # Near-exact phrase boost (normalized punctuation/whitespace).
            try:
                _tpl_n = re.sub(r"[^a-z0-9]+", " ", tpl).strip()
                _bl_n = re.sub(r"[^a-z0-9]+", " ", bl)
                _sl_n = re.sub(r"[^a-z0-9]+", " ", sl)
                if _tpl_n and _tpl_n in _bl_n:
                    score += 14.0
                if _tpl_n and _tpl_n in _sl_n:
                    score += 10.0
            except Exception:
                pass

        for a in anchors[:10]:
            if len(a) < 4:
                continue
            if a in sl:
                score += 5.5
            if a in head:
                score += 4.5
            elif a in first_lines:
                score += 3.5
            elif a in bl:
                score += 1.5

        # Reward chunks that look like explicit answer sections (heading/list/definition).
        if re.search(r"(?im)^\s*(?:#{1,4}\s+|[-*]\s+|\d{1,2}[.)]\s+)", body):
            score += 2.0
        return score
    except Exception:
        return 0.0


def _source_section_key(doc) -> str:
    """Stable section key for grouping nearby chunks from the same canonical page."""
    try:
        meta = (getattr(doc, "metadata", None) or {})
        src = str(meta.get("source") or "").strip().lower()
        if not src:
            return ""
        src = src.split("#", 1)[0].split("?", 1)[0]
        src = re.sub(r"/(index\.html?)?$", "", src)
        return src.rstrip("/")
    except Exception:
        return ""


def _doc_price_value(doc):
    """Best-effort numeric price for product ranking."""
    try:
        meta = (getattr(doc, "metadata", None) or {})
        price = meta.get("price")
        if isinstance(price, (int, float)):
            return float(price) if float(price) > 0 else None
        if price is not None:
            try:
                _pv = float(str(price).replace(",", "").strip())
                return _pv if _pv > 0 else None
            except Exception:
                pass
        body = str(getattr(doc, "page_content", "") or "")
        # Prefer the labeled "Price:" line — the first bare $ in a chunk can be a
        # sidebar/related-products price, not this product's.
        m = (re.search(r"Price:\s*(?:\$|£|€|\brs\.?|\bpkr)?\s*([\d,]+(?:\.\d{1,2})?)", body, re.I)
             or re.search(r"(?:\$|£|€|\brs\.?|\bpkr)\s*([\d,]+(?:\.\d{1,2})?)", body, re.I))
        if m:
            _pv = float(m.group(1).replace(",", ""))
            return _pv if _pv > 0 else None
    except Exception:
        pass
    return None


def _is_price_ranking_query(q: str) -> tuple[bool, bool]:
    """Return (is_price_ranking, descending)."""
    ql = (q or "").lower()
    asc = bool(re.search(r"\b(cheapest|lowest\s+price|lowest\s+priced|least\s+expensive|most\s+affordable|budget)\b", ql))
    desc = bool(re.search(r"\b(most\s+expensive|highest\s+price|highest\s+priced|priciest|costliest|most\s+costly)\b", ql))
    return (asc or desc, desc)


_CURRENCY_PREFIX = r'(?:rs\.?|pkr|\$|£|€|usd|gbp|eur)?\s*'


def _parse_price_bounds(q: str):
    """Parse range/budget intent → (lo, hi); either side may be None. None if no range."""
    ql = (q or "").lower().replace(",", "")
    m = re.search(r'\b(?:between|from)\s+' + _CURRENCY_PREFIX + r'(\d+(?:\.\d+)?)\s*(?:and|to|[-–])\s*' + _CURRENCY_PREFIX + r'(\d+(?:\.\d+)?)', ql)
    if m:
        return (float(m.group(1)), float(m.group(2)))
    m = re.search(r'\b(?:under|below|less\s+than|up\s+to|within|at\s+most|max(?:imum)?(?:\s+of)?|cheaper\s+than|no\s+more\s+than)\s+' + _CURRENCY_PREFIX + r'(\d+(?:\.\d+)?)', ql)
    if m:
        return (None, float(m.group(1)))
    m = re.search(r'\b(?:over|above|more\s+than|at\s+least|min(?:imum)?(?:\s+of)?|pricier\s+than)\s+' + _CURRENCY_PREFIX + r'(\d+(?:\.\d+)?)', ql)
    if m:
        return (float(m.group(1)), None)
    # Budget phrasing: "I have a budget of Rs.200" / "my budget is Rs.200" /
    # "I have Rs.200 to spend". Requires an explicit currency marker so plain
    # quantities ("I have 3 kids") never parse as a price cap.
    m = re.search(r'\bbudget\s+(?:of\s+|is\s+|around\s+)?(?:rs\.?|pkr|\$|£|€|usd|gbp|eur)\s*(\d+(?:\.\d+)?)', ql)
    if m:
        return (None, float(m.group(1)))
    m = re.search(r'\bi\s+(?:only\s+)?have\s+(?:rs\.?|pkr|\$|£|€|usd|gbp|eur)\s*(\d+(?:\.\d+)?)', ql)
    if m:
        return (None, float(m.group(1)))
    return None


def _is_review_ranking_query(q: str) -> bool:
    return bool(re.search(
        r'(?i)\b(?:most|highest|best|top)[\s-]+(?:customer\s+)?(?:reviews?|reviewed|rated|ratings?)\b'
        r'|\bmost\s+popular\b|\bmost\s+reviews\b', q or ''))


def _doc_review_count(doc):
    """Review count from metadata or chunk text ('14 reviews')."""
    try:
        meta = getattr(doc, 'metadata', None) or {}
        rv = meta.get('review_count')
        if isinstance(rv, (int, float)):
            return int(rv)
        m = re.search(r'(\d+)\s+(?:customer\s+)?reviews?\b', str(getattr(doc, 'page_content', '') or ''), re.I)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return None


def _heuristic_rerank_score(doc, q: str, title_phrase: str) -> float:
    try:
        meta = (getattr(doc, 'metadata', None) or {})
        src = str(meta.get('source') or '')
        body = str(getattr(doc, 'page_content', '') or '')
        sl = src.lower()
        bl = body.lower()

        score = 0.0

        # Penalize navigation / index heavy chunks — EXCEPT for structure questions
        # ("what categories/topics/pages do you have"), where they ARE the answer.
        _structure_q = bool(re.search(r'(?i)\b(?:categor(?:y|ies)|types?\s+of|kinds?\s+of|topics|sections|what\s+pages|site\s*map|how\s+many|overview\s+of\s+(?:the\s+)?(?:site|store|catalog))\b', q or ''))
        if any(h in sl for h in ('#site-index', '#site-navigation')):
            score += 6.0 if _structure_q else -8.0
        # Category/catalog pages state their own item counts ("21 items",
        # "1000 results") — for count/structure questions they are authoritative.
        if _structure_q and str(meta.get('content_type') or '') in ('category', 'catalog'):
            score += 4.0
        if any(h in bl for h in _NAV_HINTS):
            score -= 4.0
        if (bl.count('http://') + bl.count('https://')) >= 3:
            score -= 6.0

        # For goals/outcomes queries, quiz/exercise pages are usually not the canonical evidence.
        if _OUTCOMES_INTENT_RE.search(q or '') and any(p in sl for p in ("/chapter-quiz", "/quiz", "/exercises", "/exercise")):
            score -= 10.0
        # For scoped chapter/part outcomes queries, deprioritize non-canonical site shells/translations.
        if _OUTCOMES_INTENT_RE.search(q or '') and re.search(r"\b(?:chapter|part)\s+\d{1,4}\b", q or "", re.I):
            if any(p in sl for p in ("/roman/", "/arabic/", "/urdu/", "/certifications", "/about")):
                score -= 12.0
            if "/docs/" not in sl:
                score -= 6.0
        # For English queries, prefer canonical /docs over romanized paths to reduce mixed-language context.
        if (not _is_urdu_script(q or "")) and bool(re.search(r"[A-Za-z]", q or "")):
            if "/roman/" in sl:
                score -= 10.0
            elif "/docs/" in sl:
                score += 2.0

        # Penalize prompt-template blocks. These often mention many relevant keywords but
        # are not the canonical "outcomes/policy/spec" sections users ask for.
        if _is_prompt_template_chunk(body):
            score -= 7.0

        # Reward explicit outcomes markers (kept separate from prompt templates).
        if _has_explicit_outcomes_marker(body) and not _is_prompt_template_chunk(body):
            score += 3.0

        # Reward keyword overlap.
        kws = _meaningful_keywords(q)
        hit = sum(1 for k in kws if k in bl)
        score += hit * 2.0
        url_hit = sum(1 for k in kws[:6] if k in sl)
        score += url_hit * 1.5

        # If a title phrase exists, prioritize docs that contain it.
        if title_phrase:
            tpl = title_phrase.lower()
            tpl_slug = _slugish(title_phrase)
            if tpl in bl:
                score += 10.0
            if tpl in sl:
                score += 16.0
            if tpl_slug:
                # Strong preference for the exact slug/root chapter page.
                if tpl_slug in bl.replace(" ", "-"):
                    score += 20.0
                if tpl_slug in sl:
                    score += 24.0
                if sl.rstrip("/").endswith(tpl_slug):
                    score += 20.0
            # Partial token overlap with title phrase.
            tks = [w for w in re.findall(r"[a-zA-Z]{4,}", tpl)][:8]
            t_hit = sum(1 for t in tks if t in bl)
            score += t_hit * 2.5
            t_url_hit = sum(1 for t in tks[:6] if t in sl)
            score += t_url_hit * 1.0

        # Concept-focused boost: "what is X / what does X ..."
        _focus = _extract_focus_phrase(q)
        _scope = _extract_scope_phrase(q)
        if _focus:
            _fl = _focus.lower()
            _f_tokens = [t for t in re.findall(r"[a-zA-Z0-9]{4,}", _fl) if t not in {"what", "does", "used", "for", "mean", "means"}][:8]
            if _fl in bl:
                score += 10.0
            if _fl in sl:
                score += 7.0
            if _f_tokens:
                _fh = sum(1 for t in _f_tokens if t in bl)
                _fh_url = sum(1 for t in _f_tokens if t in sl)
                score += (_fh * 1.8) + (_fh_url * 1.6)
        if _scope:
            _slp = _scope.lower()
            _s_tokens = [t for t in re.findall(r"[a-zA-Z0-9]{4,}", _slp) if t not in {"what", "does", "used", "for", "mean", "means"}][:8]
            if _slp in bl:
                score += 9.0
            if _slp in sl:
                score += 7.0
            if _s_tokens:
                _sh = sum(1 for t in _s_tokens if t in bl)
                _sh_url = sum(1 for t in _s_tokens if t in sl)
                score += (_sh * 1.5) + (_sh_url * 1.4)

        # Reward outcome-marker docs when the question is asking for goals/learning/outcomes.
        if _OUTCOMES_INTENT_RE.search(q or ''):
            if re.search(r"\b(by the end|you will be able to|learning outcomes|objectives|goals)\b", body, re.I):
                score += 6.0

        return score + _evidence_rerank_score(doc, q, title_phrase)
    except Exception:
        try:
            return _evidence_rerank_score(doc, q, title_phrase)
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
    src_status = str(meta.get("source_status") or "").strip().lower()
    if src_status in {"removed", "deleted"}:
        return False
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
        # Drop obvious site-navigation/UI dumps, but avoid over-dropping real docs
        # that contain a small nav fragment plus substantial content.
        tl = text.lower()
        _nav_hits = 0
        for _m in (
            "skip to main content",
            "toggle theme",
            "toggle menu",
            "copy as markdown",
            "sign in to ask",
            "sign in to access",
            "ctrl+",
            "on this page",
            "leaderboard",
        ):
            if _m in tl:
                _nav_hits += 1
        _alpha_chars = sum(1 for ch in text if ch.isalpha())
        _looks_substantial = (len(text) >= 700 and _alpha_chars >= 250)
        if _nav_hits >= 3 and (not _looks_substantial):
            return False
        if ("on this page" in tl and "copy as markdown" in tl and "ctrl+" in tl and not _looks_substantial):
            return False
        # Drop long navigation/index dumps that can look "substantial" by length alone.
        _chapter_part_mentions = tl.count("chapter ") + tl.count("part ")
        if (
            _chapter_part_mentions >= 12
            and "skip to main content" in tl
            and ("toggle menu" in tl or "search..." in tl)
        ):
            return False
        # Heuristic for menu-heavy documents: many short lines + UI hints + almost no prose.
        _lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if _lines:
            _short_lines = sum(1 for ln in _lines if len(ln) <= 48)
            _short_ratio = _short_lines / max(1, len(_lines))
            _sentence_marks = text.count(".") + text.count("!") + text.count("?")
            if _nav_hits >= 2 and _short_ratio >= 0.60 and _sentence_marks <= 4 and len(_lines) >= 14:
                return False
        sample = text[:200]
        if len(sample) > 20:
            # Drop control-char heavy chunks (these poison the LLM context and often
            # appear after a bad decode).
            ctrl = sum(1 for c in sample if (ord(c) < 32 and c not in ("\n", "\r", "\t")))
            if ctrl >= 2:
                return False
            # Drop chunks with lots of replacement/mojibake markers.
            rep = sample.count("\ufffd") + sample.count("ï¿½")
            if rep / max(1, len(sample)) > 0.02:
                return False
            printable_ratio = sum(1 for c in sample if c.isprintable()) / len(sample)
            if printable_ratio < 0.70:
                return False
    except Exception:
        pass
    return True


def _minimally_usable_doc(doc) -> bool:
    """Lenient fallback visibility check used only when strict filters produce zero docs."""
    try:
        txt = str(getattr(doc, "page_content", "") or "")
        if len(txt.strip()) < 80:
            return False
        sample = txt[:400]
        ctrl = sum(1 for c in sample if (ord(c) < 32 and c not in ("\n", "\r", "\t")))
        if ctrl >= 3:
            return False
        rep = sample.count("\ufffd") + sample.count("Ã¯Â¿Â½")
        if rep >= 4:
            return False
        return True
    except Exception:
        return False

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
        # Keep 2-3 char all-uppercase acronyms/brands (HP, LG, 3M) even though < 4 chars.
        if not ww or (len(ww) < 4 and not (ww.isupper() and len(ww) >= 2)):
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
            # Use original case (not term.lower()) — ChromaDB $contains is case-sensitive;
            # lowercasing "HP" to "hp" would miss chunks that store the brand as "HP".
            raw = db._collection.get(where_document={"$contains": term}, limit=k * 2)
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
    _cnt = 0
    _small_db_full_retrieval = False
    try:
        _cnt = db._collection.count() if db else 0
        logger.info(f"[RETRIEVE] db={'set' if db else 'None'}, chunks={_cnt}, q={q[:60]}")
        # Small product catalog: retrieve everything for complete recall.
        # Prevents non-deterministic answers caused by vector search missing products.
        # 500 products × ~200 chars = ~100K chars max — capped by context budget downstream.
        if _cnt > 0 and _cnt <= 500:
            k = _cnt
            _small_db_full_retrieval = True
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

    # Tight latency path for strict chapter/part questions:
    # keep retrieval focused and avoid broad expansion drift.
    # v10: \d{1,4}[A-Za-z]? handles suffixed chapters like "Chapter 21B"
    _strict_scope_q = bool(re.search(r"\b(?:chapter|part)\s+\d{1,4}[A-Za-z]?\b", q or "", re.I))
    if not _strict_scope_q and re.search(r"^\s*in\s+[^,\n]{6,200}\s*,\s*(?:what|why|how|which|who|when)\b", q or "", re.I):
        _strict_scope_q = True
    # Also treat "Title (N)" queries (e.g. "Voice Foundations (109)") as scoped.
    if not _strict_scope_q and re.search(r"\b[A-Z][A-Za-z\s]{3,60}\s*\(\s*\d{1,4}\s*\)", q or ""):
        _strict_scope_q = True
    # Pattern 4: "[question] in [Title Case]?" — title at end of query.
    if not _strict_scope_q and re.search(
        r"\bin\s+[A-Z][A-Za-z\-']+(?:\s+(?:[A-Z][A-Za-z\-']+|[a-z]{1,4})){1,8}\s*\??$",
        q or "",
    ):
        _strict_scope_q = True
    if _strict_scope_q:
        fast = True
        # Don't cap k for small-DB full-retrieval: queries like "available in Black?"
        # or "ship to Lahore?" trigger Pattern 4 but need the full catalog, not 18 chunks.
        if not _small_db_full_retrieval:
            k = min(k, 18)

    # 1. Start with hardcoded concept expansion (fast)
    search_queries = expand_query(q)

    _title_phrase = _extract_title_phrase(q)
    _title_slug = _slugish(_title_phrase or "")
    _scope_phrase = _extract_scope_phrase(q)
    _focus_phrase = _extract_focus_phrase(q)
    _is_outcomes_intent = bool(_OUTCOMES_INTENT_RE.search(q))
    if _title_phrase and all(_title_phrase.lower() != str(sq).lower() for sq in search_queries):
        search_queries.insert(0, _title_phrase)
    if _strict_scope_q and _scope_phrase:
        _combo_qs = [_scope_phrase]
        if _focus_phrase:
            _combo_qs.extend([
                f"{_scope_phrase} {_focus_phrase}",
                f"{_focus_phrase} {_scope_phrase}",
            ])
        for _cq in _combo_qs:
            _cq = re.sub(r"\s+", " ", (_cq or "").strip())
            if _cq and all(_cq.lower() != str(sq).lower() for sq in search_queries):
                search_queries.insert(0, _cq)

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
    search_queries = search_queries[: (3 if _strict_scope_q else 6)]

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

    # Title-phrase document rescue: when the query has an extracted title phrase (e.g. "Give It a Voice",
    # "Postgres as System of Record"), search document TEXT for that phrase to pull the actual page.
    # Uses where_document $contains (SQLite FTS, case-insensitive) instead of metadata substring match
    # (ChromaDB doesn't support $contains on string metadata fields).
    if not _skip_chromadb and _title_phrase and len(_title_phrase) >= 6 and db is not None:
        try:
            from langchain_core.documents import Document as _Doc
            _phrase_raw = db._collection.get(
                where_document={"$contains": _title_phrase},
                include=["documents", "metadatas"],
                limit=10,
            )
            for _sd, _sm in zip(_phrase_raw.get("documents", []), _phrase_raw.get("metadatas", [])):
                if _sd:
                    _sk = _sd[:100]
                    if _sk not in rescue_seen:
                        rescue_seen.add(_sk)
                        rescue_results.append(_Doc(page_content=_sd, metadata=_sm or {}))
        except Exception:
            pass

    # Outcomes rescue (universal): when user asks learning outcomes/goals, directly pull chunks likely to
    # contain outcomes lists and then filter by the title phrase tokens in URL/body.
    if _is_outcomes_intent and db is not None:
        try:
            from langchain_core.documents import Document
            _tpl = (_title_phrase or '').lower()
            _tks = [w for w in re.findall(r'[a-zA-Z]{4,}', _tpl)][:10]
            # "agent(s)" is too generic to be a reliable anchor token across most KBs.
            _tks = [t for t in _tks if t not in {"agent", "agents"}]
            # Handle alphanumeric chapter IDs like "21B" (digit + optional letter)
            _pc = re.search(r"\b(part|chapter)\s+(\d{1,3}[A-Za-z]?)\b", q, re.I)
            _pc_anchor = (f"{_pc.group(1).lower()} {_pc.group(2).lower()}" if _pc else "")
            # Broad net first (common outcomes markers), then we filter hard by anchor tokens.
            # This avoids DB-specific hardcoding while still reliably surfacing outcome bullets.
            # 1) Direct anchor phrase (highest precision when present).
            # This is universal (not DB-specific): if the user asked "Part 10" and a chunk contains
            # "By completing Part 10", grab it first.
            if _pc_anchor:
                _anchor_phrase = f"by completing {_pc_anchor}"
                # $contains is case-sensitive; add "By completing Part/Chapter N" variant
                _pc_proper = re.sub(r'\b(part|chapter|section)\b', lambda m: m.group(0).capitalize(), _pc_anchor)
                _anchor_proper = f"By completing {_pc_proper}"
                for _needle in (_anchor_phrase, _anchor_phrase.title(), _anchor_proper):
                    try:
                        raw2 = db._collection.get(where_document={"$contains": _needle}, limit=80)
                        for doc_text, meta in zip(raw2.get('documents', []), raw2.get('metadatas', [])):
                            if doc_text:
                                rescue_seen.add(doc_text[:100])
                                rescue_results.append(Document(page_content=doc_text, metadata=meta or {}))
                    except Exception:
                        pass

            # 2) Broad net (marker phrases), then filter hard by anchor tokens.
            # We intentionally include "goals" because some KB pages list outcomes as verb bullets
            # without the literal "you will..." phrase.
            for _marker in (
                "you will be able to", "You will be able to",
                "you should be able to", "You should be able to",
                "by the end of this chapter", "By the end of this chapter",
                "by chapter end", "By chapter end",
                "when you finish this chapter", "When you finish this chapter",
                "when you finish this", "When you finish this",
                "learning outcomes", "Learning Outcomes", "Learning outcomes",
                "by completing", "By completing",
                "goals", "Goals",
                "objectives", "Objectives",
            ):
                try:
                    raw = db._collection.get(where_document={"$contains": _marker}, limit=250)
                except Exception:
                    continue
                for doc_text, meta in zip(raw.get('documents', []), raw.get('metadatas', [])):
                    if not doc_text:
                        continue
                    src = str((meta or {}).get('source') or '').lower()
                    body = str(doc_text).lower()
                    # Only keep chunks that look like an outcomes list.
                    if not re.search(r"(?i)\b(by (the end of|completing|after completing|upon completing|chapter end)\b|you (will|should) be able to\b|when you finish this\b|learning outcomes\b|objectives?\b|goals?\b)", doc_text):
                        continue
                    # If the query names a specific Part/Chapter, require that exact anchor to appear somewhere
                    # in the chunk or its source URL (prevents extracting the wrong outcomes list).
                    # Exception: relax when title tokens provide sufficient anchor (handles chapter-number
                    # mismatches, e.g. user says "Chapter 22" but site calls it "Chapter 11").
                    if _pc_anchor and (_pc_anchor not in body) and (_pc_anchor not in src):
                        # When _tks is empty (no title phrase extracted), let the chunk through;
                        # the broader token filter below will handle further filtering.
                        if _tks and sum(1 for t in _tks if (t in src) or (t in body)) < 2:
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
    _meta_conds = []
    _required_flags = {}
    _ram_min_req = None
    _db_name_guess = str(getattr(db, "_db_name", "") or "")
    try:
        _is_product_db = bool(_check_is_product_db(db, _db_name_guess)) if db is not None else False
    except Exception:
        _is_product_db = False
    _pm = re.search(r'(?:under|below|less\s+than|max(?:imum)?|budget\s+of|within)\s+(?:rs\.?\s*)?[\$£€]?([\d,]+)', q, re.I)
    if _pm:
        try:
            _max_price = float(_pm.group(1).replace(',', ''))
        except Exception:
            _max_price = None
    try:
        _sample = db._collection.get(limit=5, include=["metadatas"]) if db else None
        if _sample and _sample.get("metadatas"):
            if any(m.get("price") is not None for m in _sample["metadatas"] if m):
                _has_product_meta = True
    except Exception as _pfe:
        logger.warning(f"[META-FILTER] Sample check failed: {_pfe}")

    if _has_product_meta:
        # Price
        if _pm:
            try:
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

    # Product budget rescue for catalog DBs whose prices live only in text.
    if _is_product_db and _max_price is not None and db is not None:
        try:
            from langchain_core.documents import Document
            total = min(int(db._collection.count() or 0), 8000)
            raw = db._collection.get(limit=total, include=["documents", "metadatas"])
            for doc_text, meta in zip(raw.get("documents", []), raw.get("metadatas", [])):
                if not doc_text:
                    continue
                body = str(doc_text)
                bl = body.lower()
                src = str((meta or {}).get("source") or "").lower()
                _chunk_ctype = str((meta or {}).get("content_type") or "")
                if not re.search(r"/(?:products?|items?)(?:/|$|#)", src) and _chunk_ctype != "product" and not ("add to cart" in bl or "shopping cart" in bl):
                    continue
                prices = []
                for raw_price in re.findall(r"Rs\.?\s*([\d,]+(?:\.\d{1,2})?)", body, re.I):
                    try:
                        val = float(raw_price.replace(",", ""))
                    except Exception:
                        continue
                    if val >= 50:
                        prices.append(val)
                if prices and min(prices) <= _max_price:
                    key = body[:100]
                    if key not in rescue_seen:
                        rescue_seen.add(key)
                        rescue_results.append(Document(page_content=body, metadata=meta or {}))
        except Exception:
            pass

    # Defaults so post-processing always has bound locals even when Chroma retrieval is skipped
    # (e.g., api-only DB mode, null DB, or retrieval disabled).
    policy_results: list = []
    _bm25_raw: list = []
    _bm25_title_raw: list = []
    _vector_raw: list = []
    _outcomes_title_results: list = []
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
        # For outcomes/goals questions, BM25 is often the only reliable way to land on
        # the exact "Goals ..." chunk (vector search drifts to part overviews).
        # Also prewarm for product DBs: BM25 handles exact brand/model name lookup that
        # vector search misses on first query before background prewarm completes.
        if (_is_outcomes_intent or _is_product_db) and _bm25_db_name and (_bm25_db_name not in _bm25_cache):
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: _get_bm25_index(db, _bm25_db_name)),
                    timeout=10
                )
            except Exception:
                pass
        # Explicit chapter/part numbers (if present) are useful anchors for both retrieval and extractors.
        chapter_no = _extract_chapter_number(q or "")
        part_no = _extract_part_number(q or "")

        _bm25_q = q
        if _is_outcomes_intent:
            try:
                _bm25_q = re.sub(
                    r'(?i)^\s*(?:what\s+are\s+the\s+(?:goals?|objectives?|learning\s+outcomes?)\s+of\s+|what\s+will\s+i\s+learn\s+from\s+)',
                    '',
                    _bm25_q or ''
                ).strip()
            except Exception:
                pass
            # If the user named an explicit Chapter/Part number, keep it in the BM25 query.
            # This sharply reduces "nearby chapter" false positives (universal).
            try:
                if chapter_no is not None and (f"chapter {chapter_no}".lower() not in (_bm25_q or "").lower()):
                    _bm25_q = f"Chapter {chapter_no} {_bm25_q}".strip()
                if part_no is not None and (f"part {part_no}".lower() not in (_bm25_q or "").lower()):
                    _bm25_q = f"Part {part_no} {_bm25_q}".strip()
            except Exception:
                pass
        _bm25_task = loop.run_in_executor(None, lambda: _bm25_search(_bm25_q, db, _bm25_db_name, k=k))
        _bm25_title_task = None
        if _is_outcomes_intent and _title_phrase:
            _tp = _title_phrase
            try:
                if chapter_no is not None and (f"chapter {chapter_no}".lower() not in _tp.lower()):
                    _tp = f"Chapter {chapter_no} {_tp}"
                if part_no is not None and (f"part {part_no}".lower() not in _tp.lower()):
                    _tp = f"Part {part_no} {_tp}"
            except Exception:
                pass
            _bm25_title_task = loop.run_in_executor(None, lambda: _bm25_search(_tp, db, _bm25_db_name, k=min(k, 25)))
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
        _outcomes_title_results: list = []
        # Fire policy search when: (a) query explicitly mentions policy terms, OR
        # (b) DB is a product/retail catalog (_has_product_meta) — covers "Deli Punch shipping?"
        # type queries where the user asks about a product but the answer lives in a policy chunk.
        if _POLICY_Q_RE.search(q):
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
                + ([_bm25_title_task] if _bm25_title_task else [])
                + ([_policy_task] if _policy_task else [])
                + ([_outcomes_title_task] if _outcomes_title_task else [])
            )
            _all_results = await asyncio.wait_for(
                asyncio.gather(*_gather_tasks, return_exceptions=True),
                timeout=35
            )
        except asyncio.TimeoutError:
            logger.warning("ChromaDB parallel search timed out")
            # Best-effort fallback: do a small title-only search synchronously so
            # outcomes questions still land on the right chapter/part page.
            _all_results = []
            try:
                if _is_outcomes_intent and _title_phrase:
                    _fallback_title = await asyncio.wait_for(
                        loop.run_in_executor(None, lambda: db.similarity_search(_title_phrase, k=8)),
                        timeout=8
                    )
                    for _r in (_fallback_title or []):
                        _pk = _r.page_content[:100]
                        if _pk not in seen:
                            seen.add(_pk)
                            _outcomes_title_results.append(_r)
            except Exception:
                pass
        # Split results: main_results are vector tasks; optional policy/outcomes-title tasks appended.
        _n_main = len(_search_tasks) + 1 + (1 if _bm25_title_task else 0)  # search tasks + bm25 + optional bm25(title)
        _main_results = _all_results[:_n_main]
        _policy_results_raw = _all_results[_n_main] if _policy_task and len(_all_results) > _n_main else []
        policy_results = []
        if not isinstance(_policy_results_raw, Exception):
            for _pr in _policy_results_raw:
                _pk = _pr.page_content[:100]
                if _pk not in seen:
                    seen.add(_pk)
                    policy_results.append(_pr)
        if (not _outcomes_title_results) and _outcomes_title_task:
            _idx = _n_main + (1 if _policy_task else 0)
            _raw = _all_results[_idx] if len(_all_results) > _idx else []
            if not isinstance(_raw, Exception):
                for _r in _raw:
                    _pk = _r.page_content[:100]
                    if _pk not in seen:
                        seen.add(_pk)
                        _outcomes_title_results.append(_r)
        # Combine in priority order: BM25 exact matches first, then rescue, then vector.
        # Dedup in order so highest-priority group wins. This ensures the BM25 best
        # match (e.g. "Marabu Chalky Chic") appears at position 0 in the context,
        # not buried after 20+ generic same-brand docs from rescue.
        _bm25_idx = len(_search_tasks)  # BM25 is immediately after vector tasks
        _bm25_result = _main_results[_bm25_idx] if len(_main_results) > _bm25_idx else []
        _bm25_raw = [] if isinstance(_bm25_result, Exception) else _bm25_result
        _bm25_title_raw = []
        if _bm25_title_task:
            _bmt_idx = _bm25_idx + 1
            _bmt_res = _main_results[_bmt_idx] if len(_main_results) > _bmt_idx else []
            _bm25_title_raw = [] if isinstance(_bmt_res, Exception) else _bmt_res
        _vector_raw = []
        _vector_exc_count = 0
        _vector_exc_sample = None
        for _vi, _vr in enumerate(_main_results):
            if _vi == _bm25_idx:
                continue
            if _bm25_title_task and _vi == (_bm25_idx + 1):
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
    # Outcomes-title results are a high-signal extra retrieval pass (title phrase only).
    # Include them early so the correct chapter/part page outranks broad overviews.
    for _grp in (policy_results, _bm25_raw, _bm25_title_raw, _outcomes_title_results, rescue_results, _vector_raw):
        for _r in _grp:
            if not _retrieval_visible_doc(_r):
                continue
            # Universal outcomes guard: for "goals/learning outcomes" queries, drop prompt-template chunks
            # (Try With AI / Prompt 1/2/3) unless they also contain an explicit outcomes marker. This prevents
            # generic prompt scaffolds from crowding out the actual outcomes/policy/spec sections.
            if _is_outcomes_intent:
                try:
                    _txt = str(getattr(_r, "page_content", "") or "")
                    _src_l = str((getattr(_r, "metadata", None) or {}).get("source") or "").lower()
                    _q_l = (q or "").lower()
                    _strict_scope = bool(re.search(r"\b(?:chapter|part)\s+\d{1,4}\b", _q_l))
                    if _strict_scope and "/docs/" not in _src_l:
                        continue
                    if _strict_scope and any(p in _src_l for p in ("/chapter-quiz", "/quiz", "/exercises", "/exercise", "/certifications", "/about")):
                        continue
                    if _is_prompt_template_chunk(_txt) and not _has_explicit_outcomes_marker(_txt):
                        continue
                    # Scoped chapter/part queries: require the same anchor in URL or text,
                    # but allow an exact title slug match as an equivalent anchor.
                    _ch = _extract_chapter_number(q or "")
                    _pt = _extract_part_number(q or "")
                    _txt_l = _txt.lower()
                    def _scope_url_match(src_l, txt_l, title_slug, title_phrase):
                        """True if the doc URL or text matches the title phrase (slug or tokens)."""
                        if title_slug and (title_slug in src_l or title_slug in txt_l):
                            return True
                        # Token fallback: require 2+ hits across URL + body combined.
                        # "employee"/"employees" excluded — too common across unrelated AF pages.
                        _stop = {"chapter","learning","goals","objectives","outcomes","agent","agents","employee","employees"}
                        _ttf = [w.lower() for w in re.findall(r"[a-zA-Z]{5,}", title_phrase or "")][:6]
                        _ttf = [t for t in _ttf if t not in _stop]
                        if not _ttf:
                            return False
                        _url_hits = sum(1 for t in _ttf if t in src_l)
                        _body_hits = sum(1 for t in _ttf if t in txt_l)
                        return (_url_hits + _body_hits) >= 2
                    if _ch is not None and ("chapter" in (q or "").lower()):
                        _a = f"chapter {_ch}"
                        if (_a not in _src_l) and (_a not in _txt_l):
                            if not _scope_url_match(_src_l, _txt_l, _title_slug, _title_phrase):
                                continue
                    if _pt is not None and ("part" in (q or "").lower()):
                        _a = f"part {_pt}"
                        if (_a not in _src_l) and (_a not in _txt_l):
                            if not _scope_url_match(_src_l, _txt_l, _title_slug, _title_phrase):
                                continue
                except Exception:
                    pass
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

    # Universal fallback: if strict visibility rules eliminate everything but retrieval returned docs,
    # keep minimally-usable chunks so answer generation can still ground on evidence.
    if not _combined_before_cap:
        _fallback_seen: set = set()
        for _grp in (policy_results, _bm25_raw, _bm25_title_raw, _outcomes_title_results, rescue_results, _vector_raw):
            for _r in _grp:
                if not _minimally_usable_doc(_r):
                    continue
                _k = str(getattr(_r, "page_content", "") or "")[:120]
                if _k in _fallback_seen:
                    continue
                _fallback_seen.add(_k)
                _combined_before_cap.append(_r)
        if _combined_before_cap:
            logger.warning(f"[RETRIEVE] strict filters yielded 0; fallback kept {len(_combined_before_cap)} docs")
        elif _strict_scope_q and db:
            # One lexical retry for strict chapter/part queries when all retrieval paths miss.
            try:
                _tp2 = _extract_title_phrase(q or "")
                _ch2 = _extract_chapter_number(q or "")
                _pt2 = _extract_part_number(q or "")
                _rq = (q or "").strip()
                if _tp2:
                    _rq = f"{_rq} {_tp2}".strip()
                if _ch2 is not None:
                    _rq += f" chapter {_ch2}"
                if _pt2 is not None:
                    _rq += f" part {_pt2}"
                _lex = db.similarity_search(_rq[:320], k=max(24, int(k)))
                _seen2 = set()
                for _r in (_lex or []):
                    if not _retrieval_visible_doc(_r):
                        continue
                    _k2 = str(getattr(_r, "page_content", "") or "")[:120]
                    if _k2 in _seen2:
                        continue
                    _seen2.add(_k2)
                    _combined_before_cap.append(_r)
                if _combined_before_cap:
                    logger.warning(f"[RETRIEVE] strict lexical retry recovered {len(_combined_before_cap)} docs")
            except Exception:
                pass
    

    # Heuristic rerank for non-product DBs: improves chapter/part/title lookup accuracy.
    if not (_has_product_meta or _is_product_db) and _combined_before_cap:
        _title_phrase = _extract_title_phrase(q)
        chapter_no = _extract_chapter_number(q)
        part_no = _extract_part_number(q)

        def _has_outcome_marker(d):
            try:
                b = str(getattr(d, "page_content", "") or "")
                # Prompt templates can still contain the real "what we'll learn" bullet list.
                # Count as outcome marker only when an explicit outcomes marker exists.
                if _is_prompt_template_chunk(b):
                    return _has_explicit_outcomes_marker(b)
                return _has_explicit_outcomes_marker(b)
            except Exception:
                return False

        # title_phrase filter: if query names a specific Chapter/Part title, prefer docs that actually match it.
        # Concrete issue observed in HF: generic outcomes phrases ("By the end of this chapter...") can retrieve
        # the wrong chapter's learning outcomes. For chapter/part queries we must constrain aggressively.
        if _title_phrase:
            _tpl = _title_phrase.lower()
            _tks = [w for w in re.findall(r"[a-zA-Z]{4,}", _tpl)][:10]
            _tslug = re.sub(r"[^a-z0-9]+", "-", _tpl).strip("-")

            def _title_match(d):
                try:
                    b = str(getattr(d, "page_content", "") or "").lower()
                    s = str((getattr(d, "metadata", None) or {}).get("source") or "").lower()
                    if _tpl and (_tpl in b or _tpl in s):
                        return True
                    # Slug-in-URL is a strong exact signal (e.g. "give-it-a-voice" in source path)
                    if _tslug and _tslug in s:
                        return True
                    if not _tks:
                        return False
                    # Require overlap across DISTINCT tokens in body and/or URL; URL often contains the slug.
                    hits = {t for t in _tks if (t in b or t in s)}
                    # Prevent generic matches (e.g. lots of pages contain "data"). Require 2+ distinct tokens.
                    if len(hits) < 2:
                        return False
                    # Additionally require at least one "anchor" token when available (longer, rarer tokens
                    # like "engineering", "tuning", "architecture", etc.). This prevents false positives
                    # where only generic words match.
                    anchors = [t for t in _tks if len(t) >= 7]
                    if anchors:
                        return any(a in hits for a in anchors)
                    # No long anchors + strict scope: short titles like "Give It a Voice" need slug match.
                    # Token match alone is too loose when all tokens are common short words.
                    if _strict_scope_q and _tslug:
                        return _tslug in s
                    return True
                except Exception:
                    return False

            _filtered = [d for d in _combined_before_cap if _title_match(d)]

            # If this is an outcomes intent (goals/learning outcomes) and the query names a specific title,
            # apply the filter even when it yields a small set. It's better to be precise than to mix in
            # unrelated chapters that happen to contain "By the end...".
            # For strict chapter/part queries, prefer the filtered set whenever it exists so siblings
            # don't leak into the final context.
            if (_is_outcomes_intent or _strict_scope_q) and len(_filtered) >= 1:
                _combined_before_cap = _filtered
            # For non-outcomes queries, keep the old safety valve to avoid empty/narrow context.
            elif len(_filtered) >= 6:
                _combined_before_cap = _filtered

        # Universal strict-scope gate: for chapter/part queries, discard chunks
        # that do not carry matching scope anchors.
        if _strict_scope_q:
            _scoped = [d for d in _combined_before_cap if _doc_matches_strict_scope(d, q, _title_phrase)]
            if _scoped:
                _combined_before_cap = _scoped

        # If the exact outcomes page was still not surfaced, do one bounded
        # collection scan for the same section cluster. This is the universal
        # rescue path for dead root URLs and split chapter pages.
        _resc_best_sk: str | None = None
        if _is_outcomes_intent and _strict_scope_q and _title_phrase and db:
            try:
                _title_slug2 = _slugish(_title_phrase or "")
                _have_anchor = False
                if _title_slug2:
                    for _d in _combined_before_cap[:12]:
                        _src = str((getattr(_d, "metadata", None) or {}).get("source") or "").lower()
                        _txt = str(getattr(_d, "page_content", "") or "").lower()
                        if _title_slug2 in _src or _title_slug2 in _txt:
                            _have_anchor = True
                            break
                if not _have_anchor:
                    def _scan_cluster_rescue():
                        from langchain_core.documents import Document
                        # v10: increased from 2000; goals chunks for some chapters land at pos 2154-2191
                        _total = min(int(db._collection.count() or 0), 3000)
                        _raw = db._collection.get(limit=_total, include=["documents", "metadatas"])
                        _resc_docs = []
                        # v10: allow up to 3 chunks per source URL; goals pages split across
                        # chunk boundaries (preamble chunk N, question-list in chunk N+2, etc.)
                        _resc_src_count: dict = {}
                        _title_tks = [w.lower() for w in re.findall(r"[a-zA-Z]{4,}", _title_phrase or "")][:10]
                        # "your"/"agent"/"agents" excluded: too common across AF pages, causes false positives
                        _title_tks = [t for t in _title_tks if t not in {"chapter", "part", "learning", "goals", "outcomes", "according", "book", "your", "agent", "agents"}]
                        for _doc_text, _meta in zip(_raw.get("documents", []), _raw.get("metadatas", [])):
                            if not _doc_text:
                                continue
                            _src = str((_meta or {}).get("source") or "").lower()
                            _body = str(_doc_text).lower()
                            if _title_slug2 and (_title_slug2 not in _src) and (_title_slug2 not in _body):
                                # Slug mismatch (e.g. "linux-mastery" vs "linux-operations-for-agent-deployment"):
                                # fall back to requiring 2+ title token hits across URL + body.
                                # v10: 1 hit is enough when: strong marker AND chapter number anchors the page.
                                if not _title_tks:
                                    continue
                                _hit = sum(1 for t in _title_tks if (t in _src) or (t in _body))
                                if _hit < 1:
                                    continue
                                if _hit < 2:
                                    _ch_anchor = (f"chapter {chapter_no}" if chapter_no is not None else "")
                                    if not (_has_explicit_outcomes_marker(_body) and _ch_anchor and _ch_anchor in _body):
                                        continue
                            elif _title_tks:
                                _hit = sum(1 for t in _title_tks if (t in _src) or (t in _body))
                                if _hit < 2:
                                    continue
                            if not (_has_explicit_outcomes_marker(_body) or re.search(
                                r"\b(by completing|by the end|you will be able to|you should be able to"
                                r"|by chapter end|when you finish this|learning outcomes|objectives?|goals?"
                                # v10: project-chapter goals ("Chapter 24 Build Your AI Employee")
                                r"|this is a project|you will build|you'll build|now build something real)\b", _body)):
                                continue
                            _doc = Document(page_content=_doc_text, metadata=_meta or {})
                            _src_key = str((_meta or {}).get("source") or _doc_text[:120])[:160]
                            if _resc_src_count.get(_src_key, 0) >= 3:
                                continue
                            _resc_src_count[_src_key] = _resc_src_count.get(_src_key, 0) + 1
                            _resc_docs.append(_doc)
                        # v11: identify the best chapter-anchored rescue source for section-lock override
                        _src_best_scores: dict = {}
                        for _rd in _resc_docs:
                            _sk2 = _source_section_key(_rd) or ""
                            if not _sk2:
                                continue
                            _b2 = _rd.page_content.lower()
                            _s2 = str((_rd.metadata or {}).get("source", "")).lower()
                            _h2 = sum(1 for t in _title_tks if (t in _s2) or (t in _b2))
                            _ch2 = bool(chapter_no is not None and f"chapter {chapter_no}" in _b2)
                            _sc2 = _h2 + int(_ch2) * 2
                            if _sc2 > _src_best_scores.get(_sk2, 0):
                                _src_best_scores[_sk2] = _sc2
                        _best_sk2 = None
                        if _src_best_scores:
                            _bk, _bv = max(_src_best_scores.items(), key=lambda kv: kv[1])
                            if _bv >= 2:
                                _best_sk2 = _bk
                        return _resc_docs, _best_sk2
                    _resc_docs, _resc_best_sk = await asyncio.wait_for(asyncio.to_thread(_scan_cluster_rescue), timeout=8)
                    if _resc_docs:
                        _combined_before_cap = _resc_docs[:24] + _combined_before_cap
            except Exception:
                pass

        # Baseline rerank after optional filtering
        _combined_before_cap.sort(key=lambda d: _heuristic_rerank_score(d, q, _title_phrase), reverse=True)

        # v11: If rescue found a chapter-anchored source, force it to lead before section lock fires.
        # Uses startswith so multi-lesson chapters (e.g. build-first-ai-employee/project-brief)
        # are all captured, not just the single page that scored highest.
        if _resc_best_sk:
            _resc_lead = [d for d in _combined_before_cap
                          if ((_source_section_key(d) or "") == _resc_best_sk
                              or (_source_section_key(d) or "").startswith(_resc_best_sk + "/"))]
            _resc_tail = [d for d in _combined_before_cap
                          if not ((_source_section_key(d) or "") == _resc_best_sk
                                  or (_source_section_key(d) or "").startswith(_resc_best_sk + "/"))]
            if len(_resc_lead) >= 2:
                _combined_before_cap = _resc_lead + _resc_tail

        # Section lock for strict scoped lookups (Chapter/Part): keep context mostly from
        # the strongest page/section to avoid mixed nearby-chapter contamination.
        if _strict_scope_q and _combined_before_cap:
            _top_key = _source_section_key(_combined_before_cap[0])
            if _top_key:
                _same = [d for d in _combined_before_cap if _source_section_key(d) == _top_key]
                _other = [d for d in _combined_before_cap if _source_section_key(d) != _top_key]
                if len(_same) >= 2:
                    _combined_before_cap = _same[:28] + _other[:6]

        # outcomes-marker bubble: if user asks learning outcomes/goals, put explicit outcomes chunks first
        # (but only within the already title-constrained set when title_phrase was present).
        if _is_outcomes_intent:
            _combined_before_cap.sort(
                key=lambda d: (1 if _has_outcome_marker(d) else 0, _heuristic_rerank_score(d, q, _title_phrase)),
                reverse=True,
            )
    if (_has_product_meta or _is_product_db) and (_required_flags or _ram_min_req is not None or _max_price is not None):
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
        # Small DBs: let the 24000-char context budget do the truncation naturally.
        # The hard :40 cap was designed for large corpora and drops valid products in pos 41–148.
        _top_cap = (min(_cnt, 100) if _small_db_full_retrieval else 40)
        top = _post_filtered[:_top_cap]
        logger.info(f"[META-FILTER] post-filter: {len(top)} chunks remain")
    else:
        _top_cap = (min(_cnt, 100) if _small_db_full_retrieval else 40)
        top = _combined_before_cap[:_top_cap]

    # Safe defaults for later product-ranking branches.
    # These must exist even when the product-intent block does not run,
    # otherwise downstream guards can raise UnboundLocalError on non-price queries.
    _is_price_rank_q, _price_desc = _is_price_ranking_query(q or "")
    _price_bounds = _parse_price_bounds(q or "")
    _is_review_rank_q = _is_review_ranking_query(q or "")

    # Product-intent guard: favor actual catalog pages over blogs/about pages.
    # Universal behavior for ecommerce-like DBs.
    if (_has_product_meta or _is_product_db) and top:
        _q_l = (q or "").lower()
        _product_intent = bool(re.search(
            r"\b(price|cost|how much|buy|available|availability|in stock|stock|show|list|best|top|affordable|under\s+\d+|pen|pencil|notebook|book|stroller|diaper|product|products)\b",
            _q_l,
            re.I
        ))
        if _product_intent:
            _catalog_docs = []
            _other_docs = []
            for _r in top:
                _src = str((getattr(_r, "metadata", None) or {}).get("source") or "").lower()
                if re.search(r"/(?:products?|items?)(?:/|$|#)", _src) or ("/collections/" in _src):
                    _catalog_docs.append(_r)
                elif any(x in _src for x in ("/pages/", "/blogs/", "/about", "/contact")):
                    _other_docs.append(_r)
                else:
                    _other_docs.append(_r)
            if _catalog_docs:
                # Keep mostly catalog docs first; retain a small tail of others for edge cases.
                top = _catalog_docs[:36] + _other_docs[:4]
            # Query-anchor pass: keep docs that match concrete product words in query.
            _stop = {
                "show", "list", "best", "top", "affordable", "price", "prices", "cost", "available",
                "availability", "stock", "under", "with", "for", "products", "product", "have", "give",
                "school", "use",
                # Question/filler words are not product anchors — they inflate the
                # required hit count and make short-titled product lookups fail.
                "what", "when", "where", "which", "does", "much", "many", "this", "that",
                "your", "sell", "sells", "selling", "store", "shop", "site", "item", "items",
                "tell", "about", "know", "want", "need", "looking", "there", "they", "will",
            }
            _anchors = [w.lower() for w in re.findall(r"[a-zA-Z]{4,}", _q_l) if w.lower() not in _stop][:8]
            if _anchors:
                _matched = []
                _rest = []
                _modelish_anchor = any(re.search(r"\d|[-/]", a) for a in _anchors)
                for _r in top:
                    _rmd = getattr(_r, "metadata", None) or {}
                    _src = str(_rmd.get("source") or "").lower()
                    # Crawl-graph category names — product bodies rarely contain their
                    # own category word ("laptop"), the categories meta does.
                    _cat = str(_rmd.get("categories") or "").lower()
                    _txt = str(getattr(_r, "page_content", "") or "").lower()[:1400]
                    _hits = 0
                    for _a in _anchors:
                        _a2 = _a[:-1] if _a.endswith("s") else _a
                        if (_a in _src) or (_a2 and _a2 in _src) or (_a in _txt) or (_a2 and _a2 in _txt) or (_cat and (_a in _cat or (_a2 and _a2 in _cat))):
                            _hits += 1
                    # 1 anchor must require 1 hit — requiring 2 made short-titled
                    # products ("Into the Wild" → only anchor "wild") unmatchable.
                    _req_hits = min(len(_anchors), 2) if len(_anchors) <= 2 else (len(_anchors) if _modelish_anchor else max(2, len(_anchors) - 1))
                    _ok = _hits >= _req_hits
                    (_matched if _ok else _rest).append(_r)
                if _matched:
                    # For exact product lookups, keep the matched docs front-and-center.
                    if len(_anchors) >= 2:
                        top = _matched[:40]
                    else:
                        top = _matched[:34] + _rest[:6]
                # Exact-product rescue: scan the full catalog so model-number lookups
                # do not get trapped by the top-N retrieval window.
                if db is not None and not _is_price_rank_q and len(_anchors) >= 1:
                    try:
                        from langchain_core.documents import Document
                        total = min(int(db._collection.count() or 0), 8000)
                        raw = db._collection.get(limit=total, include=["documents", "metadatas"])
                        _rescued: list[tuple[int, Document]] = []
                        for doc_text, meta in zip(raw.get("documents", []), raw.get("metadatas", [])):
                            if not doc_text:
                                continue
                            _body = str(doc_text)
                            _src = str((meta or {}).get("source") or "").lower()
                            _cat = str((meta or {}).get("categories") or "").lower()
                            if not re.search(r"/(?:products?|items?)(?:/|$|#)", _src) and not (_has_product_meta or _is_product_db):
                                continue
                            _hits = 0
                            for _a in _anchors:
                                _a2 = _a[:-1] if _a.endswith("s") else _a
                                if (_a in _src) or (_a2 and _a2 in _src) or (_a in _body.lower()) or (_a2 and _a2 in _body.lower()) or (_cat and (_a in _cat or (_a2 and _a2 in _cat))):
                                    _hits += 1
                            if _hits >= _req_hits:
                                _rescued.append((_hits, Document(page_content=_body, metadata=meta or {})))
                        if _rescued:
                            _rescued.sort(
                                key=lambda t: (
                                    t[0],
                                    _doc_price_value(t[1]) if _doc_price_value(t[1]) is not None else -1.0,
                                ),
                                reverse=True,
                            )
                            top = [r for _, r in _rescued[:40]] + top[:6]
                    except Exception:
                        pass

    # ── Product catalog: dedup + GPU ranking ─────────────────────────────────
    if _has_product_meta or _is_product_db:
        # Dedup: same product+price should appear only once
        _dedup_seen, _deduped = set(), []
        for r in top:
            _pk = (str(r.metadata.get('price', '')) + r.page_content[:60]) if r.metadata else r.page_content[:80]
            if _pk not in _dedup_seen:
                _dedup_seen.add(_pk)
                _deduped.append(r)
        top = _deduped
        if _is_price_rank_q or _price_bounds or _is_review_rank_q:
            # Full-catalog deterministic scan: min/max, price-range ("between
            # $100 and $200", "under Rs.5000") and review-rank ("most reviews")
            # answers are rarely inside the similarity top-N window. Scan ALL
            # product chunks and rank/filter numerically instead.
            _rank_scan_ordered = False
            if db is not None:
                try:
                    from langchain_core.documents import Document as _PriceDoc
                    _pr_total = min(int(db._collection.count() or 0), 8000)
                    _pr_raw = db._collection.get(limit=_pr_total, include=["documents", "metadatas"])
                    _pr_stop = {
                        "cheapest", "lowest", "least", "expensive", "most", "highest", "priciest",
                        "costliest", "costly", "price", "priced", "prices", "affordable", "budget",
                        "what", "which", "your", "store", "site", "sell", "have", "entire",
                        "overall", "product", "products", "item", "items", "whats",
                        "under", "below", "between", "over", "above", "than", "less", "more",
                        "cost", "costs", "review", "reviews", "reviewed", "rated", "rating",
                        "popular", "customer", "customers", "best", "from", "with", "does",
                        # Catalog-scope words ("ABSOLUTE cheapest", "SECOND most expensive",
                        # "do you CARRY") — anchoring on them hands the ranking to whatever
                        # docs happen to contain them.
                        "absolute", "absolutely", "whole", "second", "third", "carry",
                        "offer", "offers", "offered", "currently", "right", "anything",
                        "everything", "catalog", "catalogue", "inventory", "selection",
                    }
                    _pr_anchors = [w for w in re.findall(r"[a-zA-Z]{4,}", (q or "").lower()) if w not in _pr_stop][:4]
                    # Short category tokens ("RC", "LED", "USB") are real anchors but the
                    # {4,} regex drops them. Match them word-bounded — substring matching
                    # on 2-char tokens hits random words ("rc" in "march").
                    _pr_short_stop = {
                        "rs", "pkr", "usd", "the", "and", "for", "you", "are", "can", "get",
                        "buy", "our", "has", "had", "was", "not", "but", "its", "any", "all",
                        "how", "who", "why", "per", "via", "etc", "new", "or", "do", "of",
                        "in", "on", "at", "to", "is", "we", "us", "my", "no", "so", "if",
                        "it", "an", "as", "be", "by", "up", "me",
                    }
                    _pr_anchors_short = [
                        w for w in re.findall(r"\b[a-zA-Z]{2,3}\b", (q or "").lower())
                        if w not in _pr_short_stop and w not in _pr_stop
                    ][:3]

                    def _pr_anchor_match(_bl: str, _src_l: str, _cat_l: str = "") -> int:
                        """Count anchor hits. Substring matching is deliberately loose
                        ("laptops"→"laptop"), so a single hit is weak evidence: "pens"
                        matches "sharPENer"/"PENcil" too. Callers sort by hit count so
                        multi-anchor matches ("gel"+"pens") outrank incidental ones."""
                        _h = 0
                        for _a in _pr_anchors:
                            if (_a in _bl) or (_a.rstrip("s") in _bl) or (_a in _src_l) or (
                                _cat_l and ((_a in _cat_l) or (_a.rstrip("s") in _cat_l))
                            ):
                                _h += 1
                        for _a in _pr_anchors_short:
                            if re.search(rf"\b{re.escape(_a)}\b", _bl) or re.search(rf"\b{re.escape(_a)}\b", _src_l) or (
                                _cat_l and re.search(rf"\b{re.escape(_a)}\b", _cat_l)
                            ):
                                _h += 1
                        return _h
                    _pr_all, _pr_anchored, _rv_all = [], [], []
                    _pr_hits_by_id: dict[int, int] = {}
                    for _pd_text, _pd_meta in zip(_pr_raw.get("documents", []), _pr_raw.get("metadatas", [])):
                        if not _pd_text:
                            continue
                        _pd_meta = _pd_meta or {}
                        _src_l = str(_pd_meta.get("source") or "").lower()
                        _is_prod_chunk = (
                            _pd_meta.get("price") is not None
                            or _pd_meta.get("chunk_kind") == "product"
                            or _pd_meta.get("content_type") == "product"
                            or re.search(r"/(?:products?|items?)(?:/|$|#)", _src_l)
                        )
                        if not _is_prod_chunk:
                            continue
                        _pd = _PriceDoc(page_content=str(_pd_text), metadata=_pd_meta)
                        if _is_review_rank_q:
                            _rv = _doc_review_count(_pd)
                            if _rv is not None:
                                _rv_all.append((_rv, _pd))
                        _pv = _doc_price_value(_pd)
                        if _pv is None or _pv <= 0:  # £0.00 = extraction artifact, never a real price
                            continue
                        if _price_bounds:
                            _lo, _hi = _price_bounds
                            if (_lo is not None and _pv < _lo) or (_hi is not None and _pv > _hi):
                                continue  # outside the requested range
                        _pr_all.append((_pv, _pd))
                        if _pr_anchors or _pr_anchors_short:
                            _cat_l = str(_pd_meta.get("categories") or "").lower()
                            _h = _pr_anchor_match(str(_pd_text).lower(), _src_l, _cat_l)
                            if _h:
                                _pr_anchored.append((_pv, _pd))
                                _pr_hits_by_id[id(_pd)] = _h

                    def _dedup_by_title(_pairs, _cap):
                        _seen, _out = set(), []
                        for _v, _pd in _pairs:
                            _tt = str((_pd.metadata or {}).get("canonical_product_title")
                                      or (_pd.metadata or {}).get("product_title")
                                      or _pd.page_content[:60])
                            if _tt in _seen:
                                continue
                            _seen.add(_tt)
                            _out.append(_pd)
                            if len(_out) >= _cap:
                                break
                        return _out

                    if _is_review_rank_q and _rv_all:
                        # Review ranking: deterministic, descending by count.
                        _rv_all.sort(key=lambda t: t[0], reverse=True)
                        top = _dedup_by_title(_rv_all, 20) + top[:6]
                    elif _price_bounds:
                        # Range filter: ONLY in-range items, ascending. No similarity
                        # tail — it reintroduces out-of-range items the LLM then lists.
                        # Anchored items go FIRST regardless of count: with a category
                        # word ("RC … under Rs.5000") the matching items can sit past
                        # the 30-cap in a cheap-heavy catalog and vanish from context.
                        _pr_all.sort(key=lambda t: t[0])
                        # Hits-desc before price-asc: substring anchors are loose
                        # ("pens" hits "pencil" too) — docs matching MORE anchors
                        # ("gel"+"pens") must precede single incidental hits, or a
                        # cheap-heavy catalog floods the 30-cap with off-category
                        # items before the real ones.
                        _pr_anchored.sort(key=lambda t: (-_pr_hits_by_id.get(id(t[1]), 0), t[0]))
                        _anch_ids = {id(_pd) for _pv, _pd in _pr_anchored}
                        _pr_cands = _pr_anchored + [t for t in _pr_all if id(t[1]) not in _anch_ids]
                        if _pr_cands:
                            top = _dedup_by_title(_pr_cands, 30)
                    elif _is_price_rank_q:
                        # Category-anchored subset when it exists ("cheapest tablet" → tablet
                        # chunks); otherwise whole catalog so the LLM can still pick.
                        # Threshold of 3: category words ("laptop") often appear only in a
                        # listing-page URL, and a single matching catalog chunk would hijack
                        # the ranking with its own arbitrary first price.
                        _pr_cands = _pr_anchored if len(_pr_anchored) >= 3 else _pr_all
                        # Anchor words match free text incidentally — the catalog's true
                        # max/min item may not mention its own category ("ROG Strix SCAR"
                        # never says "laptop"). Always merge the global extremes in the
                        # requested direction; the LLM filters category from the blend.
                        if _pr_cands is _pr_anchored and _pr_all:
                            _pr_all.sort(key=lambda t: t[0], reverse=_price_desc)
                            _anchored_ids = {id(_pd) for _pv, _pd in _pr_anchored}
                            _ext_tail = [t for t in _pr_all[:5] if id(t[1]) not in _anchored_ids]
                            # Anchored category docs FIRST, global extremes appended —
                            # a price re-sort of the blend puts Rs.981,000 pools at the
                            # context head and small-budget providers truncate the
                            # actual category ("second most expensive easel") away.
                            _pr_anchored.sort(key=lambda t: t[0], reverse=_price_desc)
                            _pr_cands = list(_pr_anchored) + _ext_tail
                            top = _dedup_by_title(_pr_cands, 30) + top[:6]
                            _rank_scan_ordered = True
                        elif _pr_cands:
                            _pr_cands.sort(key=lambda t: t[0], reverse=_price_desc)
                            top = _dedup_by_title(_pr_cands, 30) + top[:6]
                            _rank_scan_ordered = True
                except Exception as _pr_e:
                    logger.warning(f"[PRICE-RANK] full-catalog scan failed: {_pr_e}")
            # Review order must not be re-sorted by price; bounds order must keep
            # anchored (category-matching) items FIRST — providers with small
            # context budgets truncate the tail, and an ascending re-sort puts
            # cheap off-category items at the head ("gel pens under Rs.300" →
            # context head full of erasers, gel pens truncated away).
            # The rank scan above already ordered top (anchored-first) — a price
            # re-sort here would scramble it back to global-extremes-first.
            if not _is_review_rank_q and not _price_bounds and not _rank_scan_ordered:
                _priced, _unpriced = [], []
                for r in top:
                    _pv = _doc_price_value(r)
                    if _pv is None or _pv <= 0:  # zero/absent price can't participate in ranking
                        _unpriced.append(r)
                    else:
                        _priced.append((_pv, r))
                if _priced:
                    _priced.sort(key=lambda t: t[0], reverse=_price_desc)
                    top = [r for _, r in _priced[:40]] + _unpriced[:6]
        # Count/category questions: category & site-index chunks carry authoritative
        # numbers ("11 results", "21 items") — inject from the full catalog, since
        # similarity rarely surfaces a category page for "how many X".
        _cat_count_q = bool(re.search(r'(?i)\bhow\s+many\b|\bcategor(?:y|ies)\b|\btypes?\s+of\b|\bkinds?\s+of\b', q or ''))
        if _cat_count_q and db is not None:
            try:
                from langchain_core.documents import Document as _CatDoc
                _ct_total = min(int(db._collection.count() or 0), 8000)
                _ct_raw = db._collection.get(limit=_ct_total, include=["documents", "metadatas"])
                _ct_stop = {"many", "category", "categories", "books", "products", "items", "type",
                            "types", "kind", "kinds", "have", "your", "sell", "what", "which",
                            "total", "different", "store", "catalog", "does", "carry"}
                _ct_anchors = [w for w in re.findall(r"[a-zA-Z]{4,}", (q or "").lower()) if w not in _ct_stop][:4]
                _ct_hit, _ct_rest = [], []
                for _cd_text, _cd_meta in zip(_ct_raw.get("documents", []), _ct_raw.get("metadatas", [])):
                    if not _cd_text:
                        continue
                    _cd_meta = _cd_meta or {}
                    _ctype = str(_cd_meta.get("content_type") or "")
                    _csrc = str(_cd_meta.get("source") or "").lower()
                    if _ctype not in ("category", "catalog") and "#site-index" not in _csrc and "#site-navigation" not in _csrc:
                        continue
                    _cdoc = _CatDoc(page_content=str(_cd_text), metadata=_cd_meta)
                    _cl = str(_cd_text).lower()
                    if _ct_anchors and any((a in _cl) or (a.rstrip("s") in _cl) or (a in _csrc) for a in _ct_anchors):
                        _ct_hit.append(_cdoc)
                    else:
                        _ct_rest.append(_cdoc)
                _ct_pick = (_ct_hit[:6] + _ct_rest[:2]) if _ct_hit else _ct_rest[:6]
                if _ct_pick:
                    top = _ct_pick + top[:10]
            except Exception as _ct_e:
                logger.warning(f"[CAT-COUNT] category injection failed: {_ct_e}")
        # For "best gaming / highest VRAM" queries: sort by GPU VRAM descending in Python
        if re.search(r'\bbest\b.*\bgaming\b|\bhighest\b.*\b(?:gpu|vram)\b|\bmost\s+powerful\b', q, re.I):
            top.sort(key=lambda r: r.metadata.get('gpu_vram_gb', 0) if r.metadata else 0, reverse=True)
    # Product-title rerank: exact product pages should outrank same-brand neighbors.
    # Previously only fired for price/size queries — extended to ALL product queries so
    # name-only lookups like "Pop Up Animals Toy" also land on the exact product page.
    _has_productish_sources = any(
        (
            re.search(r"/(?:products?|items?)(?:/|$|#)", str(((getattr(r, "metadata", None) or {}).get("source")) or "").lower())
            or (getattr(r, "metadata", None) or {}).get("content_type") == "product"
        )
        for r in top[:20]
    )
    # Bounds queries ("laptops under $300") get the same exemption as ranking
    # queries: the full-catalog scan ordered top deterministically (anchored
    # in-range items first, ascending) — a title-similarity sort scrambles it
    # and the cheap off-category head wins the context window.
    if _has_productish_sources and not _is_price_ranking_query(q or "")[0] and not _parse_price_bounds(q or ""):
        top.sort(key=lambda r: _product_query_rerank_score(q, r), reverse=True)

    # Language-aware source preference:
    # For English queries, avoid mixing romanized/transliterated duplicates when
    # an equivalent non-roman page is present in the candidate set.
    try:
        _q_is_englishish = (not _is_urdu_script(q or "")) and bool(re.search(r"[A-Za-z]", q or ""))
        if _q_is_englishish and top:
            def _canon_src(_u: str) -> str:
                _u = str(_u or "")
                _u = _u.replace("/roman/docs/", "/docs/")
                _u = _u.replace("/roman/", "/")
                return _u
            _picked = {}
            _order = []
            for _r in top:
                _src = str((getattr(_r, "metadata", None) or {}).get("source") or "")
                _key = _canon_src(_src)
                _is_roman = ("/roman/" in _src)
                if _key not in _picked:
                    _picked[_key] = _r
                    _order.append(_key)
                    continue
                _prev = _picked[_key]
                _prev_src = str((getattr(_prev, "metadata", None) or {}).get("source") or "")
                _prev_is_roman = ("/roman/" in _prev_src)
                if _prev_is_roman and not _is_roman:
                    _picked[_key] = _r
            _top2 = [_picked[k] for k in _order if k in _picked]
            if _top2:
                top = _top2
            # If English query has both roman and non-roman candidates, keep non-roman for cleaner context.
            _non_roman = [r for r in top if "/roman/" not in str(((getattr(r, "metadata", None) or {}).get("source")) or "").lower()]
            if _non_roman:
                top = _non_roman
    except Exception:
        pass
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
    # Context cap/compression: keep only the highest-signal slices so unrelated chunks
    # can't dominate the prompt. This improves stability across all DBs.
    MAX_CONTEXT_CHARS = 24000
    MAX_DOC_CHARS = 1800

    def _compress_doc(text: str, anchors: list[str]) -> str:
        if not text:
            return ""
        t = _clean_text(text)
        if len(t) <= MAX_DOC_CHARS:
            return t
        if not anchors:
            return t[:MAX_DOC_CHARS]
        # Line-based contextual compression: keep lines with anchor hits plus neighbors.
        lines = [ln.strip() for ln in t.split("\n")]
        keep = set()
        al = [a.lower() for a in anchors if a]
        for i, ln in enumerate(lines):
            ll = ln.lower()
            if any(a in ll for a in al):
                for j in range(max(0, i - 1), min(len(lines), i + 2)):
                    keep.add(j)
        if keep:
            out = "\n".join(lines[i] for i in sorted(keep) if lines[i])
            return out[:MAX_DOC_CHARS] if out else t[:MAX_DOC_CHARS]
        return t[:MAX_DOC_CHARS]

    anchors = _meaningful_keywords(q)
    context_parts = []
    cur_chars = 0
    for r in top[:k]:
        raw_text = getattr(r, "page_content", "") or ""
        if _is_binary_chunk(raw_text):
            continue
        chunk = _compress_doc(raw_text, anchors)
        if not chunk:
            continue
        _r_cats = str((getattr(r, "metadata", None) or {}).get("categories") or "").strip()
        if _r_cats:
            # Crawl-graph categories made visible in context TEXT: product bodies
            # never contain their own category word ("laptop"), and the sparse-KB
            # guard, the LLM, and the deterministic parser all read text — without
            # this line a perfectly-anchored bounds answer still exits as IDK.
            chunk = f"Category: {_r_cats}\n{chunk}"
        if _strict_scope_q or _is_outcomes_intent:
            src = str((getattr(r, "metadata", None) or {}).get("source") or "").strip()
            if src:
                chunk = f"[SOURCE] {src}\n{chunk}"
        if cur_chars + len(chunk) + 2 > MAX_CONTEXT_CHARS:
            remain = max(0, MAX_CONTEXT_CHARS - cur_chars - 2)
            if remain <= 200:
                break
            context_parts.append(chunk[:remain])
            break
        context_parts.append(chunk)
        cur_chars += len(chunk) + 2
    context = "\n\n".join(context_parts)

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
    _season_count_q = re.search(
        r'(?i)\b(?:how many|what is the number of|number of)\s+seasons?\b|\bseason count\b|\bhow many seasons does\b',
        q_lower,
    )
    if _season_count_q:
        _season_answer = _fast_format_jikan_season_count(context, q)
        if _season_answer:
            return _season_answer

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


def _fast_format_jikan_season_count(context: str, q: str) -> str | None:
    """Count anime seasons from live Jikan search results already present in context."""
    try:
        q_norm = re.sub(r'\s+', ' ', q.strip())
        m = re.search(
            r'(?i)^\s*(?:how many|what is the number of|number of)\s+seasons?\s+'
            r'(?:does|did|has|have|is|are)?\s*(.+?)\s*'
            r'(?:have|has|had|does|did|is|are)?\s*\??$',
            q_norm,
        )
        subject = m.group(1).strip() if m else ""
        subject = re.sub(r'(?i)\b(?:according to|from|in|the book|book)\b.*$', '', subject).strip(" .,!?:;")
        if not subject:
            return None

        def _norm(text: str) -> str:
            return re.sub(r'[^a-z0-9]+', ' ', (text or "").lower()).strip()

        subject_norm = _norm(subject)
        if not subject_norm:
            return None

        # Only parse the anime-search live section. This avoids counting ranking/genre lists.
        lines = [ln.strip() for ln in context.splitlines() if ln.strip()]
        search_lines: list[str] = []
        in_anime_search = False
        for line in lines:
            if line.startswith("[Live data from "):
                src_name = line[len("[Live data from "):-1].lower()
                in_anime_search = "anime search" in src_name
                continue
            if in_anime_search:
                search_lines.append(line)

        if not search_lines:
            return None

        def _is_season_like(title: str) -> bool:
            t = _norm(title)
            if not t.startswith(subject_norm):
                return False
            tail = t[len(subject_norm):].strip()
            if not tail:
                return True
            if re.search(r'\b(season|cour|part|arc|season one|season two|season three)\b', tail):
                return True
            if re.match(r'^(1st|2nd|3rd|4th|5th|6th|7th|8th|9th|10th|\d+(?:st|nd|rd|th)?)\b', tail):
                return True
            # Exclude obvious non-season variants.
            if re.search(r'\b(movie|special|ova|ona|recap|summary|mini)\b', tail):
                return False
            return False

        seen_titles: set[str] = set()
        season_titles: list[str] = []
        for line in search_lines:
            title = line.split(" | ", 1)[0].strip()
            if not title or title in seen_titles:
                continue
            if _is_season_like(title):
                seen_titles.add(title)
                season_titles.append(title)

        if not season_titles:
            return None

        count = len(season_titles)
        subject_clean = subject.strip()
        return f"{subject_clean} has {count} season" + ("s" if count != 1 else "") + "."
    except Exception as _e:
        logger.info(f"[JIKAN SEASON COUNT] failed for query '{q[:80]}': {_e}")
        return None


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
