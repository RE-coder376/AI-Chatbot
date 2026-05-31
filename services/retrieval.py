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
_OUTCOMES_EXTRACTOR_VERSION = "2026-05-13.v2"

# Outcomes/learning intent (universal)
_OUTCOMES_INTENT_RE = re.compile(r"\b(what\s+will\s+i\s+learn|what\s+will\s+we\s+learn|learning\s+outcomes?|objectives?|goals?|by\s+the\s+end|you\s+will\s+be\s+able\s+to)\b", re.I)

_BULLET_LINE_RE = re.compile(r"^\s*(?:[-*•]|\d{1,2}[.)])\s+(.+?)\s*$")

def is_outcomes_question(q: str) -> bool:
    return bool(_OUTCOMES_INTENT_RE.search(q or ""))

_INTERROGATIVE_START = {"what", "which", "why", "how", "when", "where", "who"}
_OUTCOME_VERB_HINTS = {
    "understand","build","ship","use","integrate","deliver","identify","structure","apply","prepare",
    "run","evaluate","deploy","install","test","design","verify","retrofit","decide","learn","encode",
    "create","implement","configure","monitor","optimize","define","package"
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
            starters = [(it.split()[:1][0].lower() if it.split() else "") for it in items]
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
            if re.search(r"\b(?:learning-spec|readme)\.md\b|\b[a-z0-9_\\-/]+\.py\b|\b[a-z0-9_\\-/]+\.md\b", joined):
                return False
            longish = sum(1 for it in items if len(it) > 140 or it.count(".") >= 2 or it.count(":") >= 2)
            if longish >= max(2, len(items) // 2):
                return False
            if _generic_heading_count(items) >= max(2, len(items) // 2):
                return False
            # If the question names a specific chapter/part title, require overlap with that title.
            if title_tokens:
                hit = sum(1 for t in title_tokens[:6] if t and (t in joined))
                req = 2 if len(title_tokens) >= 3 else 1
                if hit < req:
                    return False
            # Learning outcomes should be mostly verb-start imperatives.
            return verb_ratio >= 0.45

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
            if re.search(r"\b(?:learning-spec|readme)\.md\b|\b[a-z0-9_\\-/]+\.py\b|\b[a-z0-9_\\-/]+\.md\b", joined):
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
            if (not strong_marker) and title_tokens:
                hit = sum(1 for t in title_tokens[:6] if t and (t in joined))
                req = 2 if len(title_tokens) >= 3 else 1
                if hit < req:
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
                if not re.search(r"\b(by completing|by the end|you will be able to|learning outcomes|objectives|goals)\b", _ll):
                    continue
                if _wants_part and (f"part {part_no}" not in _ll):
                    continue
                if _wants_ch and (f"chapter {chapter_no}" not in _ll):
                    continue
                _marker_ix = _mi
                break
        except Exception:
            _marker_ix = None

        # Marker-first extraction (safer than the old "Goals ..." heuristic):
        # If we see an explicit outcomes marker line, extract short, verb-ish lines immediately after it.
        try:
            markers = ("by completing", "by the end", "you will be able to", "learning outcomes", "objectives")
            for mi, ln in enumerate(lines[:900]):
                ll = (ln or "").lower()
                if not any(m in ll for m in markers):
                    continue
                # If the question names Chapter/Part number, require it to be present in the marker line
                # (prevents grabbing the wrong chapter's outcomes list).
                if chapter_no is not None and ("chapter" in _q_lower) and (f"chapter {chapter_no}" not in ll):
                    continue
                if part_no is not None and ("part" in _q_lower) and (f"part {part_no}" not in ll):
                    continue
                cand = []
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
                    # accept short verb-ish lines (no bullet marker)
                    w0 = (t.split()[:1][0].lower() if t.split() else "")
                    if w0 in _OUTCOME_VERB_HINTS and ("http" not in t.lower()) and ("?" not in t):
                        cand.append(t.strip(" -•\t"))
                        continue
                    # stop when we hit obvious non-outcomes section headers
                    if re.search(r"(?i)\\b(previous|next|prerequisites|authors|company|privacy|capstone|lesson progression)\\b", t):
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
                    if verb_ratio < 0.45:
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
        if not (3 <= len(out_items) <= 25):
            return None
        if not _valid_outcomes_items(out_items):
            return None
        # Normalize to a clean bullet list (keep it universal; don't require a specific marker string).
        if debug is not None:
            debug["outcomes_extractor_path"] = "bullet_block"
            debug["outcomes_extractor_block_preview"] = "\n".join(out_items[:8])[:900]
        return "Learning outcomes:\n\n" + "\n".join(f"- {it}" for it in out_items)
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
            return phrase
    except Exception:
        pass
    return ''


def _extract_chapter_number(q: str) -> int | None:
    try:
        m = re.search(r"\bchapter\s*(\d{1,4})\b", q or "", re.I)
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
        tks = [w.lower() for w in re.findall(r"[a-zA-Z0-9]{4,}", title_phrase or "")][:10]
        tks = [t for t in tks if t not in {"chapter", "part", "learning", "goals", "outcomes", "according", "book"}]

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
            if tpl and (tpl in src or tpl in body):
                phrase_hit = True
            elif tks:
                hits = {t for t in tks if (t in src or t in body)}
                if len(hits) >= 2:
                    phrase_hit = True

        if num_hit and (phrase_hit or not tks):
            return True
        if phrase_hit and (ch is None and pt is None):
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

        # For goals/outcomes queries, quiz/exercise pages are usually not the canonical evidence.
        if _OUTCOMES_INTENT_RE.search(q or '') and any(p in sl for p in ("/chapter-quiz", "/quiz", "/exercises", "/exercise")):
            score -= 10.0
        # For scoped chapter/part outcomes queries, deprioritize non-canonical site shells/translations.
        if _OUTCOMES_INTENT_RE.search(q or '') and re.search(r"\b(?:chapter|part)\s+\d{1,4}\b", q or "", re.I):
            if any(p in sl for p in ("/roman/", "/arabic/", "/urdu/", "/certifications", "/about")):
                score -= 12.0
            if "/docs/" not in sl:
                score -= 6.0

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

    # Tight latency path for strict chapter/part questions:
    # keep retrieval focused and avoid broad expansion drift.
    _strict_scope_q = bool(re.search(r"\b(?:chapter|part)\s+\d{1,4}\b", q or "", re.I))
    if _strict_scope_q:
        fast = True
        k = min(k, 18)

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
            # Broad net first (common outcomes markers), then we filter hard by anchor tokens.
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

            # 2) Broad net (marker phrases), then filter hard by anchor tokens.
            # We intentionally include "goals" because some KB pages list outcomes as verb bullets
            # without the literal "you will..." phrase.
            for _marker in ("you will be able to", "by the end of this chapter", "learning outcomes", "by completing", "goals"):
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
                    if not re.search(r"(?i)\b(by (the end of|completing|after completing|upon completing)\b|you will be able to\b|learning outcomes\b|objectives?\b|goals?\b)", doc_text):
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
        # Prewarm on-demand with a bounded budget.
        if _is_outcomes_intent and _bm25_db_name and (_bm25_db_name not in _bm25_cache):
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
                    # Scoped chapter/part queries: require the same anchor in URL or text.
                    _ch = _extract_chapter_number(q or "")
                    _pt = _extract_part_number(q or "")
                    _txt_l = _txt.lower()
                    if _ch is not None and ("chapter" in (q or "").lower()):
                        _a = f"chapter {_ch}"
                        if (_a not in _src_l) and (_a not in _txt_l):
                            continue
                    if _pt is not None and ("part" in (q or "").lower()):
                        _a = f"part {_pt}"
                        if (_a not in _src_l) and (_a not in _txt_l):
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
    

    # Heuristic rerank for non-product DBs: improves chapter/part/title lookup accuracy.
    if not _has_product_meta and _combined_before_cap:
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

            def _title_match(d):
                try:
                    b = str(getattr(d, "page_content", "") or "").lower()
                    s = str((getattr(d, "metadata", None) or {}).get("source") or "").lower()
                    if _tpl and (_tpl in b or _tpl in s):
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
                    return True
                except Exception:
                    return False

            _filtered = [d for d in _combined_before_cap if _title_match(d)]

            # If this is an outcomes intent (goals/learning outcomes) and the query names a specific title,
            # apply the filter even when it yields a small set. It's better to be precise than to mix in
            # unrelated chapters that happen to contain "By the end...".
            if _is_outcomes_intent and len(_filtered) >= 1:
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

        # Baseline rerank after optional filtering
        _combined_before_cap.sort(key=lambda d: _heuristic_rerank_score(d, q, _title_phrase), reverse=True)

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
        chunk = _compress_doc(getattr(r, "page_content", "") or "", anchors)
        if not chunk:
            continue
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
