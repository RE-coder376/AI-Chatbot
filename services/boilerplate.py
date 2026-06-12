"""
services/boilerplate.py — corpus-frequency boilerplate filter (universal junk remover).

Junk needs no string lists to identify: text that repeats across many pages of
the SAME crawl is site template/chrome ("Shop Now Location Click to enlarge",
banner slogans, footer text that survives the DOM scrub); text unique to a page
is content. The filter calibrates on the first pages of a crawl, freezes a set
of frequent lines + word-shingles, then masks them from product/catalog pages
before chunking — so the FIRST crawl of a never-seen theme comes out clean.

Why per-page structured fields survive: "Name:"/"Price:"/"Avail:" lines carry
per-product values, so their shingles never cross the document-frequency
threshold — and they are explicitly protected anyway.

Why docs/policy/info pages are exempt: a shipping banner repeated on every
product page is noise THERE, but the shipping-policy page is its canonical
home — masking it everywhere would erase the only copy the bot can answer from.
"""

from __future__ import annotations

import math
import re
from collections import Counter

SHINGLE_K = 4          # word-run length; junk blocks ≥4 words are masked mid-line
MIN_LINE_KEY_LEN = 8   # shorter normalized lines are never counted/dropped

_NORM_RE = re.compile(r"[^a-z0-9$£€]+")
# Structured field lines and headings are never dropped or masked.
_PROTECTED_LINE_RE = re.compile(
    r"(?i)^\s*(?:price|avail(?:ability)?|name|sku|brand|category|categories)\s*:"
    r"|^\s*#{2,3}\s"
)


def _norm_token(tok: str) -> str:
    return _NORM_RE.sub("", tok.lower())


def _line_tokens(line: str) -> list[str]:
    return [t for t in (_norm_token(t) for t in line.split()) if t]


class BoilerplateFilter:
    """Calibrate-once, scrub-many. All methods are sync (event-loop atomic)."""

    # Page types where masking applies. Junk classes observed in the wild all
    # live on storefront pages; "unknown" pages are storefront chrome more
    # often than prose. Everything else keeps its full text.
    SCRUB_PAGE_TYPES = {"product", "catalog", "category", "unknown"}

    def __init__(self, min_pages: int = 24, max_total_pages: int = 60,
                 df_ratio: float = 0.30, min_df: int = 4):
        self.min_pages = min_pages            # eligible pages needed to calibrate
        self.max_total_pages = max_total_pages  # calibrate anyway after this many pages
        self.df_ratio = df_ratio
        self.min_df = min_df
        self.eligible_seen = 0
        self.total_seen = 0
        self.calibrated = False
        self.frequent_lines: set[str] = set()
        self.frequent_shingles: set[str] = set()
        self._line_df: Counter = Counter()
        self._shingle_df: Counter = Counter()

    # ── calibration ─────────────────────────────────────────────────────────
    def should_scrub(self, page_meta: dict | None) -> bool:
        meta = page_meta or {}
        if meta.get("docs_like"):
            return False
        return str(meta.get("page_type") or "") in self.SCRUB_PAGE_TYPES

    def add_page(self, text: str, page_meta: dict | None = None) -> None:
        if self.calibrated:
            return
        self.total_seen += 1
        if not text or not self.should_scrub(page_meta):
            return
        self.eligible_seen += 1
        page_lines: set[str] = set()
        page_shingles: set[str] = set()
        for line in text.splitlines():
            toks = _line_tokens(line)
            if not toks:
                continue
            key = " ".join(toks)
            if len(key) >= MIN_LINE_KEY_LEN:
                page_lines.add(key)
            for i in range(len(toks) - SHINGLE_K + 1):
                page_shingles.add(" ".join(toks[i:i + SHINGLE_K]))
        self._line_df.update(page_lines)
        self._shingle_df.update(page_shingles)

    @property
    def ready(self) -> bool:
        return (self.eligible_seen >= self.min_pages
                or self.total_seen >= self.max_total_pages)

    def calibrate(self) -> None:
        if self.calibrated:
            return
        n = self.eligible_seen
        if n >= 3:
            thr = max(self.min_df, math.ceil(self.df_ratio * n))
            self.frequent_lines = {k for k, c in self._line_df.items() if c >= thr}
            self.frequent_shingles = {k for k, c in self._shingle_df.items() if c >= thr}
        # Too few eligible pages (docs sites, tiny crawls): no-op filter.
        self._line_df = Counter()
        self._shingle_df = Counter()
        self.calibrated = True

    # ── scrubbing ───────────────────────────────────────────────────────────
    def scrub(self, text: str, page_meta: dict | None = None) -> str:
        if not self.calibrated or not text:
            return text
        if not self.frequent_lines and not self.frequent_shingles:
            return text
        if page_meta is not None and not self.should_scrub(page_meta):
            return text
        out: list[str] = []
        for line in text.splitlines():
            if not line.strip():
                out.append(line)
                continue
            if _PROTECTED_LINE_RE.match(line):
                out.append(line)
                continue
            toks_norm_pairs = [(i, _norm_token(t)) for i, t in enumerate(line.split())]
            pairs = [(i, n) for i, n in toks_norm_pairs if n]
            key = " ".join(n for _, n in pairs)
            if len(key) >= MIN_LINE_KEY_LEN and key in self.frequent_lines:
                continue  # whole line is site template
            raw_toks = line.split()
            if len(pairs) >= SHINGLE_K and self.frequent_shingles:
                covered = [False] * len(raw_toks)
                for i in range(len(pairs) - SHINGLE_K + 1):
                    sh = " ".join(n for _, n in pairs[i:i + SHINGLE_K])
                    if sh in self.frequent_shingles:
                        # mark the full original-token span (punct tokens included)
                        for j in range(pairs[i][0], pairs[i + SHINGLE_K - 1][0] + 1):
                            covered[j] = True
                if any(covered):
                    kept = [t for t, c in zip(raw_toks, covered) if not c]
                    line = " ".join(kept)
                    if not line.strip():
                        continue
            out.append(line)
        return "\n".join(out)
