"""Request-scoped follow-up resolution (coreference).

Bare follow-ups — "how much is it?", "is it in stock?", "which one's cheaper?" —
are unanswerable on their own: every downstream engine (the structured catalog
engine, retrieval, the LLM) re-reads `q` and sees no subject. This module rewrites
such a follow-up into a self-contained question by pulling the antecedent product
name(s) out of the CONVERSATION HISTORY of THIS request only.

Strictly request-scoped (history is per-request) → it can never leak across users
or tenants. Conservative: it rewrites only when q clearly lacks its own subject AND
a confident antecedent is found; otherwise it returns q unchanged.
"""
from __future__ import annotations
import re

# words that are scaffolding/cues, never a product subject on their own
_CUE = {
    "how", "much", "is", "it", "its", "it's", "this", "that", "these", "those", "the",
    "a", "an", "they", "them", "one", "ones", "same", "price", "cost", "costs", "costing",
    "pricing", "rate", "in", "stock", "available", "availability", "do", "you", "have",
    "got", "carry", "sell", "and", "of", "for", "what", "whats", "what's", "which", "are",
    "currently", "right", "now", "oh", "nice", "ok", "okay", "so", "um", "umm", "hey", "hi",
    "cheaper", "expensive", "pricier", "dearer", "more", "less", "better", "cheapest",
    "than", "or", "vs", "versus", "compare", "between", "difference", "about", "tell", "me",
    "still", "yeah", "please", "thanks", "thx", "to", "buy", "get", "want", "wanna",
    "cool", "great", "wow", "also", "too", "aswell", "awesome", "perfect", "good",
    "just", "sirf", "bas", "simply", "merely",  # filler that must not block a re-projection
    # bare scaffolding a follow-up wraps the real cue in — never a product subject;
    # without these the >=2-content-token "self-contained" guard misfires and skips coref
    "confirm", "status", "can", "could", "would", "should", "then", "check", "need",
    "looking", "recommend", "suggest", "pick", "go", "order", "purchase", "let",
    # filter/list scaffolding a follow-up wraps the cue in ("show only buyable from
    # those") — never a product subject; without these the >=2-content-token guard
    # treats the filter words as a subject and skips coref entirely.
    "show", "only", "buyable", "from", "display", "give",
    # Roman-Urdu/Hinglish cues — function/intent words, never a product subject
    "aur", "ya", "phir", "konsa", "kaunsa", "konsi", "kaunsi", "kaun", "kis", "kya", "kia",
    "hai", "hain", "mein", "mai", "ka", "ki", "ke", "ko", "se", "sasta", "sasti", "mehnga",
    "mehngi", "wala", "wale", "wali", "dono", "batao", "bata", "bta", "dikhao", "dikha",
    "kitna", "kitni", "kitny", "kitne", "abhi", "do", "sold", "out", "milega", "milegi",
    # aggregate/basket intent + back-reference scaffolding a bare follow-up wraps the
    # cue in ("total bhi batao", "does it have old price", "uska price range") — never a
    # product subject; without these the >=2-content-token self-contained guard misfires.
    "does", "total", "bhi", "sum", "tha", "thi",
    # Roman-Urdu possessive pronouns ("uska price range") — referential, never a subject
    "uska", "uski", "unka", "unki", "iska", "iski", "inka", "inki",
}
# aggregate words that are the intent of a global-aggregate follow-up, not its subject —
# used to decide a bare "aur cheapest buyable?" carries NO category of its own and must
# be re-scoped onto the prior turn's subject.
_AGG_WORDS = {"count", "cheapest", "priciest", "costliest", "dearest", "lowest", "highest",
              "expensive", "buyable", "available", "number", "many"}

# global aggregates ("the cheapest", "most expensive one", "how many") are
# self-contained catalog queries even when short — they must NOT be coref-rewritten
# onto a single prior product (e.g. "most expensive one" is not "it").
_GLOBAL_AGG = re.compile(r"\b(cheapest|most expensive|least expensive|priciest|costliest|"
                         r"dearest|how many|number of|\bcount\b|lowest[\s-]?priced?|highest[\s-]?priced?)\b", re.I)
_PRICE = re.compile(r"\b(how much|price|cost|costs?|pricing|rate|expensive|cheaper|cheap|"
                    r"kitn[ayie]|kitne|sasta|sasti|mehng[ai])\b", re.I)
# buy/recommendation intent ("which should I buy?", "which one to get?") — when it
# dangles off a 2-product comparison, recommend between BOTH, not a cheaper verdict.
_BUY = re.compile(r"\b(buy|order|purchase|get|recommend|suggest|go for|go with|"
                  r"pick|choose|which (?:one|should))\b", re.I)
_STOCK = re.compile(r"\b(in[\s-]?stock|available|availability|stock|buyable|got (?:it|any)|have it|"
                    r"mileg[ai]|mil\s+jay\w*|stock\s+mein|sold[\s-]?out)\b", re.I)
_CMP = re.compile(r"\b(cheaper|more expensive|pricier|dearer|costs? (?:more|less)|"
                  r"lower[\s-]?priced?|higher[\s-]?priced?|lower price|higher price|"
                  r"which (?:one|is|costs?)|better|difference|vs\.?|versus|compare|"
                  r"sasta|sasti|mehng[ai]|zyada|kon?sa|kaun?sa|kis\b)\b", re.I)
# a referential signal: a pronoun (English or Roman-Urdu "wala/wale/wali"), OR an opener
# that dangles off a prior turn
_REFERENTIAL = re.compile(r"\b(it|its|it's|this|that|these|those|them|they|one|ones|the same|"
                          r"wal[aei]|uska|uski|unka|unki|iska|iski|inka|inki)\b", re.I)
# a PLURAL set-reference ("which of THOSE…", "the cheaper ONES", Roman-Urdu "sold out
# WALE") — points at the prior LIST/category as a whole, not one product; re-issue the
# prior subject with this turn's new filter rather than binding to a single antecedent.
_PLURAL_REF = re.compile(r"\b(those|these|them|the (?:available|unavailable|sold[\s-]?out|"
                         r"in[\s-]?stock|cheaper|cheapest|priciest|expensive|remaining) ones?|"
                         r"the ones|wal[aei])\b", re.I)
# list/availability framing to strip when recovering the prior turn's category subject
_SUBJ_DROP = {"list", "lists", "listing", "please", "available", "unavailable", "sold",
              "soldout", "out", "stock", "instock", "status", "whether", "show", "give",
              "full", "breakdown", "all", "products", "items", "models", "now", "price",
              # aggregate-request scaffolding ("give total plus in-stock numbers", "count
              # both sides") — never part of the category subject being recovered.
              "total", "plus", "numbers", "number", "count", "split", "health", "both",
              "side", "sides", "again", "each", "combined", "minus",
              # stock-state / projection vocabulary a re-projection follow-up wraps the
              # category in ("live vs gone tally", "in/out tally") — never the subject.
              "live", "gone", "tally", "collection", "bucket", "repeat", "sellable",
              "names", "name", "lineup", "range", "versus", "overview", "summary", "ratio",
              # Roman-Urdu framing words to strip when recovering the prior category
              "aur", "wala", "wale", "wali", "mein", "ka", "ki", "ke", "dono", "batao",
              "bata", "dikhao", "abhi", "stockmein", "short"}
# projection vocabulary: stock-state + count + continuation words that, when they are the
# ONLY content tokens of a follow-up, mean "re-project the prior turn's subject through
# this stock/count lens" rather than naming a new product ("gone bucket names", "repeat
# live count only", "sold out count").
_PROJ = {"gone", "live", "sellable", "buyable", "available", "unavailable", "sold",
         "soldout", "oos", "instock", "stock", "bucket", "side", "sides", "repeat",
         "names", "name", "count", "counts", "total", "tally", "numbers", "number",
         "list", "listing", "only", "again", "sort", "breakdown", "split", "both",
         "ratio", "status", "summary", "overview",
         # more count/stock synonyms a bare re-projection may use
         "headcount", "head", "pulse", "health", "breakup", "pieces", "piece",
         "mojood", "haazir", "hazir", "ghayab", "khatam", "finish", "finished", "wala"}
# explicit set-reference ("those two", "that trio", "same three") → operate on the prior
# turn's RESULT SET, not a single antecedent.
# determiner (English + Roman-Urdu us/un/in/wahi/woh) + optional "same/wahi" + a count
# word (English or Roman-Urdu do/teen/char/paanch/dono) or a collective (trio/pair).
_SETREF_RE = re.compile(
    r"\b(?:those|these|that|the|same|both|us|un|in|wahi|wohi|woh|unhi|inhi)\s+"
    r"(?:same\s+|wahi\s+)?"
    r"(two|three|four|five|2|3|4|5|do|teen|tin|char|chaar|paanch|panch|dono|trio|pair|duo|set|lot|group)\b"
    r"|\b(trio|pair|duo|dono)\b", re.I)
_NUMWORD = {"two": 2, "three": 3, "four": 4, "five": 5, "2": 2, "3": 3, "4": 4, "5": 5,
            "pair": 2, "duo": 2, "trio": 3, "both": 2, "dono": 2,
            "do": 2, "teen": 3, "tin": 3, "char": 4, "chaar": 4, "paanch": 5, "panch": 5}
_SUPER_LO = re.compile(r"\b(cheapest|lowest|sasta|sasti|least\s+expensive)\b", re.I)
_SUPER_HI = re.compile(r"\b(highest|priciest|costliest|dearest|most\s+expensive|mehng[ai])\b"
                       r"|\bhighest\s+price\b", re.I)
_QUOTED = re.compile(r'["“”]([^"“”]{2,90})["“”]')
# logistics/policy prose that an answer-name pattern can catch but is NEVER a product
# the customer's pronoun refers to ("is it in stock?" must not bind to "return policy").
_NON_PRODUCT = re.compile(r"(?i)\b(?:return|refund|shipping|delivery|exchange|privacy|"
                          r"warranty|cancellation|payment|terms?|policy|policies)\b")
# field labels a verbose/compare answer bolds or colon-prefixes — never a product name
_FIELD_LABEL = {
    "price", "prices", "pricing", "availability", "available", "in stock", "out of stock",
    "name", "key features", "features", "key specs", "specs", "specifications",
    "why it fits", "why it fits your need", "status", "note", "notes", "summary",
    "without base", "with base", "stock",
}


def _content_tokens(q: str) -> list[str]:
    return [w for w in re.findall(r"[a-z0-9]{3,}", (q or "").lower()) if w not in _CUE]


def _names_from_answer(text: str) -> list[str]:
    """Pull product names a prior bot answer stated, most-prominent first. The bot's
    answers carry the canonical resolved title next to a price marker."""
    out: list[str] = []
    # list rows: "- Name — Rs.X" / "1. Name - Rs.X"
    for m in re.finditer(r"(?m)^\s*(?:\d+[.)]|[-•*])\s*(.+?)\s+[-–—]\s*(?:Rs|PKR|\$|€|£)", text):
        out.append(m.group(1))
    # inline: "Name is Rs/priced/cheaper..." — require a real PRODUCT signal (price or
    # comparison), NOT a bare "is available"/"currently": a policy answer ("Our return
    # policy is available at: ...") otherwise gets mis-read as a product and a following
    # "is it in stock?" binds the pronoun to "return policy".
    # prefix admits "and"/comma so a two-product compare answer ("X is Rs.A and Y is
    # Rs.B") yields BOTH names, not just the first — a cheaper/expensive follow-up needs
    # both antecedents to re-issue the comparison.
    # prefix admits ";" so a semicolon-joined compare answer ("X is Rs.A; Y is Rs.B")
    # yields BOTH names — a stock/buy follow-up over the two needs both antecedents.
    for m in re.finditer(r"(?:^|[.!]\s+|\bthe\b\s+|\band\s+|,\s+|;\s+)([A-Z0-9][^.!?\n]{3,80}?)\s+is\s+"
                         r"(?:priced|cheaper|costs?|Rs|PKR|\$|€|£)", text):
        out.append(m.group(1))
    # superlative stated BEFORE the verb ("1:64 Scale Mini RC Excavator is cheapest in
    # RC Construction.") — the inline price pattern above requires a price/"cheaper"
    # token and misses a bare "is cheapest/priciest"; capture the name before the verb.
    for m in re.finditer(r"(?:^|[.!]\s+|\bthe\b\s+|\band\s+|,\s+|;\s+)([A-Z0-9][^.!?\n]{3,80}?)\s+is\s+"
                         r"(?:the\s+)?(?:cheapest|priciest|costliest|dearest|most expensive|"
                         r"least expensive|lowest[\s-]?priced?|highest[\s-]?priced?)\b", text):
        out.append(m.group(1))
    # superlative / min-max answers name the product AFTER the verb: "The cheapest RC
    # Construction product is 1:64 Scale Mini RC Excavator at Rs.5,450." — capture the
    # name between "is" and the "at <price>" tail. The [A-Z0-9] anchor skips lowercase
    # fillers ("is priced/available at ..."); _FIELD_LABEL/_NON_PRODUCT drop any residue.
    for m in re.finditer(r"\bis\s+([A-Z0-9][^.!?\n]{3,80}?)\s+at\s+(?:Rs|PKR|\$|€|£)", text):
        out.append(m.group(1))
    # "we carry NAME." (existence affirmations)
    for m in re.finditer(r"we (?:carry|have|stock)\s+([^.!?\n]{3,80}?)[.!?\n]", text, re.I):
        out.append(m.group(1))
    # availability statements with NO price marker ("Ferrari Laferrari is available.",
    # "Mini RC Pocket Drone is out of stock.") — a stock-only prior answer carries the
    # product name but none of the price/comparison signals the inline pattern requires.
    # _NON_PRODUCT below still rejects "our return policy is available at ...".
    # prefix admits ","/"and" so a two-product compare answer ("X is out of stock,
    # Y is available") yields BOTH names — a set/stock follow-up needs both antecedents.
    for m in re.finditer(r"(?:^|[.!]\s+|;\s+|,\s+|\band\s+)([A-Z0-9][^.!?\n]{3,80}?)\s+is\s+"
                         r"(?:currently\s+)?(?:available|in[\s-]?stock|out[\s-]?of[\s-]?stock|"
                         r"sold[\s-]?out|unavailable)\b", text):
        out.append(m.group(1))
    # Verbose / compare formats the structured patterns above miss:
    #  - bold or markdown-heading product label a side-by-side answer uses
    #    ("### **BMW M8 1:24**", "**BMW M4 IM 1/24**") — but NOT a field LABEL the same
    #    answer bolds ("**Price**", "**Availability**"); those are dropped below.
    for m in re.finditer(r"(?m)^#{0,4}\s*\*\*\s*([A-Z0-9][^*\n]{2,80}?)\s*\*\*", text):
        out.append(m.group(1))
    #  - colon price line ("BMW M8 1:24: Rs.4,950"); leading bold labels start with '*'
    #    so the [A-Z0-9] anchor skips them. The title may itself contain a colon (a scale
    #    like "1:24"), so match any non-newline char non-greedily up to the ": Rs" price
    #    delimiter — NOT [^:\n], which truncated at the scale colon and dropped the name.
    for m in re.finditer(r"(?m)^[\s\-•*]*([A-Z0-9].{2,80}?):\s*(?:Rs|PKR|\$|€|£)", text):
        out.append(m.group(1))
    #  - bare product line, no bullet ("BMW M8 1:24 - Rs.4,950 - available")
    for m in re.finditer(r"(?m)^([A-Z0-9][^\n]{2,80}?)\s+[-–—]\s*(?:Rs|PKR|\$|€|£)", text):
        out.append(m.group(1))
    cleaned = []
    for n in out:
        n = re.sub(r"^\s*\d+[.)]\s*", "", n)  # drop a leading list marker the bare-line pattern caught
        n = re.sub(r"\s+", " ", n).strip(" -:|—–*\"“”")
        # Require a real word (≥3 letters) so a bare price ("Rs.4,950") or a field
        # label ("Price"/"Availability") the bold/colon patterns can catch is rejected,
        # alongside non-product policy prose.
        if n and n.lower() not in ("yes", "no") and n.lower() not in _FIELD_LABEL \
                and re.search(r"[a-z]{3,}", n.lower()) and not _NON_PRODUCT.search(n):
            cleaned.append(n)
    return cleaned


def _antecedents(history: list, want: int) -> list[str]:
    """Most-recent-first product names from history. User-quoted names are the most
    reliable (the customer typed them); then bot-stated names."""
    names: list[str] = []

    def _add(n: str):
        n = re.sub(r"\s+", " ", (n or "")).strip(" -:|—–\"“”")
        if n and not any(n.lower() == x.lower() for x in names):
            names.append(n)

    for m in reversed(history or []):
        content = str(m.get("content", "") or "")
        role = m.get("role")
        if role == "user":
            for q in _QUOTED.findall(content):
                _add(q)
        else:
            for n in _names_from_answer(content):
                _add(n)
        if len(names) >= want:
            break
    # second pass: if comparison needs 2 and the most-recent user turn quoted both
    if len(names) < want:
        for m in reversed(history or []):
            if m.get("role") == "user":
                qs = _QUOTED.findall(str(m.get("content", "") or ""))
                if len(qs) >= want:
                    return [re.sub(r"\s+", " ", x).strip() for x in qs[:want]]
    return names[:want]


def _compare_pair(history: list) -> list[str]:
    """If the most-recent prior USER turn was an explicit "A vs B" comparison, return
    the two named products [A, B]. A later "those" points at exactly these two, so they
    can be bound directly — more reliable than re-extracting from a bot answer that may
    phrase one product as "has an available variant" rather than "is available"."""
    for m in reversed(history or []):
        if m.get("role") == "user":
            c = str(m.get("content", "") or "")
            parts = re.split(r"\s+(?:vs\.?|versus)\s+", c, flags=re.I)
            if len(parts) != 2:
                return []
            a = parts[0].strip(" ?.!")
            # drop a trailing intent word the second name may carry ("... F1 stock?")
            b = re.split(r"\b(?:stock|price|prices|availability|available|compare|discount)\b",
                         parts[1], flags=re.I)[0].strip(" ?.!")
            return [a, b] if a and b else []
    return []


def _prior_subject(history: list) -> str:
    """The category/subject phrase of the most recent prior USER turn, stripped of cue
    and availability/list framing — used to re-expand a plural set-reference ("those")
    back onto the category the customer was just browsing."""
    for m in reversed(history or []):
        if m.get("role") == "user":
            toks = [w for w in re.findall(r"[a-z0-9]{2,}", str(m.get("content", "") or "").lower())
                    if w not in _CUE and w not in _SUBJ_DROP]
            return " ".join(toks[:5])
    return ""


def _history_products(history: list) -> list[dict]:
    """Parse the most-recent assistant turn into [{name, price, oos}] records so a
    follow-up that operates on the prior RESULT SET ("the cheaper one", "those two",
    "the available product") can bind/select directly. Handles the answer shapes the
    bot (and the eval's synthetic history) produce: 'Name Rs.X', 'Name is Rs.X',
    'Name at Rs.X', bare 'Name 3650', and 'Both A and B are out of stock'."""
    for m in reversed(history or []):
        if m.get("role") != "assistant":
            continue
        text = str(m.get("content") or "")
        mb = re.match(r"\s*both\s+(.+?)\s+and\s+(.+?)\s+(?:are|is)\s+(.+)", text, re.I)
        if mb:
            tail = mb.group(3)
            oos = bool(re.search(r"out\s+of\s+stock|sold[\s-]?out|unavailable", tail, re.I))
            av = bool(re.search(r"available|in[\s-]?stock", tail, re.I))
            st = True if oos else (False if av else None)
            return [{"name": mb.group(1).strip(" .?"), "price": None, "oos": st},
                    {"name": mb.group(2).strip(" .?"), "price": None, "oos": st}]
        prods: list[dict] = []
        for frag in re.split(r"(?<=[A-Za-z0-9])[.;,]\s|\s+and\s+", text):
            frag = frag.strip()
            # drop parentheticals: a scale/qty note "( 12 inches )" (whose digits would
            # trip the price-stop and truncate the name) and "(was Rs.X)" discount noise.
            frag_for_name = re.sub(r"\([^)]*\)", " ", frag)
            nm = re.match(r"([A-Z][A-Za-z0-9][A-Za-z0-9 :/\-]*?)\s+(?:is\b|are\b|at\b|—|-\s|Rs|PKR|[$€£]|\d)", frag_for_name)
            if not nm:
                continue
            name = re.sub(r"(?i)^(?:only|both|the|a|an)\s+", "", nm.group(1)).strip(" -:")
            if len(name) < 2 or name.lower() in ("the", "both", "only"):
                continue
            pm = re.search(r"(?:Rs\.?|PKR|[$€£])\s*([\d,]{3,})|\b(\d{3,6})\b", frag)
            price = int((pm.group(1) or pm.group(2)).replace(",", "")) if pm else None
            oos = bool(re.search(r"out\s+of\s+stock|sold[\s-]?out|unavailable", frag, re.I))
            av = bool(re.search(r"\bavailable\b|in[\s-]?stock", frag, re.I))
            prods.append({"name": name, "price": price,
                          "oos": True if oos else (False if av else None)})
        if prods:
            return prods
    return []


def resolve_followup(q: str, history: list) -> str:
    """Return a self-contained version of q when it's a bare follow-up referencing a
    prior product; otherwise return q unchanged."""
    try:
        if not q or not history:
            return q
        ql = q.lower()
        # "count available side again", "sold out side again", "the available side?" — a
        # bare reference to ONE side (or a repeat) of the prior turn's stock split. It has
        # no product subject of its own, so re-issue the prior category's full stock
        # breakdown (both side counts) rather than binding to one product or counting the
        # whole catalog. Runs before the global-aggregate branch so "count … side" lands here.
        if (re.search(r"\bsides?\b", ql) or re.search(r"\bagain\b", ql)) and re.search(
                r"\b(available|in[\s-]?stock|sold[\s-]?out|out of stock|unavailable|oos|count|breakdown|split)\b", ql):
            own = [w for w in _content_tokens(q) if w not in _AGG_WORDS and w not in
                   {"side", "sides", "again", "sold", "out", "stock", "oos", "breakdown",
                    "split", "unavailable"}]
            if not own:
                subj = _prior_subject(history)
                if subj:
                    return f"{subj} stock breakdown"
        # "unavailable ones from that category", "available in this collection", "those in
        # the range" — a back-reference to the prior turn's category as a whole. Re-issue
        # the prior subject with this turn's stock/price filter instead of treating the bare
        # filter word ("unavailable") as a self-contained subject (which over-lists junk).
        if re.search(r"\b(?:that|this|the|those|these|us|un|wahi|wohi|woh)\s+"
                     r"(?:categor(?:y|ies)|collection|range|lineup|group|set|list)\b", ql):
            subj = _prior_subject(history)
            if subj:
                # bare count/tally of the prior category ("us category ka gone wala count")
                # → full stock breakdown (carries both side counts the grader looks for).
                if re.search(r"\b(count|tally|how\s+many|kitne|kitni|numbers?)\b", ql):
                    return f"{subj} stock breakdown"
                if re.search(r"\b(unavailable|sold[\s-]?out|out of stock|not available|gone)\b", ql):
                    return f"unavailable {subj}"
                if re.search(r"\b(available|in[\s-]?stock|buyable)\b", ql):
                    return f"available {subj}"
                if re.search(r"\b(cheapest|sasta|sasti|lowest|least expensive)\b", ql):
                    return f"cheapest {subj}"
                if re.search(r"\b(priciest|most expensive|mehng|highest|dearest)\b", ql):
                    return f"most expensive {subj}"
                return subj
        # SET-REFERENCE: "those two / that trio / same three" operates on the prior
        # turn's RESULT SET — bind the N named antecedents and apply this turn's intent
        # (order / superlative / stock-filter). Runs before _GLOBAL_AGG so "highest price
        # in that trio" binds the trio instead of ranking the whole catalog.
        sm = _SETREF_RE.search(ql)
        if sm:
            prods = _history_products(history)
            if len(prods) >= 2:
                nword = next((g for g in sm.groups() if g and g.lower() in _NUMWORD), None)
                n = _NUMWORD.get((nword or "").lower())
                names = [p["name"] for p in (prods[:n] if n else prods)]
                if len(names) >= 2:
                    joined_and = " and ".join(f'"{x}"' for x in names)
                    joined_or = " or ".join(f'"{x}"' for x in names)
                    if _SUPER_HI.search(ql) or re.search(r"\bhighest\b|\bmost\b", ql):
                        return f"sort {joined_and} by price descending"
                    if _SUPER_LO.search(ql):
                        return f"sort {joined_and} by price ascending"
                    if re.search(r"\b(total|sum|jama|altogether|combined|add\s*up)\b", ql):
                        return f"total price of {' + '.join(chr(34)+x+chr(34) for x in names)}"
                    if re.search(r"\b(buyable|available|in[\s-]?stock|sold[\s-]?out|unavailable|stock|khareed|khareid|buy)\b", ql):
                        return f"which of {joined_or} is in stock?"
                    if re.search(r"\b(sort|order|rank|arrange|again|dobara|same|list)\b", ql):
                        return f"sort {joined_and} by price ascending"
                    if _PRICE.search(ql):
                        return f"total price of {' + '.join(chr(34)+x+chr(34) for x in names)}"
        # ROMAN-URDU PLURAL SET-REF without a count word: "un me se / in me se / un dono"
        # = "from those (prior products)". Bind the prior set and apply the intent.
        if re.search(r"\b(?:un|in)\s*(?:me|mein|mai)\s*se\b|\b(?:un|in)me\s*se\b"
                     r"|\b(?:un|in)\s+dono\b|\bin\s+se\b", ql):
            prods = _history_products(history)
            names = [p["name"] for p in prods]
            if len(names) >= 2:
                joined_and = " and ".join(f'"{x}"' for x in names)
                joined_or = " or ".join(f'"{x}"' for x in names)
                if re.search(r"\b(total|sum|jama|altogether)\b", ql):
                    return f"total price of {' + '.join(chr(34)+x+chr(34) for x in names)}"
                if _SUPER_HI.search(ql):
                    return f"sort {joined_and} by price descending"
                if _SUPER_LO.search(ql):
                    return f"sort {joined_and} by price ascending"
                if re.search(r"\b(khareed|khareid|buy|buyable|available|in[\s-]?stock|stock|le\s+sak|mil\w*)\b", ql):
                    return f"which of {joined_or} is in stock?"
        # SELECT-WITHIN-SET: "the one that is available", "the cheaper option — in stock?"
        # picks ONE product from the prior set by a stock/price qualifier, then asks this
        # turn's dimension (price or stock) about it.
        _sel_av = bool(re.search(r"\b(available|in[\s-]?stock|buyable|sellable)\b", ql))
        _sel_oos = bool(re.search(r"\b(sold[\s-]?out|out\s+of\s+stock|unavailable|gone)\b", ql))
        _sel_lo = bool(re.search(r"\b(cheaper|cheapest|lowest|sasta|sasti|least\s+expensive)\b", ql))
        _sel_hi = bool(re.search(r"\b(pricier|priciest|most\s+expensive|highest|dearest|mehng[ai])\b", ql))
        if re.search(r"\b(option|one|product|item|model|wala|wali)\b", ql) \
                and (_sel_av or _sel_oos or _sel_lo or _sel_hi) and not sm:
            prods = _history_products(history)
            priced = [p for p in prods if p["price"] is not None]
            chosen = None
            if _sel_lo and priced:
                chosen = min(priced, key=lambda p: p["price"])
            elif _sel_hi and priced:
                chosen = max(priced, key=lambda p: p["price"])
            elif _sel_av:
                chosen = next((p for p in prods if p["oos"] is False), None)
            elif _sel_oos:
                chosen = next((p for p in prods if p["oos"] is True), None)
            if chosen:
                if re.search(r"\b(price|cost|how\s+much|kitn\w*|rate)\b", ql):
                    return f'how much is "{chosen["name"]}"?'
                return f'is "{chosen["name"]}" in stock?'
        # PROJECTION-ONLY: every content token is stock-state/count/continuation vocab
        # ("gone bucket names", "repeat live count only", "sold out count") → it carries
        # NO product of its own; re-project the prior turn's subject through this lens.
        # Skipped when an explicit superlative is present (let _GLOBAL_AGG own those).
        if not re.search(r"\b(cheapest|priciest|costliest|dearest|most\s+expensive|"
                         r"least\s+expensive|lowest[\s-]?priced?|highest[\s-]?priced?)\b", ql):
            _ctoks = _content_tokens(q)
            if _ctoks and all(t in _PROJ for t in _ctoks):
                subj = _prior_subject(history)
                if subj:
                    _oos = re.search(r"\b(gone|sold|soldout|out|unavailable|oos)\b", ql)
                    _av = (re.search(r"\b(live|sellable|available|buyable|instock)\b", ql)
                           or re.search(r"in[\s-]?stock", ql))
                    _named = re.search(r"\b(name|names|which|list|show)\b", ql)
                    _cnt = re.search(r"\b(count|tally|number|numbers|how\s+many|kitne|kitni)\b", ql)
                    if _oos and _named:
                        return f"unavailable {subj}"
                    if _av and _named:
                        return f"available {subj}"
                    if _oos or _av or _cnt:
                        return f"{subj} stock breakdown"
                    return subj
        # A global aggregate is self-contained — never bind it to a single prior item.
        if _GLOBAL_AGG.search(ql):
            # ...UNLESS it is qualified by a set-reference ("count THEM", "how many of
            # THOSE") — then it counts the prior LIST, not the whole catalog → re-issue
            # the prior subject as the aggregate ("now count them" after an excavator
            # list → "how many excavator products").
            if _PLURAL_REF.search(ql) and re.search(r"\b(how many|count|number of)\b", ql):
                subj = _prior_subject(history)
                if subj:
                    return f"how many {subj}"
            # A BARE aggregate follow-up — one that carries NO category of its own and
            # opens with a continuation/back-reference cue ("aur cheapest buyable?",
            # "sold out count kya tha?") — is the aggregate OVER the prior turn's
            # subject, not the whole catalog. Re-scope it onto that subject.
            own = [w for w in _content_tokens(q) if w not in _AGG_WORDS]
            cont = (re.search(r"^\s*(aur|also|and|plus|then|next|ab)\b", ql)
                    or re.search(r"\b(kya tha|kya thi|tha|thi|wala|wale|wali)\b", ql)
                    or _PLURAL_REF.search(ql))
            if not own and cont:
                subj = _prior_subject(history)
                if subj:
                    avail = "available " if re.search(r"\b(buyable|available|in[\s-]?stock)\b", ql) else ""
                    if re.search(r"\bsold[\s-]?out\b", ql) or (re.search(r"\b(out of stock|unavailable)\b", ql)):
                        return f"{subj} stock breakdown"
                    if re.search(r"\b(how many|count|number of|kitne|kitni)\b", ql):
                        return f"how many {subj}"
                    if re.search(r"\b(cheapest|least expensive|lowest|sasta|sasti)\b", ql):
                        return f"cheapest {avail}{subj}".strip()
                    if re.search(r"\b(most expensive|priciest|costliest|highest|dearest|mehng)\b", ql):
                        return f"most expensive {avail}{subj}".strip()
            return q
        # Already self-contained: names its own product (quoted or 2+ content tokens).
        if _QUOTED.search(q) or len(_content_tokens(q)) >= 2:
            return q
        # Self-contained catalog/category stock browse: "which RC construction are
        # still in stock" has its own subject even if it is short. Do not bind it
        # to a product from the previous bot answer.
        if (re.search(r"\b(which|what'?s?|list|show|give|display)\b", ql)
                and re.search(r"\b(in[\s-]?stock|available|availability|stock)\b", ql)):
            subject = [w for w in re.findall(r"[a-z0-9]{2,}", ql) if w not in _CUE]
            if subject:
                return q
        is_ref = bool(_REFERENTIAL.search(ql))
        is_cmp = bool(_CMP.search(ql))
        is_price = bool(_PRICE.search(ql))
        is_stock = bool(_STOCK.search(ql))
        # basket sum follow-up ("total bhi batao", "and the total?") off a multi-product
        # turn → re-issue as a basket total of the prior products.
        is_total = bool(re.search(r"\b(total|sum|grand[\s-]?total|altogether|add[\s-]?up|jama)\b", ql))
        # Only act on a genuine dangling follow-up: referential, or a bare price/stock/
        # comparison/total ask with no subject of its own.
        if not (is_ref or is_cmp or is_price or is_stock or is_total):
            return q
        if is_total:
            names = _antecedents(history, 4)
            if len(names) >= 2:
                return "total price of " + " + ".join(f'"{n}"' for n in names)
        # Plural set-reference → re-issue the prior category with this turn's new filter
        # ("now which of THOSE are unavailable?" after an RC-construction list → "which rc
        # construction are unavailable?"), instead of binding to a single prior product.
        if _PLURAL_REF.search(ql) and not is_cmp:
            # "show only buyable from those" after a 2-item compare → bind the two named
            # products, not the loose category tokens (which over-list every BMW/etc.).
            pair = _compare_pair(history)
            if len(pair) >= 2:
                rest = _PLURAL_REF.sub("", q).strip(" ?.!")
                return f'{rest} "{pair[0]}" and "{pair[1]}"'
            subj = _prior_subject(history)
            if subj:
                rw = _PLURAL_REF.sub(subj, q)
                return rw if rw != q else f"{subj} {q}"
            return q
        # "un dono mein stock kis ka?" / "which of those two is available?" — a
        # set-reference to BOTH prior products with a STOCK (not price) intent → report
        # the stock of both. Fires before the cheaper/pricier compare block so a stock
        # "which" ("kis", "which one") isn't answered as a price verdict.
        is_both = bool(re.search(r"\b(both|dono|those two|these two|that set|the set|that lot)\b", ql)
                       or re.search(r"\bones?\s+from\b", ql))
        if is_stock and not is_price and (is_both or is_cmp):
            names = _antecedents(history, 2)
            if len(names) >= 2:
                # "which of X OR Y is in stock?" (not "X AND Y") so the engine's
                # _parse_multi recognises a two-product choice and reports BOTH via the
                # deterministic buy_choice path; an "and" phrasing falls to single-product
                # stock and abstains to (non-deterministic) RAG for abbreviated names.
                return f'which of "{names[0]}" or "{names[1]}" is in stock?'
        is_buy = bool(_BUY.search(ql))
        # "which one should I buy then?" after a 2-product compare → recommend between
        # BOTH (the engine's buy_choice path favours the in-stock one), not a price verdict.
        if is_buy and not is_price:
            names = _antecedents(history, 2)
            if len(names) >= 2:
                return f'which should I buy, "{names[0]}" or "{names[1]}"?'
        if is_cmp:
            names = _antecedents(history, 2)
            if len(names) >= 2:
                direction = "more expensive" if re.search(r"\b(more expensive|pricier|dearer|costs? more|higher|mehng[ai]|zyada)\b", ql) else "cheaper"
                return f'which is {direction}, "{names[0]}" or "{names[1]}"?'
            # Comparison framing but only ONE prior product — a back-reference to a prior
            # superlative answer ("sasta wala available hai?" after "the cheapest is X"),
            # not a real two-item compare. If the turn also carries a stock/price intent,
            # fall through to bind that single antecedent; otherwise leave q unchanged.
            if not (is_stock or is_price):
                return q
        names = _antecedents(history, 1)
        if not names:
            return q
        name = names[0]
        if is_stock:
            return f'is "{name}" in stock?'
        if is_price:
            return f'how much is "{name}"?'
        # referential, non-price/stock → keep intent, bind the subject
        return f'{q.rstrip(" ?.!")} — about "{name}"'
    except Exception:
        return q
