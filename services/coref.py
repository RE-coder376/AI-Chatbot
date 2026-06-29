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
}

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
                          r"wal[aei])\b", re.I)
# a PLURAL set-reference ("which of THOSE…", "the cheaper ONES", Roman-Urdu "sold out
# WALE") — points at the prior LIST/category as a whole, not one product; re-issue the
# prior subject with this turn's new filter rather than binding to a single antecedent.
_PLURAL_REF = re.compile(r"\b(those|these|them|the (?:available|unavailable|sold[\s-]?out|"
                         r"in[\s-]?stock|cheaper|cheapest|priciest|expensive|remaining) ones?|"
                         r"the ones|wal[aei])\b", re.I)
# list/availability framing to strip when recovering the prior turn's category subject
_SUBJ_DROP = {"list", "lists", "listing", "please", "available", "unavailable", "sold",
              "soldout", "out", "stock", "instock", "status", "whether", "show", "give",
              # Roman-Urdu framing words to strip when recovering the prior category
              "aur", "wala", "wale", "wali", "mein", "ka", "ki", "ke", "dono", "batao",
              "bata", "dikhao", "abhi", "stockmein", "summary", "short"}
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
    for m in re.finditer(r"(?:^|[.!]\s+|;\s+)([A-Z0-9][^.!?\n]{3,80}?)\s+is\s+"
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


def resolve_followup(q: str, history: list) -> str:
    """Return a self-contained version of q when it's a bare follow-up referencing a
    prior product; otherwise return q unchanged."""
    try:
        if not q or not history:
            return q
        ql = q.lower()
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
        # Only act on a genuine dangling follow-up: referential, or a bare price/stock/
        # comparison ask with no subject of its own.
        if not (is_ref or is_cmp or is_price or is_stock):
            return q
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
        is_both = bool(re.search(r"\b(both|dono|those two|these two)\b", ql))
        if is_stock and not is_price and (is_both or is_cmp):
            names = _antecedents(history, 2)
            if len(names) >= 2:
                return f'are "{names[0]}" and "{names[1]}" in stock?'
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
