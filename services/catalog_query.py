"""Universal structured-catalog query engine.

A product catalog is a TABLE. "How many sharpeners", "cheapest gel pen",
"products under 500", "do you sell backpacks", "list kuromi items" are all the
SAME operation — filter rows, then aggregate — differing only in the aggregation
and the wording of the reply. So instead of a separate hand-written answerer per
question shape (the count/bounds/rank/absence pile this replaces), we parse the
question into an orthogonal (filter, aggregation) spec and execute it
deterministically against the product rows. New question shapes that combine the
same dimensions (e.g. "cheapest in-stock gel pen between 100 and 200") work for
free — no new code.

Per-tenant vocabulary (what counts as an accessory/refill that should not inflate
a count, e.g. "Stapler Pin", ink "Cartridge") stays in the DB config
(`count_exclude_terms`), never hardcoded here — the engine is domain-agnostic. A
term is only excluded when the user did not explicitly ask for it.
"""
from __future__ import annotations

import difflib
import re
import time

# Per-DB parsed-row cache. load_rows() does a FULL Chroma scan + parse on every
# query, which serializes badly under concurrent multi-user load. Cache the parsed
# rows keyed on (collection, row-count): a re-ingest changes the count → auto-refresh;
# otherwise reuse for _ROW_CACHE_TTL. Rows are read-only downstream, so sharing is safe.
_ROW_CACHE: dict = {}
_ROW_CACHE_TTL = 300.0


def _row_cache_key(db, total: int):
    db_name = (
        getattr(db, "_db_name", None)
        or getattr(db, "db_name", None)
        or getattr(db, "name", None)
        or getattr(getattr(db, "_client", None), "_db_name", None)
    )
    collection = getattr(db, "_collection", None)
    collection_name = getattr(collection, "name", None)
    collection_id = getattr(collection, "id", None) or id(collection)
    return (str(db_name) if db_name else id(db), str(collection_name or ""), str(collection_id), total)
from dataclasses import dataclass, field

# Universal question scaffolding — these words are never product-category anchors.
_STOP = {
    "how", "many", "number", "count", "of", "do", "does", "did", "you", "your", "yours", "we",
    "our", "ours", "they", "them", "the", "a", "an", "store", "shop", "site", "sell", "sells",
    "sold", "selling", "stock", "stocked", "carry", "carries", "carried", "have", "has", "had",
    "offer", "offers", "offered", "got", "keep", "keeps", "any", "some", "are", "is", "was",
    "were", "there", "here", "available", "availability", "in", "on", "at", "for", "with", "and",
    "or", "to", "us", "me", "my", "mine", "it", "its", "this", "that", "these", "those", "all",
    # possessive "their/theirs" — "list all the gaming toys and THEIR prices" must
    # anchor on "gaming toys", not leak "their" into the set and break the EXACT
    # collection-membership match ({gaming,toys,their} != {gaming,toys}). Deliberately
    # NOT his/her/hers — those are real product names ("Burberry Her") in other tenants.
    "their", "theirs",
    "products", "product", "items", "item", "thing", "things", "stuff", "one", "ones",
    "ok", "okay", "different", "types", "type", "kinds", "kind",
    "total", "catalog", "catalogue", "inventory", "range", "selection", "what", "whats", "which",
    "show", "list", "tell", "give", "display", "price", "prices", "priced", "pricing", "cost",
    "costs", "costing", "much", "cheap", "cheaper", "cheapest", "lowest", "least", "expensive",
    "priciest", "costliest", "dearest", "highest", "most", "affordable", "budget", "under",
    "below", "less", "than", "within", "max", "maximum", "minimum", "min", "over", "above",
    "more", "greater", "between", "up", "starting", "rs", "pkr", "rupees", "rupee", "dollar",
    "dollars", "currently", "entire", "whole", "absolute", "absolutely", "overall", "anything",
    "everything",
    # indefinite pronouns — a closed grammatical class, never a product anchor
    # ("show me SOMETHING between 600-800" must list the range, not search for a
    # product literally named "something"). anything/everything were here; the rest
    # of the class (something/nothing/someone/...) were the gap.
    "something", "somethin", "sumthin", "nothing", "someone", "anyone", "everyone", "none",
    "sale", "best", "top", "good", "quality", "options", "option", "can", "could",
    "would", "buy", "get", "from", "want", "need", "needed", "looking", "find", "please", "name",
    "actually", "right", "now", "among", "only", "ignoring", "ignore", "excluding", "exclude", "except",
    "out", "im",
    # price/value filler — never product-name anchors (else "what's the going RATE
    # for X" makes going/rate junk anchors that crowd out X's distinctive words)
    "going", "rate", "rates", "run", "runs", "damage", "charge", "charges", "charged",
    "worth", "value", "asking", "ask", "pay", "paying", "spend", "deal", "going-rate",
    # query scaffolding for category / name-substring / list filters — never anchors
    # ("most expensive in the Models CATEGORY", "products WHOSE name CONTAINS scale",
    # "include EVERY EXACT price"). Without these the engine matches rows against
    # 'category'/'contains'/'exact' and finds nothing → false "no such category".
    "category", "categories", "categorized", "categorised", "section", "department",
    # "collection"/"collections" is storefront-taxonomy scaffolding ("Bikes COLLECTION
    # cheapest model") — the collection NAME is the anchor, never the literal word, which
    # matches no title/tag and zeroes an all-anchor _hit.
    "collection", "collections",
    # availability-split scaffolding — "bike availability SUMMARY", "stock BREAKDOWN",
    # "available VS UNAVAILABLE" must anchor on the category, not these report words.
    "summary", "breakdown", "overview", "unavailable", "vs", "versus", "status",
    # list-verb inflections — "products LISTED in", "items LISTING under", "what LISTS
    # under X" must anchor on X, not keep "listed"/"listing" (which match no category
    # → false "no such category" and a fall-through to fuzzy RAG). "list" was already
    # a stop-word; its inflections were the gap that broke "products listed in <cat>".
    "listed", "listing", "lists",
    "whose", "contains", "contain", "containing", "include", "includes", "including",
    "included", "exact", "exactly", "every", "each", "named", "calling", "called",
    "answer", "yes", "no",  # "...carry X? answer yes or no" — keep X, drop the rest
    # conversational greeting/filler — "hi, do you guys have the X" must anchor on X,
    # not echo "hi guys" into the product ("we carry hi guys wall climbing car").
    "hi", "hey", "hello", "hiya", "guys", "oh", "um", "umm", "hmm", "thanks", "thank",
    # follow-up adverbs — "do you ALSO stock X", "have X TOO" must anchor on X, not
    # fold "also"/"too" into the name ("also wish women by chopard" → false absence,
    # which then breaks a pronoun follow-up that has no antecedent to bind).
    "also", "too", "aswell", "additionally", "again", "cool", "nice", "great", "wow",
    # stock/list filler — "which RC construction toys are still in stock" must
    # anchor on "rc construction", not "still/toys" which are presentation words.
    "still", "toy", "toys",
    # advisory persona words — express who-it's-for, never a product type, and never
    # appear in a real title ("BEGINNER ke liye cheap car", "for a NEWBIE"). (Purpose
    # words like "gift" are NOT globally stopped — they ARE real giftable-set titles in
    # other tenants; they're dropped conditionally in _selective_anchors instead.)
    "beginner", "beginners", "newbie", "liye", "lye",
    # texting abbreviations of stop-words — "do u HV mgahribi" must not keep "hv" as a
    # junk anchor (it dragged query-coverage below the floor and sank the real match).
    "hv", "hav", "ur", "pls", "plz", "thru", "abt", "coz", "cuz", "wanna", "gimme", "lemme",
    # availability / buy-intent / negation framing — these express the STOCK dimension
    # or question scaffolding, never a product NAME, yet they leaked as hard anchors:
    # "give UNAVAILABLE rc construction", "rc construction WHETHER available or sold
    # out", "F1 READY to buy", "figures that CANNOT BE BOUGHT" → an all-anchor _hit no
    # title satisfies → false abstain. Availability is already captured separately
    # (in_stock / out_of_stock), so the words that voice it must not also filter names.
    # ("sold"/"out"/"available"/"buy"/"order" were already stops; these were the gaps.)
    "unavailable", "availble", "avilable", "avalable", "availabe", "avaliable",
    "soldout", "outofstock",
    "buyable", "sellable", "purchasable", "orderable", "ready",
    "whether", "not", "cannot", "cant", "bought", "be", "ship", "ships", "shipped",
    # Roman-Urdu/Hinglish function & intent words — grammatical glue, never a product
    # name in any catalog, so they must never become anchors ("action figures MEIN AUR
    # DONO BATAO" must anchor on "action figures"). Universal language knowledge, not
    # per-store config. (English-ambiguous words like "do"/"me" are deliberately omitted.)
    "hai", "hain", "kya", "kia", "mein", "mai", "aur", "ya", "phir", "konsa", "kaunsa",
    "konsi", "kaunsi", "kaun", "kis", "sasta", "sasti", "mehnga", "mehngi", "mehnge",
    "zyada", "ziada", "chahiye", "chaiye", "chahye", "milega", "milegi", "mil", "jayega",
    "jaega", "jayegi", "hoga", "hogi", "sakta", "sakti", "sakte", "hun", "kitna", "kitni",
    "kitny", "kitne", "kitnay", "wala", "wali", "wale", "abhi", "karo", "batao", "bata",
    "bta", "dedo", "dikhao", "dikha", "dikhaiye", "acha", "achha", "theek", "yaar", "dono",
    "rakhte", "rakha", "nahi", "nai", "nhi", "ko", "ki", "ka", "ke", "se",
    # Roman-Urdu "including / combined with" connectives — "unavailable MILA KAR highest
    # price" = "highest price INCLUDING unavailable"; glue, never a product name.
    "mila", "kar", "milakar", "milakr", "samet", "sameth", "saath", "sath",
    # Roman-Urdu superlative/comparative & "even if" framing — "SABSE mehnga CHAHE
    # khatam HO CHUKA ho" (= priciest even if sold out); function words, never a name.
    "sabse", "sab", "chahe", "chahy", "chuka", "chuki", "ho", "counting", "even",
    # Roman-Urdu stock-state words — captured as in/out-of-stock signals elsewhere,
    # never product names ("kitne HAAZIR kitne GHAYAB", "KHATAM ho chuka").
    "haazir", "hazir", "ghayab", "ghaib", "mojood", "maujood", "khatam", "khtm",
}

_NUM = r"(?:rs\.?\s*|pkr\s*|[\$£€])?\s*([\d][\d,]*(?:\.\d{1,2})?)"


def _num(s: str):
    try:
        return float(str(s).replace(",", ""))
    except (TypeError, ValueError):
        return None


@dataclass
class Row:
    title: str
    price: float | None
    availability: str
    source: str
    hay: str  # lowercased title + categories + body head, for anchor matching
    currency: str = "Rs."  # detected per-row; engine output stays currency-agnostic
    cats: str = ""  # structured categories only (product_type + tags) for tag-precise counting
    colls: str = ""  # "|name|name|" of EXACT storefront collections (sidebar groups) for membership listing
    colls_disp: str = ""  # original-case collection titles ("A | B") for "which section is X in" answers
    original_price: float | None = None  # compare-at / regular price when on sale (else None)


@dataclass
class Spec:
    agg: str  # count | exists | min | max | list | none
    anchors: list = field(default_factory=list)
    price_min: float | None = None
    price_max: float | None = None
    in_stock: bool = False
    structured: bool = True
    strict_min: bool = False  # "strictly above/over X" → exclude p == X
    strict_max: bool = False  # "strictly below/under X" → exclude p == X
    title_only: bool = False  # "whose product NAME contains X" → match title, not body
    out_of_stock: bool = False
    include_oos: bool = False  # "list X incl. unavailable / overall / available OR sold out" → no stock filter, show status, true extremes
    alt_groups: list = field(default_factory=list)  # slash-joined synonyms ("motorcycle/bike") → match EITHER, not both


# Universal ENGLISH category synonyms — a customer's everyday word for a product
# TYPE vs the merchant's tag for the same thing. This is language knowledge, NOT a
# per-store word list: it holds in any catalog, so a store that tags its products
# "Bikes" still resolves a "motorcycle" query (and vice-versa). Bidirectional; each
# entry is matched as a phrase so multi-word values work. Kept deliberately tiny and
# only for genuinely universal type-synonyms validated against the live catalogs —
# brand/model words are never synonyms and must not go here.
_CAT_SYN: dict[str, set[str]] = {
    "motorcycle": {"bike"}, "motorbike": {"bike"}, "bike": {"motorcycle", "motorbike"},
}


def _inflect(s: str) -> set[str]:
    """Singular<->plural forms of one term (the -y/-ies and -s/-es rules)."""
    vs = {s}
    if len(s) < 3:
        return vs
    if s.endswith("ies"):
        vs.add(s[:-3] + "y")          # batteries -> battery
    elif s.endswith("y"):
        vs.add(s[:-1] + "ies")        # battery -> batteries
    if s.endswith("es") and len(s) > 4:
        vs.add(s[:-2])                # boxes -> box
    if s.endswith("s"):
        vs.add(s[:-1])                # pens -> pen
    else:
        vs.add(s + "s")               # pen -> pens
        vs.add(s + "es")              # box -> boxes
    return vs


def _variants(a: str) -> set[str]:
    """Singular<->plural so 'pens'~'pen' and 'battery'~'batteries', PLUS universal
    category synonyms ('motorcycle'~'bike') so a query word resolves against the
    merchant's actual tag. (rc is handled in _hit.)"""
    vs = _inflect(a)
    for syn in _CAT_SYN.get(a, ()):           # 'motorcycle' -> 'bike(s)'
        vs |= _inflect(syn)
    return vs


def _hit(row: Row, anchors: list[str], title_only: bool = False) -> bool:
    """ALL anchors must appear in the row (a 'gel pen' query needs both words).
    title_only restricts the match to the product title — a "whose name contains
    X" query must not match a product whose body/description merely mentions X
    (e.g. a swim ring whose description cross-sells a 'vest')."""
    if not anchors:
        return True
    h = row.title.lower() if title_only else row.hay
    for a in anchors:
        if a == "rc":
            if re.search(r"\brc\b", h) or "remote control" in h or (
                re.search(r"\bremote\b", h) and re.search(r"\bcontrol", h)):
                continue
            return False
        if not any(re.search(rf"\b{re.escape(v)}\b", h) for v in _variants(a)):
            return False
    return True


def _cats_hit(row: Row, anchors: list[str]) -> bool:
    """Like _hit but against the STRUCTURED categories field only (product_type +
    tags), never the title or body. A count of 'claws' should be the products
    tagged as claws — not every title that happens to contain the substring."""
    c = row.cats
    if not anchors or not c:
        return False
    for a in anchors:
        if a == "rc":
            if re.search(r"\brc\b", c) or "remote control" in c:
                continue
            return False
        if not any(re.search(rf"\b{re.escape(v)}\b", c) for v in _variants(a)):
            return False
    return True


def _anchor_set(anchors: list[str]) -> set[str]:
    s: set[str] = set()
    for a in anchors:
        s |= _variants(a)
    return s


def _resolve_collection(anchors: list[str], rows: list[Row]) -> list[Row] | None:
    """UNIVERSAL category resolution — map a customer's category phrasing onto the
    store's OWN storefront collections (shipped at ingest from /collections.json),
    instead of a hand-maintained per-store synonym map. This is the durable answer to
    the synonym treadmill: the merchant's taxonomy + this catalog's token frequency
    resolve "f1 cars"->"formula one f1", "formula one"->"formula one f1",
    "rc constraction"(typo)->"rc construction", "bike models"->"bikes" with no code
    change per store, and new stores work automatically.

    Each collection is scored by how well it covers the query's SELECTIVE (low
    document-frequency) anchors: a collection-name token match (full weight), a
    typo-tolerant fuzzy name match (0.8), or a MAJORITY of the collection's members'
    titles carrying the token (0.5, e.g. "excavator" lives in titles, not the
    "rc construction" name). Selectivity weighting (inverse-DF) makes the rare anchor
    decide, so "f1 cars" follows "f1" (rare) not "cars" (generic). At least one
    *selective* anchor must be covered → a generic phrase ("cheapest car") never locks
    onto a random collection. Returns the best collection's members, or None."""
    colls: dict[str, list[Row]] = {}
    for r in rows:
        for el in r.colls.split("|"):
            if el:
                colls.setdefault(el, []).append(r)
    if not colls:
        return None
    toks = [t for t in dict.fromkeys(anchors) if len(t) >= 2 and t not in _STOP]
    if not toks:
        return None
    # Per-token weight (inverse document-frequency) + selectivity, computed ONCE.
    tinfo = []
    for t in toks:
        df = _anchor_df(t, rows)
        tinfo.append((t, _variants(t), 1.0 / max(df, 0.02), df <= _GENERIC_DF))
    # Per-token member-title hit ratio, computed once per (token, collection) lazily.
    best: tuple[float, list[Row]] | None = None
    for cnm, mem in colls.items():
        cwords = re.sub(r"[^a-z0-9]+", " ", cnm.lower()).split()
        cset = set(cwords)
        score = 0.0
        cov_sel = False
        for t, tv, w, sel in tinfo:
            if tv & cset:                           # exact/plural token in collection name
                score += w
                cov_sel = cov_sel or sel
            elif any(difflib.SequenceMatcher(None, t, cw).ratio() >= 0.84 for cw in cwords):
                score += 0.8 * w                    # typo-tolerant ("constraction"~"construction")
                cov_sel = cov_sel or sel
            else:                                   # token lives in MEMBER titles, not the name
                pat = re.compile(rf"\b{re.escape(t)}\b")
                hits = sum(1 for m in mem if pat.search(m.hay))
                if hits / len(mem) >= 0.5:
                    score += 0.5 * w
                    cov_sel = cov_sel or sel
        if score > 0 and cov_sel and (best is None or score > best[0]):
            best = (score, mem)
    return best[1] if best else None


# A token present in a large share of the catalog is a generic TYPE/category word
# ("car" in a car store, "pen" in a stationery store) — it carries no selectivity,
# so demanding it as a HARD anchor wrongly collapses an aggregation set (a "cheapest
# RC drift car" then ranks only the lone product whose tags literally spell "car").
_GENERIC_DF = 0.15


def _anchor_df(tok: str, rows: list[Row]) -> float:
    """Document frequency of an anchor across the catalog (fraction of product rows
    whose hay contains the token or a plural/singular variant)."""
    if not rows:
        return 0.0
    pats = [re.compile(rf"\b{re.escape(v)}\b") for v in _variants(tok)]
    n = sum(1 for r in rows if any(p.search(r.hay) for p in pats))
    return n / len(rows)


# Occasion + recipient-persona words: they name WHO/WHAT-FOR, never a product type.
# Dropped from a multi-anchor match when a real product-type anchor remains, so
# "gift car for kids" resolves on "car" (not a coincidental "gift"/"kids"-tagged row).
_PURPOSE = {"gift", "gifts", "present", "presents",
            "kid", "kids", "child", "children", "toddler", "toddlers", "baby", "babies",
            "boy", "boys", "girl", "girls", "teen", "teens",
            "bacha", "bache", "bachay", "bachi", "bachon", "bache"}


def _selective_anchors(anchors: list[str], rows: list[Row], agg: str | None = None) -> list[str]:
    """Drop generic catalog-wide type-nouns from the MATCHING anchor set so they no
    longer over-constrain count/min/max/list — but only when a more specific (low
    document-frequency) anchor remains, so a query that is genuinely about the whole
    type ("cheapest car", "how many models") keeps every anchor. 'rc' is a feature the
    engine already treats specially everywhere (never a generic type word), so it is
    never dropped. The display label still uses the full anchor list. Data-driven from
    THIS catalog's own distribution → no per-store word lists."""
    if len(anchors) <= 1 or not rows:
        return anchors
    # Purpose words ("gift", "present") name the OCCASION, not a product type, yet some
    # stores tag random items "gift" → a 2-anchor strict hit collapses to that one
    # coincidental row (a Suzuki bike tagged "gift") instead of the asked-for category.
    # Drop them when a real (non-purpose) anchor remains ("decor gift" → "decor"); a lone
    # "gift" browse keeps it. Done here (not _STOP) so "Kuromi Gift Set" titles still match.
    _nonpurpose = [a for a in anchors if a not in _PURPOSE]
    if _nonpurpose and len(_nonpurpose) < len(anchors):
        anchors = _nonpurpose
        if len(anchors) <= 1:
            return anchors
    df = {a: _anchor_df(a, rows) for a in anchors if a != "rc"}
    # A token NO product contains (df == 0) can only zero-out an all-anchor match — a
    # descriptor the catalog doesn't use ("SUPERHERO action figures" where they're
    # tagged just "action figures"). For a browse LIST, drop it when a matching anchor
    # remains so the category still resolves instead of abstaining. NOT for count/exists
    # (a 0 is a meaningful precise answer) and NOT for min/max — there a stray unknown
    # word ("most COSTLY remote control car") is better left as a safe abstain→RAG than
    # silently broadened into a wrong extreme over an unintended subset.
    if agg in ("list", "stock"):
        # "stock" (status of a NAMED product) is included so a hard typo the corrector
        # can't fix ("hulk action FIGER stock?") drops out when a selective anchor
        # ("hulk") survives → we still report that product's availability instead of
        # abstaining. Like "list", never for count/exists/min/max (a 0 is meaningful).
        positive = [a for a in anchors if a == "rc" or df.get(a, 0.0) > 0.0]
        # Only drop the unknown when a genuinely SELECTIVE anchor (0 < df < generic)
        # survives, so "SUPERHERO action figures" resolves to the figures — but a query
        # of mostly unknown tokens (two typos: "rc CONSTRACTION AVAILBLE") doesn't strip
        # down to a lone generic ("rc") and dump the whole type; it stays unmatched →
        # graceful fuzzy/RAG instead of a confidently-wrong list.
        _surv_selective = any(0.0 < df.get(a, 0.0) < _GENERIC_DF for a in positive)
        if positive and len(positive) < len(anchors) and _surv_selective:
            anchors = positive
            df = {a: v for a, v in df.items() if v > 0.0}
            if len(anchors) <= 1:
                return anchors
    selective = [a for a in df if df[a] < _GENERIC_DF]
    if not selective:
        return anchors
    return [a for a in anchors if a == "rc" or df.get(a, 0.0) < _GENERIC_DF]


def _title_cover(title: str, anchor_set: set[str], anchor_toks: list[str] | None = None) -> float:
    """How much of THIS product's title the query names. Catches a query that adds
    descriptor words beyond the stored (canonical) title — e.g. asking for
    'Pop N Play - Quick Push Pop Game' when the row title is just 'Pop N Play'.
    The strict all-anchor _hit fails there (extra words absent from the title);
    title-coverage resolves it by measuring the other direction. A title word also
    counts when it only fuzzy-matches a query word (typo tolerance: 'leanring' names
    'learning'), so one misspelling can't sink an otherwise fully-named product."""
    tt = [t for t in re.findall(r"[a-z0-9]+", (title or "").lower()) if len(t) > 1]
    if len(tt) < 2:
        return 0.0
    return sum(1 for t in tt if t in anchor_set
               or (anchor_toks and _fuzzy_in(t, anchor_toks))) / len(tt)


def _query_cover(title: str, anchor_toks: list[str] | None) -> float:
    """Reverse of _title_cover: fraction of the QUERY's anchor words this title names.
    High when the customer typed a SUBSET of a long SEO title — "Rasha For Women By
    Al" for stored "Rasha For Women By Al Rehab EDP" — where the title's extra words
    sink one-directional title-coverage below the floor. Fuzzy so a typo'd query word
    ("Rsaha") still counts. Anchors are already stop-stripped (distinctive tokens),
    so a sibling sharing only generic words can't reach the floor."""
    aw = [t for t in (anchor_toks or []) if len(t) > 1]
    if not aw:
        return 0.0
    tt = list({t for t in re.findall(r"[a-z0-9]+", (title or "").lower()) if len(t) > 1})
    return sum(1 for a in aw if a in tt or _fuzzy_in(a, tt)) / len(aw)


_ONES = {"zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
         "six": 6, "seven": 7, "eight": 8, "nine": 9}
_TEENS = {"ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
          "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19}
_TENS = {"twenty": 20, "thirty": 30, "forty": 40, "fifty": 50, "sixty": 60,
         "seventy": 70, "eighty": 80, "ninety": 90}


def _normalize_numwords(ql: str) -> str:
    """Spelled-out numbers → digits so a model/scale typed in words resolves like its
    numeric form: 'm eight one twenty four' → 'm8 1 24' → matches 'BMW M8 1:24'.
    Universal language normalization (a tens+ones pair combines: 'twenty four'→24),
    then a single letter glued to a following digit forms a model code ('m 8'→'m8')."""
    toks = ql.split()
    out = []
    i = 0
    while i < len(toks):
        t = toks[i]
        if t in _TENS:
            v = _TENS[t]
            if i + 1 < len(toks) and toks[i + 1] in _ONES and _ONES[toks[i + 1]] != 0:
                v += _ONES[toks[i + 1]]; i += 1
            out.append(str(v))
        elif t in _TEENS:
            out.append(str(_TEENS[t]))
        elif t in _ONES:
            out.append(str(_ONES[t]))
        else:
            out.append(t)
        i += 1
    s = " ".join(out)
    return re.sub(r"\b([a-z])\s+(\d)", r"\1\2", s)


# Shipping / delivery / returns / refund / warranty / payment / order-status questions
# are POLICY prose, not catalog lookups. Used both to make parse() decline AND to
# short-circuit answer_catalog_query BEFORE the LLM router (which would otherwise
# fabricate a product plan for "used opened PRODUCT return" and answer with a catalog
# non-answer instead of handing to the policy/RAG path). "return" alone is excluded so
# the catalog verb "return every available item under 500" is never read as policy.
_POLICY_Q_RE = re.compile(
    r"\b(shipping|delivery|deliver(?:ed|ing)?|postage|courier|dispatch|"
    r"returns\b|return\s+polic(?:y|ies)|return\s+(?:an?\s+)?(?:item|product|order|purchase)s?\b|"
    r"(?:item|product|order|purchase)s?\s+(?:ko\s+|wapas\s+)?returns?\b|"
    r"returns?\s+(?:ho|kar|kr|kiya|kiye)\w*\b|wapas\s+(?:kar|ho)\w*|"
    r"refunds?|warranty|guarantee|"
    r"payment\s+method|cod|cash\s+on\s+delivery|installments?|"
    r"track(?:ing)?\s+(?:my\s+)?order|order\s+status|cancel(?:lation|\s+(?:my\s+)?order)?)\b", re.I)


def parse(q: str) -> Spec:
    """Extract orthogonal dimensions (aggregation + price bounds + stock + anchors)
    from the question. Each dimension is detected independently, so any
    combination composes — that is what makes the engine universal."""
    ql = (q or "").lower()
    # Normalize texting abbreviations BEFORE intent detection. These were only in
    # _STOP (anchor removal), so "do u hv X" kept the right anchors but the intent
    # regexes (which look for literal "have"/"you") never fired → the whole query
    # fell through to RAG. Expanding the verbs here routes SMS-speak ("do u hv",
    # "wat do u sell", "u got any") to the catalog engine like its spelled-out twin.
    ql = re.sub(r"\b(u|ur|hv|hav|pls|plz|wat|wats|abt|thru|coz|cuz|wanna|gimme|lemme|gud|gng)\b",
                lambda m: {"u": "you", "ur": "your", "hv": "have", "hav": "have",
                           "pls": "please", "plz": "please", "wat": "what", "wats": "whats",
                           "abt": "about", "thru": "through", "coz": "because", "cuz": "because",
                           "wanna": "want to", "gimme": "give me", "lemme": "let me",
                           "gud": "good", "gng": "going"}[m.group(0)], ql)

    # Spelled-out numbers → digits ("m eight one twenty four" → "m8 1 24") so a model
    # or scale typed in words matches its numeric title form. Done before anchoring.
    ql = _normalize_numwords(ql)

    # --- Split-count detection (report BOTH stock sides together) --------------------
    # Customers ask for "the available vs sold-out counts" in unbounded ways ("stock
    # divided: total, purchasable, unavailable", "count split karo", "stock ratio with
    # counts", "total available sold count", "availability breakdown"). Enumerating each
    # phrasing is the recurring brittleness — detect the SEMANTIC class instead:
    #   (a) two OPPOSITE stock states joined by a connector ("available aur sold out",
    #       slash-joined "total/in-stock/OOS"), OR
    #   (b) the two opposite states present together under an explicit COUNTING intent
    #       (count/total/how many/kitne) — covers comma/adjacent phrasings with no
    #       connector ("total available sold count"), OR
    #   (c) a split/breakdown/ratio/divided/report word in a stock/count context.
    # Any → count_split, so the engine reports total + available + sold-out. The count
    # gate in (b) keeps a casual "available … sold" (no counting intent) from triggering,
    # and a "not sold out" double-negative (handled below) carries neither a connector
    # nor a count cue, so it stays an in-stock filter.
    _AVAIL = (r"\b(?:available|in[\s-]?stock|purchasable|buyable|sellable|live|"
              r"up\s+for\s+grabs|ready[\s-]?to[\s-]?ship|in[\s-]?hand|on[\s-]?hand|"
              r"mojood|maujood|haazir|hazir|mil\s+sakt\w*|milt\w*|mileg\w*)\b")
    _SOLD = (r"\b(?:sold[\s-]?out|sold|out\s+of\s+stock|unavailable|oos|gone|done|finished|"
             r"back[\s-]?order(?:ed|s)?|backorder\w*|out[\s-]?the[\s-]?door|"
             r"khatam|khtm|ghayab|ghaib)\b")
    _CONN = r"(?:\b(?:and|or|vs\.?|versus|aur|ya)\b|[&/])"
    _CNT = r"\b(?:count|counts|how\s+many|number\s+of|numbers?|tally|total|kitne|kitni|kitny|kitna)\b"
    _SPLITW = r"\b(?:split|divided|breakdown|break[\s-]?up|breakup|ratio|report)\b"
    _two_conn = bool(re.search(_AVAIL + r"[^.?!]{0,40}" + _CONN + r"[^.?!]{0,40}" + _SOLD, ql)
                     or re.search(_SOLD + r"[^.?!]{0,40}" + _CONN + r"[^.?!]{0,40}" + _AVAIL, ql))
    _two_adj = bool(re.search(_AVAIL + r"[^.?!]{0,40}" + _SOLD, ql)
                    or re.search(_SOLD + r"[^.?!]{0,40}" + _AVAIL, ql))
    _has_cnt = bool(re.search(_CNT, ql))
    split_report = bool(re.search(_SPLITW, ql) and re.search(
        r"\b(?:stock|count|counts|availab\w*|collection|inventory|status|numbers?)\b", ql))
    # "both sides"/"each side"/"dono side" idiom for the two stock states together.
    both_sides = bool(re.search(r"\b(?:both|each|dono)\s+sides?\b", ql)
                      and re.search(r"\b(?:availab\w*|stock|count|sold)\b", ql))
    # Roman-Urdu "kitne bik gaye, kitne bache" idiom = how many SOLD vs how many LEFT.
    # Requires BOTH the sold root (bik-) and the remaining root (bach-) so the bare
    # "bachon ke liye" (= for children) never reads as a stock split.
    _urdu_split = bool(re.search(r"\bbik\w*\b", ql) and re.search(r"\bbach\w*\b", ql))
    # Roman-Urdu PARALLEL count "kitne X kitne Y" (= how many ... how many ...) — two
    # kitne/kitni clauses is itself a two-way count split ("kitne mil sakte kitne nahi"
    # = how many available vs how many not). Gated on a get/stock/availability verb so a
    # non-stock "kitne red kitne blue" doesn't fire.
    _kitne_split = bool(len(re.findall(r"\bkitn[aeiouy]\w*\b", ql)) >= 2
                        and re.search(r"\b(?:mil|milt\w*|mileg\w*|sakt\w*|stock|availab\w*"
                                      r"|bik\w*|bach\w*|nahi|nai|khatam|order\w*)\b", ql))
    # "in/out" (or "in vs out") slash/connector dichotomy carrying a count cue —
    # "RC cars in/out tally", "stock in vs out count" — wants the both-sides split.
    _inout = bool(re.search(r"\bin\s*/\s*out\b|\bin\s+(?:vs\.?|versus|and|or)\s+out\b", ql)
                  and re.search(r"\b(?:tally|count|counts|numbers?|stock|status|breakdown)\b", ql))
    both_states = (_two_conn or (_has_cnt and _two_adj) or split_report or both_sides
                   or _urdu_split or _inout or _kitne_split)

    # Comparisons ("compare A and B", "X vs Y") are NOT a single structured
    # aggregation — they need the retrieval fan-out (which matches each named
    # product in both name directions) + LLM synthesis. Hand them off; otherwise a
    # "compare the price of A and B" would be caught by the price-of list path and
    # return whichever product's title the query happens to fully name. A stock-state
    # split ("available vs unavailable") is NOT a product comparison → excluded.
    if (not both_states
            and re.search(r"\b(compare|compared|comparison|versus|vs\.?|difference\s+between)\b", ql)
            and re.search(r"\b(and|or|vs\.?|versus|than)\b", ql)):
        return Spec("none", structured=False)

    # Advisory / recommendation intent ("which toy is best for a 2 year old",
    # "recommend a gift for a toddler", "what's good for sensitive skin") is a
    # SUBJECTIVE judgement, not a table filter — let RAG/LLM answer. Without this
    # the structured path fires on incidental tokens ("2 year" matched a mic's
    # "2 Year warranty") and dumps an irrelevant product as a confident answer.
    # Excludes the structured superlatives (cheapest/most expensive) — those have
    # their own price words and never carry these advisory cues.
    if re.search(
        r"\b(recommend\w*|suggest\w*|advise|advice)\b"
        # "best/good/ideal … for <recipient/use>" — recipient list avoids catching
        # the structured "best PRICE for this pen" (→ stays a price lookup).
        r"|\b(?:best|good|ideal|suitable|appropriate|perfect|great|right)\s+(?:\w+\s+){0,2}?for\s+"
        r"(?:a\b|an\b|my\b|your\b|kids?|children|child|toddler|baby|babies|boys?|girls?|teens?|"
        r"men|women|him|her|sensitive|oily|dry|daily|everyday|beginners?|gift|travel|school|office)"
        r"|\bgift\s+for\b|\bfor\s+(?:a|an|my)\s+\d+[\s-]*(?:year|yr|month|mo)s?[\s-]*old\b"
        r"|\bwhich\s+(?:one\s+)?(?:should|would|do)\s+(?:i|you)\b", ql):
        return Spec("none", structured=False)

    # Shipping / delivery / returns / payment / order questions are POLICY prose,
    # not product-catalog lookups — but "how much is delivery" trips the price-of
    # detector and "shipping charges" trips the list path, dumping a random product.
    # Hand to normal RAG so it answers from the policy page (or says it doesn't have
    # the info) rather than returning an unrelated product.
    # NB: "return" alone is excluded — the catalog verb ("return every available
    # item under 500") must not read as a returns-policy question; only plural
    # "returns", "return policy", or "return an item/order" count as policy.
    if _POLICY_Q_RE.search(ql):
        return Spec("none", structured=False)

    # Variant questions — "what COLOURS/SIZES/OPTIONS does X come in", "what sizes are
    # available" — are answered from the product chunk's Options/Variants breakdown by
    # RAG. The structured aggregator would resolve the product and return a bare
    # "<name> - Rs.X - available" line without ever naming the colours/sizes.
    if (re.search(r"\b(colou?rs?|sizes?|shades?|variants?|versions?|options?)\b", ql)
            and re.search(r"\bcome\s+in\b|\bcomes\s+in\b|\bavailable\s+in\b|\boffered\s+in\b"
                          r"|\bdoes\s+(?:it|this|that|the)\b|\bwhat\s+(?:colou?rs?|sizes?|variants?|options?)\b", ql)):
        return Spec("none", structured=False)

    # "which/what brands do you carry", "list the categories" = ENUMERATION of
    # brands/categories, not a product lookup — "do you carry" trips the existence
    # branch and would dump products as if they were brands. Hand to RAG so it
    # answers from the store's brands/category page.
    if re.search(r"\b(which|what|list|name|tell\s+me\s+(?:the|your))\b[\w\s]*\b"
                 r"(brands?|designers?|makes?|labels?|categor(?:y|ies)|collections?)\b", ql):
        return Spec("none", structured=False)

    pmin = pmax = None
    m = re.search(r"between\s+" + _NUM + r"\s*(?:and|to|-|–)\s*" + _NUM, ql)
    if m:
        a, b = _num(m.group(1)), _num(m.group(2))
        if a is not None and b is not None:
            pmin, pmax = min(a, b), max(a, b)
    if pmax is None:
        m = re.search(r"(?:under|below|less\s+than|within|max(?:imum)?|budget(?:\s+of)?|up\s+to|cheaper\s+than|no\s+more\s+than)\s+" + _NUM, ql)
        if m:
            pmax = _num(m.group(1))
    if pmin is None:
        m = re.search(r"(?:over|above|more\s+than|greater\s+than|at\s+least|starting\s+(?:at|from)|priced\s+from)\s+" + _NUM, ql)
        if m:
            pmin = _num(m.group(1))

    # OUT-OF-STOCK intent, including negated availability ("not available", "isn't in
    # stock", "can't be bought") — without these "which figures are NOT available"
    # read as an IN-stock filter and returned the available ones (inverted answer).
    out_of_stock = bool(re.search(
        r"\b(out[\s-]*of[\s-]*stock|sold[\s-]*out|unavailable|"
        r"not\s+(?:available|in\s+stock)|n'?t\s+(?:available|in\s+stock)|"
        r"no\s+longer\s+(?:available|in\s+stock|sold)|"
        r"can'?t\s+be\s+(?:bought|purchased|ordered)|cannot\s+be\s+(?:bought|purchased|ordered)|"
        r"can'?t\s+(?:buy|order|get)|cannot\s+(?:buy|order|get|be\s+bought))\b", ql))
    # IN-STOCK intent, including buy-readiness phrasings ("ready to buy", "can I buy",
    # "buyable") that customers use instead of the literal "in stock".
    in_stock = bool(re.search(
        r"\b(in[\s-]?stock|in\s+stock|available|buyable|"
        r"ready\s+to\s+(?:buy|order|ship)|can\s+(?:i|we|you)\s+(?:\w+ly\s+)?(?:buy|order|get)|"
        # pronoun-BEFORE-can, allowing an adverb the customer drops in ("toys I can
        # ACTUALLY buy", "ones you can REALLY get") + bare "buy/order it now"
        r"(?:i|we|you)\s+can\s+(?:actually|really|even|still|just|also|now|finally|\w+ly\s+)?\s*(?:buy|order|get)|"
        r"(?:buy|order|purchase)\s+(?:it\s+)?(?:right\s+)?now|"
        r"still\s+(?:available|in\s+stock))\b", ql)) and not out_of_stock

    # "include unavailable / overall / available OR sold out / whether … sold out" =
    # the customer wants the WHOLE set with its status, or the true extreme across all
    # stock — not an in/out filter. Overrides the stock flags so a "list X including
    # the sold-out ones" returns every X (with availability), and "highest X overall"
    # ranks past out-of-stock instead of stopping at the priciest in-stock item.
    include_oos = bool(re.search(
        r"\bincl(?:uding|ude|\.|udes)?\s+(?:the\s+)?(?:unavailable|sold[\s-]?out|out[\s-]?of[\s-]?stock|oos)"
        r"|\bregardless\s+of\s+(?:stock|availability)"
        r"|\b(?:available|in\s+stock)\b[^.?!]{0,24}\b(?:or|and|/|&|vs\.?)\b[^.?!]{0,24}\b(?:sold[\s-]?out|out\s+of\s+stock|unavailable)"
        r"|\bwhether\b[^.?!]{0,40}\b(?:available|in\s+stock|sold[\s-]?out)"
        # "full availability list", "stock status (list)", "availability breakdown/
        # rundown/status" = report the WHOLE category with each item's status, not an
        # in/out filter (these read "available" yet want the sold-out ones shown too).
        # Roman-Urdu "including unavailable": "unavailable mila kar / samet" or
        # "mila kar … sold out" = rank/list ACROSS the sold-out ones, not filter to them.
        r"|\b(?:unavailable|sold[\s-]?out|out\s+of\s+stock|oos)\b[^.?!]{0,12}\b(?:bhi\s+)?(?:mila\s*kar|milakr|samet|sameth)\b"
        r"|\b(?:mila\s*kar|milakr|samet|sameth)\b[^.?!]{0,12}\b(?:unavailable|sold[\s-]?out|out\s+of\s+stock)\b"
        # "even if sold out", "chahe khatam/sold ho", "counting the sold ones too" =
        # rank ACROSS the sold-out ones.
        r"|\b(?:even\s+if|chahe)\b[^.?!]{0,20}\b(?:khatam|sold|out\s+of\s+stock|unavailable|bik\w*)\b"
        r"|\bcounting\b[^.?!]{0,20}\b(?:sold|unavailable|out\s+of\s+stock|gone)\b"
        r"|\b(?:sold\s+ones?|sold[\s-]?out\s+ones?)\b[^.?!]{0,8}\btoo\b"
        r"|\bfull\s+availabilit\w*"
        r"|\bstock\s+status\b"
        r"|\bavailabilit\w*\s+(?:list|status|breakdown|overview|rundown)\b"
        r"|\b(?:overall|of\s+all|in\s+total|in\s+the\s+(?:whole|entire)\s+(?:catalog|store))\b", ql))
    if include_oos:
        out_of_stock = False
        in_stock = False

    # Double negative: "available … NOT sold out" / "not out of stock" is an IN-stock
    # filter, not OOS — but the out_of_stock regex above matched the bare "sold out"
    # inside "not sold out" and inverted the answer. Override to in-stock.
    if not both_states and not include_oos and re.search(
            r"\b(?:not|n'?t|never)\s+(?:sold[\s-]?out|out\s+of\s+stock)\b", ql):
        out_of_stock = False
        in_stock = True

    # both_states (computed up front) wants BOTH stock sides → don't filter to one.
    if both_states:
        out_of_stock = False
        in_stock = False
        include_oos = True

    # Slash-joined alternatives ("motorcycle/bike", "F1 / Formula One", "superhero/
    # action") are SYNONYMS — the customer wants EITHER, but the all-anchor _hit would
    # demand BOTH and under-return (or abstain). Record the groups; execution keeps
    # only the catalog's actual term (highest doc-frequency) per group.
    alt_groups = [tuple(sorted({m.group(1), m.group(2)}))
                  for m in re.finditer(r"\b([a-z]{2,})\s*/\s*([a-z]{2,})\b", ql)]

    # Exclusive bounds: "strictly below/under" and "more/greater than", "below",
    # "under", "above", "over" exclude the boundary value. Inclusive operators
    # ("up to", "no more than", "at most", "at least", "within", "max") keep it.
    _strict = bool(re.search(r"\bstrict(?:ly)?\b", ql))
    strict_max = pmax is not None and (_strict or bool(
        re.search(r"\b(below|under|less\s+than|cheaper\s+than|fewer\s+than)\b", ql)))
    strict_min = pmin is not None and (_strict or bool(
        re.search(r"\b(above|over|more\s+than|greater\s+than)\b", ql)))

    count_q = bool(re.search(r"\b(how\s+many|number\s+of|count\s+of|count)\b", ql))
    exist_q = bool(re.search(
        r"\bdo(?:es)?\s+(?:you|we|they|the\s+store)\b.*\b(?:sell|stock|carry|carries|have|has|offer|got|keep)\b"
        r"|\b(?:are|is)\s+there\s+(?:any|some)\b"
        r"|\bdo\s+(?:you|u)\s+(?:sell|stock|carry|have|offer|got)\b"
        r"|\b(?:have\s+(?:you|we)|(?:you|we)\s+have)\s+got\b"
        r"|\bgot\s+(?:any|some)\b", ql))
    cheapest = bool(re.search(
        r"\b(cheap|cheapest|lowest[\s-]+priced?|least\s+expensive|most\s+affordable|lowest\s+cost|"
        r"costs?\s+the\s+least|"
        # Roman-Urdu/Hinglish "cheapest" — "sab se sasta", bare "sasta/sasti"
        r"sab\s+se\s+sast[aie]|sast[aie])\b", ql))
    priciest = bool(re.search(
        r"\b(most\s+expensive|highest[\s-]+priced?|priciest|costliest|most\s+costly|dearest|"
        r"highest\s+cost|costs?\s+the\s+most|"
        # Roman-Urdu/Hinglish "most expensive" — "sab se mehnga", bare "mehnga/mehngi"
        r"sab\s+se\s+mehng[aie]|mehng[aie])\b", ql))
    list_q = bool(re.search(r"\b(show|list|which|what'?s?|give|display|products?|items?|sell|have|available|unavailable|sold\s+out|out\s+of\s+stock)\b", ql))
    # Single-product price/availability lookup ("price of X", "how much is X",
    # "is X available"). Routed to the list path so the full-catalog scan (with the
    # title-coverage fallback) resolves the EXACT named product instead of the
    # similarity top-N returning same-prefix siblings. Bare price queries with no
    # product fall through the no-anchor guard below to ordinary RAG.
    priceof_q = bool(re.search(r"\b(price|prices|priced|pricing|cost|costs|costing|how\s+much|rate|rates)\b", ql))
    # Stock-STATUS question about a named product ("is X in stock?", "is X available?",
    # "do you have X in stock?"). Distinct from the in_stock FILTER ("show in-stock X"):
    # the answer must REPORT the product's availability, not silently drop it when OOS.
    stock_status_q = bool(re.search(
        r"\b(?:is|are|r)\b[^?]*\b(?:in[\s-]?stock|in\s+stock|avail\w*|sold\s+out|out\s+of\s+stock)\b"
        r"|\b(?:do|does)\s+(?:you|we|they)\b[^?]*\bin\s+stock\b"
        r"|\bavailabilit\w*\b"
        r"|\bstock\s+status\b"   # "<product> stock status", "what's the stock status of X"
        r"|\bavail\w*\s*\?\s*$"  # bare "<product> availble?" — a typo'd availability ask
        r"|\bstock\s*\?\s*$"     # bare "<product> stock?" — elliptical "in stock?" ask
        # "can I order/buy/get <named product>?" presupposes the product exists and asks
        # whether it's purchasable → report its availability (incl. OOS), never abstain
        # because an in-stock filter dropped the sold-out row.
        r"|\bcan\s+(?:i|we|you)\s+(?:still\s+)?(?:order|buy|get|purchase)\b"
        r"|\bstill\s+(?:available|in\s+stock)\b", ql))

    if count_q:
        agg = "count"
    elif cheapest:
        agg = "min"
    elif priciest:
        agg = "max"
    elif pmax is not None or pmin is not None:
        # A price bound makes it a bounds/list query even when phrased "do you
        # have any X under 50" — the incidental "do you have" must not route it to
        # the (subject-less) existence branch.
        agg = "list"
    elif stock_status_q:
        # Resolve the named product and report its availability (handles OOS too).
        agg = "stock"
    elif exist_q:
        agg = "exists"
    elif list_q or out_of_stock or in_stock:
        # A bare stock filter with no list verb ("action figures customers cannot
        # buy", "available BMWs") is still a filtered-list request — without this it
        # fell through to RAG and dumped the whole catalog. Single-product stock
        # lookups ("is X available") are caught earlier by stock_status_q, so this
        # only fires on category/filter phrasings.
        agg = "list"
    elif priceof_q:
        agg = "list"
    elif both_states:
        # A split request with no list/count verb ("Action figures stock split
        # summary?") still wants the both-sides count — route it here rather than
        # falling through to RAG (which dumped a one-sided list).
        agg = "count_split"
    else:
        return Spec("none", structured=False)

    # A category query naming both stock states → split count (overrides the bare
    # count/list/stock agg the chain picked). Needs a category anchor (below).
    if both_states and agg in ("count", "list", "stock"):
        agg = "count_split"

    # Anchors include digit tokens (a model/size/style number disambiguates a
    # specific SKU — "Spiral Notebook Style 43" must not match every style, "Paint
    # 500ml" / "Set of 48" pin the variant). Drop numbers already consumed as price
    # bounds so "products under 500" doesn't anchor on 500.
    _price_tokens = set()
    for _v in (pmin, pmax):
        if _v is not None:
            _price_tokens.add(f"{_v:.0f}")
            _price_tokens.add(str(_v))
            # Thousands-separated prices ("Rs.3,025") tokenize to a digit fragment
            # ("025") that survived the bare integer filter and leaked as an anchor.
            for _part in f"{_v:,.0f}".split(","):
                _price_tokens.add(_part)
    # A quoted span is the reliable discriminator: the user (and the hard quiz)
    # quote the exact category / name-substring being asked about ("Ball Point"
    # category, name contains "fresh"). Anchor on the quoted content so question
    # scaffolding ("Using the full current catalog, count all items in …") can
    # never leak into the filter — the stop-word treadmill never catches every
    # phrasing ("using"/"full"/"current"/"across"/"enumerate"/…). Inside explicit
    # quotes we trust the user and skip _STOP (drop only price-bound fragments).
    # No quotes → fall back to stop-word subtraction over the whole question.
    _quoted = re.findall(r'["“”]([^"“”]{1,80})["“”]', q or "")
    if _quoted:
        _qtext = " ".join(_quoted).lower()
        anchors = [w for w in re.findall(r"[a-z0-9]{2,}", _qtext)
                   if w not in _price_tokens][:8]
    else:
        anchors = [w for w in re.findall(r"[a-z0-9]{2,}", ql)
                   if w not in _STOP and w not in _price_tokens][:8]
    # In a split count the only anchor is the CATEGORY; the stock-state/report words
    # that voice the split ("split", "stock", "available", Roman-Urdu "aur"/"mein"/
    # "batao") must not survive as product anchors or they zero the all-anchor _hit.
    if agg == "count_split":
        _split_report_words = {"split", "stock", "available", "availability", "unavailable",
                               "summary", "breakdown", "overview", "status", "aur", "ya",
                               "mein", "main", "batao", "bata", "btao", "bta", "dono", "vs", "versus",
                               # breakdown/count vocabulary the semantic detector now admits —
                               # strip so only the CATEGORY anchor survives for membership lookup.
                               "ratio", "count", "counts", "report", "divided", "numbers", "number",
                               "form", "purchasable", "sold", "soldout", "oos", "total", "live",
                               "kitne", "kitni", "kitny", "kitna", "karo", "chahiye", "do",
                               # broadened stock-state / count synonyms + Roman-Urdu fillers —
                               # strip so only the CATEGORY anchor survives membership lookup.
                               "gone", "sellable", "tally", "buyable", "style", "in", "out",
                               "frame", "frames", "item", "items", "piece", "pieces",
                               "bik", "bika", "bikay", "gaye", "gaya", "bache", "bachay", "bacha",
                               "kaise", "kaisa", "kaisi", "hua", "hui", "bata", "btao", "bata",
                               # Roman-Urdu stock-state synonyms now admitted to the detector
                               "haazir", "hazir", "ghayab", "ghaib", "mojood", "maujood",
                               "khatam", "khtm", "finish", "finished", "headcount", "ready",
                               # english-creative + Roman-Urdu availability idioms the detector
                               # now admits — strip every token so only the CATEGORY survives:
                               # "up for grabs", "ready to ship", "in/on hand", "back-ordered",
                               # "out the door", "done", "dead", "sellable headcount", inventory,
                               # "mil sakte / milega", "nahi".
                               "up", "for", "grabs", "ship", "hand", "on", "to", "the", "door",
                               "back", "order", "ordered", "done", "dead", "inventory", "breakup",
                               "mil", "milt", "milta", "milte", "milti", "mileg", "milega",
                               "milegi", "milenge", "sakta", "sakte", "sakti", "nahi", "nai"}
        anchors = [a for a in anchors if a not in _split_report_words]

    # A bare "list / what products do you sell" with no category and no price
    # bound is an open-ended browse — leave that to normal RAG, don't dump a
    # price-sorted slice of the whole catalog.
    if agg == "list" and not anchors and pmin is None and pmax is None:
        return Spec("none", structured=False)
    # Likewise a bare existence question with no subject is not answerable here.
    if agg == "exists" and not anchors:
        return Spec("none", structured=False)
    # A split count needs a category to split — without one it's an open browse → RAG.
    if agg == "count_split" and not anchors:
        return Spec("none", structured=False)
    # A stock-status question needs a named product; "what's available" with no
    # subject is an open browse → RAG. The status report must NOT filter by stock
    # (it has to surface an OOS product to report it), so clear the in_stock filter.
    if agg == "stock":
        if not anchors:
            return Spec("none", structured=False)
        if re.search(r"\b(which|what'?s?|list|show|give|display)\b", ql):
            agg = "list"
            # "list X" defaults to in-stock — UNLESS the customer asked for the whole
            # set with its status ("…whether available or sold out"), where include_oos
            # must keep both states. Don't re-impose the filter it just cleared.
            if not out_of_stock and not include_oos:
                in_stock = True
        else:
            in_stock = False

    # "whose product name contains/includes X", "named X", "titled X" → the user
    # is FILTERING on the title, so don't let a body mention of X pull in an
    # unrelated product. Must require a filter verb after "name": the bare phrase
    # "include each complete product name and exact price" is an OUTPUT instruction
    # (it appears in every list template, including category queries) and must NOT
    # flip a category query to title matching. (Plain category queries keep body
    # matching + category-tag precision.)
    title_only = bool(re.search(
        r"\b(?:whose\s+)?(?:product\s+)?name\s+(?:contain\w*|includ\w*|has\b|having\b|with\b|matching\b|"
        r"that\s+(?:contain\w*|includ\w*|has\b))"
        r"|\bnamed\b|\btitled?\b|\bcalled\b", ql))

    return Spec(agg, anchors, pmin, pmax, in_stock,
                strict_min=strict_min, strict_max=strict_max, title_only=title_only,
                out_of_stock=out_of_stock, include_oos=include_oos, alt_groups=alt_groups)


def load_rows(db, limit: int = 12000) -> list[Row]:
    """One full scan of the product rows — deduped by normalized title. Prices come
    from structured metadata or a labeled 'Price:' line only (never a body-text
    scan, which manufactures phantom prices from model numbers). Result is cached
    per-DB keyed on row count (auto-invalidates after a re-ingest)."""
    try:
        total = min(int(db._collection.count() or 0), limit)
    except Exception:
        total = -1
    _ck = _row_cache_key(db, total)
    _now = time.time()
    _hit = _ROW_CACHE.get(_ck)
    if _hit is not None and _hit[1] == total and (_now - _hit[2]) < _ROW_CACHE_TTL:
        return _hit[0]
    try:
        raw = db._collection.get(limit=max(total, 0), include=["documents", "metadatas"])
    except Exception:
        return []
    rows: list[Row] = []
    seen: dict[str, Row] = {}
    for doc, meta in zip(raw.get("documents", []) or [], raw.get("metadatas", []) or []):
        meta = meta or {}
        doc = str(doc or "")
        if not doc:
            continue
        src = str(meta.get("source") or meta.get("source_canonical") or "")
        src_l = src.lower()
        kind = str(meta.get("chunk_kind") or "").lower()
        ctype = str(meta.get("content_type") or "").lower()
        # Structured-shape signal: a positive `price` meta + a product title carrier
        # (name/title meta or a "Product:" line). Catches per-product chunks tagged
        # with a category/listing URL and no chunk_kind (webscraper.io-style ingests)
        # that the URL/kind tests miss. Listing/nav chunks lack a single price meta.
        try:
            _has_price_meta = meta.get("price") is not None and float(meta.get("price")) > 0
        except (TypeError, ValueError):
            _has_price_meta = False
        _has_title_meta = any(str(meta.get(k) or "").strip() for k in ("name", "product_title", "canonical_product_title"))
        _has_prod_line = bool(re.search(r"(?im)^product:\s*\S", doc))
        is_prod = (
            bool(re.search(r"/(?:products?|items?)(?:/|$|#)", src_l))
            or (kind == "product" and ctype == "product" and "/collections/" not in src_l)
            or (_has_price_meta and (_has_title_meta or _has_prod_line) and "/collections/" not in src_l)
        )
        if not is_prod:
            continue
        title = ""
        # `name` (webscraper.io-style) and a leading "Product:" doc line are the
        # universal title carriers when a crawl/ingest didn't set *_product_title.
        for k in ("canonical_product_title", "product_title", "page_title", "name"):
            t = str(meta.get(k) or "").strip()
            if t:
                title = re.sub(r"\s+", " ", t).strip(" -:|")
                break
        if not title:
            m = re.search(r"(?im)^product:\s*([^\n]{3,120})", doc)
            if m:
                title = re.sub(r"\s+", " ", m.group(1)).strip(" -:|")
        if not title or len(title) < 4:
            continue
        tl = title.lower()
        if tl in {"fun", "home", "shop", "catalog", "catalogue", "products", "collection", "page"}:
            continue
        if len(re.findall(r"[a-zA-Z0-9]+", title)) <= 1 and not re.search(r"\d", title):
            continue
        key = re.sub(r"[^a-z0-9]+", " ", tl).strip()
        if not key:
            continue
        price = None
        try:
            mv = meta.get("price")
            if mv is not None and float(mv) > 0:
                price = float(mv)
        except (TypeError, ValueError):
            pass
        if price is None:
            m = re.search(r"(?im)^price:\s*(?:rs\.?|pkr|[\$£€])?\s*([\d,]+(?:\.\d{1,2})?)", doc)
            if m:
                v = _num(m.group(1))
                price = v if (v and v > 0) else None
        # Original (compare-at) price when the product is on sale — from structured
        # metadata, else the "(originally Rs.X…)" the ingest renders into the chunk.
        orig_p = None
        try:
            ov = meta.get("original_price")
            if ov is not None and float(ov) > 0:
                orig_p = float(ov)
        except (TypeError, ValueError):
            orig_p = None
        if orig_p is None:
            mo = re.search(r"(?i)originally\s+(?:rs\.?|pkr|[\$£€])?\s*([\d,]+(?:\.\d{1,2})?)", doc)
            if mo:
                v = _num(mo.group(1))
                orig_p = v if (v and v > 0) else None
        if orig_p is not None and price is not None and orig_p <= price:
            orig_p = None
        # Currency follows the data, not a hardcoded "Rs." — read the symbol off the
        # labeled Price: line (then any body symbol) so $/£/€ stores render correctly.
        cur = "Rs."
        cm = re.search(r"(?im)^price:\s*(Rs\.?|PKR|[\$£€])", doc) or re.search(r"([\$£€])\s*[\d]", doc)
        if cm:
            sym = cm.group(1)
            cur = sym if sym in ("$", "£", "€") else "Rs."
        avail = str(meta.get("availability") or "").strip().lower()
        if not avail:
            # Prefer the chunk's structured schema.org marker ("Avail: InStock/
            # OutOfStock") over a loose body scan, which false-flags a product OOS
            # on any stray "out of stock" (variant badge / related-product card).
            am = re.search(r"(?i)\bavail(?:ability)?:\s*(\S+)", doc)
            if am:
                tok = am.group(1).lower()
                if re.search(r"outofstock|out[_\s]?of[_\s]?stock|soldout|sold[_\s]?out|discontinued|backorder", tok):
                    avail = "out of stock"
                elif re.search(r"instock|in[_\s]?stock|preorder|pre[_\s-]?order|available", tok):
                    avail = "available"
            if not avail:
                avail = "out of stock" if re.search(r"(?i)\b(out of stock|sold out|currently unavailable)\b", doc) else "available"
        # Membership matches title + curated categories only — NOT the body
        # description. A prose mention ("pairs well with our sharpener") must not
        # count an unrelated product into the "sharpener" category.
        cats_l = str(meta.get("categories") or "").lower()
        # Exact storefront-collection membership, normalized to "|name|name|" so a
        # category query can test `|<phrase>|` for precise membership — collection
        # names often collide with title/tag words ("Scissors", "Fujifilm").
        _colls_raw = str(meta.get("collections") or "")
        colls_l = ""
        if _colls_raw:
            _parts = [re.sub(r"[^a-z0-9]+", " ", p.lower()).strip() for p in _colls_raw.split("|")]
            _parts = [p for p in _parts if p]
            if _parts:
                colls_l = "|" + "|".join(_parts) + "|"
        hay = " ".join([tl, cats_l])
        _colls_disp = " | ".join(p.strip() for p in _colls_raw.split("|") if p.strip())
        row = Row(title, price, avail, src, hay, cur, cats_l, colls_l, _colls_disp, orig_p)
        # Dedup by normalized title, but PREFER the priced chunk: a catalog that was
        # both crawled and catalog-ingested has two chunks per product (a crawl chunk
        # often without a structured price + a priced ingest chunk). Keeping whichever
        # came first dropped the price for products whose unpriced chunk sorted first
        # (then compare/basket couldn't use them). Upgrade in place when a later
        # duplicate carries a price the kept one lacks; also fill missing categories.
        prev = seen.get(key)
        if prev is None:
            seen[key] = row
            rows.append(row)
        elif prev.price is None and price is not None:
            prev.price = price
            prev.currency = cur
            if prev.original_price is None and orig_p is not None:
                prev.original_price = orig_p
            if avail and not re.search(r"out\s+of\s+stock|sold\s+out", prev.availability):
                prev.availability = avail
            if not prev.cats and cats_l:
                prev.cats = cats_l
                prev.hay = " ".join([prev.title.lower(), cats_l])
            if not prev.colls and colls_l:
                prev.colls = colls_l
                prev.colls_disp = _colls_disp
    _ROW_CACHE[_ck] = (rows, total, _now)
    return rows


def _excl_re(cfg: dict | None, anchors: list[str]):
    aset = set(anchors) | {a.rstrip("s") for a in anchors}
    terms = [str(t).lower().strip() for t in ((cfg or {}).get("count_exclude_terms") or [])
             if str(t).lower().strip() and str(t).lower().strip().rstrip("s") not in aset]
    if not terms:
        return None
    return re.compile(r"\b(?:" + "|".join(re.escape(t) for t in terms) + r")\b", re.I)


def _dedup(xs):
    return list(dict.fromkeys(x for x in xs if x))


def _price_s(p: float, cur: str = "Rs.") -> str:
    body = f"{p:,.0f}" if float(p).is_integer() else f"{p:,.2f}"
    return f"{cur}{body}"


def _price_disp(r: "Row") -> str:
    """Price for display — appends the struck-through original when on sale so a
    structured price/list answer also surfaces the discount ("Rs.3,284 (was Rs.5,120)")."""
    if r.price is None:
        return ""
    s = _price_s(r.price, r.currency)
    if r.original_price is not None and r.original_price > r.price:
        s += f" (was {_price_s(r.original_price, r.currency)})"
    return s


# DB-type gate thresholds — a product catalog is a corpus where product rows are a
# MEANINGFUL SHARE of the chunks, not a handful of accidental matches inside a docs
# corpus. A docs/RAG DB (e.g. agentfactory: 7.5k doc chunks) must never trip the
# catalog engine just because a lesson contains a "$9" example or a chapter-ingest
# mislabeled sections as products.
_MIN_CATALOG_ROWS = 5        # absolute floor of distinct product rows
_MIN_CATALOG_FRAC = 0.10     # product rows must be >=10% of all chunks
_DOCS_DBTYPES = {"docs", "documentation", "document", "text", "knowledge", "kb",
                 "rag", "article", "articles", "blog", "wiki", "manual", "guide"}
_PRODUCT_DBTYPES = {"product", "products", "catalog", "catalogue", "store", "shop",
                    "ecommerce", "e-commerce", "retail", "inventory"}


def is_catalog_db(db, rows: list, cfg: dict | None = None) -> bool:
    """Is this DB a product catalog (a TABLE of priced items) or a text/docs corpus?

    The catalog engine only owns the former. Order of authority: explicit config
    flag → docs/product type keyword → docs_path_hints (a docs-site marker) →
    corpus-share heuristic. Returning False sends the question to ordinary RAG/LLM.
    """
    cfg = cfg or {}
    flag = cfg.get("is_product_db")
    if flag is None:
        flag = cfg.get("product_db")
    if flag is True:
        return True
    if flag is False:
        return False
    db_type = str(cfg.get("db_type") or cfg.get("mode") or cfg.get("catalog_mode") or "").lower().strip()
    if db_type in _DOCS_DBTYPES:
        return False
    if db_type in _PRODUCT_DBTYPES:
        return True
    # A configured docs-site (per-language doc paths) is a documentation corpus.
    if cfg.get("docs_path_hints"):
        return False
    # Heuristic: product rows must be both numerous AND a real share of the corpus.
    n = len(rows)
    if n < _MIN_CATALOG_ROWS:
        return False
    try:
        total = int(db._collection.count() or 0)
    except Exception:
        total = 0
    if total > 0 and (n / total) < _MIN_CATALOG_FRAC:
        return False
    return True


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


_NAME_BOUNDARY = re.compile(r'["“”]\s*(?:and|with|or|versus|vs\.?|plus|,)\s*["“”]', re.I)


def _extract_names(q: str) -> list[str]:
    """Pull the individual product names a multi-product question references.
    Quoted names are the reliable signal (the hard quiz quotes each product, and
    real users naming a specific product tend to quote/capitalise it). Falls back
    to splitting on connectors only when there are no quotes."""
    # A product name can itself contain quotes (inch marks: a pool sized
    # 9'8"X6'3"), which defeats naive "([^"]+)" pairing and mis-splits the names
    # → a basket resolves the wrong product. Split instead on the QUOTE-delimited
    # connector BETWEEN names ('" and "', '", "'): inner inch-quotes never form
    # that boundary, so each name survives intact.
    if _NAME_BOUNDARY.search(q):
        tmp = _NAME_BOUNDARY.sub("\x00", q)
        m = re.search(r'["“”](.+)["“”]', tmp, re.S)  # first quote … last quote
        if m:
            names = [n.strip(" .\"'“”") for n in m.group(1).split("\x00")]
            names = [n for n in names if len(n) >= 3]
            if len(names) >= 2:
                return names[:5]
    names = [n.strip() for n in re.findall(r'[\"“”]([^\"“”]{3,})[\"“”]', q) if n.strip()]
    if len(names) >= 2:
        return names[:5]
    # A colon can introduce the names ("Compare on price: A vs B") OR trail them with a
    # question clause ("A vs B: which can I buy?"). Use the post-colon segment ONLY when
    # the connectors (and therefore the names) are there; otherwise keep the full string
    # and let the trailing clause be stripped below — so "A vs B: which can I buy?" still
    # yields [A, B] instead of the question.
    colon = re.search(r":\s+", q)
    seg = q
    if colon and not re.search(r"\d\s*:\s*$", q[:colon.start() + 1]):
        after = q[colon.end():]
        if re.search(r"\b(?:versus|vs\.?|and|or|against)\b|,", after):
            seg = after
    # A trailing interrogative clause introduced by a COMMA ("X or Y, which can I buy
    # today?") would be comma-split into a junk name ("today") and block resolution of
    # the real names; strip it from the segment before the connector split.
    seg = re.sub(r"(?i),\s*(?:which|what|who|can|should|is|are|do(?:es)?|will|how|kis|kon?sa|kaun?sa|kaun)\b.*$", "", seg)
    parts = re.split(r"\s+(?:versus|vs\.?|v\.?|and|or|aur|ya|plus|against|compared\s+(?:to|with))\s+|\s*\+\s*|,\s*", seg)
    # leading ordering verb ("Sort Mini Can, ...", "rank these A, B by price") the
    # connector-split leaves on the first name — drop it so the bare product resolves.
    parts = [re.sub(r"(?i)^\s*(?:sort|order|rank|arrange)\s+(?:these\s+|the\s+)?", "", p) for p in parts]
    # leading comparison scaffolding the connector-split leaves on the first name
    # ("availability comparison FOR Aston Martin F1 1/18" → "Aston Martin F1 1/18")
    parts = [re.sub(r"(?i)^\s*(?:availability|stock|price|cost)?\s*compar(?:e|ing|ison)\s+(?:for|of|between|with)?\s*", "", p) for p in parts]
    parts = [re.sub(r"(?i)^\s*(?:compare|which\s+(?:one\s+)?(?:is|costs?|should\s+i\s+buy|can\s+i\s+buy)|is)\s+", "", p) for p in parts]
    parts = [re.sub(r"(?i)^\s*(?:more|less|cheaper|pricier|dearer|expensive)\s+", "", p) for p in parts]
    parts = [re.sub(r"(?i)\s+(?:on|for)\s+(?:price|prices|cost|stock|availability)(?:\s+and\s+(?:price|prices|cost|stock|availability))*.*$", "", p) for p in parts]
    # a bare trailing dimension clause the connector-split couldn't separate from the
    # LAST name ("Big RC Excavator PRICE AND STOCK", "Iron Man action figure PRICE
    # DIFFERENCE") — strip the comparison axis words so the product name resolves.
    parts = [re.sub(r"(?i)\s+(?:price|prices|cost|costs|stock|stock\s+status|availability)"
                    r"(?:\s+(?:and|&|,|or)\s+(?:price|prices|cost|costs|stock|availability|difference))*"
                    r"(?:\s+difference)?\s*$", "", p) for p in parts]
    # trailing "stock compare"/"comparison"/"compared" the connector-split leaves on the
    # last name ("Lamborghini URUS stock compare") — the dimension strip above stops at
    # "stock" because "compare" follows it, so the verb survives and breaks resolution.
    parts = [re.sub(r"(?i)\s+(?:stock|price|cost|availability)?\s*(?:compare|comparison|compared)\s*[?.!]*$", "", p) for p in parts]
    # trailing question/compare clause a name carries when the tail wasn't split off
    # ("Mini Can RC Drift Cars: which can I buy?", "Iron Man action figure — which costs more")
    parts = [re.sub(r"(?i)\s*[:—–-]\s*(?:which|what|who|can|should|is|are|do(?:es)?|will|how)\b.*$", "", p) for p in parts]
    # Roman-Urdu trailing interrogative the connector-split leaves on the last name
    # ("iron man MEIN MEHNGA KONSA HAI?", "X SASTA KAUNSA") → strip from the first
    # Roman-Urdu locative/compare marker so the bare product resolves. These tokens never
    # appear inside an English product title, so English names are untouched.
    parts = [re.sub(r"(?i)\s+(?:mein|konsa|konsi|kaunsa|kaunsi|kon?s[ai]|kaun?s[ai]|kis|sasta|sasti|"
                    r"mehng[ai]|zyada|sab\s+se|dono|available|availab\w*|buyable)\b.*$", "", p) for p in parts]
    # trailing ordering/dimension clause the connector-split couldn't separate from the
    # LAST name ("BMW 760li CHEAPEST TO EXPENSIVE SORT KARO", "1:64 Micro Drift BY PRICE
    # LOW TO HIGH", "Ferrari Laferrari TOTAL PRICE") — strip the ranking/basket axis so
    # the bare product resolves. The "X to Y" range and "by price" phrasings never occur
    # inside a real product title, so genuine names are untouched.
    parts = [re.sub(r"(?i)\s+(?:cheap\w*|low\w*|least|most|high\w*|expensive|priciest|dearest)\s+to\s+\w+.*$", "", p) for p in parts]
    # trailing "<pole> first/last" the connector-split leaves on the LAST name
    # ("Ferrari Laferrari CHEAPEST FIRST", "M8 PRICIEST LAST") — strip the ranking
    # direction so the bare product resolves. Pole words never occur in a real title.
    parts = [re.sub(r"(?i)\s+(?:cheap\w*|low\w*|least|most\s+expensive|priciest|dearest|"
                    r"high\w*|expensive|pric\w*|costl\w*)\s+(?:priced\s+)?(?:first|last)\b.*$", "", p) for p in parts]
    parts = [re.sub(r"(?i)\s+by\s+(?:price|prices|cost|costs|stock|availability)\b.*$", "", p) for p in parts]
    parts = [re.sub(r"(?i)\s+(?:ascending|descending|increasing|decreasing)\b.*$", "", p) for p in parts]
    parts = [re.sub(r"(?i)\s+(?:sort|order|rank|arrange)(?:\s+(?:kar\w*|kr\w*|kren|karen|do))?\s*[?.!]*$", "", p) for p in parts]
    parts = [re.sub(r"(?i)\s+(?:total|combined|altogether|together|sum)(?:\s+(?:price|prices|cost|costs))?\s*[?.!]*$", "", p) for p in parts]
    parts = [re.sub(r"[?.\"]+$", "", p).strip(" .\"") for p in parts]
    # Keep multi-word names; ALSO a lone distinctive word (a one-word product name like
    # "Deadpool" in "Deadpool vs Iron Man action figure", where the shared type-noun
    # attaches only to the other side). Single words must be >=5 chars and not stop-words
    # so connector-split junk ("the", "and") is excluded — and _answer_multi's resolution
    # floor rejects any one-worder that isn't a real product anyway.
    parts = [p for p in parts
             if (len(p) >= 4 and len(p.split()) >= 2)
             or (len(p.split()) == 1 and len(p) >= 5 and p.lower() not in _STOP)]
    return parts[:5] if len(parts) >= 2 else names


_CMP_RE = re.compile(
    r"\bcompar(?:e|es|ing|ison)\b|\bcheaper\b|\bpricier\b|\bdearer\b|\bmore\s+expensive\b|\bless\s+expensive\b"
    r"|\bdifference\s+between\b|\bby\s+how\s+much\b|\bhow\s+much\s+(?:more|less|cheaper)\b"
    r"|\bwhich\s+(?:one\s+)?(?:is|costs?)\b|\bversus\b|\bvs\.?\b|\bagainst\b|\bcosts?\s+(?:less|more)\b", re.I)
_ORDER_RE = re.compile(r"\b(order|rank|sort|arrange|list\s+these)\b", re.I)
_PRICE_DIM_RE = re.compile(
    r"\b(expensive|cheap(?:er|est)?|price[ds]?|cost(?:s|ly)?|least|most|"
    r"low(?:est)?\s+to\s+high(?:est)?|high(?:est)?\s+to\s+low(?:est)?|"
    r"ascending|descending|increasing|decreasing)\b", re.I)
# Basket = "buy/total/combined" of several named items. Broad on purpose: customers
# phrase it many ways ("total cost of buying X and Y", "buys one X plus one Y, what
# combined amount should they pay", "X together with Y"). Compare/order are detected
# first and win, so these verbs can't hijack a comparison/ranking. NOT 'cost' alone
# (that word appears in "which costs more" comparisons).
_BASKET_RE = re.compile(
    r"\b(total|sum|altogether|combined|basket|plus|together|spend|spends|owe|"
    r"purchas\w*|buy|buys|buying|bought|pay|pays|paying)\b", re.I)
# Ascending = cheapest-first. Catch every natural phrasing: a cheap/low pole word
# appearing before an expensive/high pole word across a "to" ("cheapest to most
# expensive", "low to high", "least to most expensive"), plus the explicit single
# forms. "most expensive to cheapest" can't match (poles reversed) → descending.
_ASC_RE = re.compile(
    r"\b(?:cheap(?:est)?|low(?:est)?|least|lower)\b[^.?!]*?\bto\b[^.?!]*?"
    r"\b(?:most|expensive|high(?:est)?|pric\w*|dear\w*|cost\w*)"
    r"|\b(?:cheap(?:est)?|low(?:est)?)\s+first\b"
    r"|\bleast\s+to\s+most\b|\blow(?:est)?\s+to\s+high(?:est)?\b"
    r"|\b(?:ascending|increasing)\b", re.I)
# Descending = priciest-first, stated explicitly. Used to disambiguate the order
# op when no ascending cue is present (default stays descending either way).
_DESC_RE = re.compile(
    r"\b(?:most\s+expensive|priciest|dearest|highest|expensive)\b[^.?!]*?\bto\b[^.?!]*?"
    r"\b(?:cheap\w*|low\w*|least)"
    r"|\b(?:most\s+expensive|priciest|dearest|highest)\s+first\b"
    r"|\bhigh(?:est)?\s+to\s+low(?:est)?\b|\b(?:descending|decreasing)\b", re.I)


def _is_oos(r: Row) -> bool:
    return bool(re.search(r"out\s+of\s+stock|sold\s+out|unavailable|not\s+available", r.availability or ""))


def _parse_multi(q: str) -> dict | None:
    """Detect a multi-product reasoning question (compare / order / basket) and the
    product names it spans. Returns None for everything else."""
    ql = (q or "").lower()
    is_order = bool(_ORDER_RE.search(ql) and _PRICE_DIM_RE.search(ql))
    is_cmp = bool(_CMP_RE.search(ql)) or bool(_CMP_MARK.search(ql)) or bool(re.search(r"\bwhich\b.*\bbuy\b", ql) and re.search(r"\bor\b|\bvs\b|\bversus\b", ql))
    # Buy-choice: two named products joined by or/vs with an availability/buy cue
    # ("need the available option: X or Y?", "X or Y — which can I get?"). Report each
    # one's stock and recommend the buyable one. Resolves like a stock comparison; if
    # the names don't resolve to specific products the caller falls back to the list
    # spec, so a category "or" query is never hijacked.
    is_choice = bool(re.search(r"\b(?:or|vs\.?|versus|ya)\b", ql)
                     and re.search(r"\b(available|availab\w*|in[\s-]?stock|buy|buyable|purchase|order|get|kaunsi|kaunsa|konsi|konsa)\b", ql))
    is_cmp = is_cmp or is_choice
    is_basket = bool(_BASKET_RE.search(ql)) and not bool(re.search(r"\bwhich\b|\bcompare\b|\bin[\s-]?stock\b|\bavailable\b", ql))
    if not (is_order or is_cmp or is_basket):
        return None
    names = _extract_names(q)
    if len(names) < 2:
        return None
    # order and compare have distinct, specific verbs; basket is the remaining
    # multi-product intent. Compare wins over basket so "which costs more" is never
    # summed.
    op = "order" if is_order else ("compare" if is_cmp else "basket")
    # Compare direction: "which costs more / more expensive" names the dearer item;
    # default ("cheaper / costs less") names the cheaper. Either way both prices and
    # the exact gap are stated, so the answer carries the fact regardless.
    cmp_more = bool(re.search(r"\bmore\s+expensive\b|\bcosts?\s+more\b|\bpricier\b|\bdearer\b|\bhigher\s+price", ql)) or bool(_MORE_MARK.search(ql))
    cmp_stock = bool(re.search(r"\bstock\b|\bavailable\b|\bavailability\b|\bbuy\b", ql)) or is_choice
    return {"op": op, "names": names, "asc": bool(_ASC_RE.search(ql)), "cmp_more": cmp_more,
            "stock": cmp_stock, "buy_choice": is_choice}


def _raw(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _edit_le(a: str, b: str, k: int) -> bool:
    """True if a within k edits of b, counting an adjacent transposition as ONE
    (Damerau) so 'leanring'~'learning'. Bounded DP — tokens are short."""
    la, lb = len(a), len(b)
    if abs(la - lb) > k:
        return False
    prev2 = None
    prev = list(range(lb + 1))
    for i in range(1, la + 1):
        cur = [i] + [0] * lb
        for j in range(1, lb + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
            if i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1]:
                cur[j] = min(cur[j], prev2[j - 2] + 1)
        prev2, prev = prev, cur
    return prev[lb] <= k


def _one_transpose(a: str, b: str) -> bool:
    """True iff b differs from a by exactly ONE adjacent transposition (same length).
    Distinct from a substitution: 'cute'~'cuet' (swap) is True, 'card'~'cart' (sub) is
    False — so short-word fuzz can recover an end-transposition typo without collapsing
    two different real words."""
    if len(a) != len(b) or a == b:
        return False
    diff = [i for i in range(len(a)) if a[i] != b[i]]
    return (len(diff) == 2 and diff[1] == diff[0] + 1
            and a[diff[0]] == b[diff[1]] and a[diff[1]] == b[diff[0]])


def _fuzzy_in(tok: str, toks: list[str]) -> bool:
    """tok matches some word in toks within a length-scaled edit budget. Conservative
    on short words (where one edit flips a different real word, card↔cart): exact only
    under 5 chars, 1 edit for 5-7, 2 for 8+. Used only as a fallback when the exact /
    singular-plural match already failed, so the 0.85 coverage floor still guards."""
    for u in toks:
        if u == tok:
            return True
        n = min(len(tok), len(u))
        # A single adjacent transposition is the most common typo ("cuet"→"cute",
        # "Candels"→"Candles", "Hamzasotrepk"→"Hamzastorepk") and counts as ONE edit
        # at ANY length — plain Levenshtein scores it as 2, so the k-budget (1 for
        # 5-7 chars) would wrongly reject mid-length transpositions. _one_transpose
        # excludes substitutions, so 4-char near-pairs (card/cart) stay distinct.
        if len(tok) == len(u) and _one_transpose(tok, u):
            return True
        k = 0 if n < 5 else (1 if n < 8 else 2)
        if k and _edit_le(tok, u, k):
            return True
    return False


_VOCAB_CACHE: dict = {}


def _catalog_vocab(rows: list[Row]) -> set[str]:
    """Distinctive words the catalog actually uses (title + structured category
    tokens, length >= 5). Cached per row-list identity. Used to repair a typo'd
    anchor against real merchant vocabulary."""
    key = id(rows)
    v = _VOCAB_CACHE.get(key)
    if v is None:
        v = set()
        for r in rows:
            for t in re.findall(r"[a-z]{5,}", (r.title + " " + r.cats).lower()):
                v.add(t)
        _VOCAB_CACHE.clear()        # only the current row-list matters
        _VOCAB_CACHE[key] = v
    return v


def _correct_anchors(anchors: list[str], rows: list[Row]) -> list[str]:
    """Repair anchors the catalog does NOT contain (document-frequency 0) by mapping
    each to the nearest real vocabulary word within the SAME edit budget the rest of
    the engine uses (_fuzzy_in: 1 edit for 5-7 chars, 2 for 8+). Only fires on absent
    tokens, so it can never alter a query whose words all exist — a misspelled CATEGORY
    ('construccion'->'construction') resolves while a genuinely absent product still
    matches nothing. 'rc' is engine-special and never touched."""
    if not rows:
        return anchors
    vocab = None
    out = []
    for a in anchors:
        if a == "rc" or len(a) < 5 or _anchor_df(a, rows) > 0.0:
            out.append(a)
            continue
        if vocab is None:
            vocab = _catalog_vocab(rows)
        best, best_k = None, 99
        for v in vocab:
            if v[0] != a[0] or abs(len(v) - len(a)) > 2:
                continue
            n = min(len(a), len(v))
            k = 1 if n < 8 else 2
            if _edit_le(a, v, k):
                # nearest by edit distance; ties -> the more frequent catalog word
                d = 0 if _edit_le(a, v, 0) else (1 if _edit_le(a, v, 1) else 2)
                score = (d, -_anchor_df(v, rows))
                if best is None or score < best_k:
                    best, best_k = v, score
        out.append(best or a)
    return out


def _tie_break_key(r: Row, raw_name: str):
    """Deterministic ranking among rows that resolve EQUALLY well to a name, so a
    duplicate/near-duplicate listing never gets picked at random. Prefer the row
    whose raw title (punctuation+case) matches the query — e.g. a query naming
    'Pool ( 34" X 10" )' picks that exact listing over the 'Pool 34" x 10"' twin —
    then an in-stock row, then the closest title length, then a stable URL."""
    raw = _raw(r.title)
    in_stock = 1 if _is_oos(r) else 0
    return (0 if raw == _raw(raw_name) else 1, in_stock, abs(len(raw) - len(_raw(raw_name))), r.source or "")


def _resolve_detail(name: str, rows: list[Row]) -> tuple[Row | None, list[Row]]:
    """Resolve ONE named product. Returns (chosen_row, equally-good candidates).
    Exact normalized-title matches win; otherwise a bidirectional coverage match
    with a high floor so same-category siblings are rejected. When several rows
    tie, _tie_break_key chooses deterministically and `candidates` exposes the
    tie so the caller can clarify a genuine ambiguity (identical listings)."""
    key = _norm(name)
    qtoks = [t for t in key.split() if len(t) > 1]
    if not qtoks:
        return None, []
    qset: set[str] = set()
    for t in qtoks:
        qset |= _variants(t)
    exact = [r for r in rows if _norm(r.title) == key]
    if exact:
        best = min(exact, key=lambda r: _tie_break_key(r, name))
        return best, exact
    best = None
    best_key = (-1.0, -1.0, -1e9)
    tied: list[Row] = []
    for r in rows:
        rtoks = [t for t in _norm(r.title).split() if len(t) > 1]
        if not rtoks:
            continue
        rset: set[str] = set()
        for t in rtoks:
            rset |= _variants(t)
        # Fuzzy fallback (only when the exact/variant match misses) tolerates a typo'd
        # query word — "leanring" still names "learning" — so a misspelling can't drop
        # the right product below a clean sibling.
        cover = sum(1 for t in rtoks if t in qset or _fuzzy_in(t, qtoks)) / len(rtoks)    # stored title named by query
        qcover = sum(1 for t in qtoks if t in rset or _fuzzy_in(t, rtoks)) / len(qtoks)   # query named by stored title
        if not (cover >= 0.85 or qcover >= 0.85) or (cover + qcover) / 2 < 0.5:
            continue
        sk = (max(cover, qcover), min(cover, qcover), -abs(len(rtoks) - len(qtoks)))
        if sk > best_key:
            best_key, best, tied = sk, r, [r]
        elif sk == best_key:
            tied.append(r)
    if best is not None and len(tied) > 1:
        best = min(tied, key=lambda r: _tie_break_key(r, name))
    return best, (tied if best is not None else [])


def _resolve_one(name: str, rows: list[Row]) -> Row | None:
    return _resolve_detail(name, rows)[0]


def _ambiguous_alts(name: str, rows: list[Row]) -> list[Row]:
    """Return the price-distinct candidates when a name resolves to TWO listings
    that the tie-break cannot separate (same rank key) — a genuine ambiguity the
    bot must surface rather than guess between (e.g. two identical product
    listings at different prices). Empty when resolution is unambiguous."""
    best, cands = _resolve_detail(name, rows)
    if best is None or len(cands) < 2:
        return []
    # Compare on the MEANINGFUL rank fields (raw title, stock, length), not the
    # URL tiebreaker — two identical listings differ only by URL, so including it
    # would make them look distinguishable and suppress the clarification.
    bk = _tie_break_key(best, name)[:-1]
    top = [c for c in cands if _tie_break_key(c, name)[:-1] == bk and c.price is not None]
    if len({c.price for c in top}) > 1:
        return top
    return []


def _answer_multi(mp: dict, rows: list[Row]) -> tuple[str, list[str]] | None:
    """Resolve every named product, then compute the comparison / ordering / total.
    Returns None (→ RAG) unless EVERY named product resolves to a priced row, so a
    partial resolution never produces a confidently-wrong half-answer."""
    found = []
    for name in mp["names"]:
        # Genuine ambiguity: the name matches two indistinguishable listings at
        # different prices. Don't guess one into a basket/comparison — surface the
        # choice so the answer is never confidently wrong.
        alts = _ambiguous_alts(name, rows)
        if len(alts) > 1:
            opts = "; ".join(f"{_price_s(a.price, a.currency)}" for a in sorted(alts, key=lambda r: r.price))
            return (f'There are {len(alts)} listings for "{name}" at different prices ({opts}). '
                    f"Which one did you mean?"), _dedup(a.source for a in alts)
        r = _resolve_one(name, rows)
        if r is None or r.price is None:
            return None
        found.append(r)
    # Dedup by product URL: a name containing embedded quotes (e.g. a pool sized
    # 9'8"X6'3") mis-splits into fragments that resolve to the SAME row, which
    # would otherwise double-count it in the basket total. Genuine multi-product
    # questions name distinct products → distinct sources, so this is lossless.
    _seen, _uniq = set(), []
    for r in found:
        k = r.source or r.title
        if k in _seen:
            continue
        _seen.add(k)
        _uniq.append(r)
    found = _uniq
    if len(found) < 2:
        return None
    cur = found[0].currency
    srcs = _dedup(r.source for r in found)
    op = mp["op"]
    if op == "basket":
        total = sum(r.price for r in found)
        lines = ["Based on our catalog:"]
        lines += [f"- {r.title}: {_price_s(r.price, r.currency)} ({'out of stock' if _is_oos(r) else 'available'})" for r in found]
        lines.append(f"\nTotal cost: {_price_s(total, cur)}")
        return "\n".join(lines), srcs
    if op == "order":
        # stable sort by price ALONE keeps equal-priced items in the order the customer
        # named them ("BMW M8, BMW M4, BMW 760li" → M8 before 760li at the same Rs.4,950),
        # which is the natural reading; an alphabetical tiebreak would reorder them.
        ordered = sorted(found, key=lambda r: r.price if mp["asc"] else -r.price)
        head = "From least to most expensive:" if mp["asc"] else "From most to least expensive:"
        lines = [head] + [f"{i}. {r.title} — {_price_s(r.price, r.currency)}" for i, r in enumerate(ordered, 1)]
        return "\n".join(lines), _dedup(r.source for r in ordered)
    # compare: state both prices and the exact difference; name the product the
    # question asked about (dearer for "which costs more", else the cheaper).
    pair = sorted(found, key=lambda r: r.price)
    cheaper, dearer = pair[0], pair[-1]
    diff = dearer.price - cheaper.price
    if mp.get("buy_choice"):
        # Recommend by availability, not price: name the buyable option(s) and flag the
        # out-of-stock one explicitly ("X is available; Y is out of stock").
        _av = [r for r in found if not _is_oos(r)]
        _so = [r for r in found if _is_oos(r)]
        if _av and _so:
            verdict = (f"{', '.join(r.title for r in _av)} is available; "
                       f"{', '.join(r.title for r in _so)} is out of stock.")
        elif _av:
            verdict = f"Both are available — {cheaper.title} is the cheaper at {_price_s(cheaper.price, cur)}."
        else:
            verdict = "Both are currently out of stock."
    else:
        verdict = (f"{dearer.title} is more expensive by {_price_s(diff, cur)}."
                   if mp.get("cmp_more")
                   else f"{cheaper.title} is cheaper by {_price_s(diff, cur)}.")
    def _cmp_line(r):
        suffix = f" ({'out of stock' if _is_oos(r) else 'available'})" if mp.get("stock") else ""
        return f"- {r.title}: {_price_s(r.price, r.currency)}{suffix}"
    txt = (f"{_cmp_line(cheaper)}\n"
           f"{_cmp_line(dearer)}\n\n{verdict}")
    return txt, srcs


# Coarse "is this plausibly a product question?" gate for the LLM net. Broad on
# purpose (a quoted span or any price/buy/compare signal); the LLM does the precise
# classification. Keeps the extra call off pure policy/greeting questions.
_PRODUCTISH = re.compile(
    r'["“”]|\b(price[ds]?|cost(?:s|ly)?|how\s+much|cheap(?:er|est)?|expensive|afford|'
    r'total|combined|altogether|buy|buys|buying|purchas\w*|pay|spend|stock|stocked|'
    r'carry|carries|available|availability|sell|sells|which|compare|versus|vs\.?|'
    r'than|set\s+of|pack\s+of|do\s+you\s+have|in\s+stock)\b', re.I)

# Greeting / smalltalk that is never a catalog question — used to skip the LLM
# router for trivial turns so the understanding layer fires on everything ELSE
# without us hand-coding a surface regex per new phrasing (SMS-speak, pronouns,
# price-ranges). The router itself returns "other" for any non-catalog question,
# so a missed skip is harmless (falls through to RAG), just one wasted call.
_ROUTER_SKIP = re.compile(
    r'^\s*(hi|hey+|hello|yo|sup|as?salaa?m\w*|salaa?m\w*|'
    r'thanks?|thank\s+you|thx|ty|ok(?:ay)?|cool|nice|great|'
    r'good\s+(?:morning|evening|afternoon|day)|bye|goodbye|see\s+ya|'
    r'how\s+are\s+you|who\s+are\s+you|what\s+can\s+you\s+do)\b[\s!.?]*$', re.I)


def _anchors_from(text: str) -> list:
    return [w for w in re.findall(r"[a-z0-9]{2,}", (text or "").lower()) if w not in _STOP][:8]


def _grounded_in_q(name: str, q: str) -> bool:
    """A router-extracted product/category must actually appear in the question.
    The LLM router is instructed to copy names the customer *literally wrote*, but
    a weak model sometimes echoes a few-shot EXAMPLE product instead — e.g. for a
    content-free follow-up ("is it in stock?", "how much is it?") it copies the
    prompt's sample item. Un-grounded, that becomes a confidently-wrong "we don't
    carry X" for an X the customer never named (and, across tenants, masquerades as
    a data leak). Require at least one distinctive (3+ char, non-stop) token of the
    name to be present in the query."""
    qtoks = {w for w in re.findall(r"[a-z0-9]{3,}", (q or "").lower()) if w not in _STOP}
    ntoks = [w for w in re.findall(r"[a-z0-9]{3,}", (name or "").lower()) if w not in _STOP]
    return bool(ntoks) and any(t in qtoks for t in ntoks)


def _ground_plan(plan: dict | None, q: str) -> dict | None:
    """Keep only the parts of an LLM-extracted plan that are grounded in q, so a
    few-shot echo can never drive execution. Returns the cleaned plan, or None when
    the intent needed named products / a category and none survived (→ hand to RAG)."""
    if not plan:
        return plan
    plan = dict(plan)
    plan["products"] = [p for p in (plan.get("products") or []) if _grounded_in_q(p, q)]
    cat = plan.get("category")
    if cat and not _grounded_in_q(cat, q):
        plan["category"] = None
    kind = plan.get("kind")
    prods, cat = plan["products"], plan.get("category")
    if kind in ("lookup", "exists") and not prods:
        return None
    if kind in ("compare", "order", "basket") and len(prods) < 2:
        return None
    if (kind in ("count", "cheapest", "priciest", "filter")
            and not prods and not cat
            and plan.get("price_min") is None and plan.get("price_max") is None):
        return None
    return plan


# Common Roman-Urdu / Hinglish function & intent words. Used ONLY to DETECT that a
# question is code-mixed (not English) so the LLM understanding layer is made primary
# instead of the English regex mis-routing it. Detection needs just one hit, so spelling
# variants don't have to be exhaustive — the LLM handles the actual understanding. This
# is universal language knowledge, not per-store/per-phrasing config.
# Deliberately EXCLUDES tokens that are also ordinary English ("me", "do", "stock",
# "kar"~car) so a pure-English question never trips the gate (it stays on the fast,
# free regex path). Any single hit from this set means the question is code-mixed.
_URDU_MARKERS = frozenset(
    "hai hain kya kia mein mai se kam zyada ziada aur ya phir nahi nai nhi "
    "konsa kaunsa konsi kaunsi kaun kis sasta sasti mehnga mehngi mehnge "
    "chahiye chaiye chahye milega milegi jayega jaega jayegi hoga hogi sakta sakti "
    "sakte hun kitna kitni kitny kitne kitnay wala wali wale abhi karo "
    "batao bata bta dedo dikhao dikha dikhaiye acha achha theek yaar "
    "dono rakhte rakha".split()
)


def _is_code_mixed(q: str) -> bool:
    """True when the question carries Roman-Urdu/Hindi markers → route understanding
    through the LLM (which reads any language) instead of the English-only regex."""
    return any(t in _URDU_MARKERS for t in re.findall(r"[a-z]+", (q or "").lower()))


_AVAIL_MARK = re.compile(r"\b(available|in[\s-]?stock|stock\s+mein|mileg[ai]|mil\s+jay\w*|buyable|in\s+stock)\b", re.I)
_OOS_MARK = re.compile(r"\b(sold[\s-]?out|out\s+of\s+stock|unavailable|stock\s+mein\s+nahi|nahi\s+hai)\b", re.I)
_BOTH_MARK = re.compile(r"\b(dono|both|available\s+(?:and|aur|or|ya|&|vs)\s+(?:sold|out|unavailable))\b"
                        # Roman-Urdu "kitne bik gaye … kitne bache" = sold vs remaining split
                        r"|bik\w*[^.?!]{0,30}bach\w*|bach\w*[^.?!]{0,30}bik\w*", re.I)
_CMP_MARK = re.compile(r"\b(which|konsa|kaunsa|kon?sa|kaun?sa|kis|cheaper|sasta|sasti|"
                       r"mehng[ai]|zyada|expensive|pricier|dearer|better|costs?\s+(?:more|less))\b", re.I)
_MORE_MARK = re.compile(r"\b(mehng[ai]|zyada|expensive|pricier|dearer|costs?\s+more|higher)\b", re.I)
# "Buyable / advisory" intent — the customer wants something they can PURCHASE now
# (a gift, a recommendation, "one I can get", "jo mil jaye / buy ho sake"). Mirrors
# the advisory rule in _refine_plan but is applied to the deterministic REGEX spec too,
# so a sold-out item never leaks into a budget/recommendation list when the LLM router
# didn't fire. Language-agnostic markers; universal across stores, no per-phrasing config.
_BUYABLE_INTENT = re.compile(
    r"\b(available|in[\s-]?stock|buyable|mileg[ai]|mil\s+jay\w*|mil\s+sak\w*|"
    r"buy\s+(?:ho|kar)\s+sak\w*|khareed\w*|kharid\w*|le\s+sak\w*|order\s+kar\s+sak\w*|"
    r"jo\s+(?:mil|buy|le)\b|recommend\w*|suggest\w*|gift|gifts|present|presents|tohfa|tuhfa)\b", re.I)


def _refine_plan(plan: dict, q: str) -> dict:
    """Deterministically correct the dimensions a small LLM most often drops, using
    language-agnostic markers (English + Roman-Urdu). The LLM still does the hard part
    (understanding + naming products); this only enforces UNAMBIGUOUS signals it missed,
    so it's a universal safety net, not per-store config. Only ADDS signal, never erases
    a product the LLM found."""
    if not plan:
        return plan
    ql = q or ""
    av, oos = bool(_AVAIL_MARK.search(ql)), bool(_OOS_MARK.search(ql))
    # stock dimension: both states, or a clear out-only / in-only ask the LLM left null
    if not plan.get("stock_query"):
        if bool(_BOTH_MARK.search(ql)) or (av and oos):
            plan["stock_query"] = "both"
        elif oos and not av:
            plan["stock_query"] = "out"
        elif av and not oos:
            plan["stock_query"] = "in"
    # two named products + a comparison cue → it's a comparison, even if the LLM called
    # it a lookup/filter (Roman-Urdu "X ya Y konsa..." is easy to mis-tag).
    if len([p for p in (plan.get("products") or []) if p]) >= 2 and _CMP_MARK.search(ql):
        if plan.get("kind") in ("lookup", "filter", "exists", "browse"):
            plan["kind"] = "compare"
        if not plan.get("compare_dir"):
            plan["compare_dir"] = "more" if _MORE_MARK.search(ql) else "cheaper"
        # An availability/buy comparison ("stock kis ka hai", "konsa buy kar sakta hun")
        # must report each side's status — carry the stock dimension even when no explicit
        # available/sold-out word was used.
        if plan.get("kind") == "compare" and not plan.get("stock_query") and re.search(
                r"\b(stock|available|availab\w*|buy|buyable|kharid|le\s+sakta|kar\s+sakta)\b", ql, re.I):
            plan["stock_query"] = "in"
    # Advisory / gift-recommendation intent ("recommend a gift under 4000", "bachay ke
    # liye koi acha gift suggest karo") = the customer wants something they can BUY now —
    # never surface a sold-out item as a recommendation. Force the in-stock filter unless
    # they explicitly asked for out/both stock states.
    if (not plan.get("stock_query") and not plan.get("in_stock")
            and re.search(r"\b(recommend\w*|suggest\w*|advise|advice|gift|gifts|present|presents"
                          r"|tohfa|tuhfa|acha|accha|achi|acchi|behtar|behtreen)\b", ql, re.I)):
        plan["in_stock"] = True
    return plan


def _plan_to_specs(plan: dict) -> tuple[dict | None, "Spec | None"]:
    """Map an LLM-extracted plan onto the engine's existing (mp | Spec) execution.
    Returns (mp, spec) with at most one non-None. None,None = not executable here."""
    kind = plan.get("kind")
    names = [n for n in (plan.get("products") or []) if n]
    sq = plan.get("stock_query")  # "in" | "out" | "both" | None
    if kind in ("compare", "order", "basket"):
        if len(names) < 2:
            return None, None
        # An availability comparison ("kaunsa available?", "stock kis ka hai") → carry
        # stock so each side's status shows and the buyable one is recommended.
        _stockish = sq in ("in", "out", "both")
        return {"op": kind, "names": names[:6],
                "asc": plan.get("order_dir") == "asc",
                "cmp_more": plan.get("compare_dir") == "more",
                "stock": _stockish, "buy_choice": _stockish and kind == "compare"}, None
    pmin, pmax, in_stock = plan.get("price_min"), plan.get("price_max"), bool(plan.get("in_stock"))
    out_of_stock = sq == "out"
    if in_stock and out_of_stock:
        in_stock = out_of_stock = False
    cat = plan.get("category") or ""
    # Both states asked together ("available aur sold out dono") → split count.
    if sq == "both":
        anch = _anchors_from(cat or (names[0] if names else ""))
        if not anch:
            return None, None
        return None, Spec("count_split", anch, pmin, pmax, include_oos=True)
    if kind == "count":
        return None, Spec("count", _anchors_from(cat or (names[0] if names else "")), pmin, pmax, in_stock, out_of_stock=out_of_stock)
    if kind == "cheapest":
        return None, Spec("min", _anchors_from(cat or (names[0] if names else "")), pmin, pmax, in_stock, out_of_stock=out_of_stock)
    if kind == "priciest":
        return None, Spec("max", _anchors_from(cat or (names[0] if names else "")), pmin, pmax, in_stock, out_of_stock=out_of_stock)
    if kind == "exists":
        # "do you have X?" / Roman-Urdu "X hai?" — report availability, not just yes/no:
        # the stock reporter confirms it's carried AND states in/out of stock (one product
        # or a small category), so an OOS item is never answered as a bare "yes we carry".
        anch = _anchors_from(names[0] if names else cat)
        if not anch:
            return None, None
        return None, Spec("stock", anch, pmin, pmax)
    if kind == "lookup":
        # Single named product: the STOCK reporter answers both "how much" (it prints the
        # price) and "is it available" (it prints status, even when out of stock) — so a
        # Roman-Urdu "kitny ka hai"/"mil jayega" both resolve correctly.
        if not names:
            anch = _anchors_from(cat)
            return (None, Spec("list", anch, pmin, pmax, in_stock, out_of_stock=out_of_stock)) if anch or pmin is not None or pmax is not None else (None, None)
        return None, Spec("stock", _anchors_from(names[0]), pmin, pmax)
    if kind in ("filter", "browse"):
        anch = _anchors_from(names[0] if names else cat)
        if not anch and pmin is None and pmax is None:
            return None, None
        return None, Spec("list", anch, pmin, pmax, in_stock, out_of_stock=out_of_stock)
    return None, None


_LOCATE_CAT_RE = re.compile(
    r"(?i)^\s*(?:which|what|in\s+what|under\s+what)\s+"
    r"(?:categor(?:y|ies)|section|collection|department|aisle|group)\s+"
    r"(?:is|are|does|do|would)\s+(.+?)"
    r"(?:\s+(?:in|under|listed|placed|found|located|belong(?:s)?(?:\s+to)?|part\s+of))?\s*\??$")
_LOCATE_WHERE_RE = re.compile(
    r"(?i)^\s*where\s+(?:can|do|would|should|will|might)?\s*(?:i|you|we|one)?\s*"
    r"(?:find|locate|get|see|buy|browse)\s+(.+?)\s*\??$")


_VARIANT_Q_RE = re.compile(
    r"\b(colou?rs?|sizes?|shades?|variants?|versions?|options?)\b.*"
    r"(?:\bcome\s+in\b|\bcomes\s+in\b|\bavailable\b|\boffered\b|\bdoes\s+(?:it|this|that|the)\b|\bhave\b)"
    r"|\bwhat\s+(?:colou?rs?|sizes?|variants?|options?)\b", re.I)


def _is_variant_q(q: str) -> bool:
    """A 'what colours/sizes does X come in' question — answered by RAG from the
    product chunk's Options/Variants lines, not the structured aggregator (which
    would return a bare price/stock line). Must short-circuit BEFORE the LLM router,
    or the router re-resolves it to a catalog listing."""
    return bool(q) and bool(_VARIANT_Q_RE.search(q))


def _locate_target(q: str) -> str | None:
    """Extract the PRODUCT name from a 'which category/section is X in' / 'where do I
    find X' question. Returns None when the question isn't a reverse-location query."""
    if not q:
        return None
    m = _LOCATE_CAT_RE.match(q.strip()) or _LOCATE_WHERE_RE.match(q.strip())
    return m.group(1).strip() if m else None


def _answer_locate(target: str, rows: list[Row]) -> tuple[str, list[str]] | None:
    """Resolve `target` to a product and report the storefront collection(s) it sits in."""
    anchors = [t for t in re.sub(r"[^a-z0-9 ]", " ", target.lower()).split()
               if t not in _STOP and len(t) > 1]
    if not anchors:
        return None
    cand = [r for r in rows if _hit(r, anchors, title_only=True)]
    if not cand:
        aset = _anchor_set(anchors)
        scored = [(_title_cover(r.title, aset, anchors), r) for r in rows]
        scored = sorted([(c, r) for c, r in scored if c >= 0.85], key=lambda x: -x[0])
        cand = [scored[0][1]] if scored else []
    if not cand:
        return None
    # Prefer a resolved product that actually carries collection membership.
    r = next((x for x in cand if x.colls_disp), cand[0])
    cols = [c for c in r.colls_disp.split(" | ") if c]
    if not cols:
        return (f"{r.title} is in our catalog, but I don't have a specific category "
                f"listed for it."), ([r.source] if r.source else [])
    joined = ", ".join(cols)
    word = "category" if len(cols) == 1 else "categories"
    return (f"{r.title} is listed under the {joined} {word}.", [r.source] if r.source else [])


def answer_catalog_query(q: str, db, cfg: dict | None = None, max_list: int = 12) -> tuple[str | None, list[str]]:
    """Single front-door for structured catalog questions. Returns (text, sources),
    or (None, []) when the question is not a structured-catalog question (caller
    then falls back to ordinary retrieval/LLM).

    Deterministic regex parser runs first (exact, no LLM). If it declines AND the DB
    is a real catalog, ONE structured LLM extraction (the router) is tried as a net,
    so unusual phrasings still resolve — but execution stays fully deterministic, so
    a number is never invented; an unresolved entity falls through to RAG."""
    try:
        if not q or db is None:
            return None, []
        # Variant questions ("what colours/sizes does X come in") → RAG reads the
        # chunk's Options/Variants. Must return BEFORE the LLM router, which would
        # otherwise re-resolve to a bare catalog listing without the colours/sizes.
        if _is_variant_q(q):
            return None, []
        # Policy intent (shipping/returns/refund/warranty/payment/order-status) → hand
        # straight to RAG/policy BEFORE the LLM router. parse() already declines these,
        # but a code-mixed policy query ("used opened product return ho sakta hai?")
        # sets _force_llm, and the router would fabricate a product plan and answer with
        # a catalog non-answer instead of letting the policy page text answer.
        if _POLICY_Q_RE.search((q or "").lower()):
            return None, []
        # Reverse lookup — "which section/category is X in", "where do I find X".
        # The mirror of category listing: name a product, get the storefront
        # collection(s) it lives under (answers the customer browsing the sidebar).
        _loc = _locate_target(q)
        if _loc:
            _lrows = load_rows(db)
            if _lrows and is_catalog_db(db, _lrows, cfg):
                _ans = _answer_locate(_loc, _lrows)
                if _ans:
                    return _ans
        spec = parse(q)
        mp = _parse_multi(q)
        # A confident deterministic multi-product parse (explicit "X or/ya/vs Y" choice,
        # compare, basket with >=2 named products) is high-precision — the LLM router
        # below must not be allowed to downgrade it to a single product. Without this,
        # code-mixed "BMW S1000RR ya Kawasaki H2R available kaunsi?" set _force_llm and
        # the non-deterministic router sometimes replaced both names with one → flake.
        _det_mp = mp if (isinstance(mp, dict) and len(mp.get("names") or []) >= 2) else None
        # Same principle for a confident deterministic structured spec the small router
        # routinely degrades on code-mixed input: a count_split (both-stock-states, incl.
        # Roman-Urdu "kitne bik gaye kitne bache"), OR a min/max that carries a STOCK
        # dimension (include_oos / out_of_stock — e.g. "clearance mein unavailable mila
        # kar highest price"). The router drops that dimension and re-ranks the whole
        # catalog, so keep the regex spec when it confidently resolved one.
        _det_split = spec if (getattr(spec, "structured", False) and spec.anchors and (
            spec.agg == "count_split"
            or (spec.agg in ("min", "max")
                and (getattr(spec, "include_oos", False) or spec.out_of_stock)))) else None
        rows = None
        _from_router = False
        # Make the LLM understanding layer PRIMARY when the question is code-mixed
        # (Roman-Urdu/Hinglish) — the English regex would mis-route it. Also fire it,
        # as before, whenever the regex fully declined. English structured questions
        # skip it entirely → fast, free, unchanged.
        _force_llm = _is_code_mixed(q)
        if (not spec.structured and not mp) or _force_llm:
            # Deterministic router declined (or non-English input). Fire the LLM extractor
            # on ANY non-trivial question so unusual phrasings — SMS-speak, Roman-Urdu,
            # pronoun ranges, paraphrase — are understood instead of needing a hand-coded
            # regex per phrasing. The router returns "other" for genuine non-catalog
            # questions (safe → RAG); skip only obvious greetings/smalltalk.
            _wc = len((q or "").split())
            _skip = bool(_ROUTER_SKIP.match(q)) or (
                not _force_llm and not _PRODUCTISH.search(q) and (_wc < 2 or _wc > 30))
            plan = None
            if not _skip:
                rows = load_rows(db)
                if rows and is_catalog_db(db, rows, cfg):
                    try:
                        from services.catalog_router import extract_plan, heuristic_plan
                        # LLM extractor first; deterministic backstop on LLM outage. Ground
                        # the result in q so a weak model echoing a few-shot product on a
                        # content-free follow-up never becomes a wrong/cross-tenant anchor.
                        plan = _refine_plan(_ground_plan(extract_plan(q) or heuristic_plan(q), q), q)
                    except Exception:
                        plan = None
            # "browse" falls through to RAG unless it carries a price bound.
            _k = plan.get("kind") if plan else None
            _has_bound = bool(plan) and (plan.get("price_min") is not None or plan.get("price_max") is not None)
            if plan and _k not in (None, "other") and not (_k == "browse" and not _has_bound):
                _mp2, _spec2 = _plan_to_specs(plan)
                if _mp2 is not None or (_spec2 is not None and getattr(_spec2, "structured", False)):
                    # Keep the confident deterministic multi-product parse / count_split
                    # if present; only take the router's when the deterministic one was absent.
                    mp = _det_mp if _det_mp is not None else _mp2
                    spec = _det_split if _det_split is not None else _spec2
                    _from_router = True
            # If the LLM produced nothing usable, fall back to the regex parse when it
            # had one (so a partial English structure still answers); else hand to RAG.
            if not _from_router and not spec.structured and not mp:
                return None, []
        if rows is None:
            rows = load_rows(db)
            if not rows:
                return None, []
            # DB-type gate: only a real product catalog gets the structured engine; a
            # text/docs corpus (with stray priced rows) falls through to RAG/LLM.
            if not is_catalog_db(db, rows, cfg):
                return None, []
        # Deterministic "buyable/advisory" refinement — apply the in-stock filter to a
        # budget/list/min/max spec when the customer signalled they want something
        # PURCHASABLE ("X gift under N", "under N jo mil jaye / buy ho sake", "available
        # …"). This mirrors _refine_plan for the REGEX path so a sold-out item never
        # leaks when the LLM router didn't fire. Only ADDS the filter (never overrides an
        # explicit out/both/include-oos request). Universal, language-agnostic.
        if (getattr(spec, "structured", False) and spec.agg in ("list", "min", "max")
                and not spec.in_stock and not spec.out_of_stock
                and not getattr(spec, "include_oos", False)
                and _BUYABLE_INTENT.search(q or "")):
            spec.in_stock = True
        # Multi-product reasoning (compare / order / basket): fan out to resolve each
        # named product, then compute. Owns the question outright — a correct answer
        # or hand to RAG; never a single-product dump masquerading as a comparison.
        if mp:
            _mres = _answer_multi(mp, rows)
            if _mres:
                return _mres
            # Multi resolution failed (e.g. a broad "X or Y" that isn't two specific
            # products) → fall back to the single-spec parse rather than dropping to RAG,
            # so a valid category/list answer isn't lost to a buy-choice misfire.
            if not getattr(spec, "structured", False):
                return None, []
        return _execute_spec(spec, rows, cfg, max_list, from_router=_from_router, raw_q=q)
    except Exception:
        return None, []


def _execute_spec(spec: "Spec", rows: list[Row], cfg: dict | None, max_list: int,
                  from_router: bool = False, raw_q: str = "") -> tuple[str | None, list[str]]:
    """Deterministic execution of a (filter + aggregation) spec against the rows."""
    try:
        excl = _excl_re(cfg, spec.anchors)

        # Slash-joined synonyms ("motorcycle/bike") mean EITHER, but _hit demands ALL —
        # collapse each group to the catalog's actual term (highest doc-frequency) so a
        # synonym the store doesn't tag can't zero the set. Applied before generic relax.
        _anchors = list(spec.anchors)
        for grp in getattr(spec, "alt_groups", None) or []:
            present = [a for a in _anchors if a in grp]
            if len(present) > 1:
                keep = max(present, key=lambda a: _anchor_df(a, rows))
                _anchors = [a for a in _anchors if a == keep or a not in present]

        # Repair anchors the catalog doesn't contain (typos) against real merchant
        # vocabulary before matching, so "construccion"->"construction" resolves. Only
        # touches absent (df 0) tokens, so existing exact matches are untouched.
        _anchors = _correct_anchors(_anchors, rows)

        # Generic type-nouns ("car"/"model") demanded as hard anchors collapse an
        # aggregation set; relax them for matching while the label keeps full anchors.
        _m_anchors = _selective_anchors(_anchors, rows, spec.agg)

        _base = [r for r in rows if _hit(r, _m_anchors, spec.title_only)]
        _strict_hit = bool(_base)  # did strict all-anchor matching resolve anything?
        # LOST-CATEGORY RECOVERY: when the matching anchors are empty, the _hit above
        # matched the WHOLE catalog. For a PRICE-BOUNDED aggregation this would dump every
        # cheap item — the failure mode when the LLM router drops the category or returns
        # None under load ("F1 …under 9000" → empty spec + price bound). If the RAW query
        # actually names a category resolvable to a storefront collection, answer THAT set
        # instead of the catalog. Additive + universal: fires only on empty anchors, so a
        # real category query (has anchors) and a genuine price-only browse (no resolvable
        # category) are both untouched; needs a price bound so open min/max stay catalog-wide.
        _recovered_lock = False
        if (not _m_anchors and raw_q and spec.agg in ("list", "min", "max", "count", "count_split")
                and (spec.price_min is not None or spec.price_max is not None)):
            _rec = _resolve_collection(_anchors_from(raw_q), rows)
            if _rec:
                _base = _rec
                _strict_hit = True
                _recovered_lock = True
        if not _base and spec.anchors:
            # Strict all-anchor match found nothing — fall back to title-coverage so
            # a fully-named product still resolves. Only triggers when strict yields
            # 0, so a non-empty count can never be inflated.
            # The fallback's real job is the SUPERSET case: the query names the whole
            # stored title plus extra descriptors (coverage→1.0, e.g. "Pop N Play -
            # Quick Push Pop Game" → row "Pop N Play"). A loose 0.7 floor admitted
            # same-category SIBLINGS sharing only generic words ("Goldfish Fountain
            # Pen Single" for a query naming a DIFFERENT fountain pen) → a
            # confidently-wrong product. Require near-full coverage AND keep only the
            # best-covered rows, so an exact superset wins outright and a query for an
            # absent product yields nothing (→ graceful RAG/IDK, not a wrong sibling).
            # Match BIDIRECTIONALLY (this fallback only fires when the strict all-anchor
            # _hit found nothing, i.e. a specific NAMED product that didn't exact-match):
            #  - title-coverage catches a query that NAMES a stored title plus extras;
            #  - query-coverage catches a query that is a SUBSET of a long SEO title
            #    ("Rasha For Women By Al" → "...Rehab EDP") and, crucially, prefers a
            #    typo'd DISTINCTIVE token ("mgahribi"→"Maghribi") over a generic one
            #    ("by al"), so a misspelled brand resolves to its products instead of an
            #    unrelated sibling — and a truly absent name ("kawaii") matches nothing.
            # The avg-coverage floor rejects siblings sharing only generic words. Applies
            # to every aggregation: category queries match via _hit and never reach here.
            _aset = _anchor_set(spec.anchors)
            _scored = []
            for r in rows:
                tc = _title_cover(r.title, _aset, spec.anchors)
                qc = _query_cover(r.title, spec.anchors)
                if (tc >= 0.85 or qc >= 0.85) and (tc + qc) / 2 >= 0.5:
                    _scored.append((max(tc, qc), r))
            _scored.sort(key=lambda x: -x[0])
            _top = _scored[0][0] if _scored else 0.0
            _base = [r for c, r in _scored if c >= _top - 1e-9]
        # Collection-exact override: when the anchor names a STOREFRONT COLLECTION
        # (sidebar/menu group) verbatim, the authoritative member set is that
        # collection — never title/tag substring matches. "Fujifilm" the collection
        # is 2 cameras; "fujifilm" as a brand tag is on every film. Exact membership
        # returns the real group the customer is browsing, not coincidental name hits.
        _coll_locked = _recovered_lock  # recovered collection is authoritative → skip tag-narrowing
        if spec.anchors and spec.agg in ("count", "exists", "list", "count_split"):
            def _sig(toks):
                return frozenset(t for t in toks if len(t) >= 2 and t not in _STOP)
            _A = _sig(re.sub(r"[^a-z0-9]+", " ", " ".join(spec.anchors).lower()).split())
            if _A:
                # A row is a collection member iff one of its collection names has the
                # SAME significant-token set as the query (order-free, ignoring 1-char
                # tokens like the "z" in "Cra-Z-Art" and scaffolding words like "in").
                def _coll_match(r):
                    for el in r.colls.split("|"):
                        if el and _sig(el.split()) == _A:
                            return True
                    return False
                _members = [r for r in rows if _coll_match(r)]
                if _members:
                    _base = _members
                    _coll_locked = True
        # Fuzzy/subset collection resolution — the universal tier that ends the synonym
        # treadmill. When the exact same-token-set match above didn't lock (the common
        # case: "f1 cars" vs collection "formula one f1", "formula one" subset of it, a
        # typo, or a member-title word), resolve the phrasing onto the store's own
        # collection vocabulary. The storefront collection is authoritative for a
        # category query, so its members override title-substring _hit guesses.
        # Gated to strict-miss only: when strict all-anchor _hit DID resolve products,
        # that exact match stands (no hijacking "is <Product> available?" into a whole
        # collection). Fires only for phrasings strict matching couldn't resolve — the
        # cases that today fall to weak title-coverage or empty → near-zero regression.
        if (not _coll_locked and not _strict_hit and spec.anchors and not spec.title_only
                and spec.agg in ("count", "exists", "list", "count_split", "min", "max")):
            _rmem = _resolve_collection(_anchors, rows)
            if _rmem:
                _base = _rmem
                _coll_locked = True
        sel = []
        _oos = []  # matched the query but excluded only by the in-stock filter
        for r in _base:
            if excl and excl.search(r.title):
                continue
            # Only exclude clearly out-of-stock rows. Availability strings vary by
            # source ("available", "in stock", ""), so treat anything not explicitly
            # out-of-stock as in stock rather than demanding an exact "available".
            if spec.in_stock and re.search(r"out\s+of\s+stock|sold\s+out|unavailable|not\s+available", r.availability):
                _oos.append(r)
                continue
            if spec.out_of_stock and not _is_oos(r):
                continue
            sel.append(r)
        # Price bounds apply to every aggregation when present (count of X under Y,
        # cheapest X over Y, …) and require a known price.
        if spec.price_min is not None or spec.price_max is not None:
            def _in_bounds(p):
                if p is None:
                    return False
                if spec.price_min is not None:
                    if (p <= spec.price_min) if spec.strict_min else (p < spec.price_min):
                        return False
                if spec.price_max is not None:
                    if (p >= spec.price_max) if spec.strict_max else (p > spec.price_max):
                        return False
                return True
            sel = [r for r in sel if _in_bounds(r.price)]

        # Category precision (shared by count/min/max/list): in a tag-bearing catalog,
        # narrow to rows whose STRUCTURED categories match the anchor (GT/attribute
        # semantics) so "most expensive in the Models category" / "how many in Nautica"
        # rank the real category, not every title that happens to contain the word.
        # SKIPPED for name-substring queries (title_only): "every item whose NAME
        # contains monochrome" must keep ALL title matches — narrowing to the rows
        # that happen to also carry a coincidental "monochrome" category tag (e.g.
        # one marker set) silently drops the products the user actually asked for.
        # Also skipped for untagged/HTML-crawl DBs (tagged ratio < 50%).
        if not _coll_locked and spec.anchors and not spec.title_only and spec.agg in ("count", "min", "max", "list", "count_split"):
            _tagged = sum(1 for r in rows if r.cats.strip())
            if rows and (_tagged / len(rows)) >= 0.5:
                _tag_sel = [r for r in sel if _cats_hit(r, _m_anchors)]
                if _tag_sel:
                    sel = _tag_sel

        # Single named-product precision: when the query FULLY NAMES a specific
        # product (every significant word of a matched title is in the anchor set),
        # drop same-prefix SIBLINGS that only add descriptors — a lookup for
        # "BMW M8 1:24" must not also list "RC BMW M8 Diecast 1:24" or the "...Frame".
        # Parenthetical variant suffixes ("( 12 inches )") are ignored so an exact
        # name still resolves. Needs >=2 real anchors (a one-word category like
        # "bikes" must never collapse to a coincidental exact title) and is skipped
        # for name-substring (title_only) and price-bounded browses where the whole
        # matched set is the point.
        _real_anchors = [a for a in spec.anchors if a not in _STOP]
        if (sel and not spec.title_only and len(_real_anchors) >= 2
                and spec.price_min is None and spec.price_max is None):
            _aset_fn = _anchor_set(spec.anchors)
            def _fully_named(r):
                _t = re.sub(r"\([^)]*\)", " ", (r.title or "").lower())
                toks = [t for t in re.findall(r"[a-z0-9]{2,}", _t) if t not in _STOP]
                return bool(toks) and all(t in _aset_fn for t in toks)
            _named = [r for r in sel if _fully_named(r)]
            if _named and len(_named) < len(sel):
                sel = _named

        label = " ".join(spec.anchors) if spec.anchors else "products"

        if spec.agg == "stock":
            if not sel:
                # A stock-status question presupposes the product exists. If our exact
                # match can't resolve it, abstain to RAG (embeddings / honest "no info")
                # rather than assert absence — never a confident false "we don't carry".
                return None, []
            _oos_re = re.compile(r"out\s+of\s+stock|sold\s+out|unavailable|not\s+available")
            def _stk(r):
                st = "out of stock" if _oos_re.search(r.availability or "") else "in stock"
                pr = f" ({_price_disp(r)})" if r.price is not None else ""
                return r.title, st, pr
            if len(sel) == 1:
                t, st, pr = _stk(sel[0])
                msg = (f"Yes — {t} is available and in stock{pr}." if st == "in stock"
                       else f"{t} is currently out of stock{pr}.")
                return msg, _dedup([sel[0].source])[:max_list]
            lines = [f"- {t} — {st}{pr}" for t, st, pr in (_stk(r) for r in sel[:max_list])]
            if len(sel) > max_list:
                lines.append(f"…and {len(sel) - max_list} more — narrow it down (e.g. \"only in-stock\").")
            return "Here's the current availability:\n" + "\n".join(lines), _dedup(r.source for r in sel)[:max_list]

        if spec.agg == "count_split":
            if not sel:
                if from_router:
                    return None, []
                return (f"No — we don't currently carry any {label}. Is there something "
                        f"else I can help you find?"), []
            _av = [r for r in sel if not _is_oos(r)]
            _so = [r for r in sel if _is_oos(r)]
            n = len(sel)
            def _il(r):
                return f"- {r.title} — {_price_disp(r)}" if r.price is not None else f"- {r.title}"
            parts = [f"Of {n} {label} product{'s' if n != 1 else ''}: "
                     f"{len(_av)} available, {len(_so)} out of stock (sold out)."]
            if _av:
                parts.append("**Available:**\n" + "\n".join(_il(r) for r in _av[:max_list]))
            if _so:
                parts.append("**Out of stock / sold out:**\n" + "\n".join(_il(r) for r in _so[:max_list]))
            return "\n\n".join(parts), _dedup(r.source for r in sel)[:max_list]

        if spec.agg in ("count", "exists"):
            n = len(sel)
            if n == 0:
                # Found it, but the "in stock" filter removed it → it IS carried, just
                # out of stock. Don't mislead the customer into thinking we never have it.
                if spec.in_stock and _oos:
                    o = _oos[0]
                    pr = f" (normally {_price_s(o.price, o.currency)})" if o.price is not None else ""
                    return (f"We do carry {o.title}{pr}, but it's currently out of stock. "
                            f"Would you like me to suggest similar in-stock options?"), _dedup(r.source for r in _oos)[:max_list]
                # Router-sourced plan that resolved to nothing = LOW CONFIDENCE (a weak
                # LLM may have mis-classified a policy/shipping/smalltalk question as a
                # product existence query). Abstain to RAG instead of asserting a
                # hardcoded "we don't carry X" — never confidently wrong on a guess.
                # The high-precision regex parser keeps its crisp negative.
                if from_router:
                    return None, []
                if spec.agg == "exists":
                    return (f"No — we don't currently carry any {label}. I couldn't find any "
                            f"in our catalog. Is there something else I can help you find?"), []
                return (f"No — we don't currently carry any {label}. Is there something else "
                        f"I can help you find?"), []
            # Show the price alongside each item when known — answers the common
            # "do you have X and its price" multi-intent in one shot. When the query
            # explicitly FILTERED by stock ("count the sold-out bikes", "how many
            # available X"), state each item's status so an OOS-only / in-stock-only
            # list never reads like a neutral catalog dump.
            _stk_suffix = (" - out of stock" if spec.out_of_stock
                           else " - available" if spec.in_stock else "")
            def _il(r):
                base = f"- {r.title} — {_price_disp(r)}" if r.price is not None else f"- {r.title}"
                return base + _stk_suffix
            body = "\n".join(_il(r) for r in sel[:max_list])
            more = f"\n…and {n - max_list} more." if n > max_list else ""
            if spec.agg == "count":
                _kind = ("sold-out " if spec.out_of_stock else "available " if spec.in_stock else "")
                head = (f"We have {n} {_kind}{label} product{'s' if n != 1 else ''}:"
                        if spec.anchors else f"We have {n} {_kind}products in our catalog:")
            else:
                head = f"Yes — we carry {label}. We have {n} option{'s' if n != 1 else ''}:"
            return f"{head}\n{body}{more}", _dedup(r.source for r in sel)[:max_list]

        # min / max / list — all need a price.
        priced = [r for r in sel if r.price is not None]
        if not priced:
            if spec.agg == "list" and sel:
                body = "\n".join(f"- {r.title}" for r in sel[:max_list])
                if len(sel) > max_list:
                    body += f"\n…and {len(sel) - max_list} more — narrow it down (e.g. \"only in-stock\")."
                return f"Here are matching products from the catalog:\n{body}", _dedup(r.source for r in sel)[:max_list]
            return None, []
        # Extremes (cheapest / most expensive) should name something a customer can
        # actually buy: prefer in-stock rows, falling back to all only if EVERY priced
        # match is out of stock (so we still answer rather than claim absence).
        # SKIPPED when the question wants the true extreme across all stock ("highest
        # priced X overall") — then a pricier out-of-stock item is the right answer.
        if spec.agg in ("min", "max") and not spec.include_oos:
            _avail = [r for r in priced if not re.search(
                r"out\s+of\s+stock|sold\s+out|unavailable|not\s+available", r.availability or "")]
            if _avail:
                priced = _avail
        priced.sort(key=lambda r: (-r.price if spec.agg == "max" else r.price, r.title.lower()))
        n_show = max_list if spec.agg == "list" else 5
        items = priced[:n_show]
        # Name what was matched so a category/collection browse echoes the group the
        # customer asked for ("the Diecast Decor products"), not an anonymous header.
        head = (f"Here are the matching {label} products:"
                if spec.agg == "list" and spec.anchors
                else "Here are matching products from the catalog:")
        lines = [head]
        for i, r in enumerate(items, 1):
            lines.append(f"{i}. {r.title} - {_price_disp(r)} - {r.availability}")
        # Never truncate silently: a "show me all X" answer that drops items reads
        # as the full catalog — say how many more exist so the customer can ask on.
        if len(priced) > n_show:
            lines.append(f"…and {len(priced) - n_show} more — narrow it down (e.g. \"only in-stock\" or \"under Rs.5000\").")
        return "\n".join(lines), _dedup(r.source for r in items)[:max_list]
    except Exception:
        return None, []
