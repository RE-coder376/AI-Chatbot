import re
from types import SimpleNamespace

import pytest

from services.catalog_query import Row, _ROW_CACHE, _execute_spec, load_rows, parse


def _row(title, price, availability="available"):
    return Row(
        title=title,
        price=price,
        availability=availability,
        source=f"https://example.test/{title.lower().replace(' ', '-')}",
        hay=title.lower(),
    )


def _collection_row(title, price, collection, availability="available"):
    row = _row(title, price, availability)
    normalized = re.sub(r"[^a-z0-9]+", " ", collection.lower()).strip()
    row.colls = f"|{normalized}|"
    row.colls_disp = collection
    return row


class _FakeCollection:
    name = "langchain"
    id = "same-collection-id"

    def __init__(self, title, price):
        self._title = title
        self._price = price

    def count(self):
        return 1

    def get(self, limit, include):
        return {
            "documents": [f"Product: {self._title}\nPrice: Rs.{self._price}\nAvail: InStock"],
            "metadatas": [
                {
                    "source": f"https://example.test/products/{self._title.lower().replace(' ', '-')}",
                    "chunk_kind": "product",
                    "content_type": "product",
                    "product_title": self._title,
                    "price": self._price,
                    "availability": "available",
                }
            ],
        }


def test_load_rows_cache_is_tenant_isolated_for_same_collection_name_and_count():
    _ROW_CACHE.clear()
    db_a = SimpleNamespace(_db_name="tenant_a", _collection=_FakeCollection("Tenant A Product", 100))
    db_b = SimpleNamespace(_db_name="tenant_b", _collection=_FakeCollection("Tenant B Product", 200))

    rows_a = load_rows(db_a)
    rows_b = load_rows(db_b)

    assert rows_a[0].title == "Tenant A Product"
    assert rows_b[0].title == "Tenant B Product"


def test_load_rows_preserves_collection_names_containing_pipe():
    class Collection:
        name = "langchain"
        id = "pipe-collection"

        def count(self):
            return 1

        def get(self, limit, include):
            return {
                "documents": ["Product: Clearance Bag\nPrice: Rs.1,750\nAvailability: InStock"],
                "metadatas": [{
                    "source": "https://example.test/products/clearance-bag",
                    "chunk_kind": "product",
                    "product_title": "Clearance Bag",
                    "price": 1750,
                    "availability": "available",
                    "collections": '["Under Rs. 1,750 | Clearance Sale", "Bags for Women"]',
                }],
            }

    _ROW_CACHE.clear()
    rows = load_rows(SimpleNamespace(_db_name="pipe_tenant", _collection=Collection()))

    assert rows[0].colls == "|under rs 1 750 clearance sale|bags for women|"


@pytest.mark.parametrize(
    ("question", "aggregation", "expected"),
    [
        ("Ignoring sold-out items, which product costs the least?", "min", "Available Cheap - Rs.20"),
        ("Among available products only, what costs the most?", "max", "Available Expensive - Rs.981,000"),
        ("Show me the priciest product I can actually buy right now.", "max", "Available Expensive - Rs.981,000"),
    ],
)
def test_customer_extreme_phrasings_use_full_available_catalog(question, aggregation, expected):
    rows = [
        _row("Sold Out Cheap", 5, "out of stock"),
        _row("Available Cheap", 20),
        _row("Available Expensive", 981000),
        _row("Sold Out Expensive", 2000000, "sold out"),
    ]

    spec = parse(question)
    answer, _ = _execute_spec(spec, rows, None, 5)

    assert spec.agg == aggregation
    assert spec.anchors == []
    assert expected in answer
    assert "Sold Out" not in answer


def test_count_split_preserves_hand_in_hand_bags_collection():
    rows = [
        _collection_row("Black Handbag", 2500, "Hand Bags"),
        _collection_row("Tan Handbag", 2200, "Hand Bags", "sold out"),
        _collection_row("Canvas Tote", 1800, "Bags for Women"),
    ]

    spec = parse("Hand Bags mein kitne mojood kitne nahi")
    answer, _ = _execute_spec(spec, rows, None, 10)

    assert spec.agg == "count_split"
    assert spec.anchors == ["hand", "bags"]
    assert "Of 2 hand bags products: 1 available, 1 out of stock" in answer


def test_price_named_collection_is_not_filtered_by_its_own_title():
    collection = "Under Rs. 1,750 | Clearance Sale"
    rows = [
        _collection_row("Mustard Bag", 1499, collection),
        _collection_row("Tan Bag", 1499, collection),
        _collection_row("Beige Bag", 1750, collection),
        _collection_row("Pink Bag", 1750, collection),
        _collection_row("Blue Bag", 1499, collection, "sold out"),
        _collection_row("Silver Bag", 1750, collection, "sold out"),
    ]

    question = "how many Under Rs. 1,750 | Clearance Sale do you have?"
    spec = parse(question)
    answer, _ = _execute_spec(spec, rows, None, 10, raw_q=question)

    assert spec.agg == "count"
    assert "We have 6" in answer


@pytest.mark.parametrize(
    "question",
    [
        "Do you stock the Talkng Flash Card Lernning Toy?",
        "im looking for a talking flash crad learning toy, do u have it?",
        "Got any Talking Flash Cards Learnng Toy?",
    ],
)
def test_typoed_inventory_questions_resolve_the_named_product(question):
    rows = [
        _row("Talking Flash Card Learning Toy", 1350),
        _row("Talking Flash Cards Early Educational Device 224 Cards Rechargeable", 1500),
    ]

    spec = parse(question)
    answer, _ = _execute_spec(spec, rows, None, 5)

    assert spec.agg == "exists"
    assert "Talking Flash Card Learning Toy" in answer
    assert "Rs.1,350" in answer
    assert "Early Educational Device" not in answer


@pytest.mark.parametrize(
    "question",
    [
        "do u have Rasha For Women By Al?",   # subset of the long SEO title
        "do you have Rsaha For Women By Al?",  # + a typo in the distinctive word
    ],
)
def test_subset_of_seo_title_resolves_named_product(question):
    """A customer types part of a long SEO title (and maybe a typo); the title's
    extra trailing words must not sink the match into a false 'not carried'."""
    rows = [
        _row("Rasha For Women By Al Rehab EDP", 990),
        _row("Sultan For Men & Women Attar By Al Haramain", 2499),
        _row("CR7 For Men By Cristiano Ronaldo EDT", 3500),
    ]

    spec = parse(question)
    answer, _ = _execute_spec(spec, rows, None, 5)

    assert spec.agg == "exists"
    assert "Rasha For Women By Al Rehab EDP" in answer
    assert "don't" not in answer.lower() and "not carry" not in answer.lower()
    # generic-word siblings must NOT be dumped as matches
    assert "Sultan" not in answer and "CR7" not in answer


def test_typoed_distinctive_token_beats_generic_sibling():
    """A typo on the DISTINCTIVE word ("mgahribi"→"Maghribi") must resolve to that
    brand's products, not fall through to a sibling sharing only generic words
    ("by al"). Applies to the price/list path, not just exists."""
    rows = [
        _row("Rasha For Women By Al Rehab EDP", 990),
        _row("Rawdha By Ahmed Al Maghribi", 10750),
        _row("Zeleny For Unisex By Ahmed Al Maghribi", 4999),
        _row("Sultan For Men & Women By Al Haramain", 2499),
    ]
    spec = parse("how much is the mgahribi by al?")
    answer, _ = _execute_spec(spec, rows, None, 5)
    assert "Maghribi" in answer
    assert "Rasha" not in answer and "Sultan" not in answer


def test_absent_typoed_name_does_not_dump_siblings():
    """A query for a product that simply isn't carried ("kwaai") must not dump
    unrelated products — say it's absent (exists) or hand off to RAG (price)."""
    rows = [
        _row("Today To Do Spiral Gold Rings Notepad", 695),
        _row("Revolution Gel Pen Set", 450),
    ]
    spec = parse("do you have kwaai and whats the price?")
    answer, _ = _execute_spec(spec, rows, None, 5)
    assert "Today To Do" not in answer and "Revolution" not in answer


@pytest.mark.parametrize(
    "question",
    [
        "and do you also stock the Wish For Women By Chopard?",
        "do you have the Wish For Women By Chopard too?",
    ],
)
def test_followup_adverb_does_not_pollute_anchor(question):
    """"also"/"too" in a follow-up ("do you ALSO stock X") must not fold into the
    product name — that produced "also wish ..." → a false absence that then broke
    a pronoun follow-up with no antecedent to bind."""
    rows = [
        _row("Wish For Women By Chopard EDP", 9250),
        _row("Ajayeb Dubai By Lattafa For Men", 3000),
    ]
    spec = parse(question)
    answer, _ = _execute_spec(spec, rows, None, 5)
    assert spec.agg == "exists"
    assert "also" not in spec.anchors and "too" not in spec.anchors
    assert "Wish For Women By Chopard EDP" in answer
    assert "don't" not in answer.lower()
