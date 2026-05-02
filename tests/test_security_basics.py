import asyncio
from types import SimpleNamespace

import pytest
from evals import eval_v1


def test_health_ok(client):
    r = client.get("/health")
    assert r.status_code == 200
    data = r.json()
    assert "status" in data
    assert "active_db" in data


def test_chat_missing_question(client):
    r = client.post("/chat", json={})
    assert r.status_code == 400


def test_widget_key_routing_invalid_key_401(client):
    r = client.post("/chat", headers={"X-Widget-Key": "bad"}, json={"question": "hi", "history": [], "stream": True})
    assert r.status_code == 401


def test_widget_key_routing_valid_key_200(app_module, client, two_tenants, monkeypatch):
    # Avoid any LLM/RAG work: patch the streaming generator to emit a deterministic response.
    async def _fake_stream(*args, **kwargs):
        yield 'data: {"type":"chunk","content":"ok"}\n\n'
        yield 'data: {"type":"metadata","capture_lead":false,"sources":[]}\n\n'
        yield 'data: {"type":"done"}\n\n'

    monkeypatch.setattr(app_module, "chat_stream_generator", _fake_stream, raising=True)
    r = client.post(
        "/chat",
        headers={"X-Widget-Key": two_tenants["a"]["wk"]},
        json={"question": "hi", "history": [], "stream": True},
    )
    assert r.status_code == 200, r.text
    assert "data:" in r.text


def test_eval_answer_via_stream_captures_workflow_trace(app_module, monkeypatch):
    async def _fake_stream(*args, **kwargs):
        yield 'data: {"type":"chunk","content":"I do not have details."}\n\n'
        yield 'data: {"type":"metadata","capture_lead":false,"sources":["https://example.test/doc"],"workflow_trace":{"events":[{"event":"guard_exit","details":{"guard":"sparse_kb_context_miss"}}]},"workflow_debug":{"guard_decisions":{"exit_guard":"sparse_kb_context_miss"},"answer_artifacts":{"system_prompt_final":"prompt","raw_llm_answer":"raw"}}}\n\n'
        yield 'data: {"type":"done"}\n\n'

    monkeypatch.setattr(app_module, "chat_stream_generator", _fake_stream, raising=True)
    answer, sources, workflow_trace, workflow_debug = asyncio.run(app_module._eval_answer_via_stream("q", {}, object(), "a"))

    assert answer == "I do not have details."
    assert sources == ["https://example.test/doc"]
    assert workflow_trace["events"][0]["event"] == "guard_exit"
    assert workflow_debug["guard_decisions"]["exit_guard"] == "sparse_kb_context_miss"


def test_product_query_rerank_score_prefers_exact_product_slug(app_module):
    exact = SimpleNamespace(
        metadata={"source": "https://www.thestationerycompany.pk/products/piano-wipe-easy-whiteboard-marker-pack-of-12"},
        page_content="Piano Wipe Easy Whiteboard Marker Pack Of 12 Rs.1,200",
    )
    neighbor = SimpleNamespace(
        metadata={"source": "https://www.thestationerycompany.pk/products/piano-wipe-easy-70-whiteboard-marker-single-piece"},
        page_content="Piano Wipe-Easy 70 Whiteboard Marker Single Piece Rs.125",
    )

    exact_score = app_module._product_query_rerank_score(
        "What is the price of Piano Wipe Easy Whiteboard Marker Pack Of 12?",
        exact,
    )
    neighbor_score = app_module._product_query_rerank_score(
        "What is the price of Piano Wipe Easy Whiteboard Marker Pack Of 12?",
        neighbor,
    )

    assert exact_score > neighbor_score


def test_prepare_crawl_page_strips_storefront_boilerplate_and_detects_product(app_module):
    raw = (
        "Piano Wipe Easy Whiteboard Marker Pack Of 12. "
        "Price: Rs.1,200. Availability: In stock. Description: Smooth whiteboard markers for classrooms. "
        "You may also like Fancy Pen Set. Recently viewed Random Notebook. Your cart Subtotal Checkout."
    )

    cleaned, meta = app_module._prepare_crawl_page(
        raw,
        "https://www.thestationerycompany.pk/products/piano-wipe-easy-whiteboard-marker-pack-of-12",
        title_hint="Piano Wipe Easy Whiteboard Marker Pack Of 12",
    )

    assert "You may also like" not in cleaned
    assert "Recently viewed" not in cleaned
    assert meta["page_type"] == "product"
    assert meta["product"]["price_label"].lower().startswith("rs.")
    assert meta["quality_score"] >= 0.6
    assert "price" in meta["used_structured_fields"]
    assert meta["retrieve_eligible"] is True
    assert meta["extraction_mode"] == "structured_product"


def test_smart_chunk_page_uses_structured_product_summary_for_rs_pages(app_module):
    text = (
        "Name: Daler Rowney Turpentine Oil. Price: Rs.2,045. "
        "Availability: In stock. Description: Artist-grade turpentine oil for paint thinning."
    )
    clean, meta = app_module._prepare_crawl_page(
        text,
        "https://www.thestationerycompany.pk/products/daler-rowney-turpentine-oil",
        title_hint="Daler Rowney Turpentine Oil",
    )

    docs = app_module._smart_chunk_page(
        clean,
        "https://www.thestationerycompany.pk/products/daler-rowney-turpentine-oil",
        page_meta=meta,
    )

    assert len(docs) == 1
    assert "Product: Daler Rowney Turpentine Oil" in docs[0].page_content
    assert "Price: Rs.2,045" in docs[0].page_content
    assert docs[0].metadata["price"] == 2045.0


def test_retrieval_visible_doc_hides_structural_and_contaminated_chunks(app_module):
    structural = SimpleNamespace(
        metadata={"source": "https://example.com/profile", "structural": True},
        page_content="Profile progress dashboard",
    )
    contaminated = SimpleNamespace(
        metadata={"source": "https://example.com/products/x", "contaminated": True},
        page_content="premium fashion designer wear",
    )
    normal = SimpleNamespace(
        metadata={"source": "https://example.com/products/y"},
        page_content="Real product content",
    )

    assert app_module._retrieval_visible_doc(structural) is False
    assert app_module._retrieval_visible_doc(contaminated) is False
    assert app_module._retrieval_visible_doc(normal) is True


def test_prepare_crawl_page_unknown_fallback_stays_low_confidence(app_module):
    cleaned, meta = app_module._prepare_crawl_page(
        "Widget Widget hello compare share click here",
        "https://example.com/misc/widget",
        title_hint="Widget",
    )

    assert cleaned
    assert meta["page_type"] == "unknown"
    assert meta["quality_score"] < 0.5
    assert meta["retrieve_eligible"] is False
    assert meta["quarantine_reason"] == "low_quality"


def test_merge_variant_docs_dedups_same_title_and_price(app_module):
    doc_a = SimpleNamespace(
        metadata={
            "source": "https://example.com/products/a",
            "product_title": "Piano Marker",
            "price": 1200.0,
        },
        page_content="Product: Piano Marker\nPrice: Rs.1,200\nVariant: Blue",
    )
    doc_b = SimpleNamespace(
        metadata={
            "source": "https://example.com/products/b",
            "product_title": "Piano Marker",
            "price": 1200.0,
        },
        page_content="Product: Piano Marker\nPrice: Rs.1,200\nVariant: Red",
    )

    merged = app_module._merge_variant_docs([doc_a, doc_b])

    assert len(merged) == 1
    assert merged[0].metadata["variant_count"] == 2
    assert "Variants:" in merged[0].page_content
    assert merged[0].metadata["dedup_applied"] is True


def test_prepare_crawl_page_category_is_structural_lite(app_module):
    cleaned, meta = app_module._prepare_crawl_page(
        "Category: Markers Sort by Featured Showing 1 to 24 Product type marker collections filters.",
        "https://example.com/collections/markers",
        title_hint="Markers",
    )

    assert cleaned
    assert meta["page_type"] == "category"
    assert meta["retrieve_eligible"] is False
    assert meta["quarantine_reason"] == "category"


def test_prepare_crawl_page_weak_product_fallback_gets_quarantined(app_module):
    cleaned, meta = app_module._prepare_crawl_page(
        "Piano Marker Pack of 12. Smooth marker for school use.",
        "https://example.com/products/piano-marker-pack-of-12",
        title_hint="Piano Marker Pack of 12",
    )

    assert cleaned
    assert meta["page_type"] == "product"
    assert meta["body_fallback_used"] is True
    assert meta["retrieve_eligible"] is False
    assert meta["quarantine_reason"] == "weak_product_fallback"
    assert meta["quality_score"] < 0.5


def test_json_safe_converts_non_serializable_values(app_module):
    payload = {
        "items": {"a", "b"},
        "nested": {"obj": object()},
    }

    safe = app_module._json_safe(payload)

    assert isinstance(safe["items"], list)
    assert isinstance(safe["nested"]["obj"], str)


def test_csrf_blocks_write_without_token(app_module, client, two_tenants):
    # POST /admin/branding is CSRF protected. Without X-CSRF-Token, middleware should block.
    r = client.post(
        "/admin/branding",
        headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a"},
        json={"password": "ownerpw", "db_name": "a", "branding": {"bot_name": "X"}},
    )
    assert r.status_code == 403


def test_csrf_allows_write_with_token(client, two_tenants):
    token_resp = client.get("/admin/csrf-token", headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a"})
    assert token_resp.status_code == 200
    token = token_resp.json()["csrf_token"]

    r = client.post(
        "/admin/branding",
        headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a", "X-CSRF-Token": token},
        json={"password": "ownerpw", "db_name": "a", "branding": {"bot_name": "X"}},
    )
    assert r.status_code == 200


def test_tenant_isolation_branding_client_cannot_access_other_db(client, two_tenants):
    # Client A can access branding for A...
    ok = client.get("/admin/branding", headers={"Authorization": "Bearer clientA", "X-Admin-DB": "a"})
    assert ok.status_code == 200
    # ...but not for B.
    bad = client.get("/admin/branding", headers={"Authorization": "Bearer clientA", "X-Admin-DB": "b"})
    assert bad.status_code == 401


def test_tenant_isolation_databases_endpoint(client, two_tenants):
    r = client.get("/admin/databases", headers={"Authorization": "Bearer clientA", "X-Admin-DB": "a"})
    assert r.status_code == 200
    dbs = r.json().get("databases", [])
    assert [d["name"] for d in dbs] == ["a"]


def test_tenant_isolation_db_stats_endpoint(client, two_tenants):
    r = client.get("/admin/db-stats", headers={"Authorization": "Bearer clientA", "X-Admin-DB": "a"})
    assert r.status_code == 200
    stats = r.json().get("stats", [])
    assert all(s.get("name") == "a" for s in stats)


def test_owner_gates_reject_client_password(app_module, client, two_tenants, monkeypatch):
    # /debug/retrieve should be owner-only.
    monkeypatch.setattr(app_module, "local_db", object(), raising=False)

    async def _fake_retrieve(*args, **kwargs):
        return ("", 0, [])

    monkeypatch.setattr(app_module, "retrieve_context", _fake_retrieve, raising=True)
    r = client.post("/debug/retrieve", json={"password": "clientA", "question": "hi"})
    assert r.status_code == 401

    # /admin/sync-github should be owner-only.
    r2 = client.post("/admin/sync-github", headers={"Authorization": "Bearer clientA", "X-Admin-DB": "a"})
    assert r2.status_code == 401

    # /admin/reindex should be owner-only (CSRF fires first → 403, auth → 401, both are rejections).
    r3 = client.post("/admin/reindex", json={"password": "clientA"})
    assert r3.status_code in (401, 403)


def test_owner_gates_evals_run_and_smoke_owner_run(app_module, client, two_tenants, monkeypatch):
    # Acquire CSRF token as owner (required for /admin/* POST routes).
    token_resp = client.get("/admin/csrf-token", headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a"})
    assert token_resp.status_code == 200
    token = token_resp.json()["csrf_token"]

    # Client must not be able to run evals (owner-only).
    r = client.post(
        "/admin/evals/run",
        headers={"Authorization": "Bearer clientA", "X-Admin-DB": "a", "X-CSRF-Token": token},
        json={"password": "clientA", "db_name": "a", "mode": "retrieve", "tests": [{"q": "hi", "expect": {"type": "ANSWER"}}]},
    )
    assert r.status_code == 401

    # Owner run should work even without a real ChromaDB: monkeypatch DB retrieval to a dummy object.
    class _Doc:
        def __init__(self, source: str):
            self.metadata = {"source": source}

    class _DummyDB:
        def similarity_search(self, q, k):
            return [_Doc("https://example.com/source1")]

    async def _fake_eval_retrieve_docs(q, tenant_db, k=8):
        return (1, [{"source": "https://example.com/source1", "preview": "about this topic"}], ["https://example.com/source1"])

    monkeypatch.setattr(app_module, "_get_or_create_db", lambda name: _DummyDB(), raising=True)
    monkeypatch.setattr(app_module, "_owner_eval_blocker", lambda name: "", raising=True)
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name, fingerprint=None: (tests, 0), raising=True)
    async def _fake_audit(*args, **kwargs):
        return {}
    monkeypatch.setattr(app_module, "_safe_runtime_tenant_audit", _fake_audit, raising=True)
    monkeypatch.setattr(app_module, "_owner_eval_judge_key", lambda data=None: "", raising=True)
    monkeypatch.setattr(app_module, "_eval_retrieve_docs", _fake_eval_retrieve_docs, raising=True)

    ok = client.post(
        "/admin/evals/run",
        headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a", "X-CSRF-Token": token},
        json={
            "password": "ownerpw",
            "db_name": "a",
            "mode": "retrieve",
            "tests": [
                {"id": "t1", "difficulty": "easy", "q": "What is this about?", "expect": {"type": "ANSWER", "expected_source": "example.com"}},
                {"id": "t2", "difficulty": "easy", "q": "What is 2+2?", "expect": {"type": "REFUSE"}},
            ],
        },
    )
    assert ok.status_code == 200
    data = ok.json()
    assert data["db"] == "a"
    assert "scores" in data and "overall" in data["scores"]
    assert data["results"][0]["overall_status"] in ("PASS", "FAIL")
    assert data["results"][0]["question"] == "What is this about?"


def test_owner_evals_run_fallback_prefers_saved_or_kb_strategy(app_module, client, two_tenants, monkeypatch):
    token_resp = client.get("/admin/csrf-token", headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a"})
    assert token_resp.status_code == 200
    token = token_resp.json()["csrf_token"]

    class _DummyDB:
        def similarity_search(self, q, k):
            return []

    seen = {}

    async def _fake_build(base_url, owner_password, db_name, count, strategy="kb"):
        seen["strategy"] = strategy
        return [{"id": "t1", "difficulty": "easy", "q": "What is AgentFactory?", "expect": {"type": "ANSWER", "expected_source": ""}}]

    async def _fake_eval_retrieve_docs(q, tenant_db, k=8):
        return (0, [], [])

    monkeypatch.setattr(app_module, "_get_or_create_db", lambda name: _DummyDB(), raising=True)
    monkeypatch.setattr(app_module, "_owner_eval_blocker", lambda name: "", raising=True)
    monkeypatch.setattr(app_module, "_load_eval_set", lambda name: {"strategy": "kb", "tests": []}, raising=True)
    monkeypatch.setattr(app_module, "_build_owner_eval_tests", _fake_build, raising=True)
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name, fingerprint=None: (tests, 0), raising=True)
    async def _fake_audit(*args, **kwargs):
        return {}
    monkeypatch.setattr(app_module, "_safe_runtime_tenant_audit", _fake_audit, raising=True)
    monkeypatch.setattr(app_module, "_eval_retrieve_docs", _fake_eval_retrieve_docs, raising=True)

    ok = client.post(
        "/admin/evals/run",
        headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a", "X-CSRF-Token": token},
        json={"password": "ownerpw", "db_name": "a", "mode": "retrieve"},
    )
    assert seen["strategy"] == "kb"


def test_owner_eval_judge_key_prefers_request_value(app_module, monkeypatch):
    monkeypatch.setattr(app_module.os, "getenv", lambda key, default="": "env-judge-key" if key == "JUDGE_API_KEY" else default, raising=True)
    assert app_module._owner_eval_judge_key({"judge_key": "request-key"}) == "request-key"
    assert app_module._owner_eval_judge_key({}) == "env-judge-key"


def test_admin_generate_evals_respects_requested_count(app_module, client, two_tenants, monkeypatch):
    token_resp = client.get("/admin/csrf-token", headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a"})
    token = token_resp.json()["csrf_token"]
    seen = {}

    async def _fake_build(base_url, owner_password, db_name, count, strategy="kb"):
        seen["count"] = count
        return [{"q": f"Q{i}", "source": "chunk_topic"} for i in range(count)]

    monkeypatch.setattr(app_module, "_owner_eval_blocker", lambda name: "", raising=True)
    monkeypatch.setattr(app_module, "_build_owner_eval_tests", _fake_build, raising=True)
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name, fingerprint=None: (tests, 0), raising=True)
    async def _fake_audit(*args, **kwargs):
        return {}
    monkeypatch.setattr(app_module, "_safe_runtime_tenant_audit", _fake_audit, raising=True)

    r = client.post(
        "/admin/evals/generate",
        headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a", "X-CSRF-Token": token},
        json={"password": "ownerpw", "db_name": "a", "count": 5, "strategy": "kb"},
    )
    assert r.status_code == 200, r.text
    assert seen["count"] == 5
    assert r.json()["count"] == 5


def test_admin_run_evals_respects_requested_count(app_module, client, two_tenants, monkeypatch):
    token_resp = client.get("/admin/csrf-token", headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a"})
    token = token_resp.json()["csrf_token"]
    seen = {}

    async def _fake_build(base_url, owner_password, db_name, count, strategy="kb"):
        seen["count"] = count
        return [{"id": f"t{i}", "q": f"Q{i}", "expect": {"type": "ANSWER"}, "source": "chunk_topic"} for i in range(count)]

    async def _fake_eval_answer(*args, **kwargs):
        return ("answer", [], {"events": []}, {"guard_decisions": {}, "answer_artifacts": {}})

    monkeypatch.setattr(app_module, "_owner_eval_blocker", lambda name: "", raising=True)
    monkeypatch.setattr(app_module, "_load_eval_set", lambda name: {}, raising=True)
    monkeypatch.setattr(app_module, "_build_owner_eval_tests", _fake_build, raising=True)
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name, fingerprint=None: (tests, 0), raising=True)
    async def _fake_audit(*args, **kwargs):
        return {}
    monkeypatch.setattr(app_module, "_safe_runtime_tenant_audit", _fake_audit, raising=True)
    monkeypatch.setattr(app_module, "_owner_eval_judge_key", lambda data=None: "", raising=True)
    monkeypatch.setattr(app_module, "_get_or_create_db", lambda name: object(), raising=True)
    monkeypatch.setattr(app_module, "_eval_answer_via_stream", _fake_eval_answer, raising=True)
    monkeypatch.setattr(app_module, "_eval_retrieve_docs", lambda q, tenant_db, k=8: (0, [], []), raising=True)

    r = client.post(
        "/admin/evals/run",
        headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a", "X-CSRF-Token": token},
        json={"password": "ownerpw", "db_name": "a", "count": 5, "mode": "chat", "strategy": "kb"},
    )
    assert r.status_code == 200
    assert seen["count"] == 5
    assert r.json()["counts"]["total"] == 5


def test_admin_run_evals_chat_mode_scores_retrieval_even_when_sources_are_suppressed(app_module, client, two_tenants, monkeypatch):
    token_resp = client.get("/admin/csrf-token", headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a"})
    token = token_resp.json()["csrf_token"]

    async def _fake_build(base_url, owner_password, db_name, count, strategy="kb"):
        return [{"id": "t1", "difficulty": "easy", "q": "What is AgentFactory?", "expect": {"type": "ANSWER"}, "source": "chunk_topic"}]

    async def _fake_eval_answer(*args, **kwargs):
        return (
            "I don't have enough information right now.",
            [],
            {"events": []},
            {"guard_decisions": {"cleaned_answer_class": "IDK", "source_suppressed_reason": "idk"}, "answer_artifacts": {"raw_llm_answer": "Grounded answer", "cleaned_answer": "I don't have enough information right now."}},
        )

    async def _fake_eval_retrieve_docs(q, tenant_db, k=8):
        return (
            2,
            [{"source": "https://example.com/source1", "preview": "AgentFactory is an AI agent learning platform with guided chapters."}],
            ["https://example.com/source1"],
        )

    monkeypatch.setattr(app_module, "_owner_eval_blocker", lambda name: "", raising=True)
    monkeypatch.setattr(app_module, "_build_owner_eval_tests", _fake_build, raising=True)
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name, fingerprint=None: (tests, 0), raising=True)
    async def _fake_audit(*args, **kwargs):
        return {}
    monkeypatch.setattr(app_module, "_safe_runtime_tenant_audit", _fake_audit, raising=True)
    monkeypatch.setattr(app_module, "_owner_eval_judge_key", lambda data=None: "", raising=True)
    monkeypatch.setattr(app_module, "_get_or_create_db", lambda name: object(), raising=True)
    monkeypatch.setattr(app_module, "_eval_answer_via_stream", _fake_eval_answer, raising=True)
    monkeypatch.setattr(app_module, "_eval_retrieve_docs", _fake_eval_retrieve_docs, raising=True)

    r = client.post(
        "/admin/evals/run",
        headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a", "X-CSRF-Token": token},
        json={"password": "ownerpw", "db_name": "a", "count": 1, "mode": "chat", "strategy": "kb"},
    )

    assert r.status_code == 200
    data = r.json()
    assert data["counts"]["retrieval_total"] == 1
    assert data["results"][0]["retrieval_status"] in ("PASS", "FAIL")
    assert data["scores"]["retrieval"] >= 0.0


def test_runtime_chunk_seed_items_generates_product_questions(app_module):
    class _DummyDB:
        pass

    async def _run():
        return await app_module._runtime_chunk_seed_items(_DummyDB(), "store", 3)

    async def _fake_eval_retrieve_docs(q, tenant_db, k=6):
        return (
            2,
            [
                {
                    "source": "https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops",
                    "preview": 'Product: Acer Spin 5 SP513-51 Black Price: $999.99 Full specs: Acer Spin 5 SP513-51 Black, 13.3", 8GB, Windows 10',
                }
            ],
            ["https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops"],
        )

    setattr(app_module, "_eval_retrieve_docs", _fake_eval_retrieve_docs)
    items = asyncio.run(_run())

    assert items
    assert items[0].q == "What is the pricing for Acer Spin 5 SP513-51 Black?"
    assert items[0].source == "chunk_topic"


def test_filter_eval_tests_for_tenant_trusts_tenant_local_chunk_sources(app_module, monkeypatch):
    monkeypatch.setattr(
        eval_v1,
        "_tenant_scope_tokens",
        lambda db_name: {"agentfactory", "curriculum", "agent", "agents", "build", "learning"},
    )
    tests = [
        {"q": "What is Build Merging Skill?", "source": "chunk_topic", "reference_answer": "Build merging skill helps combine agent outputs in the curriculum."},
        {"q": "What premium fountain pens do you stock?", "source": "chunk_topic", "reference_answer": "We stock luxury fountain pens and ink refills."},
        {"q": "What premium fountain pens do you stock and what's the price range?", "source": "analytics"},
    ]
    kept, dropped = app_module._filter_eval_tests_for_tenant(tests, "agentfactory")
    assert len(kept) == 1
    assert kept[0]["source"] == "chunk_topic"
    assert dropped == 2


def test_filter_eval_tests_for_tenant_uses_expect_reference_and_expected_source(app_module, monkeypatch):
    monkeypatch.setattr(
        eval_v1,
        "_tenant_scope_tokens",
        lambda db_name: {"store", "web", "scraper", "laptops", "phones", "electronics"},
    )
    tests = [
        {
            "q": "What does Give Your Employee An Identity cover?",
            "source": "chunk_topic",
            "expect": {
                "reference_text": "This chapter covers identity design in the AgentFactory curriculum.",
                "expected_source": "https://agentfactory.panaversity.org/chapters/give-your-employee-an-identity",
            },
        },
        {
            "q": "What is the pricing for ASUS VivoBook laptops?",
            "source": "chunk_topic",
            "expect": {
                "reference_text": "The store lists ASUS VivoBook laptop prices and specs.",
                "expected_source": "https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops",
            },
        },
    ]

    kept, dropped = app_module._filter_eval_tests_for_tenant(tests, "store")

    assert len(kept) == 1
    assert kept[0]["q"] == "What is the pricing for ASUS VivoBook laptops?"
    assert dropped == 1


def test_filter_eval_tests_for_tenant_keeps_runtime_grounded_rows(app_module, monkeypatch):
    monkeypatch.setattr(
        eval_v1,
        "_tenant_scope_tokens",
        lambda db_name: {"store"},
    )
    tests = [
        {
            "q": "What is the pricing for Acer Spin 5 SP513-51 Black?",
            "source": "chunk_topic",
            "runtime_grounded": True,
            "expect": {
                "reference_text": "Acer Spin 5 SP513-51 Black pricing and specs are listed in the store.",
                "expected_source": "https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops",
            },
        }
    ]

    kept, dropped = app_module._filter_eval_tests_for_tenant(tests, "store")

    assert len(kept) == 1
    assert dropped == 0


def test_filter_eval_tests_for_tenant_uses_runtime_fingerprint_when_config_is_generic(app_module, monkeypatch):
    monkeypatch.setattr(eval_v1, "_tenant_scope_tokens", lambda db_name: {"store"}, raising=True)
    fingerprint = {
        "db_name": "store",
        "scope_tokens": ["stationery", "journal", "pen", "planner"],
        "source_hosts": ["stationerystudio.pk"],
        "status": "runtime_only",
    }
    tests = [
        {
            "q": "What is the pricing for Daily Journal Planner?",
            "source": "chunk_topic",
            "expect": {
                "reference_text": "Daily Journal Planner is listed with stationery details.",
                "expected_source": "https://stationerystudio.pk/products/daily-journal-planner",
            },
        },
        {
            "q": "What is the pricing for ASUS VivoBook laptops?",
            "source": "chunk_topic",
            "expect": {
                "reference_text": "Laptop product pricing is available.",
                "expected_source": "https://webscraper.io/test-sites/e-commerce/allinone/computers/laptops",
            },
        },
    ]

    kept, dropped = app_module._filter_eval_tests_for_tenant(tests, "store", fingerprint)

    assert len(kept) == 1
    assert kept[0]["q"] == "What is the pricing for Daily Journal Planner?"
    assert dropped == 1


def test_build_runtime_tenant_fingerprint_marks_runtime_only_when_config_is_generic(app_module, monkeypatch):
    monkeypatch.setattr(
        app_module,
        "get_config",
        lambda db_name="": {"business_name": "Our Company", "topics": "", "business_description": ""},
        raising=True,
    )
    fingerprint = app_module._build_runtime_tenant_fingerprint(
        "store",
        {"business_name": "Our Company", "topics": "", "business_description": ""},
        [
            {
                "source": "https://stationerystudio.pk/products/daily-journal-planner",
                "preview": "Daily Journal Planner with stationery details and weekly planning pages.",
                "probe": "product",
            }
        ],
    )

    assert fingerprint["status"] == "runtime_only"
    assert fingerprint["config_is_generic"] is True
    assert "stationerystudio.pk" in fingerprint["source_hosts"]
    assert "stationery" in fingerprint["scope_tokens"] or "journal" in fingerprint["scope_tokens"]


def test_admin_run_evals_uses_deterministic_fallback_when_judge_errors(app_module, client, two_tenants, monkeypatch):
    token_resp = client.get("/admin/csrf-token", headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a"})
    token = token_resp.json()["csrf_token"]

    class _DummyDB:
        def similarity_search(self, q, k):
            return []

    async def _fake_build(base_url, owner_password, db_name, count, strategy="kb"):
        return [{"id": "t1", "difficulty": "easy", "q": "How does the curriculum work?", "expect": {"type": "ANSWER"}, "source": "chunk_topic"}]

    async def _fake_eval_answer(*args, **kwargs):
        return (
            "I don't have enough information right now.",
            [],
            {"events": []},
            {
                "guard_decisions": {
                    "deterministic_retrieval_diagnosis": "retrieval_ok",
                    "cleaned_answer_class": "IDK",
                    "source_suppressed_reason": "idk",
                },
                "answer_artifacts": {
                    "raw_llm_answer": "The curriculum is organized in guided chapters.",
                    "cleaned_answer": "I don't have enough information right now.",
                },
            },
        )

    async def _fake_eval_retrieve_docs(q, tenant_db, k=8):
        return (
            1,
            [{"source": "https://example.com/curriculum", "preview": "The curriculum is organized in guided chapters with projects."}],
            ["https://example.com/curriculum"],
        )

    class _JudgeError:
        def __call__(self, *args, **kwargs):
            return eval_v1.JudgeVerdict(error="judge_retryable_http_429")

    monkeypatch.setattr(app_module, "_get_or_create_db", lambda name: _DummyDB(), raising=True)
    monkeypatch.setattr(app_module, "_owner_eval_blocker", lambda name: "", raising=True)
    monkeypatch.setattr(app_module, "_build_owner_eval_tests", _fake_build, raising=True)
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name, fingerprint=None: (tests, 0), raising=True)
    async def _fake_audit(*args, **kwargs):
        return {}
    monkeypatch.setattr(app_module, "_safe_runtime_tenant_audit", _fake_audit, raising=True)
    monkeypatch.setattr(app_module, "_eval_answer_via_stream", _fake_eval_answer, raising=True)
    monkeypatch.setattr(app_module, "_eval_retrieve_docs", _fake_eval_retrieve_docs, raising=True)
    monkeypatch.setattr(eval_v1, "judge_answer", _JudgeError(), raising=True)

    r = client.post(
        "/admin/evals/run",
        headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a", "X-CSRF-Token": token},
        json={"password": "ownerpw", "db_name": "a", "mode": "chat", "strategy": "kb", "judge_key": "separate-key", "count": 1},
    )

    assert r.status_code == 200
    row = r.json()["results"][0]
    assert row["judge"]["likely_failure_source"] == "prompt_overconstraint"
    assert row["judge"]["exact_failure_step"] == "source_suppression:idk"
