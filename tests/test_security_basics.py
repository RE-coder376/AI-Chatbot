import asyncio

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
    assert r.status_code == 200
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
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name: (tests, 0), raising=True)
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
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name: (tests, 0), raising=True)
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
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name: (tests, 0), raising=True)

    r = client.post(
        "/admin/evals/generate",
        headers={"Authorization": "Bearer ownerpw", "X-Admin-DB": "a", "X-CSRF-Token": token},
        json={"password": "ownerpw", "db_name": "a", "count": 5, "strategy": "kb"},
    )
    assert r.status_code == 200
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
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name: (tests, 0), raising=True)
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


def test_filter_eval_tests_for_tenant_trusts_tenant_local_chunk_sources(app_module):
    tests = [
        {"q": "What is Build Merging Skill?", "source": "chunk_topic", "reference_answer": "Build merging skill helps combine agent outputs in the curriculum."},
        {"q": "What premium fountain pens do you stock?", "source": "chunk_topic", "reference_answer": "We stock luxury fountain pens and ink refills."},
        {"q": "What premium fountain pens do you stock and what's the price range?", "source": "analytics"},
    ]
    kept, dropped = app_module._filter_eval_tests_for_tenant(tests, "agentfactory")
    assert len(kept) == 1
    assert kept[0]["source"] == "chunk_topic"
    assert dropped == 2


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
    monkeypatch.setattr(app_module, "_filter_eval_tests_for_tenant", lambda tests, db_name: (tests, 0), raising=True)
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
