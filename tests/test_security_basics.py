import pytest


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

    monkeypatch.setattr(app_module, "_get_or_create_db", lambda name: _DummyDB(), raising=True)

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
