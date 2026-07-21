def test_catalog_reingest_creates_new_database(app_module, monkeypatch):
    class Collection:
        def delete(self, **kwargs):
            return None

    class Database:
        _collection = Collection()

    database = Database()
    calls = []

    monkeypatch.setattr(
        app_module,
        "_get_or_create_db",
        lambda db_name: calls.append(db_name) or database,
    )
    monkeypatch.setattr(
        app_module,
        "_get_db_instance",
        lambda db_name: (_ for _ in ()).throw(AssertionError("read-only loader used")),
    )
    monkeypatch.setattr(app_module, "_sanitize_docs_for_chroma", lambda docs: docs)
    monkeypatch.setattr(app_module, "_add_documents_deterministic", lambda db, docs: None)

    from services import catalog_api

    monkeypatch.setattr(catalog_api, "build_catalog_docs", lambda *args, **kwargs: [object()])
    monkeypatch.setattr(catalog_api, "get_last_collection_coverage", lambda: {})

    result = app_module.catalog_reingest_products("new_store", "https://example.com")

    assert calls == ["new_store"]
    assert result["products_ingested"] == 1
