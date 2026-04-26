import importlib
import os
from pathlib import Path

import pytest
import services.config as _svc_config


@pytest.fixture()
def app_module(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Provides an isolated app module instance with:
    - background startup tasks disabled (UNIT_TEST=1)
    - isolated config/databases/active_db files under tmp_path
    """
    monkeypatch.setenv("UNIT_TEST", "1")
    monkeypatch.setenv("ADMIN_PASSWORD", "ownerpw")

    import app as app_mod
    importlib.reload(app_mod)

    # Re-point global paths into the tmp dir so tests don't touch real tenant data.
    db_dir = tmp_path / "databases"
    db_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(app_mod, "DATABASES_DIR", db_dir, raising=True)
    monkeypatch.setattr(app_mod, "CONFIG_FILE", tmp_path / "config.json", raising=True)
    monkeypatch.setattr(app_mod, "ACTIVE_DB_FILE", tmp_path / "active_db.txt", raising=True)
    monkeypatch.setattr(_svc_config, "DATABASES_DIR", db_dir, raising=True)
    monkeypatch.setattr(_svc_config, "CONFIG_FILE", tmp_path / "config.json", raising=True)
    monkeypatch.setattr(_svc_config, "ACTIVE_DB_FILE", tmp_path / "active_db.txt", raising=True)

    # Ensure a stable active DB name for any fallback paths.
    (tmp_path / "active_db.txt").write_text("a", encoding="utf-8")

    # Reset in-memory state that can leak across tests.
    app_mod._widget_key_cache.clear()
    app_mod._csrf_tokens.clear()
    app_mod._db_instance_cache.clear()
    app_mod._intro_q_cache.clear()
    app_mod._bm25_cache.clear()

    return app_mod


@pytest.fixture()
def client(app_module):
    from fastapi.testclient import TestClient

    return TestClient(app_module.app)


def _make_db(app_module, name: str, admin_password: str, widget_key: str):
    db_path = app_module.DATABASES_DIR / name
    db_path.mkdir(parents=True, exist_ok=True)
    (db_path / "config.json").write_text("{}", encoding="utf-8")
    # Store as plaintext here; app supports legacy plaintext and will hash on future saves.
    (db_path / app_module.DB_SECRETS_FILE).write_text(
        app_module.json.dumps({"admin_password": admin_password, "widget_key": widget_key}, indent=2),
        encoding="utf-8",
    )


@pytest.fixture()
def two_tenants(app_module):
    _make_db(app_module, "a", "clientA", "wk_a")
    _make_db(app_module, "b", "clientB", "wk_b")
    return {"a": {"pw": "clientA", "wk": "wk_a"}, "b": {"pw": "clientB", "wk": "wk_b"}}

