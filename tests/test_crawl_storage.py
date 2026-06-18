import ast
import hashlib
import logging
from pathlib import Path
import re
import uuid

import pytest

from services.db_instances import _tmp_chroma_dir


def test_tmp_chroma_dir_honors_configured_root(monkeypatch):
    configured_root = Path("/tmp/crawl-storage-test")
    monkeypatch.setenv("CHROMA_TMP_ROOT", str(configured_root))

    assert _tmp_chroma_dir("agentfactory") == configured_root / "chroma_agentfactory"


def test_deterministic_add_propagates_storage_failure():
    tree = ast.parse(Path("app.py").read_text(encoding="utf-8"))
    wanted = {"_stable_chunk_id", "_add_documents_deterministic"}
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in wanted]
    namespace = {
        "hashlib": hashlib,
        "logging": logging,
        "logger": logging.getLogger(__name__),
        "re": re,
        "uuid": uuid,
    }
    exec(compile(ast.Module(body=nodes, type_ignores=[]), "app.py", "exec"), namespace)
    deterministic_add = namespace["_add_documents_deterministic"]

    class FailingCollection:
        def get(self, **_kwargs):
            return {"ids": []}

    class FailingDb:
        _collection = FailingCollection()

        def add_documents(self, *_args, **_kwargs):
            raise OSError("database or disk is full")

    class Doc:
        page_content = "storage failure regression"
        metadata = {"source": "https://example.test/page"}

    with pytest.raises(RuntimeError, match="database or disk is full"):
        deterministic_add(FailingDb(), [Doc()])


def test_modal_crawl_has_disk_backed_staging():
    source = Path("modal_app.py").read_text(encoding="utf-8")

    assert "ephemeral_disk=32768" in source
    assert '"CHROMA_TMP_ROOT": "/tmp/chroma"' in source
    assert '"CHROMA_TMP_ALWAYS_REFRESH": "1"' in source
