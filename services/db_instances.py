"""
services/db_instances.py ? ChromaDB instance management and caching.
Uses late imports for app.py globals (local_db, embeddings_model, legacy_embeddings).
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from services.config import DATABASES_DIR

logger = logging.getLogger(__name__)

_db_instance_cache: dict = {}   # db_name → Chroma instance (LRU, max 50)
_DB_CACHE_MAX = 1  # Render free tier: 512MB RAM — only 1 extra DB instance in cache

def _ensure_tmp_chroma(db_name: str, src_path: Path) -> Path:
    """Copy ChromaDB from overlayfs → /tmp (tmpfs) to avoid SQLite WAL mode failures on Docker.
    On Windows this is unnecessary (no overlayfs) — use src_path directly.
    Falls back to src_path if src has no DB yet (GH sync still running) or tmp copy is corrupt."""
    import sys as _sys, shutil as _shutil
    if _sys.platform == "win32":
        return src_path  # No WAL fix needed on Windows
    # If source has no sqlite3 yet (GH sync still downloading), use src directly
    if not (src_path / "chroma.sqlite3").exists():
        return src_path
    tmp_path = Path(f"/dev/shm/chroma_{db_name}")
    if not (tmp_path / "chroma.sqlite3").exists():
        tmp_path.mkdir(parents=True, exist_ok=True)
        for item in src_path.iterdir():
            if item.name in ("config.json", "visitor_history"):
                continue
            try:
                if item.is_file():
                    _shutil.copy2(str(item), str(tmp_path / item.name))
                elif item.is_dir():
                    _shutil.copytree(str(item), str(tmp_path / item.name), dirs_exist_ok=True)
            except Exception as _e:
                logger.warning(f"[ChromaDB] copy {item.name} → /tmp failed: {_e}")
        logger.info(f"[ChromaDB] Copied {db_name} → /dev/shm/chroma_{db_name} (overlayfs WAL fix)")
    # Validate tmp — if corrupt (no collection), wipe and use src directly
    try:
        import chromadb as _cdb
        _cdb.PersistentClient(path=str(tmp_path)).get_collection("langchain")
    except Exception:
        logger.warning(f"[ChromaDB] tmp for {db_name} is corrupt — wiping, using src directly")
        _shutil.rmtree(str(tmp_path), ignore_errors=True)
        return src_path
    return tmp_path

def _get_or_create_db(db_name: str):
    """Return Chroma instance for db_name, creating it fresh if no chroma.sqlite3 exists yet."""
    import app as _app
    existing = _get_db_instance(db_name) if db_name else None
    if existing:
        return existing
    # No existing DB — create a new empty one so ingest can populate it
    if not db_name:
        return _app.local_db  # fall back to active DB
    db_path = DATABASES_DIR / db_name
    db_path.mkdir(parents=True, exist_ok=True)
    from langchain_chroma import Chroma
    tmp_dir = _ensure_tmp_chroma(db_name, db_path)
    instance = Chroma(persist_directory=str(tmp_dir), embedding_function=_app.embeddings_model)
    instance._db_name = db_name
    _db_instance_cache[db_name] = instance
    return instance

def _get_db_instance(db_name: str):
    """Return a Chroma instance for any DB (not just active). Cached per db_name."""
    import app as _app
    if db_name in _db_instance_cache:
        return _db_instance_cache[db_name]
    db_path = DATABASES_DIR / db_name
    if not db_path.exists():
        return None
    # Don't create a new Chroma instance if no DB file exists (API-only DBs like mal)
    if not (db_path / "chroma.sqlite3").exists():
        return None
    db_cfg_file = db_path / "config.json"
    db_cfg = {}
    if db_cfg_file.exists():
        try: db_cfg = json.loads(db_cfg_file.read_text(encoding="utf-8-sig"))
        except: pass
    emb_setting = db_cfg.get("embedding_model", "bge")

    if emb_setting == "minilm_old":
        # Legacy DBs indexed with all-MiniLM-L6-v2 (384-dim)
        if _app.legacy_embeddings is None:
            try:
                from langchain_huggingface import HuggingFaceEmbeddings
                _app.legacy_embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
            except ImportError:
                logger.warning("[DB] sentence-transformers not installed, falling back to fastembed")
                _app.legacy_embeddings = _app.embeddings_model
        emb = _app.legacy_embeddings
    else:
        # All other DBs (bge, multilingual, unset) — crawler always writes bge-small-en-v1.5 (384-dim)
        # so we must query with the same model. bge-base-en-v1.5 is 768-dim and would mismatch.
        emb = _app.embeddings_model  # FastEmbed BAAI/bge-small-en-v1.5, 384-dim
    tmp_dir = _ensure_tmp_chroma(db_name, db_path)
    instance = Chroma(persist_directory=str(tmp_dir), embedding_function=emb)
    instance._db_name = db_name  # stored for BM25 lookup
    # LRU eviction — remove oldest entry when over limit
    if len(_db_instance_cache) >= _DB_CACHE_MAX:
        oldest = next(iter(_db_instance_cache))
        _db_instance_cache.pop(oldest, None)
    _db_instance_cache[db_name] = instance
    return instance
