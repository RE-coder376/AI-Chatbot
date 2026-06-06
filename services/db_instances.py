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

def _sqlite_mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except Exception:
        return None

def _tmp_chroma_dir(db_name: str) -> Path:
    return Path(f"/dev/shm/chroma_{db_name}")

def _resolve_source_dir(db_path: Path) -> Path | None:
    """Return the live Chroma source directory, preferring nested chroma/ when present."""
    for candidate in (db_path / "chroma", db_path):
        if (candidate / "chroma.sqlite3").exists():
            return candidate
    return None

def _ensure_tmp_chroma(db_name: str, src_path: Path, refresh: bool = False) -> Path:
    """Copy ChromaDB from overlayfs → /tmp (tmpfs) to avoid SQLite WAL mode failures on Docker.
    On Windows this is unnecessary (no overlayfs) — use src_path directly.
    Falls back to src_path if src has no DB yet (GH sync still running) or tmp copy is corrupt."""
    import sys as _sys, shutil as _shutil
    if _sys.platform == "win32":
        return src_path  # No WAL fix needed on Windows
    # If source has no sqlite3 yet (GH sync still downloading), use src directly
    if not (src_path / "chroma.sqlite3").exists():
        return src_path
    tmp_path = _tmp_chroma_dir(db_name)
    src_sqlite = src_path / "chroma.sqlite3"
    tmp_sqlite = tmp_path / "chroma.sqlite3"
    src_mtime = _sqlite_mtime(src_sqlite)
    tmp_mtime = _sqlite_mtime(tmp_sqlite)
    needs_refresh = refresh or (not tmp_sqlite.exists()) or (src_mtime is not None and (tmp_mtime is None or src_mtime > tmp_mtime + 1e-6))
    if needs_refresh:
        _shutil.rmtree(str(tmp_path), ignore_errors=True)
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

def _is_cached_instance_stale(db_name: str, instance) -> bool:
    """Return True when the cached Chroma instance points at older data than the on-disk DB."""
    db_path = DATABASES_DIR / db_name
    src_dir = _resolve_source_dir(db_path)
    if src_dir is None:
        return False
    try:
        persist_dir = Path(getattr(instance, "_persist_directory", "") or "")
    except Exception:
        persist_dir = Path("")
    if not str(persist_dir):
        persist_dir = _tmp_chroma_dir(db_name)
    cached_sqlite = persist_dir / "chroma.sqlite3"
    src_sqlite = src_dir / "chroma.sqlite3"
    src_mtime = _sqlite_mtime(src_sqlite)
    cached_mtime = _sqlite_mtime(cached_sqlite)
    if src_mtime is None:
        return False
    if cached_mtime is None:
        return True
    return src_mtime > cached_mtime + 1e-6

def _get_or_create_db(db_name: str, refresh: bool = False):
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
    tmp_dir = _ensure_tmp_chroma(db_name, db_path, refresh=refresh)
    instance = Chroma(persist_directory=str(tmp_dir), embedding_function=_app.embeddings_model)
    instance._db_name = db_name
    _db_instance_cache[db_name] = instance
    return instance

def _get_db_instance(db_name: str, refresh: bool = False):
    """Return a Chroma instance for any DB (not just active). Cached per db_name."""
    import app as _app
    from langchain_chroma import Chroma
    if refresh:
        _db_instance_cache.pop(db_name, None)
    if db_name in _db_instance_cache and not refresh:
        cached = _db_instance_cache[db_name]
        if not _is_cached_instance_stale(db_name, cached):
            return cached
        logger.info(f"[DB] Cached Chroma for '{db_name}' is stale — rebuilding from disk")
        _db_instance_cache.pop(db_name, None)
    db_path = DATABASES_DIR / db_name
    if not db_path.exists():
        return None
    # Don't create a new Chroma instance if no DB file exists (API-only DBs like mal).
    # However, on some Linux deployments we may still have a valid tmpfs copy in /dev/shm.
    src_dir = _resolve_source_dir(db_path)
    tmp_sqlite = Path(f"/dev/shm/chroma_{db_name}/chroma.sqlite3")
    if src_dir is None and not tmp_sqlite.exists():
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
    if src_dir is not None:
        tmp_dir = _ensure_tmp_chroma(db_name, src_dir, refresh=refresh)
    else:
        # Source sqlite missing but tmpfs copy exists; use it directly.
        tmp_dir = Path(f"/dev/shm/chroma_{db_name}")
        logger.info(f"[DB] Using tmpfs-only Chroma for '{db_name}' from {tmp_dir}")
    instance = Chroma(persist_directory=str(tmp_dir), embedding_function=emb)
    instance._db_name = db_name  # stored for BM25 lookup
    # LRU eviction — remove oldest entry when over limit
    if len(_db_instance_cache) >= _DB_CACHE_MAX:
        oldest = next(iter(_db_instance_cache))
        _db_instance_cache.pop(oldest, None)
    _db_instance_cache[db_name] = instance
    return instance
