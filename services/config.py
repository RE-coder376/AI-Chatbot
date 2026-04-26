"""
services/config.py — Atomic JSON I/O, global config, per-DB config, and DB secrets.

Standalone — no FastAPI imports, no app-level state.
"""
import json
import logging
import os
import re
import shutil
import threading
from pathlib import Path

from services.auth import _hash_password, _is_password_hash, _validate_db_name

logger = logging.getLogger(__name__)

DATABASES_DIR = Path("databases")
CONFIG_FILE = Path("config.json")
ACTIVE_DB_FILE = Path("active_db.txt")
DB_SECRETS_FILE = "secrets.json"
DB_SECRET_KEYS = {"admin_password", "smtp_password", "sendgrid_keys", "widget_key", "api_sources"}


def _atomic_write_json(path: Path, data) -> None:
    """Write JSON atomically — crash during write leaves original intact."""
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
        shutil.move(str(tmp), str(path))
    except Exception as e:
        logger.error(f"Atomic write failed for {path}: {e}")
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def _safe_db_secret_name(db_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "_", (db_name or "").strip()).upper()


def _load_db_secrets(db_name: str) -> dict:
    """Load untracked/env per-DB secrets without requiring them in tracked config.json."""
    name = (db_name or "").strip()
    if not name or not re.match(r"^[a-zA-Z0-9_\-]+$", name):
        return {}
    data = {}
    secret_file = DATABASES_DIR / name / DB_SECRETS_FILE
    if secret_file.exists():
        try:
            raw = json.loads(secret_file.read_text(encoding="utf-8-sig"))
            if isinstance(raw, dict):
                data.update({k: v for k, v in raw.items() if k in DB_SECRET_KEYS})
        except Exception as e:
            logger.warning(f"Could not parse {secret_file}: {e}")
    prefix = _safe_db_secret_name(name)
    env_json = os.getenv(f"DB_{prefix}_SECRETS_JSON", "")
    if env_json:
        try:
            raw_env = json.loads(env_json)
            if isinstance(raw_env, dict):
                data.update({k: v for k, v in raw_env.items() if k in DB_SECRET_KEYS})
        except Exception as e:
            logger.warning(f"Could not parse DB_{prefix}_SECRETS_JSON: {e}")
    env_map = {
        "admin_password": os.getenv(f"DB_{prefix}_ADMIN_PASSWORD", ""),
        "smtp_password": os.getenv(f"DB_{prefix}_SMTP_PASSWORD", ""),
        "widget_key": os.getenv(f"DB_{prefix}_WIDGET_KEY", ""),
        "sendgrid_keys": os.getenv(f"DB_{prefix}_SENDGRID_KEYS_JSON", ""),
        "api_sources": os.getenv(f"DB_{prefix}_API_SOURCES_JSON", ""),
    }
    for key, value in env_map.items():
        if not value:
            continue
        if key in ("sendgrid_keys", "api_sources"):
            try:
                data[key] = json.loads(value)
            except Exception as e:
                logger.warning(f"Could not parse DB_{prefix}_{key.upper()} env JSON: {e}")
        else:
            data[key] = value
    return data


def _save_db_secrets(db_name: str, updates: dict) -> None:
    """Persist only sensitive DB settings to an untracked per-DB secrets file."""
    name = _validate_db_name(db_name)
    secret_updates = {k: v for k, v in updates.items() if k in DB_SECRET_KEYS}
    if not secret_updates:
        return
    secret_file = DATABASES_DIR / name / DB_SECRETS_FILE
    secret_file.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if secret_file.exists():
        try:
            existing = json.loads(secret_file.read_text(encoding="utf-8-sig"))
        except Exception as e:
            logger.warning(f"_save_db_secrets: could not parse {secret_file}: {e}")
    # Avoid storing plaintext admin passwords for tenants.
    if secret_updates.get("admin_password") and not _is_password_hash(secret_updates["admin_password"]):
        secret_updates["admin_password"] = _hash_password(secret_updates["admin_password"])
    safe_updates = {k: v for k, v in secret_updates.items() if v != "" or existing.get(k, "") == ""}
    existing.update(safe_updates)
    _atomic_write_json(secret_file, existing)


def get_config(db_name: str = "") -> dict:
    """Merge root config with active DB config. DB values override root (empty strings skipped).
    Per-DB secrets are loaded from untracked secrets.json/env overlays.
    If db_name is provided, uses that DB instead of active_db.txt."""
    root = {}
    if CONFIG_FILE.exists():
        try:
            root = json.loads(CONFIG_FILE.read_text(encoding="utf-8-sig"))
        except Exception as e:
            logger.warning(f"get_config: could not parse {CONFIG_FILE}: {e}")
    if not root:
        root = {"admin_password": "", "bot_name": "AI Assistant",
                "business_name": "Our Company", "branding": {}}
    active = db_name or (ACTIVE_DB_FILE.read_text(encoding="utf-8").strip() if ACTIVE_DB_FILE.exists() else "")
    if active:
        db_cfg_file = DATABASES_DIR / active / "config.json"
        if db_cfg_file.exists():
            try:
                db_cfg = json.loads(db_cfg_file.read_text(encoding="utf-8-sig"))
                for k, v in db_cfg.items():
                    if v != "" and v is not None:
                        if k == "branding" and isinstance(v, dict) and isinstance(root.get("branding"), dict):
                            merged_branding = dict(root.get("branding", {}))
                            for bk, bv in v.items():
                                if bv != "" and bv is not None:
                                    merged_branding[bk] = bv
                            root["branding"] = merged_branding
                        else:
                            root[k] = v
            except Exception as e:
                logger.warning(f"get_config: could not parse DB config for '{active}': {e}")
        for k, v in _load_db_secrets(active).items():
            if v != "" and v is not None:
                root[k] = v
    # NOTE: ADMIN_PASSWORD env var is intentionally NOT overlaid here — it's the super-admin
    # password checked separately in admin_auth(). Overlaying it would break per-DB client auth.
    env_smtp = os.getenv("SMTP_PASSWORD")
    if env_smtp:
        root["smtp_password"] = env_smtp
    env_sg = [v for k, v in os.environ.items() if k.startswith("SENDGRID_KEY_") and v]
    if env_sg:
        existing = [k for k in root.get("sendgrid_keys", []) if k]
        root["sendgrid_keys"] = list(dict.fromkeys(env_sg + existing))
    return root


def save_config(config: dict) -> None:
    """Save non-sensitive global settings to root config only."""
    _atomic_write_json(CONFIG_FILE, config)


def save_db_config(updates: dict, db_name: str = "", upload_callback=None) -> None:
    """Save public DB settings to config.json and sensitive settings to secrets.json."""
    active = db_name or (ACTIVE_DB_FILE.read_text(encoding="utf-8").strip() if ACTIVE_DB_FILE.exists() else "")
    if not active:
        save_config({**get_config(), **updates})
        return
    _save_db_secrets(active, updates)
    public_updates = {k: v for k, v in updates.items() if k not in DB_SECRET_KEYS}
    db_cfg_file = DATABASES_DIR / active / "config.json"
    existing = {}
    if db_cfg_file.exists():
        try:
            existing = json.loads(db_cfg_file.read_text(encoding="utf-8-sig"))
        except Exception as e:
            logger.warning(f"save_db_config: could not parse {db_cfg_file}: {e}")
    # Never overwrite existing non-empty values with empty strings
    safe_updates = {k: v for k, v in public_updates.items() if v != "" or existing.get(k, "") == ""}
    if safe_updates:
        existing.update(safe_updates)
        _atomic_write_json(db_cfg_file, existing)
    if upload_callback:
        threading.Thread(target=upload_callback, args=(active,), daemon=True).start()
