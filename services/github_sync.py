"""
services/github_sync.py — GitHub release sync: upload, download, delete DB zips.

Standalone — no FastAPI imports, no route handlers.
"""
import logging
import os
import subprocess
import threading
from datetime import datetime
from pathlib import Path

from services.config import ACTIVE_DB_FILE, DATABASES_DIR

logger = logging.getLogger(__name__)

KEYS_FILE = Path("keys.json")
RESTORE_ALLOWLIST_FILE = Path("restore_allowlist.txt")
DELETED_DBS_FILE = Path("databases") / "deleted_dbs.txt"

_GITHUB_USERNAME = os.getenv("GITHUB_USERNAME", "RE-coder376")
_GITHUB_REPO     = os.getenv("GITHUB_REPO", "databases")
_GITHUB_CLONE_DIR = Path("/tmp/chatbot-dbs")

# Mutable dict — always mutate in-place (never reassign) so callers that hold
# a reference to this dict see live updates without re-importing.
_github_sync_result: dict = {"status": "not_run", "detail": ""}


def _load_restore_allowlist() -> set[str]:
    """Return DB names allowed to auto-restore from GitHub snapshots."""
    allowed: set[str] = set()
    try:
        if RESTORE_ALLOWLIST_FILE.exists():
            allowed = {
                line.strip()
                for line in RESTORE_ALLOWLIST_FILE.read_text(encoding="utf-8-sig").splitlines()
                if line.strip()
            }
    except Exception as e:
        logger.warning(f"[GH-SYNC] Could not read restore allowlist: {e}")
    return allowed


def _load_deleted_db_names() -> set[str]:
    """Return DB names that must never be restored after deletion."""
    deleted: set[str] = set()
    try:
        if DELETED_DBS_FILE.exists():
            deleted.update({
                line.strip()
                for line in DELETED_DBS_FILE.read_text(encoding="utf-8-sig").splitlines()
                if line.strip()
            })
    except Exception as e:
        logger.warning(f"[GH-SYNC] Could not read deleted_dbs.txt locally: {e}")
    try:
        pat = os.environ.get("GITHUB_PAT", "").strip()
        if not pat:
            return deleted
        import base64 as _b64, requests as _req
        api_hdr = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
        url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/deleted_dbs.txt"
        r = _req.get(url, headers=api_hdr, timeout=10)
        if r.status_code == 200:
            raw = _b64.b64decode(r.json().get("content", "").replace("\n", "")).decode("utf-8-sig")
            deleted.update({line.strip() for line in raw.splitlines() if line.strip()})
    except Exception as e:
        logger.warning(f"[GH-SYNC] Could not read deleted_dbs.txt from GitHub: {e}")
    return deleted


def _save_restore_allowlist(names: set[str]) -> None:
    try:
        payload = "\n".join(sorted({str(n).strip() for n in names if str(n).strip()}))
        RESTORE_ALLOWLIST_FILE.write_text(payload + ("\n" if payload else ""), encoding="utf-8")
    except Exception as e:
        logger.warning(f"[GH-SYNC] Could not save restore allowlist: {e}")


def _github_update_restore_allowlist(db_name: str, add: bool = True) -> None:
    """Persist allowlist locally and mirror it to GitHub Contents."""
    import base64 as _b64, requests as _req
    name = (db_name or "").strip()
    if not name:
        return
    allow = _load_restore_allowlist()
    if add:
        allow.add(name)
    else:
        allow.discard(name)
    _save_restore_allowlist(allow)
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat:
        return
    api_hdr = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
    url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/restore_allowlist.txt"
    try:
        sha = None
        r = _req.get(url, headers=api_hdr, timeout=10)
        if r.status_code == 200:
            sha = r.json().get("sha")
        payload = "\n".join(sorted(allow))
        body = {
            "message": f"restore-allowlist: {'add' if add else 'remove'} {name}",
            "content": _b64.b64encode((payload + ("\n" if payload else "")).encode()).decode(),
        }
        if sha:
            body["sha"] = sha
        _req.put(url, json=body, headers=api_hdr, timeout=15)
    except Exception as e:
        logger.warning(f"[GH-SYNC] Could not update restore allowlist for {name}: {e}")


def _github_repo_url() -> str:
    pat = os.environ.get("GITHUB_PAT", "").strip()
    return f"https://{pat}@github.com/{_GITHUB_USERNAME}/{_GITHUB_REPO}.git"


def _github_release_zip_asset_names() -> set[str]:
    """Return zip asset base names currently present in the latest release."""
    import requests as _req
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat:
        return set()
    try:
        api_hdr = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
        release_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/tags/databases-latest"
        resp = _req.get(release_url, headers=api_hdr, timeout=30)
        if resp.status_code != 200:
            return set()
        return {
            a.get("name", "")[:-4]
            for a in resp.json().get("assets", [])
            if a.get("name", "").endswith(".zip")
        }
    except Exception as e:
        logger.warning(f"[GH-SYNC] Could not read release asset names: {e}")
        return set()


def _git(args: list, cwd=None) -> bool:
    try:
        r = subprocess.run(args, cwd=str(cwd or _GITHUB_CLONE_DIR),
                           capture_output=True, text=True, timeout=180)
        if r.returncode != 0:
            logger.warning(f"[GH-SYNC] git {args[1]}: {r.stderr.strip()[:200]}")
        return r.returncode == 0
    except Exception as e:
        logger.error(f"[GH-SYNC] git error: {e}")
        return False


def _github_sync_publish_missing_allowed_dbs():
    """Upload any local, allowed DBs that are not present as release zip assets."""
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat:
        return
    allow = _load_restore_allowlist()
    if not allow:
        return
    deleted = _load_deleted_db_names()
    release_names = _github_release_zip_asset_names()
    for db_dir in sorted(DATABASES_DIR.iterdir()):
        if not db_dir.is_dir():
            continue
        db_name = db_dir.name
        if db_name in deleted or db_name == "buffer":
            continue
        if db_name not in allow:
            continue
        if not (db_dir / "config.json").exists():
            continue
        if db_name not in release_names:
            # Husk guard: a tombstoned-then-revived DB can have a local dir holding only
            # config.json. Publishing that would replace the release backup with an empty zip.
            _sq = db_dir / "chroma.sqlite3"
            if not _sq.exists() or _sq.stat().st_size < 16384:
                logger.warning(f"[GH-SYNC] NOT publishing '{db_name}' — no real chroma data locally (would overwrite backup with empty zip)")
                continue
            logger.info(f"[GH-SYNC] Publishing missing allowed DB '{db_name}' to GitHub releases")
            _github_sync_upload(db_name)


def _github_sync_download(load_db_callback=None):
    """Download DB zips from GitHub Releases → extract into databases/."""
    import zipfile, requests as _req
    DATABASES_DIR.mkdir(exist_ok=True)
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat:
        _github_sync_result.update({"status": "skipped", "detail": "No GITHUB_PAT env var set"})
        logger.info("[GH-SYNC] No GITHUB_PAT set — skipping download")
        return
    _github_sync_result.clear()
    _github_sync_result.update({"status": "running", "detail": "Fetching release metadata"})
    headers = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync",
               "Accept": "application/octet-stream"}
    api_hdr = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
    try:
        release_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/tags/databases-latest"
        resp = _req.get(release_url, headers=api_hdr, timeout=30)
        if resp.status_code != 200:
            _github_sync_result.clear()
            _github_sync_result.update({"status": "failed", "detail": f"Release fetch {resp.status_code} — PAT may be expired"})
            logger.error(f"[GH-SYNC] Release not found ({resp.status_code}): {resp.text[:200]}")
            if load_db_callback:
                logger.info("[GH-SYNC] Sync failed — loading local DB anyway")
                threading.Thread(target=load_db_callback, daemon=True).start()
            return
        assets = resp.json().get("assets", [])
        zip_assets = [a for a in assets if a["name"].endswith(".zip")]
        # Download keys.json if present, but never overwrite an existing local copy.
        for asset in assets:
            if asset["name"] == "keys.json":
                if KEYS_FILE.exists() and KEYS_FILE.stat().st_size > 0:
                    logger.info("[GH-SYNC] keys.json already present locally — preserving existing file")
                    break
                logger.info("[GH-SYNC] Downloading keys.json...")
                asset_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/assets/{asset['id']}"
                r = _req.get(asset_url, headers=headers, timeout=60)
                r.raise_for_status()
                KEYS_FILE.write_bytes(r.content)
                logger.info("[GH-SYNC] ✅ keys.json restored")
                break
        # Download restore allowlist early (before zip downloads).
        try:
            allow_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/restore_allowlist.txt"
            if RESTORE_ALLOWLIST_FILE.exists() and RESTORE_ALLOWLIST_FILE.stat().st_size > 0:
                logger.info("[GH-SYNC] restore_allowlist.txt already present locally — preserving existing file")
            else:
                r = _req.get(allow_url, headers=api_hdr, timeout=10)
                if r.status_code == 200:
                    import base64 as _b64
                    raw = _b64.b64decode(r.json().get("content", "").replace("\n", ""))
                    RESTORE_ALLOWLIST_FILE.write_bytes(raw)
                    logger.info("[GH-SYNC] ✅ restore_allowlist.txt restored")
        except Exception as e:
            logger.warning(f"[GH-SYNC] restore_allowlist.txt restore failed: {e}")
        # Download active_db.txt early (before zip downloads) so priority logic below uses the saved value.
        # In Modal the app bundle can contain a stale baked active_db.txt (often
        # agentfactory). Runtime admin switches are persisted to GitHub Contents,
        # so startup must prefer that remote value over the baked file.
        try:
            import base64 as _b64
            local_active = ACTIVE_DB_FILE.read_text(encoding="utf-8").strip() if ACTIVE_DB_FILE.exists() else ""
            r = _req.get(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/active_db.txt", headers=api_hdr, timeout=10)
            if r.status_code == 200:
                raw = _b64.b64decode(r.json().get("content", "").replace("\n",""))
                ACTIVE_DB_FILE.write_bytes(raw)
                logger.info(f"[GH-SYNC] ✅ active_db.txt restored from GitHub ({raw.decode().strip()})")
            elif local_active:
                logger.info(f"[GH-SYNC] active_db.txt GitHub restore unavailable ({r.status_code}) — preserving local file ({local_active})")
        except Exception as e:
            logger.warning(f"[GH-SYNC] active_db.txt restore failed: {e}")
        if not zip_assets:
            logger.warning("[GH-SYNC] No zip assets found in release")
            return
        allowed = _load_restore_allowlist()
        deleted = _load_deleted_db_names()
        if deleted:
            import shutil
            for db_name in sorted(deleted):
                db_extract_dir = DATABASES_DIR / db_name
                if db_extract_dir.exists():
                    try:
                        shutil.rmtree(db_extract_dir)
                        logger.info(f"[GH-SYNC] Removed deleted DB '{db_name}' from local disk")
                    except Exception as _re:
                        logger.warning(f"[GH-SYNC] Could not remove deleted DB '{db_name}' locally: {_re}")

        def _download_zip(asset):
            db_name = asset["name"][:-4]
            if db_name in deleted:
                logger.info(f"[GH-SYNC] Skipping restore for {db_name} — marked deleted")
                return
            if allowed and db_name not in allowed:
                logger.info(f"[GH-SYNC] Skipping restore for {db_name} — not in restore allowlist")
                return
            # Volume-authoritative guard: the persistent Modal volume is the live
            # working copy. Never restore a GitHub backup OVER an already-populated
            # DB — a cold-start restore of a stale zip silently clobbers a fresh
            # crawl that hasn't been re-published yet (the recurring data-loss bug).
            # Only restore into a missing/empty/husk DB dir (true disaster recovery).
            # Disable with GH_SYNC_PRESERVE_VOLUME=0.
            if os.environ.get("GH_SYNC_PRESERVE_VOLUME", "1").strip().lower() not in ("0", "false", "no", "off"):
                try:
                    _existing = DATABASES_DIR / db_name
                    _sqlite = _existing / "chroma.sqlite3"
                    _populated = False
                    if _sqlite.exists() and _sqlite.stat().st_size >= 100 * 1024:
                        _populated = True
                    else:
                        # Fallback: a real vector segment (data_level0.bin) present.
                        for _seg in _existing.rglob("data_level0.bin"):
                            if _seg.stat().st_size > 0:
                                _populated = True
                                break
                    if _populated:
                        _local_mb = sum(f.stat().st_size for f in _existing.rglob("*") if f.is_file()) / 1024 / 1024
                        logger.info(
                            f"[GH-SYNC] Preserving volume copy of {db_name} "
                            f"({_local_mb:.1f}MB on disk) — skipping restore of GitHub zip "
                            f"({asset['size']/1024/1024:.1f}MB). Set GH_SYNC_PRESERVE_VOLUME=0 to force."
                        )
                        return
                except Exception as _pv_err:
                    logger.warning(f"[GH-SYNC] volume-preserve check failed for {db_name}: {_pv_err}")
            size_mb = asset["size"] / 1024 / 1024
            logger.info(f"[GH-SYNC] Downloading {db_name}.zip ({size_mb:.1f}MB)...")
            _github_sync_result["detail"] = f"Restoring {db_name}.zip ({size_mb:.1f}MB)"
            tmp_zip = Path(f"/tmp/{db_name}_sync.zip")
            asset_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/assets/{asset['id']}"
            with _req.get(asset_url, headers=headers, stream=True, timeout=600) as r:
                r.raise_for_status()
                with open(tmp_zip, "wb") as fout:
                    for chunk in r.iter_content(chunk_size=65536):
                        fout.write(chunk)
            db_extract_dir = DATABASES_DIR / db_name
            db_extract_dir.mkdir(exist_ok=True)
            # Clean up any stale .tmp_sync files before extracting
            for _stale in db_extract_dir.rglob("*.tmp_sync"):
                try: _stale.unlink()
                except Exception: pass
            with zipfile.ZipFile(tmp_zip, "r") as z:
                for member in z.namelist():
                    rel = member[len(db_name)+1:] if member.startswith(f"{db_name}/") else member
                    rel = rel.replace("\\", "/")  # normalize Windows backslash paths
                    if not rel:
                        continue
                    dest = db_extract_dir / rel
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    if member.endswith("/"):
                        dest.mkdir(exist_ok=True)
                    else:
                        # Hard lock: preserve any existing config.json. Restores may not
                        # overwrite a config that already exists on disk.
                        if rel == "config.json" and dest.exists():
                            try:
                                import json as _json
                                existing = _json.loads(dest.read_text(encoding="utf-8"))
                                if isinstance(existing, dict) and existing:
                                    logger.info(f"[GH-SYNC] Preserving existing config.json for {db_name}")
                                    continue
                            except Exception:
                                logger.info(f"[GH-SYNC] Preserving existing config.json for {db_name} (unreadable existing file)")
                                continue
                        tmp_dest = dest.with_name(dest.name + ".tmp_sync")
                        with z.open(member) as src, open(tmp_dest, "wb") as dst:
                            while True:
                                buf = src.read(65536)
                                if not buf:
                                    break
                                dst.write(buf)
                        os.replace(str(tmp_dest), str(dest))
                    # Fix permissions
                    try:
                        if dest.is_file(): dest.chmod(0o644)
                        elif dest.is_dir(): dest.chmod(0o755)
                    except Exception:
                        pass
            tmp_zip.unlink(missing_ok=True)
            logger.info(f"[GH-SYNC] ✅ {db_name} restored")

        # Download active DB first, then small DBs (<5MB), then large ones in background
        env_db = os.environ.get("ACTIVE_DB", "").strip()
        active_asset = next((a for a in zip_assets if a["name"][:-4] == env_db and a["name"][:-4] not in deleted), None)
        other_assets = [a for a in zip_assets if a != active_asset and a["name"][:-4] not in deleted]
        # Split: small DBs download immediately after active, large ones go to background
        small_assets = [a for a in other_assets if a.get("size", 0) < 5 * 1024 * 1024]
        large_assets  = [a for a in other_assets if a.get("size", 0) >= 5 * 1024 * 1024]
        if active_asset:
            _download_zip(active_asset)
        for asset in small_assets:  # store, mal, buffer etc. — download immediately
            try: _download_zip(asset)
            except Exception as e: logger.warning(f"[GH-SYNC] Small DB sync failed for {asset['name']}: {e}")
        def _bg_sync_rest():
            pending = [asset["name"][:-4] for asset in large_assets]
            if pending:
                _github_sync_result.clear()
                _github_sync_result.update({
                    "status": "running",
                    "detail": f"Restoring large DBs: {', '.join(pending[:3])}" + ("..." if len(pending) > 3 else ""),
                    "pending_large_dbs": pending,
                })
            for asset in large_assets:
                try: _download_zip(asset)
                except Exception as e: logger.warning(f"[GH-SYNC] Background sync failed for {asset['name']}: {e}")
            _github_sync_result.clear()
            _github_sync_result.update({"status": "ok", "detail": f"{len(zip_assets)} DBs restored"})
            logger.info("[GH-SYNC] Sync finished after background restore")
        if large_assets:
            threading.Thread(target=_bg_sync_rest, daemon=True).start()
        else:
            _github_sync_result.clear()
            _github_sync_result.update({"status": "ok", "detail": f"{len(zip_assets)} DBs restored"})
            logger.info("[GH-SYNC] All databases restored from GitHub")
        # Restore crawled_urls.txt and crawl_times.json for each DB (backed up to Contents API)
        for db_dir in sorted(DATABASES_DIR.iterdir()):
            if not db_dir.is_dir(): continue
            db_n = db_dir.name
            for fname, dest in [
                (f"crawled_urls_{db_n}.txt", "crawled_urls.txt"),
                (f"crawl_times_{db_n}.json", "_crawl_times.json"),
            ]:
                try:
                    target_file = db_dir / dest
                    if target_file.exists() and target_file.stat().st_size > 0:
                        logger.info(f"[GH-SYNC] Preserving existing {dest} for {db_n}")
                        continue
                    r = _req.get(
                        f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/{fname}",
                        headers=headers, timeout=30)
                    if r.status_code == 200:
                        import base64 as _b64
                        raw = _b64.b64decode(r.json().get("content", "").replace("\n",""))
                        target_file.write_bytes(raw)
                        logger.info(f"[GH-SYNC] ✅ {dest} restored for {db_n}")
                except Exception as e:
                    logger.warning(f"[GH-SYNC] {dest} restore failed for {db_n}: {e}")
        # No automatic DB deletion on startup sync.
        # Explicit admin delete is the only path allowed to remove a DB.
        logger.info("[GH-SYNC] ✅ All databases restored from GitHub")
        # Set active DB priority:
        # 1. ACTIVE_DB env var (explicit deployment override — always wins)
        # 2. Existing active_db.txt if it points to a valid DB (preserves admin-panel switches)
        # 3. Alphabetically first available DB
        env_db = os.environ.get("ACTIVE_DB", "").strip()
        current = ACTIVE_DB_FILE.read_text(encoding="utf-8").strip() if ACTIVE_DB_FILE.exists() else ""
        # Priority: 1. file (admin switch, restored from GitHub above) 2. env var 3. alphabetical
        if current and (DATABASES_DIR / current).exists():
            chosen = current
        elif env_db and (DATABASES_DIR / env_db).exists():
            chosen = env_db
        else:
            available = sorted(d.name for d in DATABASES_DIR.iterdir() if d.is_dir())
            chosen = available[0] if available else ""
        if chosen:
            ACTIVE_DB_FILE.write_text(chosen, encoding="utf-8")
            logger.info(f"[GH-SYNC] Active DB set → {chosen}")
            if load_db_callback:
                threading.Thread(target=load_db_callback, daemon=True).start()
    except Exception as e:
        _github_sync_result.clear()
        _github_sync_result.update({"status": "failed", "detail": str(e)})
        logger.error(f"[GH-SYNC] Download error: {e}")


def _github_sync_download_one(db_name: str) -> bool:
    """Download and extract one DB zip from the GitHub release."""
    import zipfile
    import requests as _req

    db_name = (db_name or "").strip()
    if not db_name:
        return False
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat:
        logger.warning("[GH-SYNC] Cannot restore %s: no GITHUB_PAT set", db_name)
        return False
    DATABASES_DIR.mkdir(exist_ok=True)
    api_hdr = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
    bin_hdr = {**api_hdr, "Accept": "application/octet-stream"}
    tmp_zip = Path(f"/tmp/{db_name}_single_sync.zip")
    try:
        release_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/tags/databases-latest"
        resp = _req.get(release_url, headers=api_hdr, timeout=30)
        if resp.status_code != 200:
            logger.warning("[GH-SYNC] Single restore release fetch failed for %s: %s", db_name, resp.status_code)
            return False
        asset = next((a for a in resp.json().get("assets", []) if a.get("name") == f"{db_name}.zip"), None)
        if not asset:
            logger.warning("[GH-SYNC] Single restore missing asset: %s.zip", db_name)
            return False
        if db_name in _load_deleted_db_names():
            logger.warning("[GH-SYNC] Single restore skipped for %s — marked deleted", db_name)
            return False
        if db_name not in _load_restore_allowlist():
            logger.warning("[GH-SYNC] Single restore skipped for %s — not in restore allowlist", db_name)
            return False
        asset_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/assets/{asset['id']}"
        logger.info("[GH-SYNC] Single restore downloading %s.zip (%.1fMB)", db_name, asset.get("size", 0) / 1024 / 1024)
        with _req.get(asset_url, headers=bin_hdr, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(tmp_zip, "wb") as fout:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        fout.write(chunk)
        db_extract_dir = DATABASES_DIR / db_name
        db_extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(tmp_zip, "r") as z:
            for member in z.namelist():
                rel = member[len(db_name) + 1:] if member.startswith(f"{db_name}/") else member
                rel = rel.replace("\\", "/")
                if not rel:
                    continue
                dest = db_extract_dir / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                if member.endswith("/"):
                    dest.mkdir(exist_ok=True)
                    continue
                if rel == "config.json" and dest.exists() and dest.stat().st_size > 0:
                    continue
                tmp_dest = dest.with_name(dest.name + ".tmp_sync")
                with z.open(member) as src, open(tmp_dest, "wb") as dst:
                    while True:
                        buf = src.read(65536)
                        if not buf:
                            break
                        dst.write(buf)
                os.replace(str(tmp_dest), str(dest))
        tmp_zip.unlink(missing_ok=True)
        _github_sync_result["last_single_restore"] = f"{db_name} ok"
        logger.info("[GH-SYNC] Single restore complete: %s", db_name)
        return True
    except Exception as e:
        logger.warning("[GH-SYNC] Single restore failed for %s: %s", db_name, e)
        try:
            tmp_zip.unlink(missing_ok=True)
        except Exception:
            pass
        return False


def _github_backup_crawled_urls(db_name: str):
    """Upload crawled_urls.txt to GitHub Contents API (small file, no size limit issue).
    Called after every auto-crawl so fresh deploys don't re-crawl all pages."""
    import requests as _req
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat: return
    seen_file = DATABASES_DIR / db_name / "crawled_urls.txt"
    if not seen_file.exists(): return
    try:
        content_b64 = __import__("base64").b64encode(seen_file.read_bytes()).decode()
        api_hdr = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
        api_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/crawled_urls_{db_name}.txt"
        sha = None
        try:
            r = _req.get(api_url, headers=api_hdr, timeout=10)
            if r.status_code == 200:
                sha = r.json().get("sha")
        except Exception: pass
        payload = {"message": f"crawl-urls: {db_name} {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                   "content": content_b64}
        if sha: payload["sha"] = sha
        r2 = _req.put(api_url, json=payload, headers=api_hdr, timeout=30)
        if r2.status_code in (200, 201):
            logger.info(f"[GH-SYNC] ✅ crawled_urls_{db_name}.txt backed up")
        else:
            logger.warning(f"[GH-SYNC] crawled_urls backup failed: {r2.status_code}")
    except Exception as e:
        logger.warning(f"[GH-SYNC] crawled_urls backup error: {e}")


def _github_backup_crawl_times(db_name: str):
    """Upload _crawl_times.json to GitHub Contents API so timestamp survives HF Space restarts."""
    import requests as _req
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat: return
    times_file = DATABASES_DIR / db_name / "_crawl_times.json"
    if not times_file.exists(): return
    try:
        content_b64 = __import__("base64").b64encode(times_file.read_bytes()).decode()
        api_hdr = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
        api_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/crawl_times_{db_name}.json"
        sha = None
        try:
            r = _req.get(api_url, headers=api_hdr, timeout=10)
            if r.status_code == 200:
                sha = r.json().get("sha")
        except Exception: pass
        payload = {"message": f"crawl-times: {db_name} {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                   "content": content_b64}
        if sha: payload["sha"] = sha
        r2 = _req.put(api_url, json=payload, headers=api_hdr, timeout=30)
        if r2.status_code in (200, 201):
            logger.info(f"[GH-SYNC] ✅ crawl_times_{db_name}.json backed up")
        else:
            logger.warning(f"[GH-SYNC] crawl_times backup failed: {r2.status_code}")
    except Exception as e:
        logger.warning(f"[GH-SYNC] crawl_times backup error: {e}")


def _github_upload_active_db(db_name: str):
    """Upload active_db.txt to GitHub Contents API so it persists across HF restarts."""
    import requests as _req, base64 as _b64
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat: return
    hdrs = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
    content = _b64.b64encode(db_name.encode()).decode()
    sha = None
    try:
        r = _req.get(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/active_db.txt", headers=hdrs, timeout=10)
        if r.status_code == 200: sha = r.json().get("sha")
    except Exception: pass
    payload = {"message": f"chore: active DB → {db_name}", "content": content}
    if sha: payload["sha"] = sha
    try:
        r = _req.put(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/active_db.txt", headers=hdrs, json=payload, timeout=15)
        if r.status_code in (200, 201): logger.info(f"[GH-SYNC] ✅ active_db.txt uploaded ({db_name})")
        else: logger.warning(f"[GH-SYNC] active_db.txt upload failed: {r.status_code}")
    except Exception as e:
        logger.warning(f"[GH-SYNC] active_db.txt upload error: {e}")


def _github_sync_upload(db_name: str):
    """Zip DB and upload to GitHub Releases as an asset (supports files >100MB).
    Uses a temp file on disk — NOT BytesIO — to avoid OOM on Render 512MB."""
    import zipfile, requests as _req, shutil as _shutil
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat: return
    db_path = DATABASES_DIR / db_name
    if not db_path.exists(): return
    tmp_zip = Path(f"/tmp/{db_name}_upload.zip")
    try:
        logger.info(f"[GH-SYNC] Zipping {db_name} to temp file...")
        with zipfile.ZipFile(tmp_zip, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
            for f in db_path.rglob("*"):
                if not f.is_file(): continue
                rel = f.relative_to(db_path)
                rel_str = str(rel).replace("\\", "/")  # normalize for Linux extraction
                if rel_str.startswith("visitor_history"): continue
                if rel_str in ("config.local.json",): continue
                z.write(f, rel_str)
        file_size = tmp_zip.stat().st_size
        logger.info(f"[GH-SYNC] {db_name}.zip → {file_size/1024/1024:.1f}MB")
        api_hdr = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
        # Get the databases-latest release
        rel_resp = _req.get(
            f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/tags/databases-latest",
            headers=api_hdr, timeout=30)
        if rel_resp.status_code != 200:
            logger.error(f"[GH-SYNC] Release not found ({rel_resp.status_code}) — run upload_dbs_to_github.py first")
            return
        release_data = rel_resp.json()
        release_id = release_data["id"]
        assets_map = {a["name"]: a for a in release_data.get("assets", [])}
        # Husk guard: a catastrophically failed crawl can produce a near-empty DB
        # which would overwrite a healthy multi-MB backup (book.zip died this way).
        _old_asset = assets_map.get(f"{db_name}.zip")
        if _old_asset and _old_asset.get("size", 0) > 1_000_000 and file_size < _old_asset["size"] * 0.10:
            logger.error(
                f"[GH-SYNC] REFUSING upload for {db_name}: new zip {file_size/1024/1024:.1f}MB is <10% of "
                f"existing backup {_old_asset['size']/1024/1024:.1f}MB — looks like a failed crawl. "
                f"Delete the asset manually to force."
            )
            _github_sync_result["upload_error"] = f"{db_name}: refused (husk guard)"
            return
        # Step 1: upload under temp name first — old asset stays intact until upload confirmed
        temp_name = f"{db_name}_uploading.zip"
        # Clean up any stale temp asset from a previous failed upload
        if temp_name in assets_map:
            _req.delete(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/assets/{assets_map[temp_name]['id']}", headers=api_hdr, timeout=30)
        upload_url = (f"https://uploads.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}"
                      f"/releases/{release_id}/assets?name={temp_name}")
        with open(tmp_zip, "rb") as fz:
            up = _req.post(upload_url, data=fz,
                           headers={**api_hdr, "Content-Type": "application/zip",
                                    "Content-Length": str(file_size)},
                           timeout=600)
        if up.status_code not in (200, 201):
            logger.error(f"[GH-SYNC] Upload failed {up.status_code}: {up.text[:200]}")
            return
        new_asset_id = up.json()["id"]
        # Step 2: upload confirmed — now safe to delete old asset
        if f"{db_name}.zip" in assets_map:
            del_r = _req.delete(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/assets/{assets_map[f'{db_name}.zip']['id']}", headers=api_hdr, timeout=30)
            if del_r.status_code not in (204, 200):
                logger.warning(f"[GH-SYNC] Old asset delete returned {del_r.status_code} — step 3 may fail")
            import time as _time; _time.sleep(2)  # let GitHub propagate deletion before re-uploading same name
        # Step 3: re-upload with final name (GitHub has no rename API)
        with open(tmp_zip, "rb") as fz:
            final_up = _req.post(
                f"https://uploads.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/{release_id}/assets?name={db_name}.zip",
                data=fz, headers={**api_hdr, "Content-Type": "application/zip", "Content-Length": str(file_size)},
                timeout=600)
        if final_up.status_code in (200, 201):
            # Success — temp copy no longer needed
            _req.delete(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/assets/{new_asset_id}", headers=api_hdr, timeout=30)
            logger.info(f"[GH-SYNC] ✅ {db_name} uploaded ({file_size/1024/1024:.1f}MB)")
            _github_sync_result["last_upload"] = f"{db_name} ok"
        else:
            # Final upload failed AFTER the old asset was deleted: the temp asset
            # ({db}_uploading.zip) is now the ONLY copy in the release — keep it.
            # (store.zip vanished entirely under the old delete-temp-always flow.)
            msg = f"{db_name} upload failed {final_up.status_code} — data preserved as {temp_name}"
            logger.error(f"[GH-SYNC] Final rename-upload failed {final_up.status_code}: {final_up.text[:200]} — keeping {temp_name} as recovery copy")
            _github_sync_result["upload_error"] = msg
    except Exception as e:
        logger.error(f"[GH-SYNC] Upload error: {e}")
        _github_sync_result["upload_error"] = f"{db_name}: {e}"
    finally:
        tmp_zip.unlink(missing_ok=True)


def _github_clear_db_data(db_name: str):
    """Persist an emptied DB to GitHub while keeping the DB itself alive.
    Uploads the current DB folder (typically config-only after clear-data)
    and removes small crawl-state backups that could otherwise repopulate state."""
    import requests as _req
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat:
        return
    _github_sync_upload(db_name)
    api_hdr = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
    for fname in [f"crawl_times_{db_name}.json", f"crawled_urls_{db_name}.txt"]:
        try:
            r = _req.get(
                f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/{fname}",
                headers=api_hdr, timeout=10
            )
            if r.status_code == 200:
                sha = r.json().get("sha")
                _req.delete(
                    f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/{fname}",
                    json={"message": f"clear-db-data: remove {fname}", "sha": sha},
                    headers=api_hdr, timeout=15
                )
                logger.info(f"[GH-SYNC] Cleared backup file {fname} for {db_name}")
        except Exception as e:
            logger.warning(f"[GH-SYNC] Could not clear backup {fname} for {db_name}: {e}")


def _github_sync_delete(db_name: str, force: bool = False):
    """Remove DB zip from GitHub releases when a DB is explicitly deleted."""
    import requests as _req
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat: return
    if not force:
        logger.warning(f"[GH-SYNC] Refusing to delete '{db_name}' without explicit force=True")
        return
    api_hdr = {"Authorization": f"token {pat}", "User-Agent": "chatbot-sync"}
    try:
        rel_resp = _req.get(
            f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/tags/databases-latest",
            headers=api_hdr, timeout=30)
        if rel_resp.status_code != 200: return
        deleted = False
        for asset in rel_resp.json().get("assets", []):
            if asset["name"] == f"{db_name}.zip":
                _req.delete(
                    f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/assets/{asset['id']}",
                    headers=api_hdr, timeout=30)
                logger.info(f"[GH-SYNC] Deleted {db_name}.zip from GitHub releases")
                deleted = True
                break
        # Add to deleted_dbs.txt in Contents API — prevents resurrection on HF restart
        # (Docker image has databases/*/config.json baked in; deleted list overrides that)
        try:
            import base64 as _b64
            _del_fname = "deleted_dbs.txt"
            _del_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/{_del_fname}"
            _dr = _req.get(_del_url, headers=api_hdr, timeout=10)
            _existing_del = set()
            _del_sha = None
            if _dr.status_code == 200:
                _del_sha = _dr.json().get("sha")
                _existing_del = set(_b64.b64decode(_dr.json().get("content","").replace("\n","")).decode("utf-8").splitlines())
            _existing_del.add(db_name)
            _new_content = "\n".join(sorted(_existing_del))
            _del_payload = {"message": f"delete: add {db_name}", "content": _b64.b64encode(_new_content.encode()).decode()}
            if _del_sha: _del_payload["sha"] = _del_sha
            _req.put(_del_url, json=_del_payload, headers=api_hdr, timeout=15)
            logger.info(f"[GH-SYNC] Added {db_name} to deleted_dbs.txt")
        except Exception as _de:
            logger.warning(f"[GH-SYNC] Could not update deleted_dbs.txt: {_de}")
        try:
            _github_update_restore_allowlist(db_name, add=False)
        except Exception as _al_e:
            logger.warning(f"[GH-SYNC] Could not update restore allowlist for deleted DB {db_name}: {_al_e}")
        # Also remove crawl_times and crawled_urls from Contents API so they can't restore the deleted DB
        for fname in [f"crawl_times_{db_name}.json", f"crawled_urls_{db_name}.txt"]:
            try:
                r = _req.get(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/{fname}",
                             headers=api_hdr, timeout=10)
                if r.status_code == 200:
                    sha = r.json().get("sha")
                    _req.delete(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/{fname}",
                                json={"message": f"delete: {fname}", "sha": sha},
                                headers=api_hdr, timeout=15)
                    logger.info(f"[GH-SYNC] Deleted {fname} from Contents API")
            except Exception as _ce:
                logger.warning(f"[GH-SYNC] Could not delete {fname}: {_ce}")
    except Exception as e:
        logger.error(f"[GH-SYNC] Delete error for {db_name}: {e}")
