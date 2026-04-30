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

_GITHUB_USERNAME = os.getenv("GITHUB_USERNAME", "RE-coder376")
_GITHUB_REPO     = os.getenv("GITHUB_REPO", "databases")
_GITHUB_CLONE_DIR = Path("/tmp/chatbot-dbs")

# Mutable dict — always mutate in-place (never reassign) so callers that hold
# a reference to this dict see live updates without re-importing.
_github_sync_result: dict = {"status": "not_run", "detail": ""}


def _github_repo_url() -> str:
    pat = os.environ.get("GITHUB_PAT", "").strip()
    return f"https://{pat}@github.com/{_GITHUB_USERNAME}/{_GITHUB_REPO}.git"


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
            return
        assets = resp.json().get("assets", [])
        zip_assets = [a for a in assets if a["name"].endswith(".zip")]
        # Download keys.json if present
        for asset in assets:
            if asset["name"] == "keys.json":
                logger.info("[GH-SYNC] Downloading keys.json...")
                asset_url = f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/assets/{asset['id']}"
                r = _req.get(asset_url, headers=headers, timeout=60)
                r.raise_for_status()
                KEYS_FILE.write_bytes(r.content)
                logger.info("[GH-SYNC] ✅ keys.json restored")
                break
        # Download active_db.txt early (before zip downloads) so priority logic below uses the saved value
        try:
            import base64 as _b64
            r = _req.get(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/active_db.txt", headers=api_hdr, timeout=10)
            if r.status_code == 200:
                raw = _b64.b64decode(r.json().get("content", "").replace("\n",""))
                ACTIVE_DB_FILE.write_bytes(raw)
                logger.info(f"[GH-SYNC] ✅ active_db.txt restored ({raw.decode().strip()})")
        except Exception as e:
            logger.warning(f"[GH-SYNC] active_db.txt restore failed: {e}")
        if not zip_assets:
            logger.warning("[GH-SYNC] No zip assets found in release")
            return

        def _download_zip(asset):
            db_name = asset["name"][:-4]
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
                        # For config.json: only extract from zip if the existing file on disk
                        # is missing or has empty business_name/topics (i.e. the zip has better data).
                        # The committed repo config.json is the authoritative source — never
                        # let an older zip silently overwrite it with blank/default values.
                        if rel == "config.json" and dest.exists():
                            try:
                                import json as _json
                                existing = _json.loads(dest.read_text(encoding="utf-8"))
                                # Repo config is authoritative. If it has identity fields set,
                                # never let an older zip overwrite them (zip may have stale defaults).
                                if existing.get("business_name") or existing.get("topics"):
                                    logger.info(f"[GH-SYNC] Skipping config.json for {db_name} — keeping committed repo version")
                                    continue
                            except Exception:
                                pass  # fall through to normal extraction
                        tmp_dest = dest.with_name(dest.name + ".tmp_sync")
                        with z.open(member) as src, open(tmp_dest, "wb") as dst:
                            dst.write(src.read())
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
        active_asset = next((a for a in zip_assets if a["name"][:-4] == env_db), None)
        other_assets = [a for a in zip_assets if a != active_asset]
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
                    r = _req.get(
                        f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/{fname}",
                        headers=headers, timeout=30)
                    if r.status_code == 200:
                        import base64 as _b64
                        raw = _b64.b64decode(r.json().get("content", "").replace("\n",""))
                        (db_dir / dest).write_bytes(raw)
                        logger.info(f"[GH-SYNC] ✅ {dest} restored for {db_n}")
                except Exception as e:
                    logger.warning(f"[GH-SYNC] {dest} restore failed for {db_n}: {e}")
        # Enforce deleted_dbs.txt — remove DB folders that were deleted (even if config.json is baked into Docker image)
        try:
            import base64 as _b64d, shutil as _sh
            _del_r = _req.get(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/contents/deleted_dbs.txt",
                              headers=api_hdr, timeout=10)
            if _del_r.status_code == 200:
                _deleted_names = set(_b64d.b64decode(_del_r.json().get("content","").replace("\n","")).decode("utf-8").splitlines())
                for _dn in _deleted_names:
                    _dn = _dn.strip()
                    if _dn and (DATABASES_DIR / _dn).exists():
                        _sh.rmtree(str(DATABASES_DIR / _dn))
                        logger.info(f"[GH-SYNC] Removed deleted DB '{_dn}' from local disk")
        except Exception as _del_e:
            logger.warning(f"[GH-SYNC] deleted_dbs.txt cleanup failed: {_del_e}")
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
    import zipfile, requests as _req
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
        # Clean up temp asset regardless of final result
        _req.delete(f"https://api.github.com/repos/{_GITHUB_USERNAME}/{_GITHUB_REPO}/releases/assets/{new_asset_id}", headers=api_hdr, timeout=30)
        if final_up.status_code in (200, 201):
            logger.info(f"[GH-SYNC] ✅ {db_name} uploaded ({file_size/1024/1024:.1f}MB)")
            _github_sync_result["last_upload"] = f"{db_name} ok"
        else:
            msg = f"{db_name} upload failed {final_up.status_code}"
            logger.error(f"[GH-SYNC] Final rename-upload failed {final_up.status_code}: {final_up.text[:200]}")
            _github_sync_result["upload_error"] = msg
    except Exception as e:
        logger.error(f"[GH-SYNC] Upload error: {e}")
        _github_sync_result["upload_error"] = f"{db_name}: {e}"
    finally:
        tmp_zip.unlink(missing_ok=True)


def _github_sync_delete(db_name: str):
    """Remove DB zip from GitHub releases when a DB is deleted."""
    import requests as _req
    pat = os.environ.get("GITHUB_PAT", "").strip()
    if not pat: return
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
