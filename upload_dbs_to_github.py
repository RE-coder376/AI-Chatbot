"""
Upload all local databases to GitHub Releases (handles any file size, fast).
Run from AI_Chatbot directory: python upload_dbs_to_github.py
"""
import os, sys, zipfile, tempfile, requests
from pathlib import Path

GITHUB_PAT      = (sys.argv[1] if len(sys.argv) > 1
                   else os.environ.get("GITHUB_PAT", "")
                   or input("Paste your GitHub PAT: ").strip())
GITHUB_USERNAME = "RE-coder376"
GITHUB_REPO     = "databases"
DATABASES_DIR   = Path("databases")
RELEASE_TAG     = "databases-latest"
API             = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}"
UPLOAD_API      = f"https://uploads.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}"
HDR             = {"Authorization": f"token {GITHUB_PAT}", "User-Agent": "chatbot-sync"}

# ── Get or create release ──────────────────────────────────────────────────────
r = requests.get(f"{API}/releases/tags/{RELEASE_TAG}", headers=HDR, timeout=30)
if r.status_code == 200:
    release = r.json()
    print(f"Using existing release id={release['id']}")
else:
    r = requests.post(f"{API}/releases", headers=HDR, timeout=30, json={
        "tag_name": RELEASE_TAG, "name": "Database Storage",
        "body": "Auto-updated DB files", "draft": False, "prerelease": False
    })
    release = r.json()
    print(f"Created release id={release['id']}")

release_id = release["id"]

# Build map of existing assets name→id for deletion
existing = {a["name"]: a["id"]
            for a in requests.get(f"{API}/releases/{release_id}/assets",
                                  headers=HDR, timeout=30).json()}

# ── Zip and upload each DB ─────────────────────────────────────────────────────
dbs = [d for d in DATABASES_DIR.iterdir() if d.is_dir()]
print(f"Found {len(dbs)} DB(s): {[d.name for d in dbs]}\n")

# Upload keys.json first
KEYS_FILE = Path("keys.json")
if KEYS_FILE.exists():
    keys_name = "keys.json"
    print(f"  Uploading {keys_name}...", end=" ", flush=True)
    if keys_name in existing:
        requests.delete(f"{API}/releases/assets/{existing[keys_name]}", headers=HDR, timeout=30)
    upload_headers = {**HDR, "Content-Type": "application/json"}
    r = requests.post(f"{UPLOAD_API}/releases/{release_id}/assets?name={keys_name}",
                      headers=upload_headers, data=KEYS_FILE.read_bytes(), timeout=60)
    print("✅ done" if r.status_code in (200,201) else f"❌ HTTP {r.status_code}")
else:
    print("  keys.json not found — skipping")

for db_path in dbs:
    db_name  = db_path.name
    zip_name = f"{db_name}.zip"

    # Zip
    print(f"  Zipping {db_name}...", end=" ", flush=True)
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    tmp.close()
    tmp_path = Path(tmp.name)
    count = 0
    with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as z:
        for f in db_path.rglob("*"):
            if not f.is_file(): continue
            rel = f.relative_to(db_path)
            if str(rel).startswith("visitor_history"): continue
            z.write(f, rel); count += 1
    size_mb = tmp_path.stat().st_size / 1024 / 1024
    print(f"{count} files → {size_mb:.1f} MB", flush=True)

    # Delete old asset if exists
    if zip_name in existing:
        requests.delete(f"{API}/releases/assets/{existing[zip_name]}",
                        headers=HDR, timeout=30)

    # Upload new asset (binary multipart — no base64, no size limit)
    print(f"  Uploading {zip_name}...", end=" ", flush=True)
    upload_headers = {**HDR, "Content-Type": "application/zip"}
    with open(tmp_path, "rb") as fh:
        r = requests.post(
            f"{UPLOAD_API}/releases/{release_id}/assets?name={zip_name}",
            headers=upload_headers,
            data=fh,
            timeout=600,
        )
    tmp_path.unlink(missing_ok=True)

    if r.status_code in (200, 201):
        print("✅ done")
    else:
        print(f"❌ failed (HTTP {r.status_code}): {r.text[:100]}")

print(f"\n✅ Done → https://github.com/{GITHUB_USERNAME}/{GITHUB_REPO}/releases/tag/{RELEASE_TAG}")
