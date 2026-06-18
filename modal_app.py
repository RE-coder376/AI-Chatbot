"""Modal deployment for the AI Chatbot — additive, does not touch the HF setup.

Deploy:   modal deploy modal_app.py
Logs:     modal app logs ai-chatbot
URL:      printed on deploy (…--ai-chatbot-serve.modal.run)

Cost model (free $30/month Starter credits):
  - serve: 1 CPU + 4 GiB ≈ $0.08/hour ONLY while awake; sleeps after 2 idle
    minutes, wakes in ~1-3 s on the next request. Never set min_containers.
  - crawl: separate 2 CPU + 8 GiB job container ≈ $0.32 per 2-hour crawl.
"""

import os
import sys

import modal

app = modal.App("ai-chatbot")

# Mirrors the HF Dockerfile: system deps, python deps, prebaked fastembed
# model and Playwright chromium. Code is added LAST so routine code changes
# deploy in seconds without rebuilding the dependency layers.
image = (
    modal.Image.debian_slim(python_version="3.11")
    .apt_install(
        "gcc", "g++", "cmake", "libxml2-dev", "libxslt1-dev",
        "tesseract-ocr", "tesseract-ocr-eng", "tesseract-ocr-ara",
    )
    .pip_install_from_requirements("requirements.txt")
    .run_commands(
        "python -c \"from langchain_community.embeddings.fastembed import FastEmbedEmbeddings; FastEmbedEmbeddings(model_name='BAAI/bge-small-en-v1.5')\" || true",
        # Prebake the multilingual model too — agentfactory queries with it (P1 fix); avoids a cold-start download.
        "python -c \"from langchain_community.embeddings.fastembed import FastEmbedEmbeddings; FastEmbedEmbeddings(model_name='sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')\" || true",
        "playwright install --with-deps chromium",
    )
    .add_local_dir(
        ".",
        remote_path="/root/app",
        ignore=[
            ".git", "__pycache__", "databases", "*.zip", "*.log", "*.bin",
            ".playwright-mcp", "evals/results", "evals/state", "*.sqlite3",
            "quiz_*.json", "server_out.log", "server_err.log",
            # NOTE: "evals" (bare) must NOT be ignored — evals/ is the eval
            # CODE package (eval_v1.py, llm_judge.py); excluding it 500s
            # every /admin/evals/* endpoint with "No module named 'evals'".
        ],
    )
)

# Persistent disk for ChromaDB databases — survives every restart/redeploy.
# First boot: the app's existing GitHub restore populates it; after that the
# data is just there and startup is fast.
vol = modal.Volume.from_name("ai-chatbot-databases", create_if_missing=True)

# Secret "ai-chatbot-secrets" must exist in the Modal workspace with the same
# env vars the HF Space used: KEYS_JSON, CONFIG_JSON, ADMIN_PASSWORD,
# GITHUB_PAT, SMTP_PASSWORD, ACTIVE_DB.
SECRETS = [modal.Secret.from_name("ai-chatbot-secrets")]


def _enter_app_dir():
    os.chdir("/root/app")
    if "/root/app" not in sys.path:
        sys.path.insert(0, "/root/app")


@app.function(
    image=image,
    cpu=1.0,
    memory=4096,
    timeout=600,
    volumes={"/root/app/databases": vol},
    secrets=SECRETS,
    scaledown_window=120,  # sleep 2 min after the last request — the $30 lever
)
@modal.concurrent(max_inputs=20)
@modal.asgi_app()
def serve():
    _enter_app_dir()
    from app import app as fastapi_app
    return fastapi_app


@app.function(
    image=image,
    cpu=2.0,
    memory=8192,
    ephemeral_disk=32768,
    timeout=4 * 3600,
    env={
        "CHROMA_TMP_ROOT": "/tmp/chroma",
        "CHROMA_TMP_ALWAYS_REFRESH": "1",
    },
    volumes={"/root/app/databases": vol},
    secrets=SECRETS,
)
def crawl(db_name: str, url: str, max_pages: int = 0, clear: bool = False):
    """Run a crawl as a standalone job (allowed here, unlike on HF):
    modal run modal_app.py::crawl --db-name tsc_pk --url https://thestationerycompany.pk
    Pass --clear to wipe the collection first (full rebuild, purges stale chunks).
    """
    _enter_app_dir()
    import asyncio
    import logging
    import app as chatbot

    os.environ["SKIP_STARTUP_GITHUB_RESTORE"] = "1"
    chatbot.init_systems()

    web_base = os.environ.get("MODAL_WEB_BASE_URL", "https://re-coder376--ai-chatbot-serve.modal.run").rstrip("/")
    admin_password = os.environ.get("ADMIN_PASSWORD", "")

    class _MainUiCrawlLogHandler(logging.Handler):
        def __init__(self):
            super().__init__(level=logging.INFO)
            self._posted: set[str] = set()

        def emit(self, record):
            try:
                msg = self.format(record)
                # Forward the full crawl stream so the main app logs/admin panel
                # behave like HF container logs, not just periodic summaries.
                # De-dupe noisy repeated health/config lines without hiding progress.
                key = msg[:240]
                if key in self._posted and "progress:" not in msg:
                    return
                self._posted.add(key)
                if len(self._posted) > 5000:
                    self._posted = set(list(self._posted)[-2500:])
                _post_main_ui_log(msg, running=True)
            except Exception:
                pass

    def _post_main_ui_log(msg: str, *, running: bool = True, done: bool = False, error: bool = False):
        if not admin_password:
            return
        try:
            import requests
            requests.post(
                f"{web_base}/admin/external-crawl-log",
                json={
                    "db_name": db_name,
                    "password": admin_password,
                    "msg": msg,
                    "running": running,
                    "done": done,
                    "error": error,
                },
                timeout=8,
            )
        except Exception:
            pass

    ui_handler = _MainUiCrawlLogHandler()
    ui_handler.setFormatter(logging.Formatter("[SERVER] %(message)s"))
    logging.getLogger().addHandler(ui_handler)
    logging.getLogger("app").addHandler(ui_handler)
    # Modal captures stdout/stderr; default logging only emits WARNING+ to stderr,
    # so INFO-level crawl progress was invisible in `modal app logs`. Attach a
    # stdout StreamHandler at INFO so every page/progress line lands in Modal logs.
    _stdout_handler = logging.StreamHandler(sys.stdout)
    _stdout_handler.setLevel(logging.INFO)
    _stdout_handler.setFormatter(logging.Formatter("[CRAWL] %(asctime)s %(message)s", "%H:%M:%S"))
    _root = logging.getLogger()
    _root.setLevel(logging.INFO)
    _root.addHandler(_stdout_handler)
    logging.getLogger("app").setLevel(logging.INFO)
    _post_main_ui_log(f"[MODAL-CRAWL] starting '{db_name}' from {url} (max_pages={max_pages})", running=True)

    async def _run():
        n = await chatbot._auto_crawl_db(db_name, url, max_pages=max_pages, clear=clear)
        vol.commit()  # persist the new chunks to the volume
        return n

    try:
        n = asyncio.run(_run())
        msg = f"[MODAL-CRAWL] done: {n} pages/chunks for '{db_name}'"
        print(msg)
        _post_main_ui_log(msg, running=False, done=True)
        return n
    except Exception as exc:
        msg = f"[MODAL-CRAWL] failed for '{db_name}': {type(exc).__name__}: {exc}"
        print(msg)
        _post_main_ui_log(msg, running=False, done=True, error=True)
        raise


@app.function(
    image=image,
    cpu=1.0,
    memory=4096,
    timeout=1200,
    volumes={"/root/app/databases": vol},
    secrets=SECRETS,
)
def gate(db_name: str, crawl_url: str = ""):
    """Run the post-crawl verification gate on demand against the volume data:
    modal run modal_app.py::gate --db-name tsc_pk
    Writes databases/<db>/gate_report.json (served at /admin/gate-report).
    """
    _enter_app_dir()
    import json as _json
    from crawl_gate import run_gate_for_db

    db_dir = "/root/app/databases/" + db_name
    web_base = os.environ.get("MODAL_WEB_BASE_URL", "https://re-coder376--ai-chatbot-serve.modal.run").rstrip("/")
    password = os.environ.get("ADMIN_PASSWORD", "")
    rep = run_gate_for_db(db_name, db_dir, crawl_url, quiz_base=web_base, password=password)
    vol.commit()  # persist gate_report.json
    print("[GATE] verdict:\n" + _json.dumps(rep, indent=2, ensure_ascii=False)[:6000])
    return rep


@app.function(
    image=image,
    cpu=2.0,
    memory=8192,
    timeout=3600,
    volumes={"/root/app/databases": vol},
    secrets=SECRETS,
)
def catalog_ingest(db_name: str, url: str = "", clear_products: bool = True):
    """Deterministic product ingestion from the store's /products.json — complete,
    order-stable catalog → constant counts/coverage (vs fluctuating BFS crawl).
    modal run modal_app.py::catalog_ingest --db-name stationery_studio
    Replaces product chunks only; run a crawl separately for policy/FAQ pages.
    """
    _enter_app_dir()
    import app as chatbot
    os.environ["SKIP_STARTUP_GITHUB_RESTORE"] = "1"
    chatbot.init_systems()
    if not url:
        url = (chatbot.get_config(db_name) or {}).get("crawl_url") or ""
    res = chatbot.catalog_reingest_products(db_name, url, clear_products=clear_products)
    vol.commit()
    print(f"[CATALOG] {res}")
    return res


@app.function(
    image=image,
    cpu=1.0,
    memory=2048,
    timeout=600,
    volumes={"/root/app/databases": vol},
    secrets=SECRETS,
)
def scrub(db_name: str):
    """In-place fix of EXISTING chunks without a recrawl — mirrors the two
    crawler_utils chokepoint fixes for data already on the volume:
      1) html.unescape() real HTML entities leaked into chunk documents
         (page-title seed headings: "Terms of service &ndash; …").
      2) repair product_title < 4 chars: promote canonical_product_title if it's
         ≥4 chars, else drop both title keys ("Fun" fragment).
    modal run modal_app.py::scrub --db-name tsc_pk
    """
    _enter_app_dir()
    import sqlite3
    import html as _html
    from pipeline_check import _REAL_ENTITY_RE

    path = "/root/app/databases/" + db_name + "/chroma.sqlite3"
    con = sqlite3.connect(path)
    c = con.cursor()

    # 1) entity unescape in chunk documents (only rows holding a real entity ref)
    c.execute("SELECT id, string_value FROM embedding_metadata "
              "WHERE key='chroma:document' AND string_value LIKE '%&%'")
    doc_fixes = 0
    for _id, sv in c.fetchall():
        if sv and _REAL_ENTITY_RE.search(sv):
            new = _html.unescape(sv)
            if new != sv:
                c.execute("UPDATE embedding_metadata SET string_value=? "
                          "WHERE id=? AND key='chroma:document'", (new, _id))
                doc_fixes += 1

    # 2) short product_title repair
    c.execute("SELECT id, key, string_value FROM embedding_metadata "
              "WHERE key IN ('product_title','canonical_product_title')")
    rows: dict = {}
    for _id, key, sv in c.fetchall():
        rows.setdefault(_id, {})[key] = sv
    title_fixes = 0
    for _id, d in rows.items():
        pt = str(d.get("product_title") or "").strip()
        if pt and len(pt) < 4:
            ct = str(d.get("canonical_product_title") or "").strip()
            if len(ct) >= 4:
                c.execute("UPDATE embedding_metadata SET string_value=? "
                          "WHERE id=? AND key='product_title'", (ct, _id))
            else:
                c.execute("DELETE FROM embedding_metadata WHERE id=? "
                          "AND key IN ('product_title','canonical_product_title')", (_id,))
            title_fixes += 1

    # 3) nav-chrome strip in documents + product titles (mirrors _finalize_docs
    #    _CHROME_RE — storefront button/badge text that leaked into pre-fix data).
    import re as _re
    _CHROME_RE = _re.compile(r"(?i)\b(?:shop now|buy online|click to (?:enlarge|zoom)|add to (?:cart|wishlist|bag)|quick view|view (?:cart|details)|sold out|pre[\s-]?order|location click|location)\b")
    c.execute("SELECT id, key, string_value FROM embedding_metadata "
              "WHERE key IN ('chroma:document','product_title','canonical_product_title')")
    chrome_fixes = 0
    for _id, key, sv in c.fetchall():
        if not sv or not _CHROME_RE.search(sv):
            continue
        if key == "chroma:document":
            new = _re.sub(r"[ \t]{2,}", " ", _CHROME_RE.sub(" ", sv))
        else:
            new = _re.sub(r"\s{2,}", " ", _CHROME_RE.sub(" ", sv)).strip(" -:|")
            if len(new) < 4:
                continue  # don't strip a title down to a fragment
        if new != sv and new.strip():
            c.execute("UPDATE embedding_metadata SET string_value=? WHERE id=? AND key=?", (new, _id, key))
            chrome_fixes += 1

    con.commit()
    try:
        c.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # fold WAL into the main file
    except Exception:
        pass
    con.close()
    vol.commit()
    print(f"[SCRUB] {db_name}: {doc_fixes} entity docs, {title_fixes} short titles, {chrome_fixes} chrome strips fixed")
    return {"entity_docs": doc_fixes, "short_titles": title_fixes, "chrome_strips": chrome_fixes}