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
    timeout=4 * 3600,
    volumes={"/root/app/databases": vol},
    secrets=SECRETS,
)
def crawl(db_name: str, url: str, max_pages: int = 0):
    """Run a crawl as a standalone job (allowed here, unlike on HF):
    modal run modal_app.py::crawl --db-name tsc_pk --url https://thestationerycompany.pk
    """
    _enter_app_dir()
    import asyncio
    import logging
    import app as chatbot

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
                if not any(token in msg for token in (
                    "[AUTO-CRAWL]", "[MODAL-CRAWL]", "[GATE]", "[CRAWL-GATE]",
                    "Verification gate", "finished:", "failed", "ERROR", "WARNING",
                )):
                    return
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
    _post_main_ui_log(f"[MODAL-CRAWL] starting '{db_name}' from {url} (max_pages={max_pages})", running=True)

    async def _run():
        n = await chatbot._auto_crawl_db(db_name, url, max_pages=max_pages)
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
