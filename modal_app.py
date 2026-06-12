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
            ".playwright-mcp", "evals", "*.sqlite3", "quiz_*.json",
            "server_out.log", "server_err.log",
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
    import app as chatbot

    chatbot.init_systems()

    async def _run():
        n = await chatbot._auto_crawl_db(db_name, url, max_pages=max_pages)
        vol.commit()  # persist the new chunks to the volume
        return n

    n = asyncio.run(_run())
    print(f"[MODAL-CRAWL] done: {n} pages/chunks for '{db_name}'")
    return n
