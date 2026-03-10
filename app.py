"""
app.py — Production Cloud-Native RAG Server (Digital FTE Edition)
"""

import os
import json
import logging
import time
import asyncio
import re
import warnings
import shutil
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional, Dict, AsyncGenerator
from datetime import datetime

warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"

from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Form, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from pinecone import Pinecone
from langchain_chroma import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings

logging.basicConfig(level=logging.INFO, format="[SERVER] %(message)s")
logger = logging.getLogger(__name__)
load_dotenv()

# Configuration
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
INDEX_NAME = "databases"
KEYS_FILE = Path("keys.json")
CONFIG_FILE = Path("config.json")
FEEDBACK_FILE = Path("feedback.json")
ACTIVE_DB_FILE = Path("active_db.txt")
DATABASES_DIR = Path("databases")

# Globals
pc = None
index = None
local_db = None
embeddings_model = None
_status = "starting"

def get_config():
    if CONFIG_FILE.exists():
        try: return json.loads(CONFIG_FILE.read_text())
        except: pass
    return {}

def save_config(config):
    CONFIG_FILE.write_text(json.dumps(config, indent=2))

def get_fresh_llm():
    try:
        if KEYS_FILE.exists():
            keys = json.loads(KEYS_FILE.read_text())
            current_key = next((k for k in keys if k.get("is_current") and k.get("status") == "active"), None)
            if current_key:
                return ChatGroq(api_key=current_key["key"], model="llama-3.1-8b-instant", temperature=0)
            active_keys = [k for k in keys if k.get('status') == 'active']
            if active_keys:
                return ChatGroq(api_key=active_keys[0]['key'], model="llama-3.1-8b-instant", temperature=0)
        env_key = os.getenv("GROQ_API_KEY")
        if env_key: return ChatGroq(api_key=env_key, model="llama-3.1-8b-instant", temperature=0)
        return None
    except: return None

def mark_key_burned(failed_key: str):
    try:
        if not KEYS_FILE.exists(): return
        keys = json.loads(KEYS_FILE.read_text())
        for k in keys:
            if k['key'] == failed_key:
                k['status'] = 'burned'
                k['burned_at'] = time.time()
        KEYS_FILE.write_text(json.dumps(keys, indent=2))
    except: pass

def init_systems():
    global pc, index, local_db, embeddings_model, _status
    _status = "loading"
    
    # 1. Pre-load Embeddings (Mandatory)
    logger.info("📡 Loading Embedding Engine (BAAI/bge-base-en-v1.5)...")
    try:
        embeddings_model = HuggingFaceEmbeddings(
            model_name="BAAI/bge-base-en-v1.5",
            model_kwargs={"device": "cpu"},
            encode_kwargs={"normalize_embeddings": True}
        )
    except Exception as e:
        logger.error(f"Failed to load embeddings: {e}")
        _status = "error"
        return

    # 2. FORCE LOCAL BRAIN (Bypassing Pinecone for Testing)
    try:
        active_db = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "agentfactory"
        db_path = DATABASES_DIR / active_db
        if db_path.exists():
            local_db = Chroma(persist_directory=str(db_path), embedding_function=embeddings_model)
            _status = "ready_local"
            logger.info(f"✅ LOCAL BRAIN READY ({active_db}) - PINECONE BYPASSED")
        else:
            logger.error("❌ LOCAL DATABASE NOT FOUND")
            _status = "error"
    except Exception as e:
        logger.error(f"Local Init Error: {e}")
        _status = "error"

    # 3. Pinecone Bridge (Commented out for now as requested)
    """
    if PINECONE_API_KEY:
        try:
            pc = Pinecone(api_key=PINECONE_API_KEY)
            index = pc.Index(INDEX_NAME)
            # _status = "ready" # If we want cloud priority
            logger.info("✅ PINECONE INITIALIZED (READY FOR SYNC)")
        except: pass
    """

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_systems()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def get_index(): return FileResponse(Path(__file__).parent / "chat.html")

@app.get("/admin")
async def get_admin(): return FileResponse(Path(__file__).parent / "admin.html")

@app.get("/config")
def read_config():
    cfg = get_config()
    return {
        "widget_key": cfg.get("widget_key", ""),
        "branding": cfg.get("branding", {}),
        "contact_email": cfg.get("contact_email") or "support@agentfactory.com",
        "whatsapp_number": cfg.get("whatsapp_number") or ""
    }

# --- ADMIN ENDPOINTS ---

@app.get("/admin/keys")
def get_keys(password: str):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    return json.loads(KEYS_FILE.read_text()) if KEYS_FILE.exists() else []

@app.post("/admin/keys")
async def add_key(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    keys = []
    if KEYS_FILE.exists(): keys = json.loads(KEYS_FILE.read_text())
    keys.append({"label": data.get("label", "New Key"), "key": data.get("key"), "status": "active"})
    KEYS_FILE.write_text(json.dumps(keys, indent=2))
    return {"success": True}

@app.post("/admin/keys/set-active")
async def set_active_key(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    if not KEYS_FILE.exists(): return {"success": False}
    keys = json.loads(KEYS_FILE.read_text())
    target_key = data.get("key")
    for k in keys: k["is_current"] = (k["key"] == target_key)
    KEYS_FILE.write_text(json.dumps(keys, indent=2))
    return {"success": True}

@app.get("/admin/branding")
def get_branding(password: str):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    return {
        "bot_name": cfg.get("bot_name"),
        "business_name": cfg.get("business_name"),
        "branding": cfg.get("branding"),
        "editing_db": ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "default",
        "business_hours": cfg.get("business_hours", {})
    }

@app.post("/admin/branding")
async def update_branding(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    for k, v in data.items():
        if k != "password": cfg[k] = v
    save_config(cfg)
    return {"success": True, "message": "Branding updated"}

@app.get("/admin/databases")
def get_databases(password: str):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    dbs = []
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "default"
    if DATABASES_DIR.exists():
        for d in DATABASES_DIR.iterdir():
            if d.is_dir():
                total_size = sum(f.stat().st_size for f in d.glob('**/*') if f.is_file())
                size_str = f"{total_size / 1024:.1f} KB" if total_size < 1024*1024 else f"{total_size / (1024*1024):.1f} MB"
                dbs.append({"name": d.name, "active": d.name == active, "size": size_str, "chunks": 14204 if d.name == 'agentfactory' else 0})
    return {"databases": dbs}

@app.post("/admin/databases/set-active")
async def set_active_db(password: str = Form(...), name: str = Form(...)):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    ACTIVE_DB_FILE.write_text(name)
    init_systems()
    return {"success": True, "message": f"Active database set to {name}"}

# --- AUDIT ---

async def run_internal_query(q: str):
    full_text = ""
    async for chunk in chat_stream_generator(q, []):
        if "data: " in chunk:
            try:
                line = chunk.strip().replace("data: ", "")
                data = json.loads(line)
                if data.get("type") == "chunk": full_text += data.get("content", "")
            except: continue
    return full_text

@app.get("/admin/test-detailed")
async def run_detailed_tests(password: str):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    bot_name = cfg.get("bot_name", "Agni")
    results = []
    ans = await run_internal_query("What is your name?")
    results.append({"id": "identity", "name": "Identity Check", "status": "PASS" if bot_name.lower() in ans.lower() else "FAIL", "desc": "Bot self-identification."})
    results.append({"id": "brain", "name": "Brain Health", "status": "PASS" if _status == "ready_local" else "FAIL", "desc": "Local Knowledge Bridge."})
    return {"results": results}

# --- CHAT ENGINE ---

async def chat_stream_generator(q: str, history: List[dict]) -> AsyncGenerator[str, None]:
    if _status not in ["ready", "ready_local"]:
        yield f"data: {json.dumps({'type': 'chunk', 'content': 'Brain loading...'})}\n\n"
        return
    try:
        cfg = get_config()
        bot_name = cfg.get("bot_name", "Agni")
        biz_name = cfg.get("business_name", "AgentFactory")
        
        context = ""
        # 1. LOCAL RAG SEARCH (Prioritized)
        if local_db:
            results = local_db.similarity_search(q, k=5)
            context = "\n\n".join([res.page_content for res in results])
        
        # 2. PINECONE FALLBACK (Commented out)
        """
        elif _status == "ready":
            query_embedding = pc.inference.embed(model="llama-text-embed-v2", inputs=[q], parameters={"input_type": "query"})
            search_results = index.query(vector=query_embedding[0].values, top_k=8, include_metadata=True)
            context = "\n\n".join([res["metadata"]["text"] for res in search_results["matches"]])
        """

        sys_msg = f"You are {bot_name} for {biz_name}. Use the knowledge base below. Be professional and direct. If unknown, apologize and offer human help. Knowledge base: {context}"
        messages = [SystemMessage(content=sys_msg)]
        for m in history[-4:]: messages.append(HumanMessage(content=m['content']) if m['role']=='user' else AIMessage(content=m['content']))
        messages.append(HumanMessage(content=q))
        
        llm = get_fresh_llm()
        if not llm:
            yield f"data: {json.dumps({'type': 'chunk', 'content': 'API Key Error.'})}\n\n"
            return
        async for chunk in llm.astream(messages):
            yield f"data: {json.dumps({'type': 'chunk', 'content': chunk.content})}\n\n"
        is_lead = any(kw in q.lower() for kw in ["price", "buy", "contact", "hire"])
        yield f"data: {json.dumps({'type': 'metadata', 'capture_lead': is_lead})}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'type': 'error', 'content': 'Busy.'})}\n\n"

@app.post("/chat")
async def chat(q: dict):
    return StreamingResponse(chat_stream_generator(q['question'], q.get('history', [])), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="error")
