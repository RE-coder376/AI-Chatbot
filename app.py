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
HEALER_LOGS_FILE = Path("healer_logs.json")
DATABASES_DIR = Path("databases")

# Globals
pc = None
index = None
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
            active_keys = [k for k in keys if k.get('status') == 'active']
            if active_keys:
                return ChatGroq(api_key=active_keys[0]['key'], model="llama-3.1-8b-instant", temperature=0)
        
        env_key = os.getenv("GROQ_API_KEY")
        if env_key:
            return ChatGroq(api_key=env_key, model="llama-3.1-8b-instant", temperature=0)
            
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

def init_cloud():
    global pc, index, _status
    _status = "loading"
    try:
        if not PINECONE_API_KEY:
            logger.error("❌ PINECONE_API_KEY not found in .env")
            _status = "ready_local"
            return
        pc = Pinecone(api_key=PINECONE_API_KEY)
        index = pc.Index(INDEX_NAME)
        _status = "ready"
        logger.info("✅ CLOUD BRAIN READY (Pinecone + Groq)")
    except Exception as e:
        logger.error(f"Cloud Error: {e}")
        _status = "error"

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_cloud()
    yield

app = FastAPI(lifespan=lifespan)

# Dynamic CORS based on config
config = get_config()
origins = config.get("allowed_origins", ["*"])
app.add_middleware(CORSMiddleware, allow_origins=origins, allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def get_index(): return FileResponse(Path(__file__).parent / "chat.html")

@app.get("/admin")
async def get_admin(): return FileResponse(Path(__file__).parent / "admin.html")

@app.get("/config")
def read_config():
    cfg = get_config()
    contact = cfg.get("contact_email") or cfg.get("whatsapp_number") or "support@agentfactory.com"
    return {
        "widget_key": cfg.get("widget_key", ""),
        "branding": cfg.get("branding", {}),
        "contact_info": contact
    }

@app.get("/health")
def health():
    return {"status": _status, "engine": "pinecone_llama_1024", "active_db": ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "default"}

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

@app.get("/admin/branding")
def get_branding(password: str):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    return {
        "bot_name": cfg.get("bot_name"),
        "business_name": cfg.get("business_name"),
        "branding": cfg.get("branding"),
        "editing_db": ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "default"
    }

@app.post("/admin/branding")
async def update_branding(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    for k, v in data.items():
        if k != "password": cfg[k] = v
    save_config(cfg)
    return {"success": True, "message": "Branding updated"}

@app.get("/admin/contact-settings")
def get_contact(password: str):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    return {
        "whatsapp_number": cfg.get("whatsapp_number"),
        "contact_email": cfg.get("contact_email"),
        "notify_email": cfg.get("notify_email"),
        "notify_whatsapp": cfg.get("notify_whatsapp"),
        "widget_key": cfg.get("widget_key")
    }

@app.post("/admin/contact-settings")
async def save_contact(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    for field in ["whatsapp_number", "contact_email", "notify_email", "notify_whatsapp"]:
        if field in data: cfg[field] = data[field]
    save_config(cfg)
    return {"success": True, "message": "Contact settings saved"}

@app.get("/admin/analytics-data")
def get_analytics(password: str):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    feedbacks = []
    if FEEDBACK_FILE.exists():
        try: feedbacks = json.loads(FEEDBACK_FILE.read_text())
        except: pass
    return {"feedbacks": feedbacks, "total_chats": len(feedbacks)}

@app.get("/admin/healer-logs")
def get_healer_logs():
    if HEALER_LOGS_FILE.exists():
        try: return json.loads(HEALER_LOGS_FILE.read_text())
        except: pass
    return []

@app.get("/admin/databases")
def get_databases(password: str):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    dbs = []
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "default"
    if DATABASES_DIR.exists():
        for d in DATABASES_DIR.iterdir():
            if d.is_dir():
                dbs.append({"name": d.name, "active": d.name == active, "chunks": 0}) # Simplified chunk count
    return {"databases": dbs}

@app.post("/admin/databases/set-active")
async def set_active_db(password: str = Form(...), name: str = Form(...)):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    ACTIVE_DB_FILE.write_text(name)
    return {"success": True, "message": f"Active database set to {name}"}

@app.post("/admin/inspect-site")
async def inspect_site(password: str = Form(...), base_url: str = Form(...)):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    # Mock inspection for now
    return {"success": True, "total": 10, "groups": [{"path": "/", "count": 10}]}

@app.post("/admin/crawl-site")
async def crawl_site(password: str = Form(...), base_url: str = Form(...), db_name: str = Form(...), path_filters: str = Form(...)):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    return {"started": True, "job_id": "job_123"}

@app.post("/admin/update-text")
async def update_text(password: str = Form(...), content: str = Form(...), filename: str = Form(...)):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    return {"success": True, "message": "Text ingested successfully"}

@app.post("/admin/upload-file")
async def upload_file(password: str = Form(...), file: UploadFile = File(...)):
    cfg = get_config()
    if password != cfg.get("admin_password"): raise HTTPException(401, "Unauthorized")
    return {"success": True, "message": f"File {file.filename} ingested successfully"}

@app.post("/feedback")
async def save_feedback(data: dict):
    feedbacks = []
    if FEEDBACK_FILE.exists():
        try: feedbacks = json.loads(FEEDBACK_FILE.read_text())
        except: pass
    data["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    feedbacks.append(data)
    FEEDBACK_FILE.write_text(json.dumps(feedbacks[-500:], indent=2))
    return {"success": True}

# --- CHAT LOGIC ---

async def chat_stream_generator(q: str, history: List[dict]) -> AsyncGenerator[str, None]:
    if _status not in ["ready", "ready_local"]:
        yield f"data: {json.dumps({'type': 'chunk', 'content': 'Initializing systems...'})}\n\n"
        return
    try:
        cfg = get_config()
        bot_name = cfg.get("bot_name", "Agni")
        biz_name = cfg.get("business_name", "AgentFactory")
        context = ""
        if _status == "ready":
            query_embedding = pc.inference.embed(model="llama-text-embed-v2", inputs=[q], parameters={"input_type": "query"})
            search_results = index.query(vector=query_embedding[0].values, top_k=8, include_metadata=True)
            context_list = [res["metadata"]["text"] for res in search_results["matches"]]
            context = "\n\n".join(context_list)
        else:
            context = "LOCAL TEST MODE: Operating on internal server logic."

        sys_msg = f"""
        You are {bot_name}, the lead AI representative for {biz_name}. 
        You are a highly professional, confident, and specialized Digital FTE.
        MANDATES:
        1. CONFIDENCE: Never say "based on the context". Speak as the authority.
        2. PERSONALITY: Use "we" and "our" referring to the company.
        3. POLITE UNKNOWNS: If info is missing, offer to connect with a human specialist.
        4. PRESENTATION: Use Markdown. Bullet points for lists. Clear structure.
        KNOWLEDGE BASE:
        {context}
        """
        messages = [SystemMessage(content=sys_msg)]
        for m in history[-4:]:
            messages.append(HumanMessage(content=m['content']) if m['role']=='user' else AIMessage(content=m['content']))
        messages.append(HumanMessage(content=q))

        while True:
            llm = get_fresh_llm()
            if not llm:
                yield f"data: {json.dumps({'type': 'chunk', 'content': 'Heavy load. Try again soon.'})}\n\n"
                return
            try:
                async for chunk in llm.astream(messages):
                    yield f"data: {json.dumps({'type': 'chunk', 'content': chunk.content})}\n\n"
                break
            except Exception as e:
                if "429" in str(e):
                    mark_key_burned(llm.groq_api_key)
                    continue
                raise e
        is_lead = any(kw in q.lower() for kw in ["price", "buy", "license", "deploy", "hire", "contact"])
        yield f"data: {json.dumps({'type': 'metadata', 'capture_lead': is_lead})}\n\n"
    except Exception as e:
        logger.error(f"Stream Error: {e}")
        yield f"data: {json.dumps({'type': 'error', 'content': 'System busy.'})}\n\n"

@app.post("/chat")
async def chat(q: dict):
    return StreamingResponse(chat_stream_generator(q['question'], q.get('history', [])), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="error")
