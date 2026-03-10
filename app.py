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
        # Priority 1: Key Vault (keys.json)
        if KEYS_FILE.exists():
            keys = json.loads(KEYS_FILE.read_text())
            active_keys = [k for k in keys if k.get('status') == 'active']
            if active_keys:
                return ChatGroq(api_key=active_keys[0]['key'], model="llama-3.1-8b-instant", temperature=0)
        
        # Priority 2: Environment Variable (Fallback for Cloud)
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
            # For local testing without Pinecone, we'll set status to ready but search will fail
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
    
    cfg["bot_name"] = data.get("bot_name")
    cfg["business_name"] = data.get("business_name")
    cfg["branding"] = {
        "header_title": data.get("header_title"),
        "header_subtitle": data.get("header_subtitle"),
        "logo_emoji": data.get("logo_emoji"),
        "logo_url": data.get("logo_url"),
        "welcome_message": data.get("welcome_message"),
        "input_placeholder": data.get("input_placeholder"),
        "page_title": data.get("page_title"),
        "font_style": data.get("font_style"),
        "primary_color": data.get("primary_color"),
        "secondary_color": data.get("secondary_color"),
        "bot_bubble_color": data.get("bot_bubble_color"),
        "user_bubble_color": data.get("user_bubble_color")
    }
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
    
    return {
        "feedbacks": feedbacks,
        "total_chats": len(feedbacks),
        "top_questions": [], # Future logic
        "unanswered": []     # Future logic
    }

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
        # 1. CLOUD SEARCH
        if _status == "ready":
            query_embedding = pc.inference.embed(
                model="llama-text-embed-v2",
                inputs=[q],
                parameters={"input_type": "query"}
            )
            
            search_results = index.query(
                vector=query_embedding[0].values,
                top_k=8,
                include_metadata=True
            )
            
            context_list = [res["metadata"]["text"] for res in search_results["matches"]]
            context = "\n\n".join(context_list)
        else:
            context = "LOCAL TEST MODE: Operating on internal server logic."

        # 2. LLM LOGIC - The "Personality" Prompt
        sys_msg = f"""
        You are {bot_name}, the lead AI representative for {biz_name}. 
        You are a highly professional, confident, and specialized Digital FTE (Full-Time Equivalent).

        MANDATES:
        1. CONFIDENCE: Never say "based on the context" or "according to the text." Speak as the authority. Just answer the question directly.
        2. PERSONALITY: You ARE an employee of {biz_name}. Use "we" and "our" when referring to the company.
        3. POLITE UNKNOWNS: If the information isn't available in your knowledge base, say: "I apologize, but I don't have the specific details on that topic in our current documentation. Would you like me to connect you with one of our human specialists for more help?"
        4. BREVITY: Be professional, helpful, and concise.

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
                yield f"data: {json.dumps({'type': 'chunk', 'content': 'I am currently experiencing heavy load. Please try again in a moment.'})}\n\n"
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

        # Metadata
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
