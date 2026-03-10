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

def init_systems():
    global pc, index, local_db, embeddings_model, _status
    _status = "loading"
    if embeddings_model is None:
        embeddings_model = HuggingFaceEmbeddings(
            model_name="BAAI/bge-base-en-v1.5",
            model_kwargs={"device": "cpu"},
            encode_kwargs={"normalize_embeddings": True}
        )
    try:
        active_db = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "agentfactory"
        db_path = DATABASES_DIR / active_db
        if db_path.exists():
            local_db = Chroma(persist_directory=str(db_path), embedding_function=embeddings_model)
            _status = "ready_local"
            logger.info(f"✅ BRAIN READY")
        else: _status = "error"
    except: _status = "error"

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_systems()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
def health(): return {"status": _status}

# --- DUAL-MODE CHAT ENGINE ---

async def chat_stream_generator(q: str, history: List[dict]) -> AsyncGenerator[str, None]:
    cfg = get_config()
    bot_name = cfg.get("bot_name", "Agni")
    biz_name = cfg.get("business_name", "AgentFactory")
    
    context = ""
    if local_db:
        results = local_db.similarity_search(q, k=8)
        context = "\n\n".join([res.page_content for res in results])

    sys_msg = f"You are {bot_name} for {biz_name}. Knowledge: {context}"
    messages = [SystemMessage(content=sys_msg)]
    for m in history[-2:]: messages.append(HumanMessage(content=m['content']) if m['role']=='user' else AIMessage(content=m['content']))
    messages.append(HumanMessage(content=q))
    
    llm = get_fresh_llm()
    if not llm:
        yield f"data: {json.dumps({'type': 'chunk', 'content': 'Error.'})}\n\n"
        return
        
    async for chunk in llm.astream(messages):
        yield f"data: {json.dumps({'type': 'chunk', 'content': chunk.content})}\n\n"
    yield f"data: {json.dumps({'type': 'metadata', 'capture_lead': True})}\n\n"

@app.post("/chat")
async def chat(request: Request):
    try:
        body = await request.json()
        q = body.get("question")
        history = body.get("history", [])
        stream_requested = body.get("stream", True)

        if stream_requested:
            return StreamingResponse(chat_stream_generator(q, history), media_type="text/event-stream")
        else:
            # FIXED NON-STREAMING LOGIC
            llm = get_fresh_llm()
            if not llm: return JSONResponse({"answer": "No keys."}, status_code=500)
            
            context = ""
            if local_db:
                results = local_db.similarity_search(q, k=8)
                context = "\n\n".join([res.page_content for res in results])
                
            cfg = get_config()
            sys_msg = f"You are {cfg.get('bot_name','Agni')} for {cfg.get('business_name','AgentFactory')}. Knowledge: {context}"
            messages = [SystemMessage(content=sys_msg)]
            for m in history[-2:]: messages.append(HumanMessage(content=m['content']) if m['role']=='user' else AIMessage(content=m['content']))
            messages.append(HumanMessage(content=q))
            
            res = await llm.ainvoke(messages)
            return JSONResponse({"answer": res.content})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="error")
