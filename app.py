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
KEYS_FILE = Path("keys.json")
CONFIG_FILE = Path("config.json")
ACTIVE_DB_FILE = Path("active_db.txt")
DATABASES_DIR = Path("databases")

# Globals
local_db = None
embeddings_model = None

def get_config():
    if CONFIG_FILE.exists():
        try: return json.loads(CONFIG_FILE.read_text())
        except: pass
    return {"admin_password": "admin"}

def get_fresh_llm():
    try:
        if KEYS_FILE.exists():
            keys = json.loads(KEYS_FILE.read_text())
            current = next((k for k in keys if k.get("is_current") and k.get("status") == "active"), None)
            if current: return ChatGroq(api_key=current["key"], model="llama-3.1-8b-instant", temperature=0)
            actives = [k for k in keys if k.get('status') == 'active']
            if actives: return ChatGroq(api_key=actives[0]['key'], model="llama-3.1-8b-instant", temperature=0)
        key = os.getenv("GROQ_API_KEY")
        return ChatGroq(api_key=key, model="llama-3.1-8b-instant", temperature=0) if key else None
    except: return None

def load_brain_on_demand():
    global local_db, embeddings_model
    if local_db is not None: return
    
    logger.info("📡 LAZY LOADING BRAIN (BAAI/bge-base-en-v1.5)...")
    embeddings_model = HuggingFaceEmbeddings(
        model_name="BAAI/bge-base-en-v1.5",
        model_kwargs={"device": "cpu"},
        encode_kwargs={"normalize_embeddings": True}
    )
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "agentfactory"
    db_path = DATABASES_DIR / active
    if db_path.exists():
        local_db = Chroma(persist_directory=str(db_path), embedding_function=embeddings_model)
        logger.info("✅ BRAIN SYNCHRONIZED")

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/health")
def health(): return {"status": "alive", "brain_loaded": local_db is not None}

async def chat_stream_generator(q: str, history: List[dict]) -> AsyncGenerator[str, None]:
    load_brain_on_demand()
    llm = get_fresh_llm()
    if not llm:
        yield f"data: {json.dumps({'type': 'chunk', 'content': 'No Keys.'})}\n\n"
        return
    
    res = local_db.similarity_search(q, k=8)
    context = "\n\n".join([r.page_content for r in res])
    cfg = get_config()
    sys_msg = f"You are {cfg.get('bot_name','Agni')} for {cfg.get('business_name','AgentFactory')}. Answer using: {context}"
    
    messages = [SystemMessage(content=sys_msg)]
    for m in history[-2:]: messages.append(HumanMessage(content=m['content']) if m['role']=='user' else AIMessage(content=m['content']))
    messages.append(HumanMessage(content=q))
    
    async for chunk in llm.astream(messages):
        yield f"data: {json.dumps({'type': 'chunk', 'content': chunk.content})}\n\n"
    yield f"data: {json.dumps({'type': 'metadata', 'capture_lead': True})}\n\n"

@app.post("/chat")
async def chat(request: Request):
    data = await request.json()
    q = data.get("question")
    hist = data.get("history", [])
    stream = data.get("stream", True)

    if stream:
        return StreamingResponse(chat_stream_generator(q, hist), media_type="text/event-stream")
    else:
        load_brain_on_demand()
        llm = get_fresh_llm()
        if not llm: return JSONResponse({"answer": "No keys."}, status_code=500)
        
        res = local_db.similarity_search(q, k=8)
        context = "\n\n".join([r.page_content for r in res])
        cfg = get_config()
        sys_msg = f"You are {cfg.get('bot_name','Agni')} for {cfg.get('business_name','AgentFactory')}. Answer using: {context}"
        
        messages = [SystemMessage(content=sys_msg)]
        for m in hist[-2:]: messages.append(HumanMessage(content=m['content']) if m['role']=='user' else AIMessage(content=m['content']))
        messages.append(HumanMessage(content=q))
        
        resp = await llm.ainvoke(messages)
        return JSONResponse({"answer": resp.content})

# ... minimized admin ...
@app.get("/admin/branding")
def get_branding(p: str): return {"bot_name": "Agni", "business_name": "AgentFactory"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="error")
