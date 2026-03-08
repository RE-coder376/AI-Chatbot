"""
app.py — Production Cloud-Native RAG Server (Pinecone Llama Edition)
"""

import os
import json
import logging
import time
import asyncio
import re
import warnings
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional, Dict, AsyncGenerator

warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"

from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse
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

# Globals
pc = None
index = None
_status = "starting"

def get_fresh_llm():
    try:
        keys = json.loads(KEYS_FILE.read_text())
        active_keys = [k for k in keys if k['status'] == 'active']
        if not active_keys: return None
        return ChatGroq(api_key=active_keys[0]['key'], model="llama-3.1-8b-instant", temperature=0)
    except: return None

def mark_key_burned(failed_key: str):
    try:
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
        pc = Pinecone(api_key=PINECONE_API_KEY)
        index = pc.Index(INDEX_NAME)
        _status = "ready"
        logger.info("✅ CLOUD BRAIN READY (1024-dim Llama)")
    except Exception as e:
        logger.error(f"Cloud Error: {e}")
        _status = "error"

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_cloud()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.get("/")
async def get_index(): return FileResponse(Path(__file__).parent / "chat.html")

@app.get("/admin/keys")
def get_keys(password: str):
    return json.loads(KEYS_FILE.read_text()) if KEYS_FILE.exists() else []

async def chat_stream_generator(q: str, history: List[dict]) -> AsyncGenerator[str, None]:
    if _status != "ready":
        yield f"data: {json.dumps({'type': 'chunk', 'content': 'Cloud Brain Initializing...'})}\n\n"
        return

    try:
        # 1. CLOUD SEARCH (Using Inference Model)
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

        # 2. LLM LOGIC
        sys_msg = f"You are a professional technical retrieval assistant. Answer ONLY using the provided Context. If not in context, say you don't know. Context: {context}"
        messages = [SystemMessage(content=sys_msg)]
        for m in history[-2:]:
            messages.append(HumanMessage(content=m['content']) if m['role']=='user' else AIMessage(content=m['content']))
        messages.append(HumanMessage(content=q))

        while True:
            llm = get_fresh_llm()
            if not llm:
                yield f"data: {json.dumps({'type': 'chunk', 'content': 'All API keys exhausted.'})}\n\n"
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
        is_lead = any(kw in q.lower() for kw in ["price", "buy", "license", "deploy"])
        yield f"data: {json.dumps({'type': 'metadata', 'capture_lead': is_lead})}\n\n"

    except Exception as e:
        logger.error(f"Stream Error: {e}")
        yield f"data: {json.dumps({'type': 'error', 'content': 'System busy.'})}\n\n"

@app.post("/chat")
async def chat(q: dict):
    return StreamingResponse(chat_stream_generator(q['question'], q.get('history', [])), media_type="text/event-stream")

@app.get("/health")
def health():
    return {"status": _status, "engine": "pinecone_llama_1024"}

@app.get("/ping")
def ping():
    return {"status": "alive"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="error")
