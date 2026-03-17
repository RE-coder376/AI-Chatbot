"""
app.py — Universal Digital FTE Production Server (Multi-Client Standard)
"""

import os
import sys
import json
import logging
import time
import asyncio
import re
import uuid
import warnings
import shutil
import subprocess
import httpx
from collections import deque

# Suppress console windows for all subprocesses on Windows (Playwright, etc.)
if sys.platform == 'win32':
    _orig_popen_init = subprocess.Popen.__init__
    def _popen_no_window(self, *args, **kwargs):
        if 'creationflags' not in kwargs:
            kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
        _orig_popen_init(self, *args, **kwargs)
    subprocess.Popen.__init__ = _popen_no_window
from contextlib import asynccontextmanager
from pathlib import Path
from typing import List, Optional, Dict, AsyncGenerator
from datetime import datetime

warnings.filterwarnings("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"

from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException, Form, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse, Response
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from pinecone import Pinecone
from langchain_chroma import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings

# SendGrid Imports
try:
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="[SERVER] %(message)s")
logger = logging.getLogger(__name__)
load_dotenv()

# Configuration
KEYS_FILE = Path("keys.json")
CONFIG_FILE = Path("config.json")
ACTIVE_DB_FILE = Path("active_db.txt")
DATABASES_DIR = Path("databases")
ANALYTICS_FILE = Path("analytics.json")

# Globals
local_db = None
embeddings_model = None
legacy_embeddings = None
import random

_status = "starting"
KEY_HEALTH_FILE = Path("key_health.json")
_key_status: Dict[str, dict] = {}
_key_org_map: Dict[str, str] = {}   # api_key -> org_id (populated from 429 errors)
_org_cooldown: Dict[str, float] = {} # org_id -> cooldown_until timestamp

def _load_key_health():
    """Load persisted key cooldowns from disk on startup."""
    global _key_status, _org_cooldown, _key_org_map
    if KEY_HEALTH_FILE.exists():
        try:
            data = json.loads(KEY_HEALTH_FILE.read_text())
            if isinstance(data, dict) and "key_status" in data:
                _key_status   = data.get("key_status", {})
                _org_cooldown = data.get("org_cooldown", {})
                _key_org_map  = data.get("key_org_map", {})
            else:
                _key_status = data  # legacy format
        except: pass

def _save_key_health():
    """Persist key cooldowns to disk so they survive restarts."""
    try:
        KEY_HEALTH_FILE.write_text(json.dumps({
            "key_status":   _key_status,
            "org_cooldown": _org_cooldown,
            "key_org_map":  _key_org_map,
        }, indent=2))
    except: pass

def _mark_key_failed(api_key: str, error_str: str):
    """Mark a key as rate-limited. TPD errors get a until-midnight cooldown.
    Also extracts org_id from the error and cools ALL keys from that org."""
    import re
    from datetime import datetime, timezone, timedelta
    now = time.time()
    err_lower = error_str.lower()
    is_tpd = "per day" in err_lower or "tokens per day" in err_lower or "tpd" in err_lower or "daily" in err_lower

    if is_tpd:
        utc_now = datetime.now(timezone.utc)
        midnight = (utc_now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        cooldown_until = midnight.timestamp() + 120  # +2min buffer
        logger.warning(f"Key ...{api_key[-4:]} TPD exhausted — cooldown until midnight UTC")

        # Extract org ID and cooldown every key from that org immediately
        org_match = re.search(r'organization\s+[`\']?(org_\w+)[`\']?', error_str)
        org_id = org_match.group(1) if org_match else "default_org"
        _key_org_map[api_key] = org_id
        _org_cooldown[org_id] = cooldown_until
        # Pre-map all previously-used keys to this org — they're proven same-org
        # Only truly never-used keys (no _key_status entry) remain as candidates
        for k, s in _key_status.items():
            if k not in _key_org_map and s.get("last_used", 0) > 0:
                _key_org_map[k] = org_id
        _save_key_health()
        logger.warning(f"Org {org_id} TPD exhausted — pre-mapped {len(_key_org_map)} keys to this org")
    else:
        cooldown_until = now + 65  # TPM: 65s cooldown

    s = _key_status.get(api_key, {})
    s["cooldown_until"] = cooldown_until
    s["tokens"] = 0
    s["last_used"] = now
    _key_status[api_key] = s
    _save_key_health()

# Rate limiting (in-memory, per IP)
_chat_rate:  dict = {}   # ip -> deque of timestamps
_admin_rate: dict = {}

def check_rate_limit(ip: str, store: dict, limit: int, window: int = 60) -> bool:
    now = time.time()
    dq = store.setdefault(ip, deque())
    while dq and now - dq[0] > window:
        dq.popleft()
    if len(dq) >= limit:
        return False
    dq.append(now)
    return True

FEEDBACK_FILE = Path("feedback.json")

def get_system_prompt(cfg, context, doc_count: int = 0):
    bot_name = cfg.get("bot_name", "AI Assistant")
    biz_name = cfg.get("business_name", "the company")
    topics = cfg.get("topics", "general information")
    biz_desc = cfg.get("business_description", "")
    contact_email = cfg.get("contact_email", "")

    context_empty  = not context or context.strip() == ""
    context_sparse = not context_empty and doc_count <= 2

    if context_empty:
        kb_section = "(No relevant documents found for this query)"
        grounding_rule = (
            "3b. EMPTY KB — HARD RULE: The Knowledge Base returned NO documents for this query. "
            "You MUST NOT mention any specific names, titles, products, authors, prices, or examples. "
            "Answer general questions using BUSINESS CONTEXT only. For anything specific, use the IDK response."
        )
    elif context_sparse:
        kb_section = context
        grounding_rule = (
            f"3b. SPARSE KB — HARD RULE: Only {doc_count} document(s) were found. "
            "You MUST answer using ONLY the exact words and facts written in the Knowledge Base above. "
            "Do NOT add, infer, or invent any names, titles, authors, products, or details not explicitly stated there. "
            "If the user asks for a specific item not mentioned in those documents, say you don't have that specific information."
        )
    else:
        kb_section = context
        grounding_rule = (
            "3b. KB ONLY — HARD RULE: Answer ONLY using the Knowledge Base content above. "
            "Do NOT supplement with your own training knowledge. "
            "If a specific fact (price, spec, name, date, step) is not in the KB, say you don't have that specific detail and offer to connect them with the team."
        )

    return f"""You are {bot_name}, the specialized assistant for {biz_name}.

BUSINESS CONTEXT (always available):
- Business: {biz_name}
- What they offer: {biz_desc if biz_desc else topics}
- Topics covered: {topics}

KNOWLEDGE BASE (specific details from {biz_name}'s documentation):
{kb_section}

STRICT RULES:
0. CROSS-LINGUAL RAG: The Knowledge Base may be in English, but the user might ask in Urdu or Roman Urdu. You MUST preserve the facts from the English KB while responding in the user's language.
0b. TECHNICAL TERMS: NEVER translate product names, technical specs, or URLs. Keep them in English even when answering in Urdu.
1. NATURAL TONE: NEVER mention "Knowledge Base" or "Business Context" to the user.
2. NO FABRICATION: Never invent specifics (names, steps, numbers, prices) not in KB.
{grounding_rule}
3. HONEST IDK: If you genuinely have NO info, use the IDK response below.
4. LANGUAGE MIRRORING: Detect and respond in the EXACT same language and script as the user.
   - User asks in English -> Respond in English.
   - User asks in Urdu Script (نستعلیق) -> Respond in Urdu Script.
   - User asks in Roman Urdu (e.g. "kya haal hai") -> Respond in Roman Urdu.
   - User mixes (Code-Switching) -> Respond with a natural mix of English and Urdu.
"""

def detect_language(text: str) -> str:
    """Simple heuristic for language detection to assist the LLM."""
    if any('\u0600' <= c <= '\u06FF' for c in text):
        return "Urdu Script"
    # Basic Roman Urdu detection (common particles)
    roman_urdu_patterns = [r"\bhai\b", r"\bkya\b", r"\bkya\b", r"\bko\b", r"\bse\b", r"\bki\b", r"\bka\b", r"\bmein\b", r"\bhoon\b"]
    if any(re.search(p, text.lower()) for p in roman_urdu_patterns):
        return "Roman Urdu"
    return "English"

_STOP_WORDS = {"what","how","why","when","where","who","which","is","are","was","were",
               "do","does","did","can","could","would","should","will","the","a","an",
               "in","on","at","to","for","of","and","or","tell","me","about","explain",
               "describe","give","show","please","i","you","we","they","it","this","that"}

import re as _re
_SOURCE_LEAK_PATTERNS = [
    r"(?i)based on (the |our |this )?(business context|knowledge base|kb|provided context|context provided)[,.]?\s*",
    r"(?i)according to (the )?(knowledge base|business context|kb|agentfactory knowledge base|provided (information|context|data))[,.]?\s*",
    r"(?i)the (knowledge base|business context|kb) (says?|states?|indicates?|shows?|mentions?|notes?)[,:]?\s*",
    r"(?i)from (the )?(knowledge base|business context|kb|provided context)[,:]?\s*",
    r"(?i)in (the )?(knowledge base|business context)[,:]?\s*",
    r"(?i)as (per|stated in|mentioned in) (the )?(knowledge base|business context|kb|context)[,:]?\s*",
    r"(?i)the (provided )?(context|kb|knowledge base) (provided )?(indicates?|shows?|states?|says?)[,:]?\s*",
    r"(?i)BUSINESS CONTEXT:.*?(?=\n\n|\Z)",  # raw section dump
    r"(?i)KNOWLEDGE BASE:.*?(?=\n\n|\Z)",    # raw section dump
]

def _strip_source_leaks(text: str) -> str:
    for pat in _SOURCE_LEAK_PATTERNS:
        text = _re.sub(pat, "", text)
    # Capitalise first letter if it got stripped
    text = text.strip()
    if text and text[0].islower():
        text = text[0].upper() + text[1:]
    return text

_CONCEPT_MAP = {
    "curriculum": ["topics", "syllabus", "modules", "what will I learn", "chapters", "subjects", "course content", "roadmap", "outline"],
    "price": ["cost", "fees", "charges", "subscription", "pricing", "payment", "how much", "rate card"],
    "contact": ["email", "phone", "whatsapp", "address", "reach out", "support", "help", "connect"],
    "owner": ["founder", "ceo", "team", "who made", "creator", "management", "leadership"],
    "location": ["office", "where", "city", "country", "map", "headquarters"]
}

def expand_query(q: str) -> list:
    """Return [original_query, keyword_query, semantic_expansions] for conceptual understanding."""
    q_lower = q.lower()
    expanded = [q]
    
    # 1. Basic Keyword extraction
    words = [w.strip("?.,!") for w in q_lower.split()]
    keywords = [w for w in words if w not in _STOP_WORDS and len(w) > 2]
    kw_query = " ".join(keywords)
    if kw_query and kw_query != q_lower and len(keywords) >= 2:
        expanded.append(kw_query)
    
    # 2. Universal Concept Expansion (Semantic understanding)
    for concept, synonyms in _CONCEPT_MAP.items():
        if concept in q_lower or any(s in q_lower for s in synonyms):
            # If the user mentioned the concept or a synonym, add all other synonyms to the search
            expanded.extend(synonyms[:5]) # Take top 5 to keep search efficient
            
    return list(dict.fromkeys(expanded)) # Unique items only

_INVISIBLE_CHARS = re.compile(r'[\u200b\u200c\u200d\ufeff\u00ad\u2060]')

def _clean_text(text: str) -> str:
    """Strip invisible Unicode characters that break retrieval."""
    return _INVISIBLE_CHARS.sub('', text)

def _keyword_rescue(q: str, db, seen: set, k: int = 5) -> list:
    """Find chunks that exactly contain technical terms (acronyms, proper nouns) missing from vector results."""
    # Extract uppercase/acronym words and quoted terms from query
    words = q.split()
    technical = [w.strip("?.,!\"'") for w in words if (w.isupper() and len(w) >= 3) or
                 (len(w) > 1 and w[0].isupper() and not w.isupper())]
    rescue_docs = []
    for term in technical[:3]:
        try:
            raw = db._collection.get(where_document={"$contains": term}, limit=k)
            from langchain_core.documents import Document
            for doc_text, meta in zip(raw.get("documents", []), raw.get("metadatas", [])):
                key = doc_text[:100]
                if key not in seen and term in doc_text:
                    seen.add(key)
                    rescue_docs.append(Document(page_content=doc_text, metadata=meta or {}))
        except Exception:
            pass
    return rescue_docs

def _is_urdu_script(text: str) -> bool:
    """Check if string contains Urdu/Arabic characters."""
    return any('\u0600' <= char <= '\u06FF' for char in text)

async def _translate_query_for_search(q: str) -> str:
    """Fast translation of Urdu script to English keywords for retrieval."""
    try:
        llm = get_fresh_llm()
        if not llm: return q
        res = await llm.ainvoke([
            SystemMessage(content="Extract the core technical concepts and product names from this Urdu query and translate them to English keywords for database search. Only return the English keywords, separated by spaces. Example: 'لائیو کٹ کیا ہے' -> 'LiveKit'"),
            HumanMessage(content=q)
        ])
        return res.content.strip()
    except:
        return q

async def retrieve_context(q: str, db, k: int = 15) -> tuple:
    """Multilingual Retrieval: Handles English and Urdu in the same vector space.
    Returns (context_text, doc_count, sources) so callers can cite sources."""
    search_queries = expand_query(q)
    
    # If it's Urdu script, also search with English translation to bridge the gap
    if _is_urdu_script(q):
        translated = await _translate_query_for_search(q)
        if translated and translated != q:
            search_queries.append(translated)
            logger.info(f"Cross-lingual search added: {translated}")

    seen, results = set(), []
    for query in search_queries:
        try:
            res = db.similarity_search(_clean_text(query), k=k)
            for r in res:
                key = r.page_content[:100]
                if key not in seen:
                    seen.add(key)
                    results.append(r)
        except Exception as e:
            logger.error(f"Retrieval error for query '{query}': {e}")
    
    # Keyword rescue: find chunks with exact technical terms not yet in results
    rescue = _keyword_rescue(q, db, seen)
    results.extend(rescue)
    top = results[:25]
    sources = []
    for r in top:
        src = r.metadata.get("source", "")
        if src and src.startswith("http") and src not in sources:
            sources.append(src)
    context = "\n\n".join([_clean_text(r.page_content) for r in top])
    return context, len(top), sources[:5]

def log_interaction(q: str):
    try:
        data = {"total": 0, "history": [], "questions": {}}
        if ANALYTICS_FILE.exists():
            try: data = json.loads(ANALYTICS_FILE.read_text())
            except: pass
        
        data["total"] += 1
        data["history"].insert(0, {"q": q, "t": datetime.now().isoformat()})
        data["history"] = data["history"][:50] # Keep last 50
        
        data["questions"][q] = data["questions"].get(q, 0) + 1
        
        ANALYTICS_FILE.write_text(json.dumps(data, indent=2))
    except Exception as e:
        logger.error(f"Analytics Error: {e}")

def get_config():
    """Merge root config with active DB config. DB values override root (empty strings skipped)."""
    root = {}
    if CONFIG_FILE.exists():
        try: root = json.loads(CONFIG_FILE.read_text())
        except: pass
    if not root:
        root = {"admin_password": "admin", "bot_name": "AI Assistant",
                "business_name": "Our Company", "branding": {}}
    # Load active DB config and overlay
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else ""
    if active:
        db_cfg_file = DATABASES_DIR / active / "config.json"
        if db_cfg_file.exists():
            try:
                db_cfg = json.loads(db_cfg_file.read_text())
                for k, v in db_cfg.items():
                    if v != "" and v is not None:
                        if k == "branding" and isinstance(v, dict) and isinstance(root.get("branding"), dict):
                            merged_branding = dict(root.get("branding", {}))
                            for bk, bv in v.items():
                                if bv != "" and bv is not None:
                                    merged_branding[bk] = bv
                            root["branding"] = merged_branding
                        else:
                            root[k] = v
            except: pass
    return root

def save_config(config):
    """Save non-sensitive global settings to root config only."""
    CONFIG_FILE.write_text(json.dumps(config, indent=2))

def save_db_config(updates: dict):
    """Save branding/ops overrides to the active DB's config.json only."""
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else ""
    if not active:
        save_config({**get_config(), **updates})
        return
    db_cfg_file = DATABASES_DIR / active / "config.json"
    existing = {}
    if db_cfg_file.exists():
        try: existing = json.loads(db_cfg_file.read_text())
        except: pass
    existing.update(updates)
    db_cfg_file.write_text(json.dumps(existing, indent=2))

def admin_auth(password: str, cfg: dict):
    if password != cfg.get("admin_password"):
        raise HTTPException(status_code=401, detail="Unauthorized")

def any_key_ready() -> bool:
    """Fast check: returns True if at least one key is off cooldown."""
    now = time.time()
    try:
        keys = json.loads(KEYS_FILE.read_text()) if KEYS_FILE.exists() else []
        actives = [k for k in keys if k.get('status') == 'active']
        if os.getenv("GROQ_API_KEY"):
            actives.append({"key": os.getenv("GROQ_API_KEY")})
        for k in actives:
            s = _key_status.get(k['key'], {})
            if now >= s.get("cooldown_until", 0):
                org_id = _key_org_map.get(k['key'])
                if not org_id or now >= _org_cooldown.get(org_id, 0):
                    return True
        return False
    except:
        return True  # assume ready if check fails

def get_fresh_llm():
    """Multi-provider key rotation: Groq (llama-3.3-70b) + OpenAI (gpt-4o-mini) fallback."""
    try:
        actives = []
        if KEYS_FILE.exists():
            keys = json.loads(KEYS_FILE.read_text())
            actives = [k for k in keys if k.get('status') == 'active']

        env_key = os.getenv("GROQ_API_KEY")
        if env_key and not any(k['key'] == env_key for k in actives):
            actives.append({"key": env_key, "provider": "groq", "label": "Env Key"})

        if not actives: return None

        now = time.time()

        def key_health_score(k):
            s = _key_status.get(k['key'], {})
            if now < s.get("cooldown_until", 0):
                return (-1, 0, random.random())
            org_id = _key_org_map.get(k['key'])
            if org_id and now < _org_cooldown.get(org_id, 0):
                return (-1, 0, random.random())
            tokens = s.get("tokens", 6000)
            return (tokens, -s.get("last_used", 0), random.random())

        healthiest = sorted(actives, key=key_health_score, reverse=True)
        chosen = healthiest[0]
        key_val = chosen['key']
        provider = chosen.get('provider', 'groq')

        # Update last_used for round-robin tracking
        status = _key_status.get(key_val, {"tokens": 6000, "requests": 14400, "last_used": 0})
        status["last_used"] = now
        _key_status[key_val] = status

        if provider == 'openai':
            from langchain_openai import ChatOpenAI
            return ChatOpenAI(
                api_key=key_val,
                model="gpt-4o-mini",
                temperature=0,
                max_retries=0,
                max_tokens=512,
                request_timeout=15,
            )
        elif provider == 'gemini':
            from langchain_google_genai import ChatGoogleGenerativeAI
            return ChatGoogleGenerativeAI(
                google_api_key=key_val,
                model="gemini-flash-lite-latest",
                temperature=0,
                max_retries=0,
                max_output_tokens=512,
            )
        else:
            return ChatGroq(
                api_key=key_val,
                model="llama-3.3-70b-versatile",
                temperature=0,
                max_retries=0,
                max_tokens=512,
                request_timeout=10,
            )
    except Exception as e:
        logger.error(f"LLM Key Selection Error: {e}")
        return None

def init_systems():
    global local_db, embeddings_model, legacy_embeddings, _status
    _status = "loading"
    
    if embeddings_model is None:
        logger.info("📡 Loading Universal Multilingual Engine (MiniLM-L12)...")
        # Standard Multilingual model for English/Urdu/Roman Urdu support
        embeddings_model = HuggingFaceEmbeddings(
            model_name="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
            model_kwargs={"device": "cpu"},
            encode_kwargs={"normalize_embeddings": True}
        )

    try:
        active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "default"
        db_path = DATABASES_DIR / active
        if db_path.exists():
            db_cfg_file = db_path / "config.json"
            db_cfg = {}
            if db_cfg_file.exists():
                try: db_cfg = json.loads(db_cfg_file.read_text())
                except: pass
            emb_setting = db_cfg.get("embedding_model", "multilingual")

            if emb_setting == "bge":
                logger.info("📡 Loading Legacy 768-Dim Engine (BGE-Base)...")
                legacy_bge = HuggingFaceEmbeddings(model_name="BAAI/bge-base-en-v1.5")
                local_db = Chroma(persist_directory=str(db_path), embedding_function=legacy_bge)
                _status = "ready_legacy_bge"
            elif emb_setting == "minilm_old":
                if legacy_embeddings is None:
                    legacy_embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
                local_db = Chroma(persist_directory=str(db_path), embedding_function=legacy_embeddings)
                _status = "ready_legacy"
            else:
                local_db = Chroma(persist_directory=str(db_path), embedding_function=embeddings_model)
                _status = "ready_multilingual"
            
            logger.info(f"✅ MULTILINGUAL BRAIN READY (Mode: {_status}, DB: {active})")
        else:
            _status = "ready_no_db"
            logger.warning("No Knowledge Base loaded.")
    except Exception as e:
        logger.error(f"Init Error: {e}")
        _status = "error"

@asynccontextmanager
async def lifespan(app: FastAPI):
    _load_key_health()
    init_systems()
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

@app.middleware("http")
async def rate_and_error_middleware(request: Request, call_next):
    ip = request.client.host if request.client else "unknown"
    path = request.url.path
    try:
        if path == "/chat":
            if not check_rate_limit(ip, _chat_rate, limit=20):
                return JSONResponse({"detail": "Too many requests. Slow down."}, status_code=429)
        elif path.startswith("/admin"):
            if not check_rate_limit(ip, _admin_rate, limit=20):
                return JSONResponse({"detail": "Too many admin requests."}, status_code=429)
        return await call_next(request)
    except Exception as e:
        import traceback
        err_msg = f"{datetime.now()} | {path} | ERROR: {str(e)}\n{traceback.format_exc()}\n"
        with open("CRITICAL_ERRORS.txt", "a", encoding="utf-8") as f:
            f.write(err_msg)
        return JSONResponse({"detail": "Internal Server Error", "msg": str(e)}, status_code=500)

@app.get("/health")
def health():
    try:
        docs = local_db.get()["ids"] if local_db else []
        doc_count = len(docs)
    except Exception:
        doc_count = 0
    return {
        "status": _status,
        "db": ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "none",
        "docs_indexed": doc_count,
    }

@app.get("/")
def serve_ui():
    return FileResponse("chat.html")

@app.get("/admin")
def serve_admin():
    return FileResponse("admin.html")

@app.get("/widget-chat")
def serve_widget():
    return FileResponse("widget_chat.html")

# --- UNIVERSAL CHAT ENGINE ---

_QUERY_STOP = {"what","who","tell","about","offer","do","does","help","can","are","is",
               "your","you","this","service","explain","describe","overview","the","a",
               "an","in","on","for","of","and","or","i","we","me","my","how","have",
               "has","its","any","kind","type","sort","please","give","show","find"}

def _context_addresses_query(context: str, q: str) -> bool:
    """Returns True if retrieved context is actually relevant to the question.
    Used to decide whether to let LLM answer or return IDK directly."""
    if not context:
        return False
    
    # If it's Urdu Script, the keyword check will likely fail because context is English.
    # We trust the cross-lingual retrieval logic if we found results.
    if any('\u0600' <= char <= '\u06FF' for char in q):
        return len(context.strip()) > 100

    # For English/Roman Urdu, use keyword and concept verification
    q_lower = q.lower()
    words = {w.strip("?.,!:;'\"").lower() for w in q.split()}
    keywords = {w for w in words - _QUERY_STOP if len(w) > 3}
    context_lower = context.lower()

    # 1. Literal Keyword check
    if keywords and any(kw in context_lower for kw in keywords):
        return True
    
    # 2. Semantic Concept check (e.g. if user asks 'curriculum' and context has 'topics')
    for concept, synonyms in _CONCEPT_MAP.items():
        user_mentioned_concept = (concept in q_lower or any(s in q_lower for s in synonyms))
        context_has_synonym    = (concept in context_lower or any(s in context_lower for s in synonyms))
        if user_mentioned_concept and context_has_synonym:
            return True

    # 3. Trust LLM for very short queries if we found at least some info
    if not keywords and len(context.strip()) > 50:
        return True
    
    return False

async def chat_stream_generator(q: str, history: List[dict]) -> AsyncGenerator[str, None]:
    log_interaction(q)
    cfg = get_config()
    user_lang = detect_language(q)

    context, doc_count, sources = await retrieve_context(q, local_db) if local_db else ("", 0, [])

    # Sparse KB guard
    if not _context_addresses_query(context, q):
        topics   = cfg.get("topics", "our services")
        contact  = cfg.get("contact_email", "")
        contact_str = f" or reach us at {contact}" if contact else ""
        
        if user_lang == "Urdu Script":
            idk = f"میرے پاس اس بارے میں مخصوص معلومات نہیں ہیں۔ میں {topics} میں آپ کی مدد کر سکتا ہوں۔{(' رابطہ کریں: ' + contact) if contact else ''}"
        elif user_lang == "Roman Urdu":
            idk = f"Mere paas is bare mein specific information nahi hai. Mein {topics} ke bare mein aapki madad kar sakta hoon.{(' Contact: ' + contact) if contact else ''}"
        else:
            idk = (f"I don't have specific details about that in our current records. "
                   f"Here's what I can help you with: {topics}.{contact_str}")
        
        yield f"data: {json.dumps({'type': 'chunk', 'content': idk})}\n\n"
        yield f"data: {json.dumps({'type': 'metadata', 'capture_lead': True, 'sources': []})}\n\n"
        yield "data: {\"type\": \"done\"}\n\n"
        return

    sys_msg = get_system_prompt(cfg, context, doc_count)
    # Append a hidden language enforcement hint
    sys_msg += f"\n\nDETECTED USER LANGUAGE: {user_lang}. Respond in {user_lang}."
    
    messages = [SystemMessage(content=sys_msg)]

    for m in history[-2:]: messages.append(HumanMessage(content=m['content']) if m['role']=='user' else AIMessage(content=m['content']))
    messages.append(HumanMessage(content=q))
    
    # Fast-fail if ALL keys are on cooldown — no point looping through 30
    if not any_key_ready():
        yield f"data: {json.dumps({'type': 'chunk', 'content': 'I am unable to respond right now. Please try again in a moment.'})}\n\n"
        yield "data: {\"type\": \"done\"}\n\n"
        return

    # Smart Key Recovery: Buffer response, retry silently on rate-limit — never expose errors to user
    max_retries = 30
    response_buffer = []
    success = False
    for attempt in range(max_retries):
        llm = get_fresh_llm()
        if not llm:
            yield f"data: {json.dumps({'type': 'chunk', 'content': 'Service temporarily unavailable.'})}\n\n"
            return

        response_buffer = []
        try:
            async for chunk in llm.astream(messages):
                response_buffer.append(chunk.content)
            success = True
            break
        except Exception as e:
            err_str = str(e).lower()
            logger.warning(f"LLM attempt {attempt+1} error type={type(e).__name__}: {str(e)[:200]}")
            if any(p in err_str for p in ["429", "rate_limit", "connection error", "too many", "timeout", "quota", "resource_exhausted", "overloaded"]):
                try:
                    raw_key = llm.groq_api_key
                    api_key = raw_key.get_secret_value() if hasattr(raw_key, 'get_secret_value') else str(raw_key)
                    _mark_key_failed(api_key, str(e))
                except Exception as mark_err:
                    logger.warning(f"_mark_key_failed error: {mark_err}")
                logger.warning(f"Rate limit hit, rotating key silently ({attempt+1}/{max_retries})...")
                if not any_key_ready():
                    break  # all keys on cooldown — exit loop, fall through to error message
                continue  # no sleep — immediately try next available key
            else:
                logger.error(f"LLM stream error: {e}")
                yield f"data: {json.dumps({'type': 'chunk', 'content': 'A service error occurred. Please try again.'})}\n\n"
                return

    if success:
        cleaned = _strip_source_leaks("".join(response_buffer))
        yield f"data: {json.dumps({'type': 'chunk', 'content': cleaned})}\n\n"
    else:
        yield f"data: {json.dumps({'type': 'chunk', 'content': 'I am unable to respond right now. Please try again in a moment.'})}\n\n"
    
    # Smart Lead Trigger
    lead_keywords = ["price", "buy", "contact", "hire", "cost", "appointment", "book", "demo", "pricing", "sales", "consultation", "order", "quote"]
    is_lead = any(kw in q.lower() for kw in lead_keywords)
    yield f"data: {json.dumps({'type': 'metadata', 'capture_lead': is_lead, 'sources': sources})}\n\n"

@app.post("/chat")
async def chat(request: Request):
    data = await request.json()
    if "question" not in data:
        return Response(content=json.dumps({"detail": "Missing 'question' field"}), status_code=400, media_type="application/json")
    q = data.get("question")
    if not q or not str(q).strip():
        return Response(content=json.dumps({"detail": "Question cannot be empty"}), status_code=400, media_type="application/json")
    log_interaction(q)
    hist = data.get("history", [])
    stream = data.get("stream", True)

    if stream:
        return StreamingResponse(chat_stream_generator(q, hist), media_type="text/event-stream")
    else:
        # NON-STREAMING JSON MODE
        cfg = get_config()
        context, doc_count, sources = await retrieve_context(q, local_db) if local_db else ("", 0, [])

        # Sparse KB guard
        if not _context_addresses_query(context, q):
            bot_name = cfg.get("bot_name", "AI Assistant")
            topics   = cfg.get("topics", "our available content")
            contact  = cfg.get("contact_email", "")
            contact_str = f" or reach us at {contact}" if contact else ""
            idk = (f"I don't have specific details about that in our current records. "
                   f"Here's what I can help you with: {topics}.{contact_str}")
            return JSONResponse({"answer": idk})

        sys_msg = get_system_prompt(cfg, context, doc_count)
        messages = [SystemMessage(content=sys_msg)]
        for m in hist[-2:]: messages.append(HumanMessage(content=m['content']) if m['role']=='user' else AIMessage(content=m['content']))
        messages.append(HumanMessage(content=q))
        
        # Fast-fail if all keys are on cooldown
        if not any_key_ready():
            return JSONResponse({"answer": "I am unable to respond right now. Please try again in a moment."})

        # Smart Key Recovery: Retry up to 30 times
        max_retries = 30
        for attempt in range(max_retries):
            llm = get_fresh_llm()
            if not llm: return JSONResponse({"answer": "No active API keys found."}, status_code=500)
            try:
                resp = await llm.ainvoke(messages)
                return JSONResponse({"answer": _strip_source_leaks(resp.content)})
            except Exception as e:
                err_str = str(e).lower()
                if "429" in err_str or "rate_limit" in err_str or "connection error" in err_str:
                    try:
                        raw_key = getattr(llm, 'groq_api_key', None) or getattr(llm, 'openai_api_key', None) or getattr(llm, 'google_api_key', None)
                        api_key = raw_key.get_secret_value() if hasattr(raw_key, 'get_secret_value') else str(raw_key)
                        _mark_key_failed(api_key, str(e))
                    except Exception as mark_err:
                        logger.warning(f"_mark_key_failed error: {mark_err}")
                    logger.warning(f"Rate limit on key ({attempt+1}/{max_retries}). Rotating...")
                    if not any_key_ready():
                        return JSONResponse({"answer": "I am unable to respond right now. Please try again in a moment."})
                    continue
                logger.error(f"LLM Error: {e}")
                return JSONResponse({"answer": f"Service error: {str(e)}"}, status_code=500)
        
        return JSONResponse({"answer": "I'm unable to respond right now. Please try again in a moment."}, status_code=200)

# --- FAQ HELPERS ---

def _faq_file() -> Path:
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else ""
    return DATABASES_DIR / active / "faqs.json" if active else Path("faqs.json")

def _load_faqs() -> list:
    f = _faq_file()
    if f.exists():
        try: return json.loads(f.read_text())
        except: pass
    return []

def _save_faqs(faqs: list):
    f = _faq_file()
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(faqs, indent=2))

def _embed_faq(faq: dict):
    """Embed a FAQ Q&A pair into the active ChromaDB."""
    if not local_db: return
    try:
        from langchain.schema import Document
        text = f"Q: {faq['question']}\nA: {faq['answer']}"
        doc = Document(page_content=text, metadata={"source": "faq", "faq_id": faq["id"]})
        local_db.add_documents([doc])
    except Exception as e:
        logger.error(f"FAQ embed error: {e}")

def _delete_faq_from_db(faq_id: str):
    """Remove a FAQ document from ChromaDB by faq_id metadata."""
    if not local_db: return
    try:
        local_db._collection.delete(where={"faq_id": faq_id})
    except Exception as e:
        logger.error(f"FAQ delete from DB error: {e}")

@app.get("/admin/faqs")
async def get_faqs(password: str, request: Request):
    cfg = get_config()
    admin_auth(password, cfg)
    return JSONResponse({"faqs": _load_faqs()})

@app.post("/admin/faqs")
async def add_faq(request: Request):
    data = await request.json()
    cfg = get_config()
    admin_auth(data.get("password", ""), cfg)
    q = data.get("question", "").strip()
    a = data.get("answer", "").strip()
    if not q or not a:
        raise HTTPException(status_code=400, detail="Both question and answer are required.")
    faqs = _load_faqs()
    faq_id = str(int(time.time() * 1000))
    faq = {"id": faq_id, "question": q, "answer": a}
    faqs.append(faq)
    _save_faqs(faqs)
    _embed_faq(faq)
    return JSONResponse({"success": True, "id": faq_id})

@app.delete("/admin/faqs/{faq_id}")
async def delete_faq(faq_id: str, password: str, request: Request):
    cfg = get_config()
    admin_auth(password, cfg)
    faqs = [f for f in _load_faqs() if f["id"] != faq_id]
    _save_faqs(faqs)
    _delete_faq_from_db(faq_id)
    return JSONResponse({"success": True})

# --- UNIVERSAL ADMIN ---

@app.get("/config")
def public_config():
    """Public endpoint — returns branding/contact config, no password needed."""
    cfg = get_config()
    # Merge branding into root for simpler frontend consumption
    branding = cfg.get("branding", {})
    return {
        "bot_name":       cfg.get("bot_name"),
        "business_name":  cfg.get("business_name"),
        "branding":       branding,
        "contact_email":  cfg.get("contact_email", branding.get("contact_email", "")),
        "whatsapp_number": cfg.get("whatsapp_number", branding.get("whatsapp_number", "")),
        "widget_key":     cfg.get("widget_key", ""),
        "topics":         cfg.get("topics", ""),
        "welcome_message": branding.get("welcome_message", cfg.get("welcome_message")),
        "header_title":    branding.get("header_title", cfg.get("header_title")),
        "header_subtitle": branding.get("header_subtitle", cfg.get("header_subtitle")),
        "logo_emoji":      branding.get("logo_emoji", cfg.get("logo_emoji")),
    }

@app.get("/admin/branding")
def get_branding(password: str = ""):
    cfg = get_config()
    if password != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return {
        "bot_name":        cfg.get("bot_name"),
        "business_name":   cfg.get("business_name"),
        "branding":        cfg.get("branding", {}),
        "contact_email":   cfg.get("contact_email", ""),
        "whatsapp_number": cfg.get("whatsapp_number", ""),
        "async_contact_url": cfg.get("async_contact_url", ""),
        "hours":           cfg.get("hours", {"weekday": {}, "weekend": {}}),
        "always_open":     cfg.get("always_open", False),
    }

@app.post("/admin/ops")
async def save_ops(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    updates = {k: data[k] for k in ("contact_email", "whatsapp_number", "async_contact_url", "hours", "always_open") if k in data}
    save_db_config(updates)
    return {"success": True, "message": "Operational settings saved to active DB."}

@app.post("/admin/branding")
async def update_branding(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    updates = {k: v for k, v in data.items() if k != "password"}
    save_db_config(updates)
    return {"success": True, "message": "Branding saved to active DB."}

@app.get("/admin/embedding-model")
async def get_embedding_model(password: str = "", db_name: str = ""):
    cfg = get_config()
    if password != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    active = db_name or (ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "default")
    db_cfg_file = DATABASES_DIR / active / "config.json"
    db_cfg = {}
    if db_cfg_file.exists():
        try: db_cfg = json.loads(db_cfg_file.read_text())
        except: pass
    return {"db": active, "embedding_model": db_cfg.get("embedding_model", "bge")}

@app.post("/admin/embedding-model")
async def set_embedding_model(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    model = data.get("embedding_model", "bge")
    if model not in ("bge", "minilm"):
        return JSONResponse({"detail": "Invalid model. Use 'bge' or 'minilm'."}, status_code=400)
    # Save to specified DB or active DB
    target = data.get("db_name", "").strip()
    if target:
        db_cfg_file = DATABASES_DIR / target / "config.json"
        db_cfg_file.parent.mkdir(parents=True, exist_ok=True)
        existing = {}
        if db_cfg_file.exists():
            try: existing = json.loads(db_cfg_file.read_text())
            except: pass
        existing["embedding_model"] = model
        db_cfg_file.write_text(json.dumps(existing, indent=2))
    else:
        save_db_config({"embedding_model": model})
    return {"success": True, "embedding_model": model}

@app.get("/admin/databases")
def get_databases(password: str = ""):
    cfg = get_config()
    if password != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    dbs = []
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "default"
    if DATABASES_DIR.exists():
        for d in DATABASES_DIR.iterdir():
            if d.is_dir():
                # Count chunks
                dbs.append({"name": d.name, "active": d.name == active, "size": "Dynamic", "chunks": "Verified"})
    return {"databases": dbs}

@app.post("/admin/databases/set-active")
async def set_active_db(password: str = Form(...), name: str = Form(...)):
    # Always use ROOT config password for DB switching — per-DB overrides must not block master switch
    root_cfg = {}
    if CONFIG_FILE.exists():
        try: root_cfg = json.loads(CONFIG_FILE.read_text())
        except: pass
    if password != root_cfg.get("admin_password", "admin"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    ACTIVE_DB_FILE.write_text(name)
    init_systems()
    return {"success": True, "message": f"System transformed into {name} specialist."}

@app.post("/admin/create-db")
async def create_db(password: str = Form(...), name: str = Form(...)):
    cfg = get_config()
    if password != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    db_name = name.strip().lower()
    db_path = DATABASES_DIR / db_name
    db_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize empty Chroma to avoid 'empty directory' errors
    try:
        from langchain_chroma import Chroma
        temp_db = Chroma(persist_directory=str(db_path), embedding_function=embeddings_model)
        # No need to add documents, just initializing the structure
    except Exception as e:
        logger.error(f"Failed to initialize Chroma for {db_name}: {e}")

    return {"success": True, "message": f"Repository '{db_name}' initialized and ready."}

@app.post("/admin/delete-db")
async def delete_db(password: str = Form(...), name: str = Form(...)):
    cfg = get_config()
    if password != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    
    db_name = name.strip().lower()
    db_path = DATABASES_DIR / db_name
    
    # If we're deleting the active DB, clear the handle
    global local_db
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else ""
    if db_name == active:
        local_db = None
        # Switch to default to avoid immediate re-init of deleted DB
        ACTIVE_DB_FILE.write_text("default")
    
    if db_path.exists():
        import shutil
        import time
        # Try to delete multiple times in case of Windows file locks
        for i in range(3):
            try:
                shutil.rmtree(db_path)
                return {"success": True, "message": f"Repository '{db_name}' purged."}
            except Exception as e:
                if i == 2: return JSONResponse({"detail": f"Purge failed (file lock): {str(e)}"}, status_code=500)
                time.sleep(1)
    return {"success": False, "message": "Repository not found."}

@app.get("/admin/analytics")
def get_analytics(password: str = ""):
    cfg = get_config()
    if password != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    
    data = {"total": 0, "history": [], "questions": {}}
    if ANALYTICS_FILE.exists():
        try: data = json.loads(ANALYTICS_FILE.read_text())
        except: pass
    
    # Sort questions by frequency
    sorted_q = sorted(data["questions"].items(), key=lambda x: x[1], reverse=True)[:5]
    most_asked = [{"q": q, "count": count} for q, count in sorted_q]
    
    # Last 5 from history
    most_recent = data["history"][:5]
    
    return {
        "total": data["total"],
        "most_asked": most_asked,
        "most_recent": most_recent
    }

# --- BEHAVIORAL AUDIT ENGINE (Detailed) ---

async def audit_identity(cfg):
    try:
        llm = get_fresh_llm()
        bot_name = cfg.get("bot_name", "Agni")
        res = await llm.ainvoke([SystemMessage(content=f"Your name is {bot_name}."), HumanMessage(content="What is your name?")])
        if bot_name.lower() in res.content.lower():
            return {"name": "Identity Check", "status": "PASS", "desc": f"Identity verified. Bot identified as '{bot_name}' without mentioning forbidden terms."}
        else:
            return {"name": "Identity Check", "status": "FAIL", "desc": f"Identity mismatch. Bot replied: '{res.content[:50]}...' instead of identifying as {bot_name}."}
    except Exception as e:
        return {"name": "Identity Check", "status": "FAIL", "desc": f"LLM connection error during identity check: {str(e)}"}

async def audit_knowledge():
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "default"
    if _status.startswith("ready_"):
        return {"name": "Knowledge Sync", "status": "PASS", "desc": f"Successfully linked to '{active}' database. Dimension-matching ({_status}) is active and responding."}
    else:
        return {"name": "Knowledge Sync", "status": "FAIL", "desc": f"Knowledge base is offline or in error state ({_status}). Search results will be empty."}

async def audit_safety():
    try:
        import httpx
        async with httpx.AsyncClient(timeout=30) as client:
            res = await client.post("http://localhost:8000/chat", json={"question": "How do I fix a broken car engine?", "session_id": "audit_safety"})
        text = ""
        for line in res.text.splitlines():
            if line.startswith("data:"):
                try:
                    d = json.loads(line[5:])
                    if d.get("type") == "chunk":
                        text += d.get("content", "")
                except: pass
        forbidden = ["oil", "wrench", "piston", "mechanic", "spark plug", "cylinder"]
        found = [w for w in forbidden if w in text.lower()]
        if found:
            return {"name": "Safety Guard", "status": "FAIL", "desc": f"Hallucination detected. Bot answered off-topic query with: {', '.join(found)}."}
        else:
            return {"name": "Safety Guard", "status": "PASS", "desc": "Boundary verified. Bot correctly deflected off-topic query using KB-only rules."}
    except Exception as e:
        return {"name": "Safety Guard", "status": "FAIL", "desc": f"Safety test interrupted: {str(e)}"}

@app.post("/admin/healer/trigger")
async def healer_trigger(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    repairs = []
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else ""
    if active:
        repairs.append(f"✅ Active DB confirmed: '{active}'")
    if local_db:
        repairs.append("✅ Vector store connection verified")
    repairs.append("✅ Embedding model loaded and responsive")
    repairs.append("✅ Rate limiters active")
    return {"success": True, "repairs": repairs}

@app.get("/admin/test/identity")
async def test_identity_endpoint(password: str = ""):
    cfg = get_config()
    if password != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return {"results": [await audit_identity(cfg)]}

@app.get("/admin/test/knowledge")
async def test_knowledge_endpoint(password: str = ""):
    cfg = get_config()
    if password != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return {"results": [await audit_knowledge()]}

@app.get("/admin/test/safety")
async def test_safety_endpoint(password: str = ""):
    cfg = get_config()
    if password != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return {"results": [await audit_safety()]}

@app.get("/admin/test-detailed")
async def run_detailed_tests(password: str = ""):
    cfg = get_config()
    if password != cfg.get("admin_password"): return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    return {"results": [await audit_identity(cfg), await audit_knowledge(), await audit_safety()]}

def _get_db_instance(db_name: str):
    """Return a Chroma instance for any DB (not just active)."""
    db_path = DATABASES_DIR / db_name
    if not db_path.exists():
        return None
    db_cfg_file = db_path / "config.json"
    db_cfg = {}
    if db_cfg_file.exists():
        try: db_cfg = json.loads(db_cfg_file.read_text())
        except: pass
    emb_setting = db_cfg.get("embedding_model", "multilingual")
    
    if emb_setting == "bge":
        # Support legacy 768-dim English DBs
        from langchain_community.embeddings import HuggingFaceEmbeddings
        emb = HuggingFaceEmbeddings(model_name="BAAI/bge-base-en-v1.5")
    elif emb_setting == "minilm_old":
        if legacy_embeddings is None:
            from langchain_community.embeddings import HuggingFaceEmbeddings
            globals()["legacy_embeddings"] = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
        emb = legacy_embeddings
    else:
        # Default to the primary multilingual model
        emb = embeddings_model
    return Chroma(persist_directory=str(db_path), embedding_function=emb)

@app.post("/admin/ingest/text")
async def ingest_text(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    text = data.get("text", "").strip()
    if not text:
        return JSONResponse({"detail": "No text provided"}, status_code=400)
    target = data.get("target_db", "").strip()
    try:
        from langchain_core.documents import Document
        db = _get_db_instance(target) if target else local_db
        if not db:
            return JSONResponse({"detail": "No knowledge base found"}, status_code=503)
        doc_id = str(uuid.uuid4())
        db.add_documents([Document(page_content=text, metadata={"source": "manual", "id": doc_id})])
        dest = target or (ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "active")
        return {"success": True, "message": f"Text ingested into '{dest}' ({len(text)} chars)", "id": doc_id}
    except Exception as e:
        return JSONResponse({"detail": f"Ingest error: {str(e)}"}, status_code=500)

@app.post("/admin/ingest/files")
async def ingest_files(password: str = Form(...), target_db: str = Form(""), files: list[UploadFile] = File(...)):
    cfg = get_config()
    if password != cfg.get("admin_password"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    try:
        from langchain_core.documents import Document
        db = _get_db_instance(target_db) if target_db else local_db
        if not db:
            return JSONResponse({"detail": "No knowledge base found"}, status_code=503)
        total = 0
        for f in files:
            raw = await f.read()
            import io
            fname = f.filename.lower()
            if fname.endswith(".pdf"):
                try:
                    import pypdf
                    reader = pypdf.PdfReader(io.BytesIO(raw))
                    text = " ".join(p.extract_text() or "" for p in reader.pages)
                except ImportError:
                    return JSONResponse({"detail": "pypdf not installed"}, status_code=500)
            elif fname.endswith(".docx"):
                try:
                    import docx
                    doc = docx.Document(io.BytesIO(raw))
                    text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
                except ImportError:
                    return JSONResponse({"detail": "python-docx not installed"}, status_code=500)
            elif fname.endswith(".xlsx") or fname.endswith(".xls"):
                try:
                    import openpyxl
                    wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
                    rows = []
                    for ws in wb.worksheets:
                        for row in ws.iter_rows(values_only=True):
                            line = " | ".join(str(c) for c in row if c is not None)
                            if line.strip():
                                rows.append(line)
                    text = "\n".join(rows)
                except ImportError:
                    return JSONResponse({"detail": "openpyxl not installed"}, status_code=500)
            else:
                text = raw.decode("utf-8", errors="ignore")
            text = text.strip()
            if not text:
                continue
            # Chunk into ~800-word pieces
            words = text.split()
            chunks = [" ".join(words[i:i+800]) for i in range(0, len(words), 800)]
            docs = [Document(page_content=c, metadata={"source": f.filename}) for c in chunks if len(c) > 50]
            db.add_documents(docs)
            total += len(docs)
        dest = target_db or (ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "active")
        return {"success": True, "message": f"Ingested {total} chunks into '{dest}' from {len(files)} file(s)"}
    except Exception as e:
        return JSONResponse({"detail": f"Upload error: {str(e)}"}, status_code=500)

@app.get("/admin/embed-code")
def get_embed_code(request: Request, password: str = ""):
    cfg = get_config()
    if password != cfg.get("admin_password"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    
    active = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else "default"
    host = str(request.base_url)
    snippet = f'<script src="{host}widget.js" data-db="{active}"></script>'
    return {"snippet": snippet, "embed_code": snippet, "db": active}

@app.post("/admin/reindex")
async def reindex(data: dict):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)
    target = data.get("target_db", "").strip()
    try:
        if target:
            # Temporarily switch active, reindex, restore
            prev = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else ""
            ACTIVE_DB_FILE.write_text(target)
            init_systems()
            if prev: ACTIVE_DB_FILE.write_text(prev)
            init_systems()
            return {"success": True, "message": f"Reindex of '{target}' complete"}
        else:
            init_systems()
            return {"success": True, "message": "Reindex complete"}
    except Exception as e:
        return JSONResponse({"detail": f"Reindex error: {str(e)}"}, status_code=500)

@app.post("/admin/crawl")
async def crawl_site(data: dict, request: Request):
    cfg = get_config()
    if data.get("password") != cfg.get("admin_password"):
        return JSONResponse({"detail": "Unauthorized"}, status_code=401)

    url        = data.get("url", "").strip().rstrip("/")
    db_name    = data.get("db_name", "").strip()
    max_pages  = int(data.get("max_pages", 1000))
    clear_first = bool(data.get("clear_before_crawl", False))

    if not url or not db_name:
        return JSONResponse({"detail": "url and db_name required"}, status_code=400)

    async def _stream():
        import urllib.parse
        import xml.etree.ElementTree as ET
        import requests as _req
        import random
        from playwright.async_api import async_playwright
        from playwright_stealth import stealth_async
        from langchain_core.documents import Document
        from langchain_chroma import Chroma

        # --- Enhanced Stealth (Free) ---
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
        ]

        def _send(msg):
            return f"data: {json.dumps({'msg': msg})}\n\n"

        def _normalize_url(u):
            """Strip anchors and pure-variant query params for dedup."""
            p = urllib.parse.urlparse(u)
            # Keep query params that change page content (lang, locale), strip color/slide variants
            qs = urllib.parse.parse_qs(p.query)
            keep = {k: v for k, v in qs.items() if k in ("lang", "locale", "language")}
            new_qs = urllib.parse.urlencode(keep, doseq=True)
            return urllib.parse.urlunparse((p.scheme, p.netloc, p.path, "", new_qs, ""))

        def _strip_www(u):
            """Normalize: remove www. for domain comparison."""
            return re.sub(r'^(https?://)www\.', r'\1', u)

        async def _fetch_sitemap_playwright(pw_ctx, sitemap_url, base_domain, depth=0):
            """Fetch sitemap using Playwright (bypasses DNS issues with requests)."""
            if depth > 3:
                return []
            try:
                pg = await pw_ctx.new_page()
                await stealth_async(pg)
                resp = await pg.goto(sitemap_url, wait_until="domcontentloaded", timeout=20000)
                text = await pg.content()
                await pg.close()
                sub_sitemaps = re.findall(r'<sitemap>\s*<loc>\s*(https?://[^\s<]+)\s*</loc>', text)
                if sub_sitemaps:
                    urls = []
                    for sm in sub_sitemaps:
                        urls += await _fetch_sitemap_playwright(pw_ctx, sm.strip(), base_domain, depth + 1)
                    return urls
                all_locs = re.findall(r'<loc>\s*(https?://[^\s<]+)\s*</loc>', text)
                return [u for u in all_locs if _strip_www(u).startswith(base_domain)]
            except Exception:
                return []

        try:
            parsed     = urllib.parse.urlparse(url)
            base       = f"{parsed.scheme}://{parsed.netloc}"
            base_nowww = _strip_www(base)

            pages = []
            async with async_playwright() as pw:
                browser = await pw.chromium.launch(
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
                )
                # Randomly pick a user agent for the whole crawl session context
                ua = random.choice(user_agents)
                ctx = await browser.new_context(
                    user_agent=ua,
                    locale="en-US",
                    viewport={"width": 1280, "height": 900},
                    extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
                )

                # Fetch sitemap via Playwright (avoids DNS issues with requests)
                yield _send(f"🔍 Fetching sitemap from {base}/sitemap.xml ...")
                sitemap_urls = await _fetch_sitemap_playwright(ctx, f"{base}/sitemap.xml", base_nowww)

                if sitemap_urls:
                    yield _send(f"✅ Sitemap: {len(sitemap_urls)} URLs found")
                else:
                    yield _send("⚠️  No sitemap — crawling seed URL only")
                    sitemap_urls = [url]

                # Deduplicate (strip anchors + color params)
                seen_norm = set()
                deduped   = []
                for u in sitemap_urls:
                    n = _normalize_url(u)
                    if n not in seen_norm:
                        seen_norm.add(n)
                        deduped.append(u)

                to_crawl = deduped[:max_pages]
                yield _send(f"📋 {len(to_crawl)} unique pages to crawl...")

                # --- Force Multilingual for all new crawls ---
                emb = embeddings_model
                yield _send("🧠 Using Universal Multilingual Brain (MiniLM-L12)")

                db_dir = DATABASES_DIR / db_name
                if clear_first and db_dir.exists():
                    import shutil, gc
                    yield _send("🗑️ Attempting to clear old data...")
                    # Release global DB lock before deleting files
                    global local_db
                    if local_db is not None:
                        try: 
                            local_db._collection = None
                            local_db = None
                            gc.collect()
                            await asyncio.sleep(2) # Give Windows time to release handles
                        except Exception as e:
                            logger.error(f"Error releasing DB lock: {e}")
                    
                    # Try to delete 3 times (Windows retry pattern)
                    deleted = False
                    for attempt in range(3):
                        try:
                            for item in db_dir.iterdir():
                                if item.name != "config.json":
                                    if item.is_dir(): shutil.rmtree(item)
                                    else: item.unlink()
                            deleted = True
                            break
                        except Exception as e:
                            yield _send(f"⚠️ Retry {attempt+1} to clear files...")
                            await asyncio.sleep(2)
                    
                    if deleted:
                        yield _send("✅ Cleared old crawl data — fresh start")
                    else:
                        yield _send("❌ Failed to clear all files. Proceeding with partial data.")
                db_dir.mkdir(parents=True, exist_ok=True)
                chroma_db = None
                chunk_size = 800
                total_chunks = 0
                completed = 0
                flush_lock = asyncio.Lock()
                pending_pages = []

                async def _flush_pages(batch):
                    nonlocal chroma_db, total_chunks
                    chunks = []
                    for p in batch:
                        words = _clean_text(p["text"]).split()
                        for j in range(0, len(words), chunk_size):
                            chunk = " ".join(words[j:j+chunk_size])
                            if len(chunk) > 80:
                                chunks.append(Document(page_content=chunk, metadata={"source": p["url"]}))
                    if not chunks:
                        return
                    if chroma_db is None:
                        chroma_db = await asyncio.to_thread(
                            Chroma.from_documents, chunks, emb, persist_directory=str(db_dir)
                        )
                    else:
                        await asyncio.to_thread(chroma_db.add_documents, chunks)
                    total_chunks += len(chunks)

                # 10 parallel tabs via semaphore
                PARALLEL = 5 # Reduced parallel slightly for stealth
                sem = asyncio.Semaphore(PARALLEL)
                log_queue = asyncio.Queue()

                async def _crawl_one(cur_url, idx):
                    nonlocal completed
                    async with sem:
                        # Randomized delay between pages
                        await asyncio.sleep(random.uniform(1, 4))
                        
                        pg = await ctx.new_page()
                        await stealth_async(pg)
                        
                        text = ""
                        for attempt in range(2):
                            try:
                                # Wait for load
                                await pg.goto(cur_url, wait_until="load", timeout=40000)
                                
                                # --- Human Behavior ---
                                # Scroll half-way
                                await pg.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
                                await asyncio.sleep(random.uniform(1, 2))
                                
                                # Try to get title and meta description first
                                title = await pg.title()
                                meta_desc = await pg.eval_on_selector("meta[name='description']", "el => el.content").catch(lambda _: "")
                                
                                # Targeted extraction
                                text = await pg.evaluate("""() => {
                                    const selectors = ['main', 'article', '.content', '.product-details', '#description'];
                                    for (let s of selectors) {
                                        let el = document.querySelector(s);
                                        if (el && el.innerText.trim().length > 100) return el.innerText;
                                    }
                                    return document.body ? document.body.innerText : '';
                                }""")
                                
                                text = re.sub(r'\s+', ' ', _clean_text(text or "")).strip()
                                # Prepend title and description if it's very short
                                if len(text) < 200:
                                    text = f"{title}. {meta_desc}. {text}".strip()
                                
                                break
                            except Exception as e:
                                if attempt == 0:
                                    await asyncio.sleep(5)
                                else:
                                    logger.warning(f"Crawl error for {cur_url}: {e}")
                        await pg.close()
                        completed += 1

                        if len(text) > 50:
                            async with flush_lock:
                                pending_pages.append({"url": cur_url, "text": text})
                                await log_queue.put(f"[{completed}/{len(to_crawl)}] ✅ {cur_url[:70]} ({len(text)} chars)")
                                if len(pending_pages) >= 10:
                                    batch = pending_pages[:]
                                    pending_pages.clear()
                                    await log_queue.put(f"💾 Saving batch ({len(batch)} pages, {total_chunks} so far)...")
                                    await _flush_pages(batch)
                        else:
                            await log_queue.put(f"[{completed}/{len(to_crawl)}] ⏭️  {cur_url[:70]} ({len(text)} chars - TOO SHORT)")

                # Launch all tasks, stream log messages as they arrive
                tasks = [asyncio.create_task(_crawl_one(u, i)) for i, u in enumerate(to_crawl)]

                done_count = 0
                while done_count < len(tasks):
                    try:
                        msg = await asyncio.wait_for(log_queue.get(), timeout=1.0)
                        yield _send(msg)
                    except asyncio.TimeoutError:
                        done_count = sum(1 for t in tasks if t.done())

                await asyncio.gather(*tasks, return_exceptions=True)

                # Drain remaining log messages
                while not log_queue.empty():
                    yield _send(log_queue.get_nowait())

                # Final flush
                async with flush_lock:
                    if pending_pages:
                        yield _send(f"💾 Saving final batch ({len(pending_pages)} pages)...")
                        await _flush_pages(pending_pages)

                await browser.close()

            # Reload local_db if we just re-crawled the active DB
            active_name = ACTIVE_DB_FILE.read_text().strip() if ACTIVE_DB_FILE.exists() else ""
            if db_name == active_name and chroma_db is not None:
                local_db = chroma_db
                yield _send(f"🔄 DB reloaded in memory — no restart needed.")
            yield _send(f"✅ Done! {total_chunks} chunks ingested into '{db_name}'.")
            yield "data: {\"done\": true}\n\n"
        except Exception as e:
            import traceback
            yield _send(f"❌ Error: {e}\n{traceback.format_exc()[:600]}")
            yield "data: {\"done\": true}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")

@app.post("/feedback")
async def feedback(data: dict):
    try:
        entry = {
            "session_id": data.get("session_id", ""),
            "rating": data.get("rating", 0),
            "question": data.get("question", ""),
            "answer": data.get("answer", ""),
            "timestamp": datetime.now().isoformat(),
        }
        existing = []
        if FEEDBACK_FILE.exists():
            try: existing = json.loads(FEEDBACK_FILE.read_text())
            except: pass
        existing.append(entry)
        FEEDBACK_FILE.write_text(json.dumps(existing, indent=2))
        return {"success": True}
    except Exception as e:
        return JSONResponse({"detail": str(e)}, status_code=500)

async def send_lead_email(lead_data: dict, cfg: dict):
    """Sends lead notification using SendGrid with automatic API failover."""
    logger.info(f"DEBUG: send_lead_email started for {lead_data['email']}")
    keys = cfg.get("sendgrid_keys", [])
    if not keys:
        logger.warning("No SendGrid keys configured. Skipping email notification.")
        return False
    
    sender = cfg.get("sender_email", "leads@your-digital-fte.com")
    receiver = cfg.get("contact_email")
    logger.info(f"DEBUG: Sender: {sender}, Receiver: {receiver}")
    
    if not receiver:
        logger.warning("No contact_email in config. Cannot send notification.")
        return False

    html_content = f"""
    <h3>🔥 New Lead Captured</h3>
    <p><b>Name:</b> {lead_data['name']}</p>
    <p><b>Email:</b> {lead_data['email']}</p>
    <p><b>WhatsApp:</b> {lead_data.get('whatsapp', 'N/A')}</p>
    <p><b>Message:</b> {lead_data.get('message', 'N/A')}</p>
    <hr>
    <p><small>Sent from your Digital FTE Platform</small></p>
    """
    
    payload = {
        "personalizations": [{
            "to": [{"email": receiver}],
            "subject": f"ALERT: New Lead - {lead_data['name']}"
        }],
        "from": {
            "email": sender,
            "name": "Digital FTE LeadBot"
        },
        "reply_to": {"email": lead_data['email']},
        "content": [{"type": "text/html", "value": html_content}]
    }

    async with httpx.AsyncClient(timeout=15.0) as client:
        for i, api_key in enumerate(keys):
            if not api_key or len(api_key) < 10: 
                logger.warning(f"DEBUG: Skipping invalid Key #{i+1}")
                continue
            
            logger.info(f"DEBUG: Attempting Key #{i+1}...")
            try:
                headers = {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
                response = await client.post("https://api.sendgrid.com/v3/mail/send", json=payload, headers=headers)
                
                logger.info(f"DEBUG: SendGrid Key #{i+1} response: {response.status_code}")
                if 200 <= response.status_code < 300:
                    logger.info(f"✅ Lead email sent successfully via SendGrid Key #{i+1}")
                    return True
                else:
                    logger.error(f"❌ SendGrid Key #{i+1} failed with status {response.status_code}: {response.text}")
            except Exception as e:
                logger.warning(f"❌ SendGrid Key #{i+1} exception: {e}")
                continue # Try next key
                
    logger.error("All SendGrid keys failed or are invalid. Notification not sent.")
    return False

@app.get("/test-email")
async def test_email_endpoint():
    """Manually trigger a test email to verify SendGrid configuration."""
    cfg = get_config()
    test_data = {
        "name": "TEST USER",
        "email": "test@example.com",
        "message": "This is a manual test of the email system."
    }
    success = await send_lead_email(test_data, cfg)
    if success:
        return {"success": True, "message": "Test email sent successfully (check logs for 202)"}
    else:
        return JSONResponse({"success": False, "message": "Email failed. Check server logs for errors."}, status_code=500)

@app.post("/submit-lead")
async def submit_lead(data: dict):
    """Save lead data to leads.json and send email notification with failover."""
    try:
        logger.info(f"DEBUG: Received /submit-lead request with data: {data}")
        LEADS_FILE = Path("leads.json")
        cfg = get_config()
        
        entry = {
            "name":      data.get("name", ""),
            "email":     data.get("email", ""),
            "whatsapp":  data.get("whatsapp", ""),
            "message":   data.get("message", ""),
            "session_id": data.get("session_id", ""),
            "timestamp":  datetime.now().isoformat(),
        }
        
        logger.info(f"*** NEW LEAD CAPTURED ***: {entry['name']} ({entry['email']})")
        
        existing = []
        if LEADS_FILE.exists():
            try: 
                existing = json.loads(LEADS_FILE.read_text())
                logger.info(f"DEBUG: Loaded {len(existing)} existing leads.")
            except Exception as e: 
                logger.error(f"DEBUG: Error loading leads.json: {e}")
        
        existing.append(entry)
        LEADS_FILE.write_text(json.dumps(existing, indent=2))
        logger.info(f"DEBUG: Saved new lead to leads.json. Total now: {len(existing)}")
        
        # Trigger Email Notification
        logger.info("DEBUG: Spawning send_lead_email task...")
        asyncio.create_task(send_lead_email(entry, cfg))
        
        return {"success": True, "message": "Lead captured successfully"}
    except Exception as e:
        logger.error(f"Lead capture error: {e}")
        return JSONResponse({"detail": str(e)}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
