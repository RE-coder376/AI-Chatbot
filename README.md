# AI Chatbot — Multi-Client RAG Platform

A production-grade RAG (Retrieval-Augmented Generation) chatbot platform. Each client gets their own knowledge base, branding, and configuration. The bot answers questions from the client's data — accurately, in real time, with full conversation memory.

---

## What It Does

- Answers customer questions using the client's own knowledge base (PDFs, docs, website content)
- Remembers users across sessions — returning customers pick up where they left off
- Streams answers in real time (character by character, like ChatGPT)
- Suggests follow-up questions after each answer
- Captures leads automatically when a customer asks about pricing, hiring, or contact
- Embeds on any website with one line of code
- Admin panel to manage branding, knowledge base, and analytics — no code needed

---

## Tech Stack

| Layer | Technology |
|---|---|
| Backend | FastAPI (async) |
| LLM | Groq API — `llama-3.3-70b-versatile` |
| Vector DB | ChromaDB (local, persistent) |
| Embeddings | HuggingFace BGE-Base (`BAAI/bge-base-en-v1.5`, 768-dim) |
| Full-text search | BM25 (`rank_bm25`) |
| Reranking | CrossEncoder (`cross-encoder/ms-marco-MiniLM-L-6-v2`) |
| Memory | SQLite (`memory.db`) |
| Frontend | Vanilla JS + SSE streaming |

---

## Project Structure

```
AI_Chatbot/
├── app.py              — Main FastAPI server (all endpoints)
├── ingest.py           — Ingest PDFs/TXTs into ChromaDB
├── sync.py             — Auto KB sync from sitemap (nightly)
├── chat.html           — Full-page chat UI
├── widget.js           — Embeddable floating widget script
├── widget_chat.html    — Chat UI optimised for iframe embed
├── admin.html          — Admin dashboard
├── config.json         — Root config (base defaults)
├── active_db.txt       — Which DB is currently active
├── memory.db           — SQLite persistent conversation memory
├── feedback.json       — Thumbs up/down ratings store
├── analytics.json      — Interaction stats
├── keys.json           — Groq API key vault (multi-key rotation)
├── sync_config.json    — Sitemap URL + sync settings (create this)
├── data/               — Source PDFs/TXTs for ingestion
└── databases/
    └── <client_name>/
        ├── config.json — Per-client config overrides
        └── chroma.db/  — Vector store for this client
```

---

## How to Run

```bash
cd C:/Users/User/Documents/Projects/AI_Chatbot

# Install dependencies
pip install fastapi uvicorn python-dotenv langchain-groq langchain-chroma \
    langchain-community chromadb sentence-transformers rank-bm25 requests

# Set your Groq API key
# Either in .env:  GROQ_API_KEY=your_key
# Or add via admin panel after starting

# Start the server
python app.py
```

Access points:
- **Chat UI:** `http://localhost:8000`
- **Admin panel:** `http://localhost:8000/admin`
- **Health check:** `http://localhost:8000/health`

---

## Ingesting Knowledge

**Option 1 — From files (terminal):**
```bash
# Put PDFs or TXTs in the data/ folder, then:
python ingest.py
```

**Option 2 — From admin panel (no terminal):**
- Open admin → Knowledge Ingestion section
- Paste text directly, or upload a PDF/TXT file
- Bot knows it immediately — no restart needed

**Option 3 — Auto-sync from website (nightly):**
```bash
# Create sync_config.json first:
# { "sitemap_url": "https://yoursite.com/sitemap.xml", "target_db": "clientname" }

python sync.py           # run once
python sync.py --watch   # run every 24h automatically
```

---

## Embedding on a Client Website

After logging into the admin panel, open the **"Embed on Your Website"** card. Copy the snippet shown and send it to the client. They paste it before `</body>` on any page:

```html
<script src="https://yourserver.com/widget.js" data-widget-key="CLIENT_KEY"></script>
```

That's it. A floating chat button appears on their site. Customers click it to chat — no page reload, no redirects.

**Widget customisation attributes:**
```html
data-color="#22c55e"     <!-- button color, default purple -->
data-position="left"     <!-- "left" or "right" (default) -->
```

---

## Multi-Client Setup

Each client gets their own isolated knowledge base and configuration.

```bash
# 1. Create a new database folder
mkdir databases/newclient

# 2. Create their config (overrides root config.json)
# databases/newclient/config.json:
{
  "bot_name": "Aria",
  "business_name": "NewClient Corp",
  "topics": "HR policies, employee handbook",
  "admin_password": "their_password",
  "widget_key": "auto-generated-uuid"
}

# 3. Switch active DB from admin panel, or:
echo "newclient" > active_db.txt

# 4. Ingest their data
python ingest.py
```

Config merge rule: root `config.json` sets defaults. Per-client `databases/<name>/config.json` overrides any non-empty field.

---

## API Endpoints

### Public

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Chat UI |
| GET | `/admin` | Admin dashboard |
| GET | `/widget.js` | Embeddable widget script |
| GET | `/widget-chat` | Widget iframe chat page |
| GET | `/config` | Public branding + contact config |
| GET | `/health` | System status (DB, BM25, reranker, memory) |
| POST | `/chat` | Main chat endpoint (streaming SSE or JSON) |
| POST | `/feedback` | Submit thumbs up/down rating |

### Admin (password required)

| Method | Endpoint | Description |
|---|---|---|
| GET | `/admin/branding` | Get full config |
| POST | `/admin/branding` | Update branding |
| POST | `/admin/ops` | Update contact settings + hours |
| GET | `/admin/databases` | List all client DBs |
| POST | `/admin/databases/set-active` | Switch active DB |
| POST | `/admin/create-db` | Create a new knowledge repository |
| POST | `/admin/delete-db` | Delete an existing repository |
| GET | `/admin/analytics` | Stats + feedback totals |
| GET | `/admin/embed-code` | Get widget embed snippet |
| POST | `/admin/crawl` | Start a website crawl (streaming SSE) |
| POST | `/admin/ingest/text` | Ingest pasted text into KB |
| POST | `/admin/ingest/files` | Upload PDF/TXT into KB |
| POST | `/admin/healer/trigger` | Run auto-repair pipeline |
| GET | `/admin/test/identity` | Audit bot identity |
| GET | `/admin/test/knowledge` | Audit KB status |
| GET | `/admin/test/safety` | Audit hallucination guard |
| GET | `/admin/test-detailed` | Run all 3 audits |

### Chat Request Format

```json
POST /chat
{
  "question": "What courses do you offer?",
  "history": [
    { "role": "user", "content": "Hi" },
    { "role": "assistant", "content": "Hello! How can I help?" }
  ],
  "session_id": "uuid-from-localstorage",
  "stream": true
}
```

### Chat Response (SSE stream)

```
data: {"type": "chunk", "content": "We offer..."}
data: {"type": "chunk", "content": " three programs"}
data: {"type": "metadata", "capture_lead": false, "low_confidence": false}
data: {"type": "suggestions", "items": ["What are prerequisites?", "How long is it?", "Is there a certificate?"]}
```

---

## Features — Full List

### Intelligence
- **Hybrid retrieval** — BM25 full-text + vector semantic search combined
- **CrossEncoder reranking** — top candidates rescored for precision
- **Website Crawler** — Built-in crawler accessible via Admin UI with real-time logs
- **Dual embedding engine** — 768-dim BGE primary, 384-dim MiniLM fallback (handles legacy DBs)
- **Query rewriting** — vague follow-ups rewritten into full questions before searching
- **6-turn conversation context** — bot remembers last 3 exchanges
- **Runtime confidence detection** — if bot doesn't know, lead card triggers automatically
- **LLM:** `llama-3.3-70b-versatile` via Groq (fast, high quality)

### Memory
- **Persistent sessions** — SQLite stores full conversation per user session ID
- **Cross-reload memory** — user closes tab, comes back later, history is there
- **New Chat button** — generates fresh session ID to start clean

### UX
- **Streaming** — answers appear word by word
- **Typing indicator** — animated dots while bot is thinking
- **Follow-up suggestions** — 3 clickable question chips after each answer
- **Thumbs up / down** — rate every answer, stored for analytics
- **Lead capture card** — email + WhatsApp buttons, triggered by intent or low confidence
- **Markdown rendering** — bot can reply with formatted lists, bold, code blocks

### Admin Panel
- Branding control (colors, emoji, name, welcome message)
- Live HTML preview of branding changes
- Knowledge base management (create, delete, switch, ingest)
- Website Crawler (UI-based streaming ingestion)
- Analytics (total interactions, top 5 questions, feedback scores)
- Embed snippet with one-click copy
- Behavioral audits (identity, knowledge, safety)
- Auto-healer (re-syncs KB, injects safety mandate)

### Operations
- **Standardized Auth** — Unified `admin_auth` helper for all secure endpoints
- **Rate limiting** — 20 req/min per IP on `/chat`, 20 req/min on `/admin`
- **Error logging** — `CRITICAL_ERRORS.txt` captures stack traces

---

## Environment Variables

```env
GROQ_API_KEY=your_groq_api_key
```

Or manage keys via admin panel → `keys.json` vault.

---

## Killing the Server

```bash
netstat -ano | grep ":8000"
taskkill /F /PID <pid>
```
