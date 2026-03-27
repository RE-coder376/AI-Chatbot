---
title: AI Chatbot
emoji: 🤖
colorFrom: indigo
colorTo: purple
sdk: docker
pinned: false
---

# Digital FTE — Universal Multilingual RAG Chatbot Platform

A production-hardened **Digital FTE (Full-Time Equivalent)** platform. Transform any business website or knowledge base into a multilingual AI assistant that speaks **English, Urdu Script (نستعلیق), and Roman Urdu** fluently.

---

## 🚀 The Digital FTE Advantage

- **Multilingual Brain:** Single unified engine for English and Urdu (both scripts).
- **Cross-Lingual RAG:** Answers questions in Urdu even if the source documentation is in English.
- **Production Hardened:** Built-in audit suite to verify identity, knowledge, and safety.
- **Hybrid Stealth Crawler:** Advanced Playwright-based crawler + **Fast-Path HTTP Ingestion** (5-10x faster for Shopify/WordPress/WooCommerce).
- **Multi-Client Architecture:** Switch between brands instantly with dynamic branding and knowledge isolation.
- **Privacy Guard:** Code-level interception of prompt injection, XSS protection, and system prompt reveal attempts.

---

## 🔥 Key Features

### Intelligence & Retrieval
- **Hybrid Search:** Combines Vector Semantic Search with BM25 keyword matching + CrossEncoder reranking.
- **Content-Level Dedup:** MD5-based hashing during crawling to skip duplicate content across product filters/collections.
- **Context Budgeting:** Dynamic per-model context window management (prevents `context_length_exceeded` errors).
- **Keyword Rescue:** Deep-scans documentation for exact technical terms (acronyms, product names).      
- **Language Mirroring:** Detects and responds in the user's exact script and language.
- **Multi-Provider LLM:** Groq → OpenAI → Gemini with automatic key rotation and failover.

### Engagement Features
- **Proactive Idle Trigger:** Bot sends a message after 45s of user inactivity.
- **Human Handoff:** Detects "talk to human" intent → shows handoff card → emails full transcript to team.
- **Lead Capture:** 2-step lead form (Name / Email / Phone / Message) with email notification.
- **Quick Reply Chips:** Suggested follow-up questions after each response.
- **Inline Source Citations:** Compact `[1] page-name` badge links with protocol validation for security.
- **Rich Cards:** LLM can emit structured `[CARD]Title|Desc|URL[/CARD]` cards.
- **CSAT Survey:** 5-star rating widget fires after every 5th message.
- **Persistent History:** Conversation history saved to localStorage, restored on reload.

### Enterprise Admin Panel
- **Fast-Path Ingestion:** Direct HTTP content extraction for 5-10x faster crawling on server-rendered sites.
- **Smart Crawl:** Stage 1 (Inspect & LLM-Rate URL groups) → Stage 2 (Targeted ingestion).
- **Visual Branding Suite:** Real-time mobile widget preview with color controls.
- **Knowledge Hub:** UI-based crawling, data clearing, file uploads, manual text ingestion, and FAQ management.
- **Analytics Dashboard:** Chart.js charts — daily queries, CSAT trend, top knowledge gaps.
- **Knowledge Gap Suggest:** LLM-powered draft answers for unanswered questions, add to KB in one click.
- **SMTP Email Config:** Configurable sender email, password, host, port per client from admin UI.
- **Behavioral Audit Suite:** Automated testing for Identity, Knowledge, and Safety.
- **System Watchdog:** Self-healing logic for dimension mismatches and configuration drift.

### Security
- **XSS Protection:** Strict HTML stripping in the chat UI via `marked.js` renderer overrides.
- **Session-Locked Admin:** Master password stored in `sessionStorage` (auto-clears on tab close) for maximum security.
- **Prompt Injection Guard:** Code-level regex blocks system prompt reveal, jailbreak, and override attempts.
- **Rate Limiting:** 20 req/min per IP on `/chat`, 10 on `/admin/*`.
- **Widget Key Auth:** Per-DB UUID key for embed authentication.
- **Admin Password:** All admin endpoints password-protected with custom confirmation modals.

### Connectivity
- **One-Line Embed:** Copy-paste script snippet for WordPress, Shopify, or custom sites.
- **SSE Streaming:** Character-by-character real-time response with Markdown support.
- **SMTP Notifications:** Lead and handoff emails via Gmail SMTP (or any provider).

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Backend** | FastAPI (Async) + SSE Streaming |
| **LLM** | Groq (`llama-3.3-70b-versatile`) → OpenAI → Gemini (auto-failover) |
| **Vector DB** | ChromaDB (Local Persistent) |
| **Embeddings** | `paraphrase-multilingual-MiniLM-L12-v2` or `BAAI/bge-base-en-v1.5` |
| **Reranking** | CrossEncoder (`ms-marco-MiniLM-L-6-v2`) |
| **Keyword Search** | BM25 (rank_bm25) |
| **Crawler** | Playwright + Playwright-Stealth |
| **Email** | Gmail SMTP (smtplib) |
| **UI** | Vanilla JS + CSS + Chart.js + Marked.js |

---

## 🚀 Quick Start

### 1. Install
```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure
Edit `config.json`:
```json
{
  "admin_password": "your_password",
  "contact_email": "you@gmail.com",
  "sender_email": "you@gmail.com",
  "smtp_password": "your_app_password"
}
```

### 3. Run
```bash
python -u app.py
```
- **Chat:** `http://localhost:8000`
- **Admin:** `http://localhost:8000/admin` (password: from config)

---

## 🛡️ Security & Safety

1. **Prompt Injection:** Code-level regex intercepts before reaching LLM — zero hallucination risk.
2. **Identity Guard:** Bot stays in persona regardless of instructions.
3. **Scope Guard:** Strictly answers only about the configured business — no general knowledge.
4. **IDK Guard:** Never fabricates — logs unknown questions to `knowledge_gaps.json`.

---

## 📦 Project Structure
```
AI_Chatbot/
├── app.py              # Main FastAPI server (~2400 lines)
├── admin.html          # Admin dashboard
├── chat.html           # Chat interface
├── widget_chat.html    # Embeddable widget version
├── config.json         # Root configuration (base defaults)
├── databases/          # Isolated client knowledge bases
│   └── <brand_name>/
│       ├── config.json # Per-client overrides
│       └── chroma.sqlite3
├── knowledge_gaps.json # Unanswered questions log
├── analytics.json      # Query history and stats
├── feedback.json       # Thumbs up/down ratings
├── leads.json          # Captured leads
└── csat_log.json       # CSAT ratings
```

---

## ⚖️ Legal & Ethical Usage
Designed for **public data ingestion only**:
- Rate-limited crawling to prevent server overload.
- Intended for publicly accessible business documentation.
- No private or authenticated data should be ingested.

---

Built for the next generation of **Digital Employees**.
