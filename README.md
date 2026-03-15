# Digital FTE — Universal Multilingual RAG Platform

A production-hardened **Digital FTE (Full-Time Equivalent)** platform. Transform any business website or knowledge base into a multilingual AI Employee that speaks **English, Urdu Script (نستعلیق), and Roman Urdu** fluently.

---

## 🚀 The Digital FTE Advantage

- **Multilingual Brain:** Single unified engine for English and Urdu (both scripts).
- **Cross-Lingual RAG:** Answers questions in Urdu even if the source documentation is in English.
- **Production Hardened:** Built-in "Billion Dollar Audit" suite to verify identity, knowledge, and safety.
- **Stealth Crawler:** Advanced crawler with bot-evasion (Stealth Masking, Human Behavior) to ingest data from any public site.
- **Multi-Client Architecture:** Switch between brands (`agentfactory`, `tsc_pk`, etc.) instantly with dynamic branding and knowledge isolation.

---

## 🔥 Key Features

### Intelligence & Retrieval
- **Universal Multilingual Brain:** Powered by `paraphrase-multilingual-MiniLM-L12-v2` (384-dim).
- **Hybrid Search:** Combines Vector Semantic Search with BM25 keyword matching.
- **Keyword Rescue:** Deep-scans documentation for exact technical terms (e.g., "LiveKit", "Pipecat").
- **Language Mirroring:** Automatically detects and responds in the user's exact script and language.

### Enterprise Admin Panel
- **Visual Branding Suite:** Real-time mobile widget preview with primary/secondary color controls.
- **Knowledge Hub:** UI-based crawling, file uploads (PDF/DOCX/XLSX), and manual FAQ management.
- **Behavioral Audit Suite:** Automated testing for Identity, Knowledge Depth, and Hallucination resistance.
- **System Watchdog:** Self-healing logic to repair dimension mismatches and configuration drift.

### Connectivity
- **One-Line Embed:** Copy-paste script snippet for WordPress, Shopify, or custom sites.
- **Lead Capture:** Automated lead generation via Email and WhatsApp buttons.
- **Streaming UI:** Character-by-character real-time response with Markdown support.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| **Backend** | FastAPI (Async) |
| **LLM** | Groq API (`llama-3.3-70b-versatile`) |
| **Vector DB** | ChromaDB (Local Persistent) |
| **Embeddings** | `paraphrase-multilingual-MiniLM-L12-v2` (384-dim) |
| **Crawler** | Playwright + Playwright-Stealth + Human-Logic |
| **Memory** | SQLite (Persistent Sessions) |
| **UI** | Vanilla JS + Tailwind-inspired CSS + SSE Streaming |

---

## 🚀 Quick Start

### 1. Installation
```bash
# Clone and enter directory
pip install -r requirements.txt
# Ensure Playwright browsers are ready
playwright install chromium
```

### 2. Run the Engine
```bash
python app.py
```
- **Admin:** `http://localhost:8000/admin` (Default Pass: `admin123`)
- **Chat:** `http://localhost:8000`

---

## 🛡️ The "Billion Dollar" Audit
The platform includes a specialized audit suite to ensure reliability before high-value deployments:
1. **Identity Audit:** Ensures the bot stays in character.
2. **Knowledge Audit:** Verifies the bot only answers from the provided DB.
3. **Safety Audit:** Confirms the "I don't know" guardrails are active.

---

## 📦 Project Structure
```
AI_Chatbot/
├── app.py              # Universal Production Server
├── admin.html          # Premium Admin Dashboard
├── chat.html           # Professional Chat Interface
├── config.json         # Root Configuration
├── databases/          # Isolated Client Knowledge Bases
│   └── <brand_name>/
│       ├── config.json # Brand Overrides (Branding, Topics)
│       └── chroma.sqlite3
└── STRATEGY.md         # Sales & Growth Roadmap
```

---

## ⚖️ Legal & Ethical Usage
This platform is designed for **Public Data Ingestion**. It follows industry-standard "politeness" rules:
- **Rate Limited Crawling:** Prevents server overload on target sites.
- **Robots.txt Respect:** (Configurable) to follow site owner guidelines.
- **No Private Data:** Designed for public business documentation only.

---
Built for the next generation of **Digital Employees**.
