# Digital FTE — Cloud-Native Enterprise RAG SaaS

A production-grade, high-performance RAG chatbot framework engineered for multi-client scalability. This system leverages **Pinecone Cloud** and **Groq LLM clusters** to deliver 100% stable, context-aware responses from massive datasets (14,000+ chunks) with sub-1s latency.

---

## 🚀 Advanced Production Features

### 1. Cloud-Powered "Infinite Brain" (Pinecone)
*   **Zero-RAM Footprint**: Offloads vector searching to Pinecone's serverless infrastructure. No more local memory crashes or hardware bottlenecks.
*   **State-of-the-Art Embeddings**: Uses **NVIDIA's llama-text-embed-v2** (1024-dimensions) for ultra-precise factual retrieval.
*   **Instant Startup**: Server becomes "Ready" in milliseconds, connecting directly to the cloud knowledge base.

### 2. "Infinite" Key Rotation Engine
*   **Multi-Key Vault**: Supports up to 10+ Groq API keys simultaneously.
*   **Automatic 429 Recovery**: If a key hits a rate limit, the bot **instantly rotates** to a fresh key mid-stream. The user never sees an error.
*   **Admin Key Dashboard**: Real-time status monitoring (Active vs. Burned) and one-click key additions via the Admin UI.

### 3. Hardened Factual Retrieval
*   **Zero-Hallucination Policy**: Strict system prompts force the AI to answer ONLY from the retrieved context. 
*   **Dynamic Vocabulary Gatekeeper**: A pre-retrieval triage layer that blocks off-topic queries (e.g., math, general facts, recipes) before they ever hit the LLM, saving tokens and protecting business logic.
*   **Parallel Hybrid Search**: Simultaneously queries cloud vector indices and deterministic metadata for 100% hit rates.

---

## 🛠 Management & Customization

### 1. Unified Admin Panel (`/admin`)
*   **API Key Vault**: Manage your "Infinite" rotation engine directly from the UI.
*   **Visual Customization**: Change Bot Branding, Colors, Fonts, and Logos (Emoji or URL) instantly.
*   **Lead Capture Engine**: Automatically identifies sales intent and triggers contact handoffs (WhatsApp/Email).

### 2. Cloud Deployment (Koyeb/Render)
The system is optimized for one-click deployment on Koyeb or Render using the included `Procfile` and `requirements.txt`.

---

## 📂 Project Architecture

```
AI_Chatbot/
│
├── app.py           — Cloud-Native FastAPI RAG Server
├── watchdog.py      — Self-Healing Process Monitor
├── admin.html       — Enterprise Management Hub
├── chat.html        — Smart-URL Frontend (Auto-detects Local vs. Cloud)
│
├── keys.json        — Encrypted Key Vault (Managed via Admin)
├── .env             — Environment Configuration (GitHub Ignored)
└── requirements.txt — Production Dependencies
```

---

## 🔒 Security & Performance
- **X-Widget-Key Authentication**: Mandatory for all client-side requests.
- **Smart URL Detection**: Frontend automatically switches between local and cloud backends.
- **Silent Logging**: All deprecation warnings and telemetry noise suppressed for 100% clean production logs.
