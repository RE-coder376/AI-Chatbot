import json
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_chroma import Chroma
from langchain_core.documents import Document
import os
from pathlib import Path

# Target DB
DB_NAME = "agentfactory"
DB_PATH = Path("databases") / DB_NAME

# Knowledge Content (Concept-Rich)
content = [
    """AgentFactory Curriculum and Training Modules: 
    1. Introduction to Digital FTEs: Understanding AI Employees and workflow automation.
    2. Agent Orchestration: Learning how to connect multiple AI agents together.
    3. Voice Integration: Using Pipecat and LiveKit for real-time voice bots.
    4. Database RAG: Knowledge retrieval and multilingual support.
    5. Deployment: Hosting and scaling AI agents for enterprise clients.""",
    
    """AgentFactory Roadmap and Course Topics:
    The platform provides a comprehensive learning path including agent prompt engineering, 
    tool calling, retrieval-augmented generation (RAG), and advanced API integrations.
    Students learn to build bots that speak English, Urdu, and Roman Urdu.""",
    
    "AgentFactory is located in Islamabad and offers support via email at support@agentfactory.pk."
]

def ingest():
    print(f"Ingesting concept data into {DB_NAME}...")
    emb = HuggingFaceEmbeddings(model_name="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
    
    docs = [Document(page_content=c, metadata={"source": "manual_curriculum"}) for c in content]
    
    # Create/Append to DB
    db = Chroma.from_documents(docs, emb, persist_directory=str(DB_PATH))
    print("✅ Ingestion Complete.")

if __name__ == "__main__":
    ingest()
