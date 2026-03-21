import os
import json
from pathlib import Path
from langchain_chroma import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.documents import Document
import shutil

# --- Configuration ---
DB_NAME = "agentfactory"
PROJECT_ROOT = Path(r"C:\Users\User\Documents\Projects\AI_Chatbot")
DB_PATH = PROJECT_ROOT / "databases" / DB_NAME
CRAWL_DATA_DIR = PROJECT_ROOT / "data_crawl_agentfactory"
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

def full_ingest():
    print(f"🚀 Starting Full Ingestion for {DB_NAME}...")
    
    # 1. Clear existing DB for a fresh start (ensures 384-dim consistency)
    if DB_PATH.exists():
        print(f"🧹 Clearing existing database at {DB_PATH}...")
        shutil.rmtree(DB_PATH)
    DB_PATH.mkdir(parents=True, exist_ok=True)

    # 2. Load Embeddings
    print(f"📦 Loading model: {MODEL_NAME}...")
    embeddings = HuggingFaceEmbeddings(model_name=MODEL_NAME)

    # 3. Load All Text Files
    print(f"📂 Reading files from {CRAWL_DATA_DIR}...")
    txt_files = list(CRAWL_DATA_DIR.glob("*.txt"))
    print(f"Found {len(txt_files)} files.")

    all_docs = []
    splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=100)

    for i, file_path in enumerate(txt_files):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            # Extract source URL if present (usually first line)
            source = "unknown"
            lines = content.split("\n")
            if lines and lines[0].startswith("Source: "):
                source = lines[0].replace("Source: ", "").strip()
                content = "\n".join(lines[1:])
            
            chunks = splitter.split_text(content)
            for chunk in chunks:
                all_docs.append(Document(
                    page_content=chunk,
                    metadata={"source": source, "file": file_path.name}
                ))
            
            if (i + 1) % 50 == 0:
                print(f"Processed {i+1}/{len(txt_files)} files...")
        except Exception as e:
            print(f"❌ Error processing {file_path.name}: {e}")

    print(f"📝 Created {len(all_docs)} chunks. Starting vectorization (this may take a few minutes)...")

    # 4. Create Vector Store in Batches to avoid memory/timeout issues
    batch_size = 500
    db = None
    for i in range(0, len(all_docs), batch_size):
        batch = all_docs[i:i + batch_size]
        if db is None:
            db = Chroma.from_documents(batch, embeddings, persist_directory=str(DB_PATH))
        else:
            db.add_documents(batch)
        print(f"✅ Ingested {i + len(batch)}/{len(all_docs)} chunks...")

    print(f"\n🎉 SUCCESS: {DB_NAME} is now fully loaded with {len(all_docs)} chunks from 900+ pages of documentation.")

if __name__ == "__main__":
    full_ingest()
