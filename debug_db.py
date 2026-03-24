import chromadb
from pathlib import Path
from langchain_chroma import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings

db_name = "agentfactory"
db_path = Path("databases") / db_name
model_name = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

print(f"Checking DB: {db_name}")
embeddings = HuggingFaceEmbeddings(model_name=model_name)
db = Chroma(persist_directory=str(db_path), embedding_function=embeddings)

# Test direct keyword search
results = db.similarity_search("LiveKit", k=2)
print(f"\nSearch for 'LiveKit' found {len(results)} docs:")
for r in results:
    print(f"- {r.page_content[:100]}...")

# Test Urdu script search
results_ur = db.similarity_search("لائیو کٹ", k=2)
print(f"\nSearch for 'لائیو کٹ' found {len(results_ur)} docs:")
for r in results_ur:
    print(f"- {r.page_content[:100]}...")
