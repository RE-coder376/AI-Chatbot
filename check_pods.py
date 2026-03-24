import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
import chromadb
from pathlib import Path

db_path = Path('databases/agentfactory')
client = chromadb.PersistentClient(path=str(db_path))
col = client.get_collection('langchain')

print(f"Total chunks: {col.count()}")

# Exact keyword search
results = col.get(where_document={"$contains": "Pods"}, limit=10, include=['documents','metadatas'])
print(f"\nChunks containing 'Pods': {len(results['ids'])}")
for i, (doc, meta) in enumerate(zip(results['documents'], results['metadatas'])):
    print(f"\n--- chunk {i+1} ---")
    print(f"source: {meta.get('source','?')[:80]}")
    print(doc[:400])
    print()

# Also try lowercase
results2 = col.get(where_document={"$contains": "atomic unit"}, limit=5, include=['documents','metadatas'])
print(f"\nChunks containing 'atomic unit': {len(results2['ids'])}")
for i, (doc, meta) in enumerate(zip(results2['documents'], results2['metadatas'])):
    print(f"\n--- chunk {i+1} ---")
    print(f"source: {meta.get('source','?')[:80]}")
    print(doc[:400])
