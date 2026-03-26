"""
Re-embed all local ChromaDB databases using fastembed native API (parallel).
Run from AI_Chatbot directory: python reembed_dbs.py
"""
import chromadb
from pathlib import Path
from fastembed import TextEmbedding

DATABASES_DIR = Path("databases")
MODEL_NAME    = "BAAI/bge-small-en-v1.5"
BATCH_SIZE    = 512

print(f"Loading fastembed model: {MODEL_NAME}")
model = TextEmbedding(model_name=MODEL_NAME)
print("Model ready.\n")

dbs = [d for d in DATABASES_DIR.iterdir() if d.is_dir()]
print(f"Found {len(dbs)} DB(s): {[d.name for d in dbs]}\n")

for db_path in dbs:
    print(f"── {db_path.name} ──")
    client = chromadb.PersistentClient(path=str(db_path))
    collections = client.list_collections()

    if not collections:
        print("  No collections — skipping\n")
        continue

    for col_meta in collections:
        col_name = col_meta.name
        col      = client.get_collection(col_name)
        total    = col.count()

        if total == 0:
            print(f"  {col_name}: empty — skipping")
            continue

        print(f"  {col_name}: {total} docs — extracting...", flush=True)
        result    = col.get(include=["documents", "metadatas"])
        docs      = result["documents"]
        metadatas = result["metadatas"]
        ids       = result["ids"]

        print(f"  Embedding {len(docs)} docs in parallel...", flush=True)
        # parallel=0 uses all CPU cores
        vectors = list(model.embed(docs, batch_size=BATCH_SIZE, parallel=0))

        client.delete_collection(col_name)
        new_col = client.create_collection(col_name)

        for i in range(0, len(docs), BATCH_SIZE):
            new_col.add(
                documents  = docs[i : i + BATCH_SIZE],
                metadatas  = metadatas[i : i + BATCH_SIZE],
                ids        = ids[i : i + BATCH_SIZE],
                embeddings = [v.tolist() for v in vectors[i : i + BATCH_SIZE]],
            )
            print(f"    saved {min(i + BATCH_SIZE, len(docs))}/{len(docs)}", end="\r", flush=True)

        print(f"  ✅ {col_name}: done                    ")

    print()

print("All done. Run upload_dbs_to_github.py to push to GitHub.")
