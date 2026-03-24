"""
Bulk ingest top N anime from Jikan API into the chatbot DB.
Batches 25 docs at a time so ChromaDB embeds efficiently.
Default: top 500 anime (20 pages). Change TOTAL_PAGES to get more.
"""
import requests, json, time
from langchain_chroma import Chroma
from langchain_huggingface import HuggingFaceEmbeddings
from langchain_core.documents import Document
from pathlib import Path

TARGET_DB   = "mal"          # change to your DB name
TOTAL_PAGES = 20             # 25 anime/page → 500 anime total
DB_PATH     = f"databases/{TARGET_DB}/chroma"
MODEL_NAME  = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

print(f"Loading embedding model...")
embeddings = HuggingFaceEmbeddings(model_name=MODEL_NAME)
db = Chroma(persist_directory=DB_PATH, embedding_function=embeddings)
print(f"Connected to '{TARGET_DB}' DB.")

total = 0
for page in range(1, TOTAL_PAGES + 1):
    print(f"\nPage {page}/{TOTAL_PAGES}...", end=" ")
    try:
        res = requests.get(
            f"https://api.jikan.moe/v4/top/anime?page={page}&limit=25",
            timeout=15
        )
        anime_list = res.json().get("data", [])
        if not anime_list:
            print("empty, stopping.")
            break

        # Build all 25 docs then batch-embed in one shot
        docs = []
        for a in anime_list:
            text = "\n".join(filter(None, [
                f"title: {a.get('title','')}",
                f"english_title: {a.get('title_english','')}",
                f"score: {a.get('score','')}",
                f"rank: {a.get('rank','')}",
                f"episodes: {a.get('episodes','')}",
                f"status: {a.get('status','')}",
                f"year: {a.get('year','')}",
                f"genres: {', '.join(g['name'] for g in a.get('genres',[]))}",
                f"studios: {', '.join(s['name'] for s in a.get('studios',[]))}",
                f"synopsis: {(a.get('synopsis') or '')[:600]}",
            ]))
            docs.append(Document(page_content=text, metadata={"source": "jikan", "mal_id": str(a.get("mal_id",""))}))

        db.add_documents(docs)   # one batch embed = fast
        total += len(docs)
        print(f"added {len(docs)} | total so far: {total}")

    except Exception as e:
        print(f"ERROR: {e}")

    time.sleep(1.2)   # Jikan rate limit: stay under 60 req/min

print(f"\nDone! {total} anime ingested into '{TARGET_DB}'.")
