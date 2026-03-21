"""
Reindex agentfactory DB: all-MiniLM-L6-v2 → paraphrase-multilingual-MiniLM-L12-v2
Run ONLY while server is stopped. Two-phase: extract then rebuild.
"""
import json, gc, shutil, sys, time, subprocess
from pathlib import Path

DB_PATH   = Path("databases/agentfactory")
DB_CFG    = DB_PATH / "config.json"
BACKUP    = Path("databases/agentfactory_minilm_backup")
CHUNKS_F  = Path("chunks_temp.json")
NEW_DB    = Path("databases/agentfactory_new")

# ─── PHASE 1: Extract chunks (runs in subprocess so file handles release) ───
PHASE1 = """
import json, sys, warnings
warnings.filterwarnings('ignore')
from pathlib import Path
try:
    from langchain_huggingface import HuggingFaceEmbeddings
except ImportError:
    from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_chroma import Chroma

DB_PATH = Path("databases/agentfactory")
print("Loading old model...", flush=True)
m = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2",
    model_kwargs={"device":"cpu"}, encode_kwargs={"normalize_embeddings":True})
db = Chroma(persist_directory=str(DB_PATH), embedding_function=m)
r = db._collection.get(include=["documents","metadatas"])
out = {"texts": r["documents"], "metas": r["metadatas"]}
Path("chunks_temp.json").write_text(json.dumps(out))
print(f"Extracted {len(out['texts'])} chunks", flush=True)
"""

# ─── PHASE 2: Rebuild with new model ────────────────────────────────────────
PHASE2 = """
import json, shutil, sys, time, warnings
warnings.filterwarnings('ignore')
from pathlib import Path
try:
    from langchain_huggingface import HuggingFaceEmbeddings
except ImportError:
    from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_chroma import Chroma

CHUNKS_F = Path("chunks_temp.json")
DB_PATH  = Path("databases/agentfactory")
BACKUP   = Path("databases/agentfactory_minilm_backup")
NEW_DB   = Path("databases/agentfactory_new")

data  = json.loads(CHUNKS_F.read_text())
texts = data["texts"]
metas = data["metas"]
print(f"Loaded {len(texts)} chunks from temp file", flush=True)

print("Loading multilingual model...", flush=True)
model = HuggingFaceEmbeddings(
    model_name="sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
    model_kwargs={"device":"cpu"}, encode_kwargs={"normalize_embeddings":True})

# Build new DB at temp path first
if NEW_DB.exists():
    shutil.rmtree(str(NEW_DB))
NEW_DB.mkdir(parents=True, exist_ok=True)
print(f"Re-embedding {len(texts)} chunks...", flush=True)
new_db = Chroma(persist_directory=str(NEW_DB), embedding_function=model)
t0 = __import__('time').time()
BATCH = 200
for i in range(0, len(texts), BATCH):
    bt = texts[i:i+BATCH]
    bm = [m or {} for m in metas[i:i+BATCH]]
    new_db.add_texts(bt, metadatas=bm)
    done = min(i+BATCH, len(texts))
    elapsed = __import__('time').time() - t0
    rate = done/elapsed if elapsed else 1
    eta  = (len(texts)-done)/rate
    print(f"  {done}/{len(texts)}  ({elapsed:.0f}s elapsed, ETA ~{eta:.0f}s)", flush=True)

print("Done embedding. Swapping DB directories...", flush=True)
# Backup old, move new into place
if BACKUP.exists():
    shutil.rmtree(str(BACKUP))
shutil.copytree(str(DB_PATH), str(BACKUP))
shutil.rmtree(str(DB_PATH))
shutil.copytree(str(NEW_DB), str(DB_PATH))
shutil.rmtree(str(NEW_DB))

# Update config
cfg = {}
DB_CFG = DB_PATH / "config.json"
if DB_CFG.exists():
    try: cfg = json.loads(DB_CFG.read_text())
    except: pass
cfg["embedding_model"] = "multilingual"
DB_CFG.write_text(json.dumps(cfg, indent=2))

CHUNKS_F.unlink(missing_ok=True)
total = __import__('time').time() - t0
print(f"\\n✅ Done! {len(texts)} chunks re-embedded in {total:.0f}s", flush=True)
print(f"   Config: embedding_model = 'multilingual'", flush=True)
print(f"   Backup at: {BACKUP}", flush=True)
print(f"   Restart the server now.", flush=True)
"""

print("=" * 55)
print("  REINDEX: all-MiniLM-L6-v2 -> multilingual-MiniLM-L12")
print("=" * 55)

# Phase 1 — extract in subprocess (releases file locks on exit)
print("\nPhase 1/2 — Extracting chunks (subprocess)...")
r1 = subprocess.run([sys.executable, "-W", "ignore", "-c", PHASE1], capture_output=False)
if r1.returncode != 0:
    print("Phase 1 FAILED. Check output above.")
    sys.exit(1)
if not CHUNKS_F.exists():
    print("chunks_temp.json not created. Aborting.")
    sys.exit(1)

time.sleep(2)  # let Windows fully release file handles

# Phase 2 — rebuild
print("\nPhase 2/2 — Rebuilding with multilingual model...")
r2 = subprocess.run([sys.executable, "-W", "ignore", "-c", PHASE2], capture_output=False)
if r2.returncode != 0:
    print("Phase 2 FAILED. Old DB backup is at:", BACKUP)
    sys.exit(1)
