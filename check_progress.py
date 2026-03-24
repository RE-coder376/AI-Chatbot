import chromadb
from pathlib import Path
import os

db_dir = Path("databases/agentfactory")
if db_dir.exists():
    try:
        client = chromadb.PersistentClient(path=str(db_dir))
        # Find the collection name (usually same as db name)
        collections = client.list_collections()
        if collections:
            coll_name = collections[0].name
            count = client.get_collection(coll_name).count()
            print(f"agentfactory: {count} chunks")
        else:
            print("agentfactory: No collections found")
    except Exception as e:
        print(f"Error checking agentfactory: {e}")
else:
    print("agentfactory directory not found")
