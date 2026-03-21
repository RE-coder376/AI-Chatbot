import chromadb
import uuid
from pathlib import Path
from langchain_chroma import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_core.documents import Document

# --- Configuration ---
DATABASES_DIR = Path("databases")
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

print(f"Loading embedding model: {MODEL_NAME}...")
embeddings = HuggingFaceEmbeddings(
    model_name=MODEL_NAME,
    model_kwargs={"device": "cpu"},
    encode_kwargs={"normalize_embeddings": True}
)

data = {
    "agentfactory": [
        "AgentFactory is a platform for building and deploying 'Digital FTEs' (AI Employees). It allows businesses to automate workflows using advanced AI agents.",
        "AgentFactory offers seamless integration with LiveKit and Pipecat for voice-enabled AI interactions.",
        "You can contact AgentFactory support at support@agentfactory.pk or visit their office in Islamabad."
    ],
    "tsc_pk": [
        "TSC.pk (Studio by TCS) is a premium fashion platform offering designer wear from Pakistan's top brands.",
        "We sell a wide variety of items including calculators, office stationery, and designer clothing.",
        "Contact TSC.pk customer service at support@tsc.pk for order tracking and inquiries."
    ],
    "estationery": [
        "eStationery.com.pk is Pakistan's largest online stationery store, offering everything from office supplies to school kits.",
        "We offer calculators, pens, notebooks, and art supplies with nationwide delivery.",
        "Email us at info@estationery.com.pk or call our helpline for assistance."
    ],
    "chishti_sabri": [
        "Chishti Sabri Store is a specialized retailer for religious books, perfumes (attar), and Islamic lifestyle products.",
        "We offer a wide range of Sufi literature and traditional Islamic products.",
        "Contact us at chishtisabristore@gmail.com for more information."
    ]
}

def ingest_sample_data():
    for db_name, chunks in data.items():
        db_path = DATABASES_DIR / db_name
        db_path.mkdir(parents=True, exist_ok=True)
        
        print(f"Ingesting {len(chunks)} chunks into '{db_name}'...")
        docs = [Document(page_content=c, metadata={"source": "emergency_ingest", "id": str(uuid.uuid4())}) for c in chunks]
        
        db = Chroma.from_documents(
            documents=docs,
            embedding=embeddings,
            persist_directory=str(db_path)
        )
        print(f"Done for {db_name}.")

if __name__ == "__main__":
    ingest_sample_data()
