import os
from langchain_chroma import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings

DB_DIR = r"C:\Users\User\Documents\Projects\AI_Chatbot\databases\agentfactory"

def check_db():
    print(f"Checking database at: {DB_DIR}")
    if not os.path.exists(DB_DIR):
        print("Error: Database directory not found.")
        return

    # Using the same model as in the audit progress
    model_name = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    embeddings = HuggingFaceEmbeddings(model_name=model_name)
    
    db = Chroma(persist_directory=DB_DIR, embedding_function=embeddings)
    
    # Peek at first few documents
    try:
        data = db.get(limit=5)
        print(f"\nTotal documents in DB: {len(db.get()['ids'])}")
        print("\n--- Sample Documents ---")
        for doc in data['documents']:
            print(f"- {doc[:200]}...")
            
        # Search for curriculum related terms
        print("\n--- Searching for 'curriculum/syllabus' related content ---")
        results = db.similarity_search("curriculum syllabus topics course content", k=5)
        for i, res in enumerate(results):
            print(f"Result {i+1}:\n{res.page_content[:300]}...\n")
            
    except Exception as e:
        print(f"Error reading DB: {e}")

if __name__ == "__main__":
    check_db()
