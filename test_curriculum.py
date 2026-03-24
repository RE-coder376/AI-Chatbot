import httpx
import json

BASE_URL = "http://localhost:8000"

def test_query(q):
    print(f"\nQ: {q}")
    try:
        resp = httpx.post(f"{BASE_URL}/chat", json={"question": q, "stream": False}, timeout=30)
        if resp.status_code == 200:
            print(f"A: {resp.json().get('answer', 'No answer')}")
        else:
            print(f"Error: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    # Assuming agentfactory is already active
    queries = [
        "What's the curriculum?",
        "What will I learn in this course?",
        "Tell me about the course syllabus",
        "whats the curriculam" # with typo
    ]
    for q in queries:
        test_query(q)
