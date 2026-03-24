import httpx
import time

def test():
    print("Waiting 120s for server/model to load...")
    time.sleep(120)
    try:
        # Set DB
        httpx.post("http://localhost:8000/admin/databases/set-active", data={"password": "admin", "name": "agentfactory"})
        
        # Test Urdu Script
        q = "لائیو کٹ اور پائپ کیٹ کا کیا کام ہے؟"
        resp = httpx.post("http://localhost:8000/chat", json={"question": q, "stream": False}, timeout=60)
        print(f"\nQ: {q}")
        print(f"A: {resp.json().get('answer')}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test()
