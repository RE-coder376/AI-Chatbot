import httpx
import sys

# Ensure output can handle Urdu characters in the console
sys.stdout.reconfigure(encoding='utf-8')

q = "لائیو کٹ اور پائپ کیٹ کا کیا کام ہے؟"
print(f"Testing Question: {q}")

try:
    # Ensure we are on AgentFactory DB
    httpx.post("http://localhost:8000/admin/databases/set-active", data={"password": "admin", "name": "agentfactory"})
    
    # Get answer
    resp = httpx.post("http://localhost:8000/chat", json={"question": q, "stream": False}, timeout=60)
    answer = resp.json().get("answer")
    print("-" * 50)
    print(f"BOT ANSWER:\n{answer}")
    print("-" * 50)
except Exception as e:
    print(f"Error: {e}")
