import httpx
import time
import json

# --- Configuration ---
BASE_URL = "http://localhost:8000"
PASSWORD = "admin123"

def log_test(db, q, lang="English"):
    print(f"\n--- Testing [{db} | {lang}] ---")
    print(f"Q: {q}")
    try:
        # Set active DB
        httpx.post(f"{BASE_URL}/admin/databases/set-active", data={"password": PASSWORD, "name": db}, timeout=30)
        
        # Ask question
        resp = httpx.post(f"{BASE_URL}/chat", json={"question": q, "stream": False}, timeout=60)
        if resp.status_code == 200:
            answer = resp.json().get("answer", "No answer")
            print(f"A: {answer}")
        else:
            print(f"Error: {resp.status_code} - {resp.text}")
    except Exception as e:
        print(f"Test failed: {e}")

def run_rigorous_audit():
    # 1. Complex Questions (2 per DB)
    tests = [
        # AgentFactory
        {"db": "agentfactory", "q": "How does AgentFactory integrate with LiveKit and Pipecat, and what is the purpose of this integration?"},
        {"db": "agentfactory", "q": "What is a Digital FTE and how can I contact your office in Islamabad for support?"},
        
        # TSC.pk
        {"db": "tsc_pk", "q": "Besides designer wear, what office-related items do you sell, and how can I track an order?"},
        {"db": "tsc_pk", "q": "What is the specific email address for customer service if I have inquiries about calculators?"},
        
        # eStationery
        {"db": "estationery", "q": "What specific supplies do you offer for school kits and office use, and do you deliver nationwide?"},
        {"db": "estationery", "q": "How can I get assistance if I want to buy pens and notebooks, and what is your helpline email?"},
        
        # Chishti Sabri
        {"db": "chishti_sabri", "q": "What range of Sufi literature do you carry, and what other Islamic lifestyle products are available?"},
        {"db": "chishti_sabri", "q": "Do you sell perfumes like attar, and what is the best way to contact you for more information?"}
    ]

    print("=== Phase 1: Complex Database Retrieval ===")
    for test in tests:
        log_test(test["db"], test["q"])
        time.sleep(15) # Rate limit safety

    # 2. Multilingual Strategy Test
    print("\n=== Phase 2: Multilingual Strategy Validation ===")
    ml_tests = [
        {"db": "agentfactory", "q": "لائیو کٹ اور پائپ کیٹ کا کیا کام ہے؟", "lang": "Urdu Script"},
        {"db": "estationery", "q": "Kiya aap calculators aur pens pure Pakistan mein deliver karte hain?", "lang": "Roman Urdu"},
        {"db": "chishti_sabri", "q": "Sufi literature ke bare mein batayein aur contact email kya hai?", "lang": "Roman Urdu"}
    ]

    for test in ml_tests:
        log_test(test["db"], test["q"], test["lang"])
        time.sleep(15)

if __name__ == "__main__":
    run_rigorous_audit()
