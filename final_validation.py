import requests
import time
import json

BASE = "http://localhost:8000"

def wait_for_server():
    print("⏳ Waiting for server to be fully awake...")
    for i in range(100):
        try:
            r = requests.get(f"{BASE}/health", timeout=2)
            if r.status_code == 200:
                data = r.json()
                print(f"✅ SERVER READY. Mode: {data.get('status')}, DB: {data.get('db')}")
                return True
        except: pass
        time.sleep(2)
    return False

def test_question(q, category):
    print(f"\n[{category}] Question: {q}")
    try:
        start = time.time()
        # Non-streaming for machine validation
        r = requests.post(f"{BASE}/chat", json={"question": q, "stream": False}, timeout=90)
        elapsed = time.time() - start
        
        if r.status_code == 200:
            ans = r.json().get("answer", "")
            if len(ans) > 20:
                print(f"   ✅ SUCCESS ({elapsed:.1f}s)")
                print(f"   🤖 Answer: {ans[:150]}...")
                return True
            else:
                print(f"   ⚠️ WEAK ANSWER: {ans}")
        else:
            print(f"   ❌ HTTP {r.status_code}: {r.text}")
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
    return False

if __name__ == "__main__":
    if wait_for_server():
        print("\n🚀 STARTING COMPREHENSIVE SYSTEM TEST")
        results = []
        
        # 1. Identity Test
        results.append(test_question("What is your name and which company do you work for?", "IDENTITY"))
        
        # 2. Technical Knowledge Test (Registry Pattern)
        results.append(test_question("Explain the Registry Pattern and how it uses a metaclass.", "KNOWLEDGE"))
        
        # 3. Product Knowledge Test (Strategic Procurement)
        results.append(test_question("What is the Strategic Procurement line in AgentFactory?", "PRODUCT"))
        
        # 4. Boundary Test (Off-topic)
        results.append(test_question("How do I bake a chocolate cake?", "SAFETY"))
        
        # 5. Lead Capture Test
        results.append(test_question("I want to hire a digital FTE, what is the cost?", "LEADBOX"))

        success_count = sum(1 for r in results if r)
        print(f"\n📊 FINAL VALIDATION SCORE: {success_count}/{len(results)}")
        if success_count == len(results):
            print("🏆 SYSTEM IS 100% PRODUCTION READY.")
        else:
            print("⚠️ SYSTEM NEEDS TUNING.")
    else:
        print("❌ SERVER FAILED TO RESPOND.")
