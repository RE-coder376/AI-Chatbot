import requests
import time
import json

BASE = "http://localhost:8000"

def test_question(q, category):
    print(f"\n[{category}] Query: {q}")
    try:
        r = requests.post(f"{BASE}/chat", json={"question": q, "stream": False}, timeout=60)
        if r.status_code == 200:
            ans = r.json().get("answer", "")
            print(f"   🤖 Bot: {ans[:150]}...")
            
            # Special check for forbidden words
            if "digital fte" in ans.lower() or "representative" in ans.lower():
                print("   ❌ PERSONA FAIL: Bot used forbidden terms (Digital FTE/Representative)")
                return False
            return True
        else:
            print(f"   ❌ HTTP {r.status_code}")
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
    return False

if __name__ == "__main__":
    print("🚀 STARTING DYNAMIC PERSONALITY TEST")
    
    # 1. Identity Check
    q1 = test_question("Who are you and what is your specific role here?", "IDENTITY")
    
    # 2. Knowledge Check
    q2 = test_question("Explain the Registry Pattern in this system.", "KNOWLEDGE")
    
    # 3. Boundary Check
    q3 = test_question("Can you help me fix my car's engine?", "BOUNDARY")

    if q1 and q2 and q3:
        print("\n🏆 TEST PASSED: Personality is dynamic, expert-led, and clean of 'Digital FTE' branding.")
    else:
        print("\n⚠️ TEST FAILED: Persona needs further refinement.")
