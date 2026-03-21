import httpx
import json

def test_leadbox():
    print("--- Testing Leadbox Trigger ---")
    
    # 1. Test IDK trigger (Pizza)
    print("\nQ: How much does a pizza cost?")
    with httpx.stream("POST", "http://localhost:8000/chat", json={
        "question": "How much does a pizza cost?",
        "stream": True
    }, timeout=60.0) as r:
        found_lead = False
        for line in r.iter_lines():
            if line.startswith("data: "):
                data = json.loads(line[6:])
                if data.get("type") == "metadata" and data.get("capture_lead"):
                    found_lead = True
                    print("✅ Leadbox Metadata Found (IDK Trigger)")
        if not found_lead:
            print("❌ Leadbox NOT Triggered for IDK")

    # 2. Test Keyword trigger (Cost)
    print("\nQ: How much does AgentFactory cost?")
    with httpx.stream("POST", "http://localhost:8000/chat", json={
        "question": "How much does AgentFactory cost?",
        "stream": True
    }, timeout=60.0) as r:
        found_lead = False
        for line in r.iter_lines():
            if line.startswith("data: "):
                data = json.loads(line[6:])
                if data.get("type") == "metadata" and data.get("capture_lead"):
                    found_lead = True
                    print("✅ Leadbox Metadata Found (Keyword Trigger)")
        if not found_lead:
            print("❌ Leadbox NOT Triggered for Keyword")

    # 3. Test New Keyword trigger (Book)
    print("\nQ: Can I book a demo?")
    with httpx.stream("POST", "http://localhost:8000/chat", json={
        "question": "Can I book a demo?",
        "stream": True
    }, timeout=60.0) as r:
        found_lead = False
        for line in r.iter_lines():
            if line.startswith("data: "):
                data = json.loads(line[6:])
                if data.get("type") == "metadata" and data.get("capture_lead"):
                    found_lead = True
                    print("✅ Leadbox Metadata Found (New Keyword Trigger)")
        if not found_lead:
            print("❌ Leadbox NOT Triggered for New Keyword")

    # 4. Test Actual Email Submission
    print("\nQ: Submitting a real lead form...")
    r = httpx.post("http://localhost:8000/submit-lead", json={
        "name": "Hamza Strategy Test",
        "email": "hamzanaimat.14@gmail.com",
        "message": "This is a billion-dollar lead test!"
    }, timeout=15.0)
    if r.status_code == 200:
        print("✅ /submit-lead endpoint responded OK. Check server logs for email status.")
    else:
        print(f"❌ /submit-lead failed with {r.status_code}: {r.text}")

if __name__ == "__main__":
    test_leadbox()
