"""
Comprehensive chatbot test suite — tests every feature.
Rate-limit aware: adds 2s delay between LLM calls.
Run: python test_comprehensive.py
"""
import requests, json, time, sys, os

BASE = "http://localhost:8000"
ADMIN_PASS = None  # auto-detected
SESSION = f"test_{int(time.time())}"
VISITOR = f"visitor_{int(time.time())}"

PASS_COUNT = 0
FAIL_COUNT = 0
WARN_COUNT = 0
RESULTS = []

def p(label, status, detail=""):
    global PASS_COUNT, FAIL_COUNT, WARN_COUNT
    sym = {"PASS": "✅", "FAIL": "❌", "WARN": "⚠️ ", "INFO": "ℹ️ "}[status]
    line = f"  {sym} [{status}] {label}"
    if detail:
        short = str(detail)[:120]
        line += f"\n       → {short}"
    print(line)
    RESULTS.append((status, label, str(detail)[:200]))
    if status == "PASS": PASS_COUNT += 1
    elif status == "FAIL": FAIL_COUNT += 1
    elif status == "WARN": WARN_COUNT += 1

def chat(question, history=None, session_id=None, visitor_id=None):
    """Send a chat message and return (answer, sources, metadata_blob)."""
    payload = {
        "question": question,
        "history": history or [],
        "session_id": session_id or SESSION,
    }
    if visitor_id:
        payload["visitor_id"] = visitor_id
    try:
        r = requests.post(f"{BASE}/chat", json=payload, stream=True, timeout=45)
        chunks, metadata = [], {}
        for line in r.iter_lines():
            if not line: continue
            line = line.decode() if isinstance(line, bytes) else line
            if not line.startswith("data: "): continue
            try:
                ev = json.loads(line[6:])
                if ev.get("type") == "chunk":
                    chunks.append(ev.get("content", ""))
                elif ev.get("type") == "metadata":
                    metadata = ev
            except: pass
        answer = "".join(chunks)
        sources = metadata.get("sources", [])
        return answer, sources, metadata
    except Exception as e:
        return f"ERROR:{e}", [], {}

def get_admin_pass():
    global ADMIN_PASS
    if ADMIN_PASS: return ADMIN_PASS
    try:
        cfg = json.load(open("config.json"))
        ADMIN_PASS = cfg.get("admin_password", "admin123")
    except:
        ADMIN_PASS = "admin123"
    return ADMIN_PASS

# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "═"*60)
print("  CHATBOT COMPREHENSIVE TEST SUITE")
print("═"*60 + "\n")

# ─── 1. SERVER HEALTH ────────────────────────────────────────────────────────
print("── 1. Server Health & Status ──")
try:
    r = requests.get(f"{BASE}/health", timeout=5)
    d = r.json()
    if d.get("status", "").startswith("ready"):
        p("Health endpoint", "PASS", f"status={d['status']} db={d.get('db')} docs={d.get('docs_indexed')}")
    else:
        p("Health endpoint", "FAIL", d)
except Exception as e:
    p("Health endpoint", "FAIL", e)
    print("Server not reachable. Exiting.")
    sys.exit(1)

# ─── 2. CONFIG ENDPOINT ──────────────────────────────────────────────────────
print("\n── 2. Config Endpoint ──")
try:
    r = requests.get(f"{BASE}/config", timeout=5)
    d = r.json()
    required = ["bot_name", "business_name"]
    for k in required:
        if d.get(k):
            p(f"Config.{k}", "PASS", d[k])
        else:
            p(f"Config.{k}", "WARN", "empty/missing")
except Exception as e:
    p("Config endpoint", "FAIL", e)

# ─── 3. ADMIN AUTH ───────────────────────────────────────────────────────────
print("\n── 3. Admin Auth ──")
pwd = get_admin_pass()
try:
    r = requests.get(f"{BASE}/admin/analytics?password={pwd}", timeout=5)
    if r.status_code == 200:
        p("Admin auth (correct password)", "PASS", f"HTTP 200")
    else:
        p("Admin auth (correct password)", "FAIL", f"HTTP {r.status_code}")
except Exception as e:
    p("Admin auth", "FAIL", e)

try:
    r = requests.get(f"{BASE}/admin/analytics?password=wrongpass", timeout=5)
    if r.status_code == 401:
        p("Admin auth (wrong password → 401)", "PASS")
    else:
        p("Admin auth (wrong password)", "WARN", f"Expected 401 got {r.status_code}")
except Exception as e:
    p("Admin auth reject", "FAIL", e)

# ─── 4. ADMIN DATABASES LIST ─────────────────────────────────────────────────
print("\n── 4. Admin: Databases ──")
try:
    r = requests.get(f"{BASE}/admin/databases?password={pwd}", timeout=5)
    if r.status_code == 200:
        resp = r.json()
        dbs = resp.get("databases", resp) if isinstance(resp, dict) else resp
        p("List databases", "PASS", f"{len(dbs)} databases: {[d.get('name') if isinstance(d,dict) else d for d in dbs]}")
    else:
        p("List databases", "FAIL", r.text[:100])
except Exception as e:
    p("List databases", "FAIL", e)

# ─── 5. ADMIN ANALYTICS ──────────────────────────────────────────────────────
print("\n── 5. Admin: Analytics ──")
try:
    r = requests.get(f"{BASE}/admin/analytics?password={pwd}", timeout=5)
    if r.status_code == 200:
        d = r.json()
        p("Analytics endpoint", "PASS", f"total_queries={d.get('total_queries')} total_sessions={d.get('total_sessions')}")
    else:
        p("Analytics endpoint", "FAIL", r.text[:100])
except Exception as e:
    p("Analytics endpoint", "FAIL", e)

# ─── 6. ADMIN KNOWLEDGE GAPS ─────────────────────────────────────────────────
print("\n── 6. Admin: Knowledge Gaps ──")
try:
    r = requests.get(f"{BASE}/admin/knowledge-gaps?password={pwd}", timeout=5)
    if r.status_code == 200:
        gaps = r.json()
        p("Knowledge gaps endpoint", "PASS", f"{len(gaps)} gaps logged")
        if gaps:
            p("Knowledge gap sample", "INFO", gaps[0] if isinstance(gaps[0], str) else json.dumps(gaps[0])[:80])
    else:
        p("Knowledge gaps endpoint", "FAIL", r.text[:100])
except Exception as e:
    p("Knowledge gaps", "FAIL", e)

# ─── 7. ADMIN FAQS ───────────────────────────────────────────────────────────
print("\n── 7. Admin: FAQs ──")
try:
    r = requests.get(f"{BASE}/admin/faqs?password={pwd}", timeout=5)
    if r.status_code == 200:
        faqs = r.json()
        p("FAQs list", "PASS", f"{len(faqs)} FAQs")
    else:
        p("FAQs list", "FAIL", r.status_code)
except Exception as e:
    p("FAQs", "FAIL", e)

# ─── 8. ADMIN TEST ENDPOINTS ─────────────────────────────────────────────────
print("\n── 8. Admin: Built-in Tests (identity/knowledge/safety) ──")
for endpoint in ["identity", "knowledge", "safety"]:
    try:
        r = requests.get(f"{BASE}/admin/test/{endpoint}?password={pwd}", timeout=30)
        if r.status_code == 200:
            d = r.json()
            status_val = d.get("status", d.get("result", "ok"))
            p(f"Admin test/{endpoint}", "PASS", str(status_val)[:100])
        else:
            p(f"Admin test/{endpoint}", "FAIL", r.text[:100])
    except Exception as e:
        p(f"Admin test/{endpoint}", "FAIL", e)
    time.sleep(3)  # rate limit

# ─── 9. EMBEDDING MODEL ENDPOINT ─────────────────────────────────────────────
print("\n── 9. Admin: Embedding Model ──")
try:
    r = requests.get(f"{BASE}/admin/embedding-model?password={pwd}", timeout=5)
    if r.status_code == 200:
        d = r.json()
        p("Embedding model endpoint", "PASS", str(d)[:100])
    else:
        p("Embedding model endpoint", "FAIL", r.status_code)
except Exception as e:
    p("Embedding model", "FAIL", e)

# ─── 10. CORE CHAT — GREETING ────────────────────────────────────────────────
print("\n── 10. Core Chat: Greeting & Source Suppression ──")
answer, sources, meta = chat("Hello!")
time.sleep(2)
if answer and not answer.startswith("ERROR"):
    p("Greeting response", "PASS", answer[:80])
else:
    p("Greeting response", "FAIL", answer)

if len(sources) == 0:
    p("Greeting → 0 sources (source suppression)", "PASS")
else:
    p("Greeting → 0 sources (source suppression)", "FAIL", f"Got {len(sources)} sources: {sources}")

# ─── 11. CORE CHAT — FACTUAL Q&A ─────────────────────────────────────────────
print("\n── 11. Core Chat: Factual Q&A ──")
qa_tests = [
    ("who is the CEO of panaversity", ["zia", "khan"], "CEO query"),
    ("what is agentfactory", ["agent", "panaversity", "framework", "digital"], "AgentFactory overview"),
    ("what courses does agentfactory offer", ["course", "program", "learn", "genai", "cloud"], "Courses query"),
]
for question, expected_keywords, label in qa_tests:
    answer, sources, meta = chat(question)
    time.sleep(3)
    lower = answer.lower()
    found = [kw for kw in expected_keywords if kw in lower]
    if answer.startswith("ERROR"):
        p(label, "FAIL", answer)
    elif len(found) >= 1:
        p(label, "PASS", f"Answer contains {found}. Sources: {len(sources)}")
    else:
        p(label, "WARN", f"Expected {expected_keywords}, got: {answer[:100]}")

# ─── 12. CORE CHAT — IDK (off-topic) ─────────────────────────────────────────
print("\n── 12. Core Chat: IDK / Off-Topic ──")
idk_tests = [
    ("what is the capital of france", "geography"),
    ("how do i write a python for loop", "coding"),
    ("who won the world cup in 2022", "sports"),
]
for question, topic in idk_tests:
    answer, sources, meta = chat(question)
    time.sleep(2)
    lower = answer.lower()
    is_idk = any(s in lower for s in ["don't have", "do not have", "not available", "can't help",
                                        "outside", "scope", "specialize", "focus on", "assist with"])
    if answer.startswith("ERROR"):
        p(f"IDK/{topic}", "FAIL", answer)
    elif is_idk or len(sources) == 0:
        p(f"IDK/{topic} (politely declined)", "PASS", answer[:100])
    else:
        p(f"IDK/{topic}", "WARN", f"May have answered off-topic: {answer[:100]}")

# ─── 13. CONVERSATION MEMORY ─────────────────────────────────────────────────
print("\n── 13. Conversation Memory (multi-turn) ──")
session_mem = f"mem_{int(time.time())}"
history = []
q1 = "My name is Zara."
a1, _, _ = chat(q1, history, session_id=session_mem)
time.sleep(2)
history = [{"role": "user", "content": q1}, {"role": "assistant", "content": a1}]

q2 = "What is my name?"
a2, sources2, _ = chat(q2, history, session_id=session_mem)
time.sleep(2)

if "zara" in a2.lower():
    p("Conversation memory (name recall)", "PASS", a2[:80])
else:
    p("Conversation memory (name recall)", "WARN", f"Expected 'Zara' in: {a2[:100]}")

if len(sources2) == 0:
    p("Memory query → 0 sources (source suppression)", "PASS")
else:
    p("Memory query → 0 sources", "WARN", f"Got {len(sources2)} sources")

# ─── 14. TRANSLATION REJECTION ───────────────────────────────────────────────
print("\n── 14. Translation Rejection ──")
t_start = time.time()
answer, sources, _ = chat("Translate 'hello world' to Spanish")
elapsed = time.time() - t_start
time.sleep(2)
lower = answer.lower()
is_declined = any(s in lower for s in ["english", "assist in english", "english only", "not able to translate"])
if answer.startswith("ERROR"):
    p("Translation rejection", "FAIL", answer)
elif is_declined:
    p("Translation rejection (declined)", "PASS", f"Elapsed: {elapsed:.1f}s — {answer[:80]}")
elif elapsed < 3:
    p("Translation rejection (fast response)", "PASS", f"Elapsed: {elapsed:.1f}s — {answer[:80]}")
else:
    p("Translation rejection", "WARN", f"Elapsed {elapsed:.1f}s: {answer[:100]}")

# ─── 15. SOURCE COUNT CHECK ───────────────────────────────────────────────────
print("\n── 15. Sources: Factual answers have sources ──")
answer, sources, meta = chat("what is the digital fte concept in agentfactory")
time.sleep(3)
if answer.startswith("ERROR"):
    p("Factual answer has sources", "FAIL", answer)
elif len(sources) > 0:
    p("Factual answer has sources", "PASS", f"{len(sources)} sources returned")
else:
    p("Factual answer has sources", "WARN", "0 sources on factual answer")

# ─── 16. QUICK REPLIES IN METADATA ───────────────────────────────────────────
print("\n── 16. Metadata: Quick Replies ──")
answer, sources, meta = chat("tell me about agentfactory courses")
time.sleep(3)
qr = meta.get("options", meta.get("quick_replies", []))
if qr:
    p("Quick replies in metadata", "PASS", f"{qr}")
else:
    p("Quick replies in metadata", "WARN", "options/quick_replies not present in metadata")

# ─── 17. VISITOR HISTORY ─────────────────────────────────────────────────────
print("\n── 17. Visitor History ──")
vid = f"visitor_test_{int(time.time())}"
# Send a message with visitor_id
answer, _, _ = chat("what is panaversity", visitor_id=vid)
time.sleep(2)

try:
    r = requests.get(f"{BASE}/history/{vid}", timeout=5)
    if r.status_code == 200:
        hist = r.json()
        if isinstance(hist, list) and len(hist) >= 1:
            p("Visitor history stored & retrieved", "PASS", f"{len(hist)} turns saved")
        else:
            p("Visitor history", "WARN", f"Unexpected format: {hist}")
    else:
        p("Visitor history", "FAIL", f"HTTP {r.status_code}: {r.text[:100]}")
except Exception as e:
    p("Visitor history", "FAIL", e)

# ─── 18. FEEDBACK (THUMBS UP/DOWN) ───────────────────────────────────────────
print("\n── 18. Feedback (thumbs up/down) ──")
try:
    payload = {
        "message_id": f"msg_{int(time.time())}",
        "rating": 1,
        "question": "what is agentfactory",
        "answer": "AgentFactory is a framework...",
        "session_id": SESSION
    }
    r = requests.post(f"{BASE}/feedback", json=payload, timeout=5)
    if r.status_code == 200:
        p("Feedback (thumbs up)", "PASS", r.json())
    else:
        p("Feedback (thumbs up)", "FAIL", f"HTTP {r.status_code}: {r.text[:100]}")
except Exception as e:
    p("Feedback", "FAIL", e)

try:
    payload["rating"] = -1
    payload["message_id"] = f"msg_{int(time.time())}_down"
    r = requests.post(f"{BASE}/feedback", json=payload, timeout=5)
    if r.status_code == 200:
        p("Feedback (thumbs down)", "PASS", r.json())
    else:
        p("Feedback (thumbs down)", "FAIL", r.text[:100])
except Exception as e:
    p("Feedback thumbs down", "FAIL", e)

# ─── 19. CSAT ────────────────────────────────────────────────────────────────
print("\n── 19. CSAT Rating ──")
for rating in [4, 5]:
    try:
        r = requests.post(f"{BASE}/csat", json={"rating": rating, "session_id": SESSION}, timeout=5)
        if r.status_code == 200:
            p(f"CSAT rating={rating}", "PASS", r.json())
        else:
            p(f"CSAT rating={rating}", "FAIL", r.text[:100])
    except Exception as e:
        p(f"CSAT rating={rating}", "FAIL", e)

# ─── 20. KNOWLEDGE GAP LOGGING ───────────────────────────────────────────────
print("\n── 20. Knowledge Gap Logging ──")
# Get gap count before
try:
    r_before = requests.get(f"{BASE}/admin/knowledge-gaps?password={pwd}", timeout=5)
    gaps_before = len(r_before.json()) if r_before.status_code == 200 else 0
except:
    gaps_before = 0

# Ask something likely not in KB
answer_gap, _, _ = chat("what is the price of platinum membership in agentfactory")
time.sleep(3)

try:
    r_after = requests.get(f"{BASE}/admin/knowledge-gaps?password={pwd}", timeout=5)
    gaps_after = len(r_after.json()) if r_after.status_code == 200 else 0
    if gaps_after > gaps_before:
        p("Knowledge gap logged (count increased)", "PASS", f"{gaps_before} → {gaps_after} gaps")
    else:
        p("Knowledge gap logged", "WARN", f"Count unchanged at {gaps_after}. Answer: {answer_gap[:80]}")
except Exception as e:
    p("Knowledge gap check", "FAIL", e)

# ─── 21. LEAD CAPTURE ────────────────────────────────────────────────────────
print("\n── 21. Lead Capture Flag ──")
# High-confidence answer should NOT trigger lead capture
answer, sources, meta = chat("what is agentfactory digital fte framework")
time.sleep(3)
capture = meta.get("capture_lead", False)
p(f"No lead capture on info answer (capture_lead={capture})", "PASS" if not capture else "WARN", answer[:60])

# ─── 22. RATE LIMITING ───────────────────────────────────────────────────────
print("\n── 22. Rate Limiting ──")
# Send 21 rapid fire requests (limit is 20/min)
hit_limit = False
for i in range(22):
    try:
        r = requests.post(f"{BASE}/chat",
            json={"question": "hi", "history": [], "session_id": f"ratelimit_test_{i}"},
            timeout=5, stream=False)
        raw = r.text
        if "rate limit" in raw.lower() or "too many" in raw.lower() or r.status_code == 429:
            hit_limit = True
            p(f"Rate limit triggered at request #{i+1}", "PASS", f"HTTP {r.status_code}")
            break
    except: pass

if not hit_limit:
    p("Rate limiting", "WARN", "20+ requests sent but no rate limit triggered")

time.sleep(5)  # cool down

# ─── 23. SUBMIT LEAD ─────────────────────────────────────────────────────────
print("\n── 23. Lead Submission ──")
try:
    payload = {
        "name": "Test User",
        "contact": "test@example.com",
        "method": "email",
        "session_id": SESSION
    }
    r = requests.post(f"{BASE}/submit-lead", json=payload, timeout=5)
    if r.status_code == 200:
        p("Submit lead", "PASS", r.json())
    else:
        p("Submit lead", "WARN", f"HTTP {r.status_code}: {r.text[:100]}")
except Exception as e:
    p("Submit lead", "FAIL", e)

# ─── 24. ADMIN BRANDING GET ──────────────────────────────────────────────────
print("\n── 24. Admin: Branding & Config ──")
try:
    r = requests.get(f"{BASE}/admin/branding?password={pwd}", timeout=5)
    if r.status_code == 200:
        p("Admin branding get", "PASS", str(r.json())[:120])
    else:
        p("Admin branding get", "FAIL", r.status_code)
except Exception as e:
    p("Admin branding", "FAIL", e)

# ─── 25. ADMIN INGEST TEXT ───────────────────────────────────────────────────
print("\n── 25. Admin: Ingest Text ──")
try:
    payload = {
        "password": pwd,
        "text": "TEST CHUNK: The AgentFactory test suite was successfully run on 2026-03-19. This is a test ingestion to verify the text ingestion endpoint works correctly.",
        "source": "test_suite"
    }
    r = requests.post(f"{BASE}/admin/ingest/text", json=payload, timeout=30)
    if r.status_code == 200:
        d = r.json()
        p("Ingest text endpoint", "PASS", str(d)[:120])
    else:
        p("Ingest text endpoint", "FAIL", f"HTTP {r.status_code}: {r.text[:100]}")
except Exception as e:
    p("Ingest text", "FAIL", e)

# ─── 26. EMBED CODE ENDPOINT ─────────────────────────────────────────────────
print("\n── 26. Admin: Embed Code ──")
try:
    r = requests.get(f"{BASE}/admin/embed-code?password={pwd}", timeout=5)
    if r.status_code == 200:
        d = r.json()
        p("Embed code endpoint", "PASS", "snippet" if "script" in str(d).lower() else str(d)[:100])
    else:
        p("Embed code endpoint", "FAIL", r.status_code)
except Exception as e:
    p("Embed code", "FAIL", e)

# ─── 27. CRAWL ENDPOINT (books.toscrape.com — small test) ────────────────────
print("\n── 27. Crawler: Inspect Site ──")
try:
    # Just inspect, don't actually crawl
    r = requests.post(f"{BASE}/admin/crawl",
        json={
            "password": pwd,
            "url": "http://books.toscrape.com",
            "db_name": "agentfactory",
            "mode": "inspect"
        },
        timeout=15)
    if r.status_code in (200, 202):
        d = r.json()
        p("Crawl inspect mode", "PASS", str(d)[:120])
    else:
        p("Crawl inspect mode", "WARN", f"HTTP {r.status_code}: {r.text[:100]}")
except Exception as e:
    p("Crawl inspect", "WARN", str(e)[:100])

# ─── 28. ADMIN HEALER ────────────────────────────────────────────────────────
print("\n── 28. Admin: Healer Trigger ──")
try:
    r = requests.post(f"{BASE}/admin/healer/trigger",
        json={"password": pwd}, timeout=10)
    if r.status_code == 200:
        p("Healer trigger", "PASS", str(r.json())[:100])
    else:
        p("Healer trigger", "WARN", f"HTTP {r.status_code}: {r.text[:100]}")
except Exception as e:
    p("Healer trigger", "WARN", str(e)[:100])

# ─── 29. STREAMED RESPONSE STRUCTURE ─────────────────────────────────────────
print("\n── 29. SSE Streaming Structure ──")
try:
    r = requests.post(f"{BASE}/chat",
        json={"question": "tell me about agentfactory onboarding", "history": [], "session_id": f"stream_test_{int(time.time())}"},
        stream=True, timeout=45)
    time.sleep(3)
    types_seen = set()
    for line in r.iter_lines():
        if not line: continue
        line = line.decode() if isinstance(line, bytes) else line
        if line.startswith("data: "):
            try:
                ev = json.loads(line[6:])
                types_seen.add(ev.get("type"))
            except: pass
    expected = {"chunk", "metadata", "done"}
    if expected.issubset(types_seen):
        p("SSE event types (chunk+metadata+done)", "PASS", f"Types seen: {types_seen}")
    elif "chunk" in types_seen:
        p("SSE streaming (at least chunk events)", "PASS", f"Types seen: {types_seen}")
    else:
        p("SSE streaming", "FAIL", f"Types seen: {types_seen}")
except Exception as e:
    p("SSE streaming", "FAIL", e)

# ─── 30. DETAILED ADMIN TEST ─────────────────────────────────────────────────
print("\n── 30. Admin: Detailed Test Suite ──")
try:
    r = requests.get(f"{BASE}/admin/test-detailed?password={pwd}", timeout=60)
    if r.status_code == 200:
        d = r.json()
        for test_name, result in d.items() if isinstance(d, dict) else [("result", d)]:
            status_str = str(result)
            p(f"test-detailed/{test_name}", "PASS" if "pass" in status_str.lower() or "ok" in status_str.lower() else "WARN", status_str[:80])
    else:
        p("Admin test-detailed", "FAIL", r.text[:100])
except Exception as e:
    p("Admin test-detailed", "WARN", str(e)[:100])

# ─── SUMMARY ─────────────────────────────────────────────────────────────────
print("\n" + "═"*60)
print(f"  TEST SUMMARY: {PASS_COUNT} PASS  {WARN_COUNT} WARN  {FAIL_COUNT} FAIL")
print("═"*60)

# Print all failures and warnings grouped
if FAIL_COUNT > 0:
    print("\n  FAILURES:")
    for status, label, detail in RESULTS:
        if status == "FAIL":
            print(f"    ❌ {label}: {detail[:100]}")

if WARN_COUNT > 0:
    print("\n  WARNINGS:")
    for status, label, detail in RESULTS:
        if status == "WARN":
            print(f"    ⚠️  {label}: {detail[:100]}")

print()
