import json, re, time, urllib.request

URL = "https://re-coder376--ai-chatbot-serve.modal.run/chat"
KEY = open("embed_tmp.json", encoding="utf-8").read()
KEY = re.search(r"wk1\.[A-Za-z0-9_\-\.]+", KEY).group(0)

# The 11 questions that FAILED last session (the ones P1-P4 target), with expected-answer keywords.
Q = [
 ("Q1",  "P2", "Which password hashing library does the user management module use?",
        [["bcrypt", "passlib", "argon"]]),
 ("Q4",  "P1", "What are the three components used to calculate Expected Credit Loss (ECL) under IFRS 9?",
        [["pd", "probability of default"], ["lgd", "loss given default"], ["ead", "exposure at default"]]),
 ("Q6",  "P3", "What does MCP stand for?",
        [["model context protocol"]]),
 ("Q7",  "P2", "What are the steps of the safety-first pattern for agents?",
        [["safety"]]),
 ("Q10", "P1", "Which database and ORM does the FastAPI for Agents module use?",
        [["sqlmodel"], ["neon", "postgres"]]),
 ("Q13", "P1", "How many learners does TutorClaw serve and at roughly what monthly cost?",
        [["16,000", "16000"], ["60"]]),
 ("Q14", "P2", "What testing library and coverage target does the TDD lesson recommend?",
        [["respx"], ["80"]]),
 ("Q16", "P2", "How is memory given to an agent?",
        [["memory"]]),
 ("Q17", "P4", "What are the seven domains?",
        [["1", "first"], ["2", "second"], ["7", "seven"]]),
 ("Q18", "P2", "What does the skill for wrapping an MCP server do?",
        [["mcp"]]),
 ("Q20", "P1", "Who leads Panaversity and who are the co-authors?",
        [["zia"], ["wania"]]),
]

def ask(q):
    body = json.dumps({"question": q, "stream": False,
                       "include_debug_artifacts": True, "admin_password": "iaah2006"}).encode()
    req = urllib.request.Request(URL, data=body, headers={
        "Content-Type": "application/json", "X-Widget-Key": KEY})
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read().decode())

def graded(ans, groups):
    al = ans.lower()
    return all(any(t in al for t in grp) for grp in groups)

passes = 0
out = []
for qid, prob, q, groups in Q:
    for attempt in range(4):
        try:
            d = ask(q)
            break
        except Exception as e:
            d = {"response": f"ERROR {e}"}
            time.sleep(8 * (attempt + 1))
    ans = d.get("response") or d.get("answer") or d.get("message") or json.dumps(d)[:200]
    ok = graded(ans, groups)
    passes += ok
    srcs = ""
    tr = d.get("workflow_trace") or d.get("debug") or {}
    print(f"{qid} [{prob}] {'PASS' if ok else 'FAIL'}")
    print("   Q:", q)
    print("   A:", re.sub(r"\s+", " ", ans)[:280])
    out.append({"qid": qid, "prob": prob, "pass": ok, "q": q, "a": ans})
    time.sleep(5)

print(f"\n==== {passes}/{len(Q)} of previously-failing questions now PASS ====")
json.dump(out, open("af_retest_out.json", "w", encoding="utf-8"), indent=1, ensure_ascii=False)
