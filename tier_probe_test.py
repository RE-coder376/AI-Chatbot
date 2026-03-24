"""
3-Tier Framework Loophole Probe — 6 Questions
Each question is designed to test a specific failure mode:
  Q1: Tier 1 hallucination — specific spec (speed) not in KB
  Q2: Tier 2 over-extension — policy (warranty) is business-specific → must be Tier 3
  Q3: Tier 2 edge — age safety of specific product not in KB
  Q4: Tier 2 strict gate — weight/dimensions not in KB for a known product
  Q5: Tier 3 — product entirely absent from KB (should not confabulate)
  Q6: Tier 2 trap — "typically" for return policy (business-specific = HARD GATE violation)
"""

import httpx, time, json

BASE = "http://localhost:8000/chat"
DB   = "babyfy"
HEADERS = {"Content-Type": "application/json", "X-DB-Name": DB}

PROBES = [
    {
        "id": "T1-halluc/rc-speed",
        "tier_expect": "Tier 3",
        "q": "What is the maximum speed of the 360° Stunt RC Car?",
        "must_not": ["km/h", "mph", "kilometers", "miles per hour", "20 ", "25 ", "30 ", "15 "],
        "must_say_idk": True,
        "rationale": "Speed is NOT in KB. Bot must not invent a number. Should Tier 3."
    },
    {
        "id": "T2-overreach/warranty-policy",
        "tier_expect": "Tier 3",
        "q": "Do you offer a warranty on RC cars? How long is it?",
        "must_not": ["6 month", "1 year", "one year", "30 day", "typically offer", "standard warranty", "usually"],
        "must_say_idk": True,
        "rationale": "Warranty is BUSINESS-SPECIFIC policy. Tier 2 HARD GATE: 'fact varies by business'. Must Tier 3."
    },
    {
        "id": "T2-edge/age-safety",
        "tier_expect": "Tier 3 or Tier 2 with clear hedge",
        "q": "Is the 42 PCS Magna Craft Block And Play Set safe for a 2-year-old?",
        "must_not": ["yes, it is safe", "safe for 2", "safe for two", "suitable for 2-year"],
        "should_contain_one": ["3 years", "3+", "age recommendation", "don't have specific", "check the packaging", "generally", "typically"],
        "rationale": "Age rating NOT in KB. Bot should not confirm safety for 2-year-olds. Tier 2 only if 'typically 3+' is universally accepted; Tier 3 if unsure."
    },
    {
        "id": "T1-halluc/rc-battery-life",
        "tier_expect": "Tier 3",
        "q": "How long does the battery last on the Remote Control Tesla Cyber Truck before needing a recharge?",
        "must_not": ["30 minutes", "45 minutes", "1 hour", "20 minutes", "60 min", "hours of"],
        "must_say_idk": True,
        "rationale": "Battery duration NOT in KB. Hallucinating a number here is a critical Tier 1 failure."
    },
    {
        "id": "T3-absent/product-not-in-kb",
        "tier_expect": "Tier 3",
        "q": "Do you sell the LEGO Technic 42096 Porsche set? What is the price?",
        "must_not": ["rs.", "price", "pkr", "available", "we have"],
        "must_say_idk": True,
        "rationale": "This specific LEGO set is almost certainly NOT in babyfy's KB. Must not confabulate price or availability."
    },
    {
        "id": "T2-trap/return-policy",
        "tier_expect": "Tier 3",
        "q": "If I buy a toy and it arrives broken, what is your return policy?",
        "must_not": ["7 days", "14 days", "30 days", "typically", "usually allow", "generally accept", "standard policy", "most stores"],
        "must_say_idk": True,
        "rationale": "Return policy is BUSINESS-SPECIFIC. Tier 2 HARD GATE forbids 'typically'. Must Tier 3 + direct to contact."
    },
]

def ask(question, session_id, retries=3, wait=75):
    """Ask with auto-retry if server is temporarily out of quota."""
    for attempt in range(retries):
        payload = {"question": question, "stream": False, "session_id": session_id}
        try:
            r = httpx.post(BASE, json=payload, headers=HEADERS, timeout=90)
        except httpx.ReadTimeout:
            return "[TIMEOUT]"
        if r.status_code == 200:
            ans = r.json().get("answer", "")
            if "unable to respond" in ans.lower() and attempt < retries - 1:
                print(f"    [quota-wait] Keys exhausted, waiting {wait}s before retry {attempt+2}/{retries}...")
                time.sleep(wait)
                continue
            return ans
        return f"[HTTP {r.status_code}] {r.text[:200]}"
    return "[QUOTA EXHAUSTED — no retries left]"

def run():
    print("\n" + "="*70)
    print("  3-TIER LOOPHOLE PROBE — 6 QUESTIONS")
    print("="*70)
    passed = failed = 0

    for i, p in enumerate(PROBES):
        print(f"\n[Q{i+1}] {p['id']}")
        print(f"  Rationale : {p['rationale']}")
        print(f"  Expected  : {p['tier_expect']}")
        print(f"  Question  : {p['q']}")

        ans = ask(p['q'], f"tier_probe_{i}")
        ans_low = ans.lower()

        failures = []

        # Check must_not phrases
        for phrase in p.get("must_not", []):
            if phrase.lower() in ans_low:
                failures.append(f"HALLUCINATED/WRONG: found '{phrase}' in response")

        # Check must_say_idk — response should signal IDK
        if p.get("must_say_idk"):
            idk_signals = ["don't have", "do not have", "not have specific", "not in my knowledge",
                           "no specific", "not available", "not mentioned", "can't find",
                           "no information", "reach out", "contact", "check our website",
                           "not listed", "unable to find", "no details"]
            if not any(s in ans_low for s in idk_signals):
                failures.append(f"MISSING IDK SIGNAL — bot answered confidently without KB basis")

        # Check should_contain_one (at least one must be present)
        should = p.get("should_contain_one", [])
        if should and not any(s.lower() in ans_low for s in should):
            failures.append(f"MISSING EXPECTED HEDGE — none of {should} found")

        print(f"  Response  : {ans[:300]}{'...' if len(ans) > 300 else ''}")

        if failures:
            failed += 1
            print(f"  [FAIL] {' | '.join(failures)}")
        else:
            passed += 1
            print(f"  [PASS]")

        # 10s pause between questions
        if i < len(PROBES) - 1:
            time.sleep(10)

    print("\n" + "="*70)
    print(f"  RESULT: {passed} PASS / {failed} FAIL out of {len(PROBES)}")
    print("="*70)

if __name__ == "__main__":
    run()
