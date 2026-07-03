"""One-shot client-activity monitor — read-only, never touches the bot or analytics.

  python tools/client_monitor.py <db> [--recent N]

Shows: query volume + sessions, the N most recent real questions (what the owner
or his customers actually typed), most-asked, unanswered questions (knowledge
gaps), captured leads, CSAT, and the last unhandled /chat error. Everything
comes from admin GET endpoints, so running this adds NOTHING to the client's
analytics (unlike a live probe).
"""
import json
import os
import sys
import urllib.parse
import urllib.request

BASE = "https://re-coder376--ai-chatbot-serve.modal.run"
PW = os.environ.get("ADMIN_PASSWORD", "iaah2006")


def get(path, **params):
    params.setdefault("password", PW)
    q = urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(f"{BASE}{path}?{q}", timeout=60) as r:
            return json.load(r)
    except Exception as e:
        return {"_error": str(e)}


def main():
    if len(sys.argv) < 2:
        sys.exit("usage: python tools/client_monitor.py <db> [--recent N]")
    db = sys.argv[1]
    n_recent = int(sys.argv[sys.argv.index("--recent") + 1]) if "--recent" in sys.argv else 10

    a = get("/admin/analytics", db_name=db)
    print(f"=== {db} ===")
    print(f"queries: {a.get('total_queries', '?')} | sessions: {a.get('total_sessions', '?')} "
          f"| avg CSAT: {a.get('avg_csat')} ({a.get('total_ratings', 0)} ratings)")

    recent = a.get("most_recent") or []
    print(f"\n-- last {min(n_recent, len(recent))} questions --")
    for item in recent[:n_recent]:
        if isinstance(item, dict):
            print(f"  {str(item.get('t', ''))[:16]}  {item.get('q', '')}")
        else:
            print(f"  {item}")

    asked = a.get("most_asked") or []
    if asked:
        print("\n-- most asked --")
        for item in asked[:5]:
            print(f"  {item}")

    gaps = get("/admin/knowledge-gaps", db_name=db)
    gap_list = gaps if isinstance(gaps, list) else (gaps.get("gaps") or gaps.get("knowledge_gaps") or [])
    if isinstance(gap_list, list) and gap_list:
        print(f"\n-- unanswered (knowledge gaps): {len(gap_list)} --")
        for g in gap_list[:5]:
            print(f"  {g.get('q', g) if isinstance(g, dict) else g}")

    leads = get("/admin/leads", db_name=db)
    if isinstance(leads.get("count"), int):
        print(f"\n-- leads: {leads['count']} --")
        for l in (leads.get("leads") or [])[:5]:
            print(f"  {l.get('name', '?')} | {l.get('email', '')} {l.get('whatsapp', '')} | {str(l.get('timestamp', ''))[:16]}")

    err = get("/admin/chat-last-error", db_name=db)
    last = err.get("last_error") or {}
    if last:
        print(f"\n-- LAST CHAT ERROR --\n  {json.dumps(last)[:300]}")
    else:
        print("\nno unhandled chat errors.")
    print("\nlatency/deep logs: modal app logs ai-chatbot  (grep RETRIEVE_MS / 429 / Traceback)")


if __name__ == "__main__":
    main()
