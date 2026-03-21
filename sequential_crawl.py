"""
sequential_crawl.py — Crawl multiple DBs one at a time via the /admin/crawl SSE endpoint.
Reliable on Windows: no bash backgrounding, no httpx stream timeouts, plain requests.

Usage:
    python sequential_crawl.py
    python sequential_crawl.py --start-from tsc_pk   # resume from a specific DB
"""

import requests
import json
import time
import sys
import argparse
from datetime import datetime

BASE_URL = "http://localhost:8000"
PASSWORD = "admin123"
MAX_PAGES = 2000

CRAWL_TARGETS = [
    {"db_name": "tsc_pk",            "url": "https://thestationerycompany.pk"},
    {"db_name": "stationery_studio", "url": "https://stationerystudio.pk"},
    {"db_name": "estationery",       "url": "https://estationery.com.pk"},
    {"db_name": "chishti_sabri",     "url": "https://chishtisabristore.com"},
    {"db_name": "islamhub",          "url": "https://islamhub.pk"},
    {"db_name": "belony",            "url": "https://belony.pk"},
]


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    try:
        print(line, flush=True)
    except UnicodeEncodeError:
        print(line.encode("ascii", errors="replace").decode("ascii"), flush=True)
    with open("sequential_crawl.log", "a", encoding="utf-8") as f:
        f.write(line + "\n")


def wait_for_server(timeout=120):
    log("Waiting for server on port 8000...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{BASE_URL}/health", timeout=5)
            if r.status_code == 200:
                data = r.json()
                log(f"Server ready: {data}")
                return True
        except Exception:
            pass
        time.sleep(3)
    log("ERROR: Server not reachable after timeout")
    return False


def crawl_one(db_name, url):
    log(f"=== Starting crawl: {db_name} -> {url} ===")
    payload = {
        "password": PASSWORD,
        "url": url,
        "db_name": db_name,
        "max_pages": MAX_PAGES,
        "clear_before_crawl": False,
    }

    success = False
    chunks_done = 0
    pages_done = 0

    try:
        # stream=True + no timeout = reads SSE until server closes connection
        with requests.post(
            f"{BASE_URL}/admin/crawl",
            json=payload,
            stream=True,
            timeout=(10, None),   # (connect_timeout, read_timeout=None = infinite)
        ) as resp:
            if resp.status_code != 200:
                log(f"ERROR: HTTP {resp.status_code} — {resp.text[:200]}")
                return False

            for raw_line in resp.iter_lines(decode_unicode=True):
                if not raw_line:
                    continue
                # SSE lines start with "data: "
                line = raw_line.lstrip()
                if not line.startswith("data:"):
                    continue
                try:
                    data = json.loads(line[5:].strip())
                except json.JSONDecodeError:
                    continue

                if data.get("done"):
                    success = True
                    break

                msg = data.get("msg", "")
                if msg:
                    log(f"  [{db_name}] {msg}")
                    # Track progress
                    if "✅ Done!" in msg:
                        try:
                            chunks_done = int(msg.split()[1])
                        except Exception:
                            pass
                    if "/" in msg and ("✅" in msg or "⏭️" in msg):
                        try:
                            pages_done = int(msg.split("[")[1].split("/")[0])
                        except Exception:
                            pass

    except requests.exceptions.ConnectionError as e:
        log(f"ERROR: Connection lost during crawl of {db_name}: {e}")
        return False
    except Exception as e:
        log(f"ERROR: Unexpected error crawling {db_name}: {e}")
        return False

    log(f"=== Finished {db_name}: pages={pages_done} success={success} ===")
    return success


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-from", default=None, help="Resume from this db_name")
    args = parser.parse_args()

    if not wait_for_server():
        sys.exit(1)

    targets = CRAWL_TARGETS
    if args.start_from:
        names = [t["db_name"] for t in targets]
        if args.start_from not in names:
            log(f"ERROR: --start-from '{args.start_from}' not in target list: {names}")
            sys.exit(1)
        idx = names.index(args.start_from)
        targets = targets[idx:]
        log(f"Resuming from {args.start_from} ({len(targets)} DBs remaining)")

    log(f"Sequential crawl starting — {len(targets)} DBs to process")
    failed = []

    for i, target in enumerate(targets, 1):
        log(f"\n{'='*60}")
        log(f"DB {i}/{len(targets)}: {target['db_name']}")
        log(f"{'='*60}")

        ok = crawl_one(target["db_name"], target["url"])
        if not ok:
            failed.append(target["db_name"])
            log(f"WARNING: {target['db_name']} failed — continuing to next")

        # Brief pause between crawls to let Playwright/OS release resources
        if i < len(targets):
            log("Waiting 10s before next crawl...")
            time.sleep(10)

    log("\n=== ALL DONE ===")
    if failed:
        log(f"Failed DBs: {failed}")
        log("Retry failed DBs with: python sequential_crawl.py --start-from <db_name>")
    else:
        log("All DBs crawled successfully!")


if __name__ == "__main__":
    main()
