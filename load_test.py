"""
Concurrent load test for AI Chatbot server.
Tests server concurrency WITHOUT burning LLM quota (uses stream=False, short questions).

Usage:
    python load_test.py [--url http://localhost:8000] [--concurrency 10] [--total 50]

Results: pass/fail per request, latency stats (p50/p95/p99), error breakdown.
"""
import asyncio
import argparse
import time
import statistics
from collections import defaultdict
import aiohttp

BASE_URL = "http://localhost:8000"
DB = "agentfactory"  # change if testing different DB

QUESTIONS = [
    "What is AgentFactory?",
    "Who is Zia Khan?",
    "What courses are available?",
    "How do I enroll?",
    "What is the price?",
    "Who is Wania Kazmi?",
    "Tell me about Panaversity",
    "What is an AI agent?",
    "How long are the courses?",
    "Is there a certificate?",
]

results = []
errors = defaultdict(int)


async def single_request(session: aiohttp.ClientSession, question: str, req_id: int):
    start = time.monotonic()
    try:
        payload = {"question": question, "history": [], "stream": False}
        async with session.post(
            f"{BASE_URL}/chat",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=90),
        ) as resp:
            latency = (time.monotonic() - start) * 1000
            body = await resp.json(content_type=None)
            answer = body.get("answer", "")
            ok = resp.status == 200 and len(answer) > 5
            status_label = "PASS" if ok else f"FAIL({resp.status})"
            if not ok:
                errors[f"HTTP {resp.status}"] += 1
            print(f"  [{req_id:03d}] {status_label:12s}  {latency:6.0f}ms  q={question[:40]!r}")
            return {"ok": ok, "latency_ms": latency, "status": resp.status}
    except asyncio.TimeoutError:
        latency = (time.monotonic() - start) * 1000
        errors["timeout"] += 1
        print(f"  [{req_id:03d}] TIMEOUT       {latency:6.0f}ms  q={question[:40]!r}")
        return {"ok": False, "latency_ms": latency, "status": 0}
    except Exception as exc:
        latency = (time.monotonic() - start) * 1000
        errors[type(exc).__name__] += 1
        print(f"  [{req_id:03d}] ERROR         {latency:6.0f}ms  {exc}")
        return {"ok": False, "latency_ms": latency, "status": 0}


async def run_batch(session, batch, offset):
    tasks = [
        single_request(session, QUESTIONS[i % len(QUESTIONS)], offset + i)
        for i in range(len(batch))
    ]
    return await asyncio.gather(*tasks)


async def main(url: str, concurrency: int, total: int):
    global BASE_URL
    BASE_URL = url.rstrip("/")

    print(f"\nLoad test: {total} requests, concurrency={concurrency}, target={BASE_URL}")
    print(f"DB: {DB}\n{'─'*60}")

    connector = aiohttp.TCPConnector(limit=concurrency + 5)
    async with aiohttp.ClientSession(connector=connector) as session:
        wall_start = time.monotonic()
        all_results = []
        for batch_start in range(0, total, concurrency):
            batch_size = min(concurrency, total - batch_start)
            batch = list(range(batch_size))
            batch_results = await run_batch(session, batch, batch_start)
            all_results.extend(batch_results)
        wall_elapsed = time.monotonic() - wall_start

    passed = sum(1 for r in all_results if r["ok"])
    failed = total - passed
    latencies = [r["latency_ms"] for r in all_results]
    latencies_sorted = sorted(latencies)

    def pct(p):
        idx = int(len(latencies_sorted) * p / 100)
        return latencies_sorted[min(idx, len(latencies_sorted) - 1)]

    print(f"\n{'═'*60}")
    print(f"RESULTS: {passed}/{total} PASS  ({failed} FAIL)")
    print(f"Wall time: {wall_elapsed:.1f}s  |  Throughput: {total/wall_elapsed:.1f} req/s")
    print(f"Latency — p50:{pct(50):.0f}ms  p95:{pct(95):.0f}ms  p99:{pct(99):.0f}ms  max:{max(latencies):.0f}ms")
    if errors:
        print(f"Errors: {dict(errors)}")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--total", type=int, default=50)
    args = parser.parse_args()
    asyncio.run(main(args.url, args.concurrency, args.total))
