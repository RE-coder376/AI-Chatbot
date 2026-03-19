
import httpx
import json
import time
import sys

# Ensure UTF-8 output for emojis in Windows logs
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def trigger_crawl(db_name, url):
    payload = {
        "password": "admin123",
        "url": url,
        "db_name": db_name,
        "max_pages": 2000,
        "clear_before_crawl": False
    }
    print(f"\n--- Starting Crawl for {db_name} ({url}) ---")
    try:
        with httpx.stream('POST', 'http://localhost:8000/admin/crawl', json=payload, timeout=None) as response:
            if response.status_code != 200:
                print(f"[{db_name}] Error: {response.status_code} - {response.read().decode()}")
                return
            for line in response.iter_lines():
                if line.startswith("data: "):
                    try:
                        data = json.loads(line[6:])
                        if "msg" in data:
                            print(f"[{db_name}] {data['msg']}")
                        if data.get("done"):
                            print(f"[{db_name}] Crawl Complete.")
                            break
                    except Exception as e:
                        print(f"[{db_name}] Parse Error: {e}")
    except Exception as e:
        print(f"[{db_name}] Request Failed: {e}")
    print(f"--- Finished {db_name} ---\n")

if __name__ == "__main__":
    tasks = [
        {"db": "tsc_pk", "url": "https://thestationerycompany.pk"},
        {"db": "stationery_studio", "url": "https://stationerystudio.pk"},
        {"db": "estationery", "url": "https://estationery.com.pk"},
        {"db": "chishti_sabri", "url": "https://chishtisabristore.com"},
        {"db": "islamhub", "url": "https://islamhub.pk"},
        {"db": "belony", "url": "https://belony.pk"}
    ]
    
    for task in tasks:
        trigger_crawl(task["db"], task["url"])
        time.sleep(2)
