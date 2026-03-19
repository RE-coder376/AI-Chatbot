import httpx
import time
import json
from pathlib import Path

def trigger_crawl(url, db_name, clear=False):
    try:
        print(f"--- Crawl Triggered for {db_name} ({url}) ---")
        with httpx.stream('POST', 'http://localhost:8000/admin/crawl', 
                         json={'password': 'admin123', 'url': url, 'db_name': db_name, 'clear_before_crawl': clear},
                         timeout=None) as response:
            if response.status_code != 200:
                print(f"Error: {response.status_code} - {response.read().decode()}")
                return
            for line in response.iter_lines():
                if line.startswith("data: "):
                    try:
                        data = json.loads(line[6:])
                        if "msg" in data:
                            print(f"[{db_name}] {data['msg']}")
                        if data.get("done"):
                            break
                    except:
                        pass
        print(f"--- Finished {db_name} ---")
    except Exception as e:
        print(f"Error triggering crawl for {db_name}: {e}")

if __name__ == '__main__':
    brands = [
        {'url': 'https://agentfactory.panaversity.org', 'db': 'agentfactory'},
        {'url': 'https://thestationerycompany.pk', 'db': 'tsc_pk'},
        {'url': 'https://stationerystudio.pk', 'db': 'stationery_studio'},
        {'url': 'https://estationery.com.pk', 'db': 'estationery'},
        {'url': 'https://chishtisabristore.com', 'db': 'chishti_sabri'}
    ]

    print('Starting crawl process...')

    # Clear agentfactory first for a clean multilingual crawl
    print(f'Clearing and triggering crawl for {brands[0]["db"]}...')
    trigger_crawl(brands[0]['url'], brands[0]['db'], clear=True)
    time.sleep(10) # Give it a moment before starting others

    for brand in brands[1:]:
        print(f'Triggering crawl for {brand["db"]}...')
        trigger_crawl(brand['url'], brand['db'])
        time.sleep(5) # Small delay between subsequent crawls

    print('All crawls triggered. Check logs for progress.')
