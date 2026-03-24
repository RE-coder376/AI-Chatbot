import requests, json, time

PASSWORD = "iaah2006"
TARGET_DB = "anime"
BASE_URL = "http://localhost:8000"

def ingest(content_dict):
    r = requests.post(f"{BASE_URL}/admin/ingest/smart-text", json={
        "password": PASSWORD,
        "content": json.dumps(content_dict),
        "format": "json",
        "target_db": TARGET_DB
    })
    return r.json()

def fetch_and_ingest():
    print("Fetching top anime from Jikan API...")
    page = 1
    total = 0
    while total < 100:
        res = requests.get(f"https://api.jikan.moe/v4/top/anime?page={page}&limit=25")
        data = res.json()
        anime_list = data.get("data", [])
        if not anime_list:
            break
        for anime in anime_list:
            payload = {
                "title": anime.get("title", ""),
                "title_english": anime.get("title_english", ""),
                "score": anime.get("score", ""),
                "rank": anime.get("rank", ""),
                "episodes": anime.get("episodes", ""),
                "status": anime.get("status", ""),
                "year": anime.get("year", ""),
                "synopsis": (anime.get("synopsis") or "")[:800],
                "genres": ", ".join(g["name"] for g in anime.get("genres", [])),
                "studios": ", ".join(s["name"] for s in anime.get("studios", [])),
                "type": anime.get("type", ""),
            }
            result = ingest(payload)
            chunks = result.get("chunks", 0)
            print(f"  [{total+1}] {payload['title']} → {chunks} chunk(s)")
            total += 1
            time.sleep(0.4)  # Jikan rate limit: 3 req/sec
        page += 1
        time.sleep(1)
    print(f"\nDone! Ingested {total} anime into '{TARGET_DB}' DB.")

if __name__ == "__main__":
    fetch_and_ingest()
