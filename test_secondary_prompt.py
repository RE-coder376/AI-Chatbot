
import asyncio
import json
import httpx

async def test_chat():
    url = "http://localhost:8000/chat"
    # Testing the specific query asked by the user
    payload = {
        "question": "What is the 37th highest rated anime?",
        "visitor_id": "test_visitor_123",
        "stream": True
    }
    
    print(f"Testing /chat with query: {payload['question']}")
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            async with client.stream("POST", url, json=payload) as response:
                if response.status_code != 200:
                    print(f"Error: Server returned status {response.status_code}")
                    print(await response.aread())
                    return

                full_response = ""
                async for line in response.aiter_lines():
                    if line.startswith("data: "):
                        try:
                            data = json.loads(line[6:])
                            if data["type"] == "chunk":
                                content = data["content"]
                                print(content, end="", flush=True)
                                full_response += content
                            elif data["type"] == "done":
                                print("\n\n[Done]")
                        except json.JSONDecodeError:
                            pass
                
                # Check for secondary prompt influence
                slang = ["Sugoi", "Kawaii", "Nani", "Otaku", "Anime"]
                found_slang = [s for s in slang if s.lower() in full_response.lower()]
                if found_slang:
                    print(f"\nSUCCESS: Found secondary prompt influence: {found_slang}")
        except Exception as e:
            print(f"\nException: {e}")

if __name__ == "__main__":
    asyncio.run(test_chat())
