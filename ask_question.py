
import httpx
import json

url = "http://localhost:8000/chat"
question = "Summarize the 'Turing LLMOps Proprietary Intelligence' curriculum, focusing on the 'data engineering fine-tuning' section. Provide this summary in Spanish."

payload = {"question": question, "stream": False}
headers = {"Content-Type": "application/json"}

try:
    response = httpx.post(url, json=payload, headers=headers, timeout=60)
    response.raise_for_status()
    answer = response.json().get("answer", "No answer received.")
    print("Question:", question)
    print("Answer:", answer)

except httpx.RequestError as e:
    print(f"An error occurred while requesting {e.request.url!r}: {e}")
except httpx.HTTPStatusError as e:
    print(f"Error response {e.response.status_code} while requesting {e.request.url!r}.")
except json.JSONDecodeError:
    print("Error: Could not decode JSON response.")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
