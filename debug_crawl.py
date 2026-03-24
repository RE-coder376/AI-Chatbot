import asyncio
from playwright.async_api import async_playwright

async def debug():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        url = "https://agentfactory.pk"
        print(f"Visiting {url}...")
        try:
            resp = await page.goto(url, wait_until="networkidle", timeout=60000)
            print(f"Status: {resp.status}")
            content = await page.content()
            print(f"Content Length: {len(content)}")
            text = await page.evaluate("() => document.body.innerText")
            print(f"Text Length: {len(text)}")
            print(f"First 500 chars of text:\n{text[:500]}")
            
            # Check for bot detection keywords
            if "captcha" in content.lower() or "bot" in content.lower() or "security check" in content.lower():
                print("--- BOT DETECTION DETECTED ---")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug())
