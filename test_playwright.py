
import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            print("Going to https://agentfactory.io...")
            resp = await page.goto("https://agentfactory.io", timeout=30000)
            print(f"Status: {resp.status}")
            title = await page.title()
            print(f"Title: {title}")
            content = await page.content()
            print(f"Content length: {len(content)}")
        except Exception as e:
            print(f"Error: {e}")
        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
