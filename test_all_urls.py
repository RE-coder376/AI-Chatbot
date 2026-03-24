
import asyncio
from playwright.async_api import async_playwright

async def test_url(p, url):
    browser = await p.chromium.launch(headless=True)
    page = await browser.new_page()
    try:
        print(f"Testing {url}...")
        resp = await page.goto(url, timeout=30000)
        print(f"  Status: {resp.status}")
        print(f"  Title: {await page.title()}")
    except Exception as e:
        print(f"  Error: {e}")
    await browser.close()

async def run():
    urls = [
        'https://agentfactory.io',
        'https://agentfactory.panaversity.org',
        'https://thestationerycompany.pk',
        'https://stationerystudio.pk',
        'https://estationery.com.pk',
        'https://chishtisabristore.com'
    ]
    async with async_playwright() as p:
        for url in urls:
            await test_url(p, url)

if __name__ == "__main__":
    asyncio.run(run())
