import asyncio
from playwright.async_api import async_playwright
import re

def _clean_text(text: str) -> str:
    return text.replace("\u200b", "")

async def debug():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        url = "https://estationery.com.pk"
        print(f"Visiting {url}...")
        try:
            await page.goto(url, wait_until="load", timeout=60000)
            await page.wait_for_timeout(5000)
            
            title = await page.title()
            print(f"Title: {title}")
            
            text = await page.evaluate("""() => {
                const selectors = ['main', 'article', '.content', '.product-details', '#description'];
                for (let s of selectors) {
                    let el = document.querySelector(s);
                    if (el && el.innerText.trim().length > 100) return el.innerText;
                }
                return document.body ? document.body.innerText : '';
            }""")
            
            text = re.sub(r'\s+', ' ', _clean_text(text or "")).strip()
            print(f"Text Length: {len(text)}")
            print(f"First 500 chars:\n{text[:500]}")
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(debug())
