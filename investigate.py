
import asyncio
from playwright.async_api import async_playwright

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        page.on("console", lambda msg: print(f"CONSOLE [{msg.type}]: {msg.text}"))

        print("Navigating...")
        await page.goto("http://localhost:8000/web/index.html", wait_until="load")

        print("Waiting 15 seconds...")
        await asyncio.sleep(15)

        # Check content div details
        details = await page.evaluate("""() => {
            const els = document.querySelectorAll('#content');
            return Array.from(els).map(el => ({
                id: el.id,
                className: el.className,
                innerHTML: el.innerHTML,
                innerText: el.innerText,
                display: window.getComputedStyle(el).display,
                visibility: window.getComputedStyle(el).visibility,
                opacity: window.getComputedStyle(el).opacity,
                childCount: el.children.length
            }));
        }""")
        
        print(f"DEBUG: Found {len(details)} elements with id='content'")
        for i, d in enumerate(details):
            print(f"Element {i}:")
            print(f"  Class: {d['className']}")
            print(f"  Display: {d['display']}, Visibility: {d['visibility']}, Opacity: {d['opacity']}")
            print(f"  Child count: {d['childCount']}")
            print(f"  Inner text (first 100 chars): {d['innerText'][:100]!r}")
            # print(f"  Inner HTML (first 100 chars): {d['innerHTML'][:100]!r}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
