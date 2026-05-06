"""
Scraper for: https://cea.nic.in/comm-trans/national-committee-on-transmission/?lang=en
Download Minutes and MoM pdfs from the page.
"""

import os
import re
import asyncio
import aiohttp
from urllib.parse import unquote
from playwright.async_api import async_playwright

from pipeline.downloader.pdf_cache import get_pdf_cache

BASE_URL = "https://cea.nic.in/comm-trans/national-committee-on-transmission/?lang=en"
BASE_DIR = "source_output/CEA-NCT-Minutes"

CACHE_DB_PATH = None
CACHE_SOURCE_KEY = "nct_meetings"
CACHE_SOURCE_NAME = "CEA-NCT-Minutes"

DOWNLOAD_SEM = asyncio.Semaphore(10)


# ===== safe filename =====
def safe_filename(url: str) -> str:
    name = unquote(url.split("/")[-1].split("?")[0])

    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)

    # split ext
    if "." in name:
        stem, ext = name.rsplit(".", 1)
        ext = "." + ext.lower()
    else:
        stem, ext = name, ".pdf"

    # ==== Clean stem ====
    stem = stem.replace("_", " ")
    stem = re.sub(r"\s+", " ", stem).strip()

    m = re.search(r"\b(\d{1,3})(st|nd|rd|th)\b", stem, re.I)

    if m:
        ordinal = f"{m.group(1)}{m.group(2).lower()}"
        return f"{ordinal}_NCT_MoM{ext}"

    m = re.search(r"\b(\d{1,3})\b", stem)
    if m:
        num = int(m.group(1))

        # convert to ordinal
        if 10 <= num % 100 <= 20:
            suffix = "th"
        else:
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(num % 10, "th")

        return f"{num}{suffix}_NCT_MoM{ext}"

    return f"{stem}{ext}" if stem else f"file{ext}"


def get_cache():
    return get_pdf_cache(CACHE_DB_PATH, CACHE_SOURCE_KEY, CACHE_SOURCE_NAME)

# ===== Extract Links =====
async def extract_links():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        await page.goto(BASE_URL)
        await page.wait_for_selector("table")

        data = await page.evaluate("""() => {
            const results = [];
            const rows = document.querySelectorAll("table tbody tr");

            rows.forEach(row => {
                const links = row.querySelectorAll("a");

                links.forEach(link => {
                    if (!link.href) return;

                    const href = link.href.toLowerCase();
                    const text = link.innerText.toLowerCase();

                    // only PDF
                    if (!href.includes(".pdf")) return;

                    // capture both Minutes + MoM
                    if (text.includes("minutes") || text.includes("mom")) {
                        results.push({
                            url: link.href,
                            title: row.innerText.trim()
                        });
                    }
                });
            });

            return results;
        }""")

        await browser.close()
        return data

# ===== Download =====
async def async_download(session, url, dest, *, title: str | None = None):
    async with DOWNLOAD_SEM:
        try:
            if os.path.exists(dest):
                return

            cache = get_cache()
            pdf_name = os.path.basename(dest)
            if cache.is_cached(pdf_name, pdf_path=dest):
                return

            async with session.get(url) as resp:
                if resp.status != 200:
                    print(f"Failed {resp.status}: {url}")
                    return

                data = await resp.read()

            with open(dest, "wb") as f:
                f.write(data)

            cache.record_download(pdf_name=pdf_name, pdf_path=dest)

            print(f"Saved: {os.path.basename(dest)}")

        except Exception as e:
            print(f"Error: {url} → {e}")

# ===== Order + Plan =====
def reorder_and_plan(dest_dir, items):
    os.makedirs(dest_dir, exist_ok=True)

    existing = {}
    for f in os.listdir(dest_dir):
        if "_" in f:
            original = f.split("_", 1)[1]
            existing[original] = f

    tasks = []
    counter = 1

    for item in items:
        url = item["url"]
        name = safe_filename(url)

        new_name = f"{counter:02d}_{name}"
        new_path = os.path.join(dest_dir, new_name)

        if name in existing:
            old_path = os.path.join(dest_dir, existing[name])
            if old_path != new_path:
                os.rename(old_path, new_path)
        else:
            tasks.append((item, new_path))

        counter += 1

    return tasks

# ===== Main =====
async def main():
    print("Extracting links...")
    items = await extract_links()

    print(f"Found {len(items)} Minutes PDFs")

    # SSL bypass
    connector = aiohttp.TCPConnector(ssl=False)

    async with aiohttp.ClientSession(connector=connector) as session:
        planned = reorder_and_plan(BASE_DIR, items)

        print(f"New files to download: {len(planned)}")

        tasks = [
            async_download(session, item["url"], dest, title=item.get("title"))
            for item, dest in planned
        ]
        await asyncio.gather(*tasks)

    print("\nDone")


if __name__ == "__main__":
    asyncio.run(main())
