"""
Scraper for: https://www.ctuil.in/bidding-calendar
Download PDFs from "Bidding Calendar" table.
"""

import os
import re
import asyncio
import aiohttp
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote

from pipeline.downloader.pdf_cache import get_pdf_cache

BASE_URL = "https://www.ctuil.in/bidding-calendar"
DOWNLOAD_DIR = "source_output/CTUIL-Bidding-Calendar"

CACHE_DB_PATH = None
CACHE_SOURCE_KEY = "bidding_calendar"
CACHE_SOURCE_NAME = "CTUIL-Bidding-Calendar"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

DOWNLOAD_SEM = asyncio.Semaphore(10)


# ==== Safe Filename ====
def safe_filename(url: str) -> str:
    name = unquote(url.split("/")[-1].split("?")[0])

    # remove illegal chars
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)

    # split extension
    if "." in name:
        stem, ext = name.rsplit(".", 1)
        ext = "." + ext.lower()
    else:
        stem, ext = name, ".pdf"

    stem = stem.replace("_", " ")

    # remove leading random numbers
    stem = re.sub(r"^\d+\s*", "", stem)

    # remove junk words
    stem = re.sub(
        r"(?i)\b(annexure[-\s]*a|draft|approved|final|dated|as on dated|as on)\b",
        "",
        stem,
    )

    # normalize spaces
    stem = re.sub(r"\s+", " ", stem).strip()

    # ---- EXTRACT DATE ----
    date_match = re.search(
        r"(\d{2}[-\.]\d{2}[-\.]\d{2,4})", stem
    )

    date = None
    if date_match:
        date = date_match.group(1).replace(".", "-")

        # normalize 2-digit year → 4-digit
        parts = date.split("-")
        if len(parts[2]) == 2:
            year = int(parts[2])
            parts[2] = f"20{year:02d}"
        date = "-".join(parts)

    # ---- BUILD FINAL NAME ----
    if date:
        return f"Bidding Calendar {date}{ext}"
    else:
        return f"Bidding Calendar{ext}"

# ==== Fetch Links ====
def fetch_pdf_links():
    response = requests.get(BASE_URL)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    pdf_links = []
    seen = set()

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        href = a["href"]

        if "bidding calendar" in text and href.lower().endswith(".pdf"):
            full_url = urljoin(BASE_URL, href)

            if full_url not in seen:
                seen.add(full_url)
                pdf_links.append(full_url)

    return pdf_links


def get_cache():
    return get_pdf_cache(CACHE_DB_PATH, CACHE_SOURCE_KEY, CACHE_SOURCE_NAME)

# ==== Download ====
async def async_download(session, url, dest):
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
                    return
                data = await resp.read()

            with open(dest, "wb") as f:
                f.write(data)

            cache.record_download(pdf_name=pdf_name, pdf_path=dest)

            print(f"[OK] {os.path.basename(dest)}")

        except Exception as e:
            print(f"[ERROR] {url} → {e}")

# ==== Shift + Incremental ====
def reorder_and_plan(dest_dir, urls):
    os.makedirs(dest_dir, exist_ok=True)

    existing = {}
    for f in os.listdir(dest_dir):
        if "_" in f:
            original = f.split("_", 1)[1]
            existing[original] = f

    tasks = []
    counter = 1

    for url in urls:
        name = safe_filename(url)
        new_name = f"{counter:02d}_{name}"
        new_path = os.path.join(dest_dir, new_name)

        if name in existing:
            old_path = os.path.join(dest_dir, existing[name])

            if old_path != new_path:
                os.rename(old_path, new_path)
        else:
            tasks.append((url, new_path))

        counter += 1

    return tasks

# ==== Main ====
async def main():
    urls = fetch_pdf_links()

    print(f"Total PDFs found: {len(urls)}")

    # IMPORTANT: assume site already gives latest first
    planned = reorder_and_plan(DOWNLOAD_DIR, urls)

    print(f"New files to download: {len(planned)}")

    async with aiohttp.ClientSession() as session:
        tasks = [async_download(session, url, dest) for url, dest in planned]
        await asyncio.gather(*tasks)

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
