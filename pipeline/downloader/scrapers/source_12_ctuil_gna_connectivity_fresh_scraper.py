"""
Scraper for: https://www.ctuil.in/gna2022updates
Download PDFs from the "Connectivity Fresh" column (latest 6 months).
"""

import os
import re
import asyncio
import aiohttp
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote

from pipeline.downloader.pdf_cache import get_pdf_cache

# ==== Config ====
BASE_URL = "https://www.ctuil.in/gna2022updates"
DOWNLOAD_DIR = "source_output/CTUIL-GNA-Connectivity-Fresh"

CACHE_DB_PATH = None
CACHE_SOURCE_KEY = "gna_connectivity_fresh"
CACHE_SOURCE_NAME = "CTUIL-GNA-Connectivity-Fresh"

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
DOWNLOAD_SEM = asyncio.Semaphore(10)

# ==== Fetch Connectivity Fresh Links ====
def fetch_connectivity_fresh_links() -> list[dict]:
    """
    Parse the GNA 2022 updates page and extract the 'Connectivity Fresh' PDF links.
    """
    res = requests.get(BASE_URL, timeout=30)
    res.raise_for_status()

    soup = BeautifulSoup(res.text, "html.parser")

    # Locate the main data table (the page has a single <table>)
    table = soup.find("table")
    if not table:
        raise RuntimeError("Could not locate the data table on the page.")

    rows = table.find_all("tr")

    results: list[dict] = []

    for row in rows:
        cells = row.find_all("td")
        if len(cells) <= 3:
            continue

        month_text = cells[0].get_text(strip=True)
        if not month_text:
            continue

        target_cell = cells[3]
        anchor = target_cell.find("a", href=True)

        if anchor:
            href = anchor["href"].strip()
            full_url = urljoin(BASE_URL, href)
            results.append({"month": month_text, "url": full_url})

        # limiting to 6 months
        if len(results) >= 6:
            break

    print(f"\n[DEBUG] Extracted {len(results)} Connectivity Fresh links:")
    for r in results:
        print(f"  {r['month']}  ->  {r['url']}")

    return results


def get_cache():
    return get_pdf_cache(CACHE_DB_PATH, CACHE_SOURCE_KEY, CACHE_SOURCE_NAME)


# ==== Download ====
async def async_download(session: aiohttp.ClientSession, url: str, dest: str, *, month: str | None = None):
    async with DOWNLOAD_SEM:
        try:
            if os.path.exists(dest):
                print(f"[SKIP] {os.path.basename(dest)}")
                return

            cache = get_cache()
            pdf_name = os.path.basename(dest)
            if cache.is_cached(pdf_name, pdf_path=dest):
                print(f"[SKIP] {os.path.basename(dest)}")
                return

            async with session.get(url) as resp:
                if resp.status != 200:
                    print(f"[FAIL] {url}  (HTTP {resp.status})")
                    return

                data = await resp.read()

            with open(dest, "wb") as f:
                f.write(data)

            cache.record_download(pdf_name=pdf_name, pdf_path=dest)

            print(f"[OK] {os.path.basename(dest)}")

        except Exception as e:
            print(f"[ERROR] {url} -> {e}")


# ==== Shift + Incremental ====
def reorder_and_plan(dest_dir: str, entries: list[dict]) -> list[tuple[str, str]]:
    """
    Naming convention:  01_Dec-25 GNA.pdf, 02_Nov-25 GNA.pdf, ...

    - If a month already exists on disk (at any position), rename it
      to the correct new position.
    - If a month is new, queue it for download.
    - Old files that no longer appear in the latest 6 are removed.
    """
    os.makedirs(dest_dir, exist_ok=True)

    # Build map: month -> existing filename on disk
    existing_by_month: dict[str, str] = {}
    for f in os.listdir(dest_dir):
        if not f.endswith(".pdf"):
            continue
        # Pattern: NN_Month-YY GNA.pdf  ->  extract "Month-YY"
        match = re.match(r"^\d{2}_(.+?) GNA\.pdf$", f)
        if match:
            existing_by_month[match.group(1)] = f

    # Months that will remain after this run
    target_months = {e["month"] for e in entries}

    # Remove old files that fell out of the latest 6
    for month, fname in list(existing_by_month.items()):
        if month not in target_months:
            old_path = os.path.join(dest_dir, fname)
            os.remove(old_path)
            print(f"[DEL] {fname}  (no longer in latest 6)")
            del existing_by_month[month]

    tasks: list[tuple[str, str]] = []

    for idx, entry in enumerate(entries, start=1):
        month = entry["month"]
        new_name = f"{idx:02d}_{month} GNA.pdf"
        new_path = os.path.join(dest_dir, new_name)

        if month in existing_by_month:
            old_name = existing_by_month[month]
            old_path = os.path.join(dest_dir, old_name)

            # Rename if position changed
            if old_name != new_name:
                # Use temp name to avoid collision during shift
                tmp_path = new_path + ".tmp"
                os.rename(old_path, tmp_path)
                os.rename(tmp_path, new_path)
                print(f"[SHIFT] {old_name} -> {new_name}")
        else:
            # New month — queue for download
            tasks.append((entry, new_path))

    return tasks


# ==== Main ====
async def main():
    entries = fetch_connectivity_fresh_links()

    print(f"\n[INFO] Total relevant links: {len(entries)}")

    planned = reorder_and_plan(DOWNLOAD_DIR, entries)

    print(f"[INFO] New files to download: {len(planned)}")

    async with aiohttp.ClientSession() as session:
        coros = [
            async_download(session, entry["url"], dest, month=entry.get("month"))
            for entry, dest in planned
        ]
        await asyncio.gather(*coros)

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
