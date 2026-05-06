"""
Scraper for: https://ctuil.in/complianceandfc
Download PDFs from "Connectivity Grantees" table.
"""
import os
import re
import asyncio
import aiohttp
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote

# ==== Config ====
BASE_URL = "https://ctuil.in/complianceandfc"
DOWNLOAD_DIR = "uploads/CTUIL-Compliance-PDFs"

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

    # removes long leading numeric sequences like 176657025776
    stem = re.sub(r"^\d{6,}", "", stem)

    # clean leftover separators/spaces
    stem = stem.lstrip("_- ").strip()

    return f"{stem}{ext}" if stem else f"file{ext}"

# ==== Fetch Main Links ====
def fetch_main_links():
    res = requests.get(BASE_URL)
    res.raise_for_status()

    soup = BeautifulSoup(res.text, "html.parser")

    links = []
    seen = set()

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()

        # ==== STRICT FILTER ====
        if "connectivity grantees" in text:
            href = a["href"].strip()
            full_url = urljoin(BASE_URL, href)

            if full_url not in seen:
                seen.add(full_url)
                links.append(full_url)

    # DEBUG (remove later if you want)
    print("\n[DEBUG] Extracted Links:")
    for l in links:
        print(l)

    return links

# ==== Download ====
async def async_download(session, url, dest):
    async with DOWNLOAD_SEM:
        try:
            if os.path.exists(dest):
                return

            async with session.get(url) as resp:
                if resp.status != 200:
                    print(f"[FAIL] {url}")
                    return

                data = await resp.read()

            with open(dest, "wb") as f:
                f.write(data)

            print(f"[OK] {os.path.basename(dest)}")

        except Exception as e:
            print(f"[ERROR] {url} → {e}")

# ==== Shift + Incremental  ====
def reorder_and_plan(dest_dir, urls):
    os.makedirs(dest_dir, exist_ok=True)

    # ==== Existing File Map ====
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

            # ==== Rename (Shift Logic) ====
            if old_path != new_path:
                os.rename(old_path, new_path)
        else:
            # ==== New File ====
            tasks.append((url, new_path))

        counter += 1

    return tasks

# ==== Main ====
async def main():
    urls = fetch_main_links()

    print(f"\n[INFO] Total relevant links: {len(urls)}")

    planned = reorder_and_plan(DOWNLOAD_DIR, urls)

    print(f"[INFO] New files to download: {len(planned)}")

    async with aiohttp.ClientSession() as session:
        tasks = [async_download(session, url, dest) for url, dest in planned]
        await asyncio.gather(*tasks)

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())