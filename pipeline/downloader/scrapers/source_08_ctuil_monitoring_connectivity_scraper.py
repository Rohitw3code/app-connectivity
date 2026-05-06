"""
Scraper for: https://www.ctuil.in/revocations

Downloads and organizes PDFs into two specific folders:
1. Expected Revocation Under 24.6
2. Connectivity Gratees Excluded From Revocation
"""

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote

BASE_URL = "https://www.ctuil.in/revocations"

# Keep folder names exactly as requested.
TARGETS = [
    {
        "name": "Expected Revocation due to Non-Compliance of 24.6 of GNA Regulation",
        "column_index": 1,
        "dest_dir": "uploads/CTUIL-Revocations-PDFs/Expected Revocation Under 24.6",
    },
    {
        "name": "Connectivity/Grantees Excluded from Expected Revocation list",
        "column_index": 3,
        "dest_dir": "uploads/CTUIL-Revocations-PDFs/Connectivity Gratees Excluded From Revocation",
    },
]

MAX_WORKERS = 10


def safe_filename(url: str) -> str:
    name = unquote(url.split("/")[-1].split("?")[0])

    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)

    # split extension
    if "." in name:
        stem, ext = name.rsplit(".", 1)
        ext = "." + ext.lower()
    else:
        stem, ext = name, ".pdf"

    stem = re.sub(r"^\d{6,}", "", stem).lstrip("_- ").strip()

    # normalize
    stem = stem.replace("_", " ")
    stem = re.sub(r"\s+", " ", stem).strip()

    lower = stem.lower()

    date = None

    # Case 1: "upto May'26", "upto Mar-26"
    m = re.search(r"upto\s+([A-Za-z]+['\-\d]*)", lower)
    if m:
        date = m.group(1)

    # Case 2: "published in Jun25"
    if not date:
        m = re.search(r"published\s+in\s+([A-Za-z]+\d{2,4})", lower)
        if m:
            date = m.group(1)

    # normalize date formatting (optional cleanup)
    if date:
        date = date.replace("'", "").replace("-", "")
        date = date.strip()

    if "excluded" in lower:
        if date:
            return f"Excluded from upto {date}{ext}"
        return f"Excluded from{ext}"

    if "revocation" in lower or "upto" in lower:
        if date:
            return f"Revocation upto {date}{ext}"
        return f"Revocation{ext}"

    return f"{stem}{ext}" if stem else f"file{ext}"

def fetch_soup() -> BeautifulSoup:
    response = requests.get(BASE_URL, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")

def fetch_pdf_links_from_column(soup: BeautifulSoup, column_index: int) -> list[str]:
    table = soup.select_one("table.tableStyle")
    if not table:
        return []

    pdf_links = []
    seen = set()

    for row in table.select("tr"):
        cols = row.find_all("td")
        if len(cols) <= column_index:
            continue

        a = cols[column_index].find("a", href=True)
        if not a:
            continue

        href = a["href"].strip()
        if not href:
            continue

        full_url = urljoin(BASE_URL, href)
        if ".pdf" in full_url.lower() and full_url not in seen:
            seen.add(full_url)
            pdf_links.append(full_url)

    return pdf_links

def download_one(url: str, dest: str) -> None:
    try:
        if os.path.exists(dest):
            return

        resp = requests.get(url, timeout=60)
        if resp.status_code != 200:
            print(f"[SKIP] {url} (HTTP {resp.status_code})")
            return

        with open(dest, "wb") as f:
            f.write(resp.content)

        print(f"[OK] {os.path.basename(dest)}")

    except Exception as e:
        print(f"[ERROR] {url}  {e}")

def reorder_and_plan(dest_dir: str, urls: list[str]) -> list[tuple[str, str]]:
    os.makedirs(dest_dir, exist_ok=True)

    existing = {}

    # Map original filename -> existing file (when already numbered as NN_original).
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
            if old_path != new_path and os.path.exists(old_path):
                os.rename(old_path, new_path)
        else:
            tasks.append((url, new_path))

        counter += 1

    return tasks

def process_target(
    soup: BeautifulSoup,
    name: str,
    column_index: int,
    dest_dir: str,
) -> None:
    urls = fetch_pdf_links_from_column(soup, column_index)

    print(f"\n=== {name} ===")
    print(f"Total PDFs found: {len(urls)}")

    # Site already returns latest first.
    planned = reorder_and_plan(dest_dir, urls)
    print(f"New files to download: {len(planned)}")

    if planned:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = [pool.submit(download_one, url, dest) for url, dest in planned]
            for _ in as_completed(futures):
                pass

def main() -> None:
    soup = fetch_soup()

    for target in TARGETS:
        process_target(
            soup=soup,
            name=target["name"],
            column_index=target["column_index"],
            dest_dir=target["dest_dir"],
        )

    print("\nDone.")


if __name__ == "__main__":
    main()
