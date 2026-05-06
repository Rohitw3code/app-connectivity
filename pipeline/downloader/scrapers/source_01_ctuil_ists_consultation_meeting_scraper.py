"""
Scraper for: https://ctuil.in/ists-consultation-meeting
Downloads PDFs from "Agenda" and "Minutes" columns for each region.

Output layout:
    source_output/CTUIL-ISTS-CMETS/agenda/   — Agenda PDFs
    source_output/CTUIL-ISTS-CMETS/minutes/  — Minutes PDFs
"""

import os
import re
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from collections import defaultdict

from pipeline.downloader.pdf_cache import get_pdf_cache

# ==== Config ====
BASE_URL   = "https://ctuil.in/ists-consultation-meeting"
SITE_ROOT  = "https://ctuil.in"
OUTPUT_DIR = "source_output/CTUIL-ISTS-CMETS"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": BASE_URL,
}

CACHE_DB_PATH = None
CACHE_SOURCE_KEY = "cmets"
CACHE_SOURCE_NAME = "CTUIL-ISTS-CMETS"

REGION_MAP = {
    "northern region":      "Northern Region",
    "western region":       "Western Region",
    "southern region":      "Southern Region",
    "eastern region":       "Eastern Region",
    "north eastern region": "North Eastern Region",
    "north-eastern region": "North Eastern Region",
}

PAGE_SEM     = asyncio.Semaphore(10)
DOWNLOAD_SEM = asyncio.Semaphore(20)


# ==== Helpers ====
def safe_filename(url: str) -> str:
    name = unquote(url.split("/")[-1].split("?")[0])
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    return name.strip("._") or "file.pdf"


def ensure_dir(*parts) -> str:
    path = os.path.join(*parts)
    os.makedirs(path, exist_ok=True)
    return path


def get_cache():
    return get_pdf_cache(CACHE_DB_PATH, CACHE_SOURCE_KEY, CACHE_SOURCE_NAME)

# ==== Robust Rename Logic ====
def extract_meeting_label(stem: str, doc_type: str) -> str:
    stem = re.sub(r"\d{10,}", "", stem)  # remove timestamps
    stem = re.sub(r"^\d+[_\-\s]*", "", stem)

    stem = stem.replace("_", " ")
    stem = re.sub(r"\s+", " ", stem).strip()

    lower = stem.lower()

    # Special classifications
    special = ""
    if re.search(r"additional\s*agenda[-\s]*\d*", lower):
        special = "Additional Agenda"
    elif "revised agenda" in lower:
        special = "Revised Agenda"
    elif "corrigendum" in lower:
        special = "Minutes Corrigendum"
    elif "addendum" in lower:
        special = "Addendum of Minutes"
    elif "annex" in lower or "exhibit" in lower:
        special = "Annex"

    # Strong meeting extraction
    match = re.search(
        r"(\d{1,3})(st|nd|rd|th)?\s*CMETS[-\s]*([A-Z]{2,3})",
        stem,
        re.IGNORECASE,
    )

    if match:
        num = match.group(1)
        suffix = match.group(2) or "th"
        region = match.group(3).upper()
        meeting = f"{num}{suffix} CMETS-{region}"
    else:
        fallback = re.search(r"(\d{1,3})\s*CMETS", stem, re.IGNORECASE)
        if fallback:
            meeting = f"{fallback.group(1)}th CMETS"
        else:
            meeting = stem

    meeting = re.sub(r"\s+", " ", meeting).strip()

    if special:
        return f"{special}_{meeting}"

    return meeting

def formatted_filename(doc_type: str, url: str) -> str:
    original = safe_filename(url)

    if "." in original:
        stem, ext = original.rsplit(".", 1)
        ext = "." + ext.lower()
    else:
        stem, ext = original, ".pdf"

    label = extract_meeting_label(stem, doc_type)
    return f"{doc_type}_{label}{ext}"


# ==== Network ====
async def fetch_html(session, url):
    async with PAGE_SEM:
        for attempt in range(3):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    r.raise_for_status()
                    return await r.text()
            except Exception:
                await asyncio.sleep(2 ** attempt)
    return ""

async def download(session, url, dest, *, region=None, doc_type=None):
    async with DOWNLOAD_SEM:
        cache = get_cache()
        pdf_name = os.path.basename(dest)
        if cache.is_cached(pdf_name, pdf_type=doc_type, pdf_path=dest):
            return

        for attempt in range(3):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=90)) as r:
                    if r.status != 200:
                        return
                    data = await r.read()

                ensure_dir(os.path.dirname(dest))
                with open(dest, "wb") as f:
                    f.write(data)

                cache.record_download(pdf_name=pdf_name, pdf_type=doc_type, pdf_path=dest)
                return  # no print (faster)

            except Exception:
                await asyncio.sleep(2 ** attempt)

# ==== Parsing ====
def get_total_pages(html):
    m = re.search(r"Displaying\s+\d+\s+to\s+\d+\s+of\s+(\d+)", html, re.I)
    if m:
        n = int(m.group(1))
        return max(1, (n + 9) // 10)
    return 30

def parse_rows(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    headers = [th.get_text(strip=True).lower()
               for th in table.find("tr").find_all(["th", "td"])]

    region_col  = next((i for i, h in enumerate(headers) if h == "region"),  3)
    notice_col  = next((i for i, h in enumerate(headers) if "notice" in h),  4)
    agenda_col  = next((i for i, h in enumerate(headers) if "agenda" in h),  5)
    minutes_col = next((i for i, h in enumerate(headers) if h in ("minutes", "mom")), 6)

    def links_from_cell(cells, col, dtype):
        if col >= len(cells):
            return []
        out = []
        for a in cells[col].find_all("a", href=True):
            href = a["href"].strip()
            if "/uploads/ists_consultation_meeting/" not in href.lower():
                continue
            full = href if href.startswith("http") else urljoin(SITE_ROOT, href)
            out.append({"region": None, "doc_type": dtype, "url": full})
        return out

    records = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue

        raw_region = cells[region_col].get_text(strip=True).lower() if region_col < len(cells) else ""
        region = REGION_MAP.get(raw_region)
        if not region:
            continue

        for entry in (
            links_from_cell(cells, notice_col,  "Notice") +
            links_from_cell(cells, agenda_col,  "Agenda") +
            links_from_cell(cells, minutes_col, "Minutes")
        ):
            entry["region"] = region
            records.append(entry)

    return records

# ==== Collect ====
async def collect_all(session):
    first_html = await fetch_html(session, f"{BASE_URL}?p=ajax&page=1&tab=0")
    if not first_html:
        return []

    total_pages = get_total_pages(first_html)
    print(f"Pages: {total_pages}")

    urls = [f"{BASE_URL}?p=ajax&page={p}&tab=0" for p in range(2, total_pages + 1)]
    htmls = await asyncio.gather(*[fetch_html(session, u) for u in urls])

    seen = set()
    all_records = []

    for html in [first_html, *htmls]:
        for rec in parse_rows(html):
            if rec["url"] not in seen:
                seen.add(rec["url"])
                all_records.append(rec)

    return all_records

# ==== Dynamic Count Logic ====
def compute_counts(records):
    counts = defaultdict(lambda: defaultdict(int))

    for r in records:
        if r["doc_type"] in ("Agenda", "Minutes"):
            counts[r["region"]][r["doc_type"]] += 1

    return counts

def print_summary(counts):
    print("\nScraper:", BASE_URL)
    print("Target : PDFs (Agenda + Minutes only) split into 5 region folders.\n")

    total_all = 0

    for region in sorted(counts):
        a = counts[region].get("Agenda", 0)
        m = counts[region].get("Minutes", 0)
        t = a + m
        total_all += t

        print(f"  {region:<23} : Agenda={a:<3} Minutes={m:<3} → {t}")

    print(f"  TOTAL{'':<30} → {total_all}\n")

# ==== Main ====
async def main():
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        print("Collecting...\n")
        all_records = await collect_all(session)

        filtered = [
            r for r in all_records
            if r["region"] and r["doc_type"] in ("Agenda", "Minutes")
        ]

        # Dynamic counts
        counts = compute_counts(filtered)
        print_summary(counts)

        grouped = defaultdict(list)
        for rec in filtered:
            grouped[rec["doc_type"]].append(rec)

        tasks = []

        for dtype, records in grouped.items():
            # Route Agenda -> agenda/  |  Minutes -> minutes/
            sub_folder = dtype.lower()     # "agenda" or "minutes"
            dest_dir   = ensure_dir(OUTPUT_DIR, sub_folder)

            for idx, rec in enumerate(records, start=1):
                clean_name = formatted_filename(dtype, rec["url"])
                numbered = f"{idx:02d}_{clean_name}"
                dest = os.path.join(dest_dir, numbered)

                tasks.append(
                    download(
                        session,
                        rec["url"],
                        dest,
                        region=rec["region"],
                        doc_type=dtype,
                    )
                )

        print(f"Total files to download: {len(tasks)}\n")

        CHUNK = 15
        for i in range(0, len(tasks), CHUNK):
            await asyncio.gather(*tasks[i:i+CHUNK])
            await asyncio.sleep(0.5)

    print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
