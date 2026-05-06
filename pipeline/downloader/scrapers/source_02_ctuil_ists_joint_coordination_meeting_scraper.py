"""
Scraper for: https://ctuil.in/ists-joint-coordination-meeting
Downloads PDFs from "Notice" and "Minutes" columns for each region.
"""

import os
import re
import asyncio

import aiohttp
from urllib.parse import urljoin, unquote
from bs4 import BeautifulSoup

# ==== Config ====
BASE_URL = "https://ctuil.in/ists-joint-coordination-meeting"
OUTPUT_DIR = "uploads/CTUIL-ISTS-JCC"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": BASE_URL,
}

# Normalize region text from table → folder name
REGION_FOLDER_MAP = {
    "northern region": "Northern Region",
    "western region": "Western Region",
    "southern region": "Southern Region",
    "eastern region": "Eastern Region",
    "north eastern region": "North Eastern Region",
    "north-eastern region": "North Eastern Region",
    "ner": "North Eastern Region",
    "nr": "Northern Region",
    "wr": "Western Region",
    "sr": "Southern Region",
    "er": "Eastern Region",
}

PAGE_SEM = asyncio.Semaphore(10)
DOWNLOAD_SEM = asyncio.Semaphore(20)


# ==== Utils ====
def safe_filename(url: str) -> str:
    name = unquote(url.split("/")[-1].split("?")[0])
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    return name.strip("._") or "file.pdf"

def normalize_region(raw: str) -> str:
    """Map raw region cell text to a folder name."""
    key = raw.strip().lower()
    return REGION_FOLDER_MAP.get(key, raw.strip())

def extract_meeting_label(stem: str, doc_type: str) -> str:
    stem = re.sub(r"^\d+[_\-\s]*", "", stem).strip()
    stem = stem.replace("_", " ")
    stem = re.sub(r"\s+", " ", stem).strip()
    stem = re.sub(r"\s*\(\d+\)\s*$", "", stem).strip()  # trailing "(1)"

    def region_code_from_text(text: str) -> str | None:
        t = text.lower()
        if re.search(r"\bner\b", t):
            return "NER"
        if re.search(r"\bnr\b", t):
            return "NR"
        if re.search(r"\ber\b", t):
            return "ER"
        if re.search(r"\bwr\b", t):
            return "WR"
        if re.search(r"\bsr\b", t):
            return "SR"
        if "north eastern" in t or "north-eastern" in t:
            return "NER"
        if "northern" in t:
            return "NR"
        if "western" in t:
            return "WR"
        if "southern" in t:
            return "SR"
        if "eastern" in t:
            return "ER"
        return None

    if doc_type == "Notice":
        stem = re.sub(r"(?i)^meeting\s+notice\s*&\s*agenda\s*(for)?\s*", "", stem).strip()
        stem = re.sub(r"(?i)^meeting\s+notice\s*(for)?\s*", "", stem).strip()
        stem = re.sub(r"(?i)^notice\s*(for)?\s*", "", stem).strip()
        stem = re.sub(r"(?i)^agenda\s*(for)?\s*", "", stem).strip()
    else:
        stem = re.sub(r"(?i)^minutes\s*(of)?\s*", "", stem).strip()
        stem = re.sub(r"(?i)^mom\s*(of)?\s*", "", stem).strip()

    # Prefer canonical JCC meeting label: "<ordinal> JCC-<REG>"
    ordinal_match = re.search(r"\b(\d{1,3})(st|nd|rd|th)\b", stem, re.IGNORECASE)
    ordinal = f"{ordinal_match.group(1)}{ordinal_match.group(2).lower()}" if ordinal_match else None

    meeting_type = None
    if re.search(r"\bjcc\b", stem, re.IGNORECASE):
        meeting_type = "JCC"
    elif re.search(r"\bjccm\b", stem, re.IGNORECASE):
        meeting_type = "JCCM"
    elif re.search(r"\bjcm\b", stem, re.IGNORECASE):
        meeting_type = "JCM"

    region_code = region_code_from_text(stem)

    if ordinal and meeting_type and region_code:
        return f"{ordinal} {meeting_type}-{region_code}"

    # Handle multi-region specials like "Special JCC WR-ER-SR ..."
    if re.search(r"\bspecial\b", stem, re.IGNORECASE):
        multi = re.search(r"\b(NER|NR|ER|WR|SR)(?:\s*[-/]\s*(NER|NR|ER|WR|SR))+(?:\s*[-/]\s*(NER|NR|ER|WR|SR))?\b", stem, re.IGNORECASE)
        if multi:
            codes = [c.upper() for c in multi.groups() if c]
            meeting_type = meeting_type or "JCC"
            return f"Special {meeting_type} " + "-".join(codes)
        if meeting_type and region_code:
            return f"Special {meeting_type}-{region_code}"

    # Fallback: return cleaned stem (still safe filename via safe_filename())
    return stem.strip()


def formatted_filename(doc_type: str, url: str) -> str:
    original = safe_filename(url)
    if "." in original:
        stem, ext = original.rsplit(".", 1)
        ext = "." + ext.lower()
    else:
        stem, ext = original, ".pdf"

    meeting_label = extract_meeting_label(stem, doc_type)
    return f"{doc_type}_{meeting_label}{ext}"


def ensure_region_dir(region: str) -> str:
    new_dir = os.path.join(OUTPUT_DIR, region)
    legacy_dir = os.path.join(OUTPUT_DIR, region.replace(" ", "_"))

    if os.path.isdir(legacy_dir) and legacy_dir != new_dir:
        os.makedirs(new_dir, exist_ok=True)
        for name in os.listdir(legacy_dir):
            src = os.path.join(legacy_dir, name)
            dst = os.path.join(new_dir, name)
            if not os.path.exists(dst):
                os.rename(src, dst)
        try:
            os.rmdir(legacy_dir)
        except OSError:
            pass

    return new_dir

# ==== Fetch HTML ====
async def async_fetch(session, url):
    async with PAGE_SEM:
        async with session.get(url) as resp:
            return await resp.text()

# ==== Download File ====
async def async_download(session, url, dest):
    async with DOWNLOAD_SEM:
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    print(f"[SKIP {resp.status}] {url}")
                    return
                data = await resp.read()

            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(data)
            print(f"[OK] {dest}")

        except Exception as e:
            print(f"[ERROR] {url} → {e}")

# ==== Get total pages ====
def get_total_pages(html: str) -> int:
    m = re.search(r"Displaying\s+\d+\s+to\s+\d+\s+of\s+(\d+)", html, re.I)
    if m:
        total = int(m.group(1))
        return (total + 9) // 10
    return 1

# ==== Extract Rows ====
def extract_rows(html: str) -> list:
    """
    Returns list of dicts:
      { "region": str, "Notice": [url, ...], "Minutes": [url, ...] }
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    header_row = table.find("tr")
    headers = [th.get_text(strip=True).lower()
               for th in header_row.find_all(["th", "td"])]

    # Detect column indexes from headers
    region_col  = next((i for i, h in enumerate(headers) if h == "region"), 3)
    notice_col  = next((i for i, h in enumerate(headers) if "notice" in h), 4)
    minutes_col = next((i for i, h in enumerate(headers) if h in ("minutes", "mom")), 5)

    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue

        region_raw = cells[region_col].get_text(strip=True) if region_col < len(cells) else "Unknown"
        region = normalize_region(region_raw)

        def get_links(col_idx):
            if col_idx >= len(cells):
                return []
            links = []
            for a in cells[col_idx].find_all("a", href=True):
                href = a["href"].strip()
                if not href or href.startswith("#") or "javascript" in href:
                    continue
                full = href if href.startswith("http") else urljoin("https://ctuil.in", href)
                links.append(full)
            return links

        rows.append({
            "region": region,
            "Notice":  get_links(notice_col),
            "Minutes": get_links(minutes_col),
        })

    return rows

# ==== Collect All Pages ====
async def collect_all(session) -> dict:
    """
    Fetch all pages (tab=0 returns full unfiltered table).
    Returns: { region: { "Notice": [urls], "Minutes": [urls] } }
    """
    first_url = f"{BASE_URL}?p=ajax&page=1&tab=0"
    first_html = await async_fetch(session, first_url)
    total_pages = get_total_pages(first_html)
    print(f"Total pages: {total_pages}")

    all_html = [first_html]
    for page_num in range(2, total_pages + 1):
        url = f"{BASE_URL}?p=ajax&page={page_num}&tab=0"
        html = await async_fetch(session, url)
        all_html.append(html)

    # Aggregate by region
    collected = {}
    for html in all_html:
        for row in extract_rows(html):
            region = row["region"]
            if region not in collected:
                collected[region] = {"Notice": [], "Minutes": []}
            collected[region]["Notice"].extend(row["Notice"])
            collected[region]["Minutes"].extend(row["Minutes"])

    return collected

# ==== Reorder Files (Shift Logic) ====
def reorder_files(dest_dir, urls, doc_type):
    os.makedirs(dest_dir, exist_ok=True)

    existing_map = {}
    for f in os.listdir(dest_dir):
        lookup_name = f.split("_", 1)[1] if "_" in f and f.split("_", 1)[0].isdigit() else f
        existing_map[lookup_name] = f

        if "." in lookup_name:
            lookup_stem, lookup_ext = lookup_name.rsplit(".", 1)
            lookup_ext = "." + lookup_ext.lower()
        else:
            lookup_stem, lookup_ext = lookup_name, ".pdf"

        normalized_name = f"{doc_type}_{extract_meeting_label(lookup_stem, doc_type)}{lookup_ext}"
        existing_map.setdefault(normalized_name, f)

    counter = 1
    for url in urls:
        original_name = formatted_filename(doc_type, url)
        new_name = f"{counter:02d}_{original_name}"
        new_path = os.path.join(dest_dir, new_name)

        if original_name in existing_map:
            old_path = os.path.join(dest_dir, existing_map[original_name])
            if old_path != new_path:
                os.rename(old_path, new_path)
        else:
            yield (url, new_path)

        counter += 1

# ==== Main ====
async def main():
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        collected = await collect_all(session)

        print("\nFound regions:")
        for region, docs in collected.items():
            print(f"  {region} → Notice: {len(docs['Notice'])} | Minutes: {len(docs['Minutes'])}")

        download_tasks = []
        for region, docs in collected.items():
            region_dir = ensure_region_dir(region)
            for doc_type in ("Notice", "Minutes"):
                urls = docs[doc_type]
                dest_dir = os.path.join(region_dir, doc_type)
                for url, dest in reorder_files(dest_dir, urls, doc_type):
                    download_tasks.append(async_download(session, url, dest))

        print(f"\nDownloading {len(download_tasks)} files...")
        await asyncio.gather(*download_tasks)
        print("\nDone.")


if __name__ == "__main__":
    asyncio.run(main())
