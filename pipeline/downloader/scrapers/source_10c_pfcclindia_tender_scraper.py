"""
PFCCL India Tender Scraper
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
import unicodedata
from pathlib import Path
from urllib.parse import urljoin, urlparse

from pipeline.downloader.pdf_cache import get_pdf_cache

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
except ImportError:
    sys.exit("Run: pip install playwright && python -m playwright install chromium")

try:
    import requests
except ImportError:
    sys.exit("Run: pip install requests")

BASE_URL   = "https://www.pfcclindia.com"
TENDER_URL = "https://www.pfcclindia.com/tender.php?AM2"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

CACHE_DB_PATH = None
CACHE_SOURCE_KEY = "pfcclindia_tender"
CACHE_SOURCE_NAME = "PFCCL-INDIA-TENDER"

# ─────────────────────────────────────────────────────────────────────────────
# KEYWORDS — matched against child link TEXT only (not URL)
# Boundary rule:  not preceded by a letter, not followed by a letter.
# This lets "RFP" hit "RFP_CTS_Part A" but not "NRFP".
# ─────────────────────────────────────────────────────────────────────────────
KEYWORDS = [
    "Corrigendum",
    "Extension",
    "Successful",
    "RFP",
    "Postponement",
    "Qualified",
    "Amendment",
]

def keyword_in_text(link_text: str) -> str | None:
    """
    Return the first matched keyword if link_text contains it, else None.
    Boundary: (?<![A-Za-z])KEYWORD(?![A-Za-z])
    """
    for kw in KEYWORDS:
        pat = rf'(?<![A-Za-z]){re.escape(kw)}(?![A-Za-z])'
        if re.search(pat, link_text, re.IGNORECASE):
            return kw
    return None


# Exact title match — full user input as substring, no tokenization
def title_matches(title: str, user_input: str) -> bool:
    return user_input.strip().lower() in title.strip().lower()


def extract_child_links(li_element) -> list[dict]:
    """
    Pull every PDF child link from a tender.
    """
    links: list[dict] = []
    seen: set[str] = set()

    def add(text: str, url: str):
        url = url.strip()
        if not url or url in seen:
            return
        # Accept if URL ends in .pdf OR link text says "(pdf file)"
        is_pdf = (
            ".pdf" in url.lower()
            or re.search(r"\(pdf\s*file\)", text, re.IGNORECASE)
        )
        if is_pdf:
            seen.add(url)
            links.append({"text": text.strip(), "url": url})

    # ==== All <a href> inside child <ul>/<ol> lists ====
    for a in li_element.query_selector_all("ul li a[href], ol li a[href]"):
        href = (a.get_attribute("href") or "").strip()
        text = a.inner_text().strip()
        if href:
            full = href if href.startswith("http") else urljoin(BASE_URL, href)
            add(text, full)

    # ==== onclick patterns in raw HTML ====
    raw = li_element.inner_html()
    for m in re.finditer(
        r"""(?:window\.open|location\.href\s*=|open\()\s*['"](.*?\.pdf[^'"]*?)['"]""",
        raw, re.IGNORECASE
    ):
        url  = m.group(1).strip()
        full = url if url.startswith("http") else urljoin(BASE_URL, url)
        add(os.path.basename(urlparse(full).path), full)

    # ==== data-href / data-src ====
    for m in re.finditer(
        r"""data-(?:href|src|url)\s*=\s*['"](.*?\.pdf[^'"]*?)['"]""",
        raw, re.IGNORECASE
    ):
        url  = m.group(1).strip()
        full = url if url.startswith("http") else urljoin(BASE_URL, url)
        add(os.path.basename(urlparse(full).path), full)

    return links


# Page Scanner
def scan_page(page, user_input: str) -> list[dict]:
    results = []

    # Each <ol> may have a start= attribute (e.g. <ol start="230">)
    ol_elements = page.query_selector_all("ol")

    for ol in ol_elements:
        start_attr = ol.get_attribute("start")
        ol_start   = int(start_attr) if start_attr and start_attr.isdigit() else 1

        li_list = ol.query_selector_all(":scope > li")
        for li_idx, li in enumerate(li_list):
            sr_no = ol_start + li_idx

            bold  = li.query_selector("b, strong")
            title = bold.inner_text().strip() if bold else ""

            if not title:
                for line in li.inner_text().split("\n"):
                    clean = line.strip().lstrip("○•·–-► ").strip()
                    if clean and not re.search(r"\(pdf\s*file\)", clean, re.I):
                        title = clean
                        break

            if not title:
                continue

            if title_matches(title, user_input):
                # ==== Title-level gate ====
                # If "Consultant" in title → skip entirely
                if re.search(r'(?<![A-Za-z])Consultant(?![A-Za-z])', title, re.IGNORECASE):
                    print(f"  Skipped tender #{sr_no} (Consultant found in title)")
                    continue

                all_links = extract_child_links(li)
                results.append({
                    "sr_no":     sr_no,
                    "title":     title,
                    "all_links": all_links,
                })
                print(f"  Matched tender #{sr_no}: {title[:70]}…")
                print(f"  {len(all_links)} child PDF link(s) found\n")

    return results


# Pagination
def paginate_all(page):
    sels = [
        "a:has-text('Next')", "a:has-text('»')", "a:has-text('›')",
        "li.next > a", "a.page-link[aria-label='Next']",
        "a[class*='next']", "button:has-text('Load More')",
        "input[value='Load More']",
    ]
    for _ in range(50):
        hit = False
        for sel in sels:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible() and btn.is_enabled():
                    btn.click()
                    page.wait_for_timeout(2500)
                    hit = True
                    break
            except Exception:
                pass
        if not hit:
            break


def goto_retry(page, url: str, retries: int = 3, wait_ms: int = 4000):
    for attempt in range(1, retries + 1):
        try:
            page.goto(url, wait_until="networkidle", timeout=35_000)
            page.wait_for_timeout(wait_ms)
            return
        except PWTimeout:
            print(f"  [warn] timeout {attempt}/{retries}")
            if attempt == retries:
                raise


# Download
def make_folder_name(user_input: str, max_len: int = 60) -> str:
    """
    Generate a short readable folder name from user input.
    Extracts: place names + GW capacity + Part-A/B suffix.
    """
    text = user_input.strip()

    # Extract Part-A / Part-B suffix
    part = ""
    m = re.search(r'Part[-\s]?([A-Z])\b', text, re.IGNORECASE)
    if m:
        part = f"Part-{m.group(1).upper()}"

    # Extract place + capacity pairs like "Lakadia (Phase-II: 7.5GW)"
    place_cap = re.findall(
        r'([A-Z][a-zA-Z\s]+?)\s*\(Phase[^)]*?(\d+(?:\.\d+)?GW)\)',
        text, re.IGNORECASE
    )

    if place_cap:
        segments = []
        for place, cap in place_cap:
            last_word = place.strip().split()[-1]
            segments.append(f"{last_word}_{cap}")
        folder = "_".join(segments)
        if part:
            folder += f"_{part}"
    else:
        # Fallback: full sanitised input
        folder = unicodedata.normalize("NFKD", text)
        folder = re.sub(r"[^\w\s\-]", "_", folder)
        folder = re.sub(r"\s+", "_", folder.strip())

    folder = re.sub(r"_+", "_", folder).strip("_")
    return folder[:max_len]


def get_cache():
    return get_pdf_cache(CACHE_DB_PATH, CACHE_SOURCE_KEY, CACHE_SOURCE_NAME)


def download_pdf(
    url: str,
    dest: Path,
    session: requests.Session,
    *,
    metadata: dict | None = None,
) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    cache = get_cache()
    if cache.is_cached(dest.name, pdf_path=dest):
        print(f"         ✓  Cached (Skipped)")
        return False
    try:
        r = session.get(url, stream=True, timeout=60)
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(65536):
                fh.write(chunk)
        kb = dest.stat().st_size // 1024
        cache.record_download(pdf_name=dest.name, pdf_path=dest)
        print(f"         ✓  {dest.name}  ({kb} KB)")
        return True
    except Exception as exc:
        print(f"         ✗  {url}")
        print(f"            {exc}")
        return False


# Orchestrator
def run(user_input: str, output_dir: Path):
    print(f"\nInput > {user_input}\n")

    session = requests.Session()
    session.headers.update({
        "User-Agent": UA,
        "Referer":    BASE_URL + "/",
        "Accept":     "application/pdf,*/*",
    })

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=UA,
            viewport={"width": 1280, "height": 900},
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
        )
        page = ctx.new_page()

        print(f"[1] Loading: {TENDER_URL}")
        goto_retry(page, TENDER_URL, wait_ms=4000)
        paginate_all(page)

        print(f'[2] Scanning for: "{user_input}"\n')
        matched_entries = scan_page(page, user_input)

        if not matched_entries:
            print("[!] No entry found whose title contains that input.")
            print("    Tip: try a shorter substring of the exact title text.")
            browser.close()
            return

        total_downloaded = 0
        # folder name derived from user input (shared across all matched entries)
        folder_name = make_folder_name(user_input)
        save_dir    = output_dir / folder_name

        for entry in matched_entries:
            # Filter child links by keyword
            to_download = []
            for link in entry["all_links"]:
                if keyword_in_text(link["text"]):
                    to_download.append(link)

            if not to_download:
                print(f"  [Tender #{entry['sr_no']}] No links matched any keyword.\n")
                continue

            print(f"Downloading {len(to_download)} matched PDF(s) …\n")

            save_dir.mkdir(parents=True, exist_ok=True)
            existing = {}
            for f in os.listdir(save_dir):
                if "_" in f and f.split("_", 1)[0].isdigit():
                    original = f.split("_", 1)[1]
                    existing[original] = f

            # Handle duplicates in new URLs to match existing
            ordered_names = []
            for pdf in to_download:
                raw_fname = os.path.basename(urlparse(pdf["url"]).path)
                if not raw_fname.lower().endswith(".pdf"):
                    raw_fname += ".pdf"
                ordered_names.append(raw_fname)
                
            seen_counts = {}
            for i, name in enumerate(ordered_names):
                seen_counts[name] = seen_counts.get(name, 0) + 1
                if seen_counts[name] > 1 and "." in name:
                    base, ext = name.rsplit(".", 1)
                    ordered_names[i] = f"{base}-{seen_counts[name]}.{ext}"
                elif seen_counts[name] > 1:
                    ordered_names[i] = f"{name}-{seen_counts[name]}"

            for serial, (pdf, raw_fname) in enumerate(zip(to_download, ordered_names), start=1):
                new_fname = f"{serial:02d}_{raw_fname}"
                dest  = save_dir / new_fname

                print(f"  [{serial:02d}]  {pdf['text']}")
                print(f"        {pdf['url']}")

                if raw_fname in existing:
                    old_path = save_dir / existing[raw_fname]
                    if old_path != dest:
                        os.rename(old_path, dest)
                        print(f"         ✓  Renamed {existing[raw_fname]} -> {new_fname}")
                    else:
                        print(f"         ✓  Already exists (Skipped)")
                else:
                    ok = download_pdf(
                        pdf["url"],
                        dest,
                        session,
                        metadata={
                            "tender_title": entry["title"],
                            "tender_sr_no": entry["sr_no"],
                            "link_text": pdf["text"],
                            "keyword": keyword_in_text(pdf["text"]),
                            "folder": folder_name,
                        },
                    )
                    if ok:
                        total_downloaded += 1
                    time.sleep(0.4)

        browser.close()

    print(f"\nDone.  {total_downloaded} PDF(s) saved to:")
    print(f"  {save_dir.resolve()}\n")


def main():
    ap = argparse.ArgumentParser(
        description="PFCCL Tender Scraper — exact match + keyword filter",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--query", "-q", default=None,
        help="Substring of the tender title to match (exact, no breakdown)")
    ap.add_argument("--output", "-o", default="./source_output/PFCCL-INDIA-TENDER",
        help="Master output directory  (default: ./source_output/PFCCL-INDIA-TENDER)")
    args = ap.parse_args()

    user_input = args.query
    if not user_input:
        print("─" * 60)
        print("PFCCL Tender Scraper — Final")
        print("─" * 60)
        user_input = input("Input > ").strip()
        if not user_input:
            sys.exit("No input given.")

    Path(args.output).mkdir(parents=True, exist_ok=True)
    run(user_input, Path(args.output))


if __name__ == "__main__":
    main()
