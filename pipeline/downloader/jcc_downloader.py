"""
pipeline/downloader/jcc_downloader.py — JCC PDF Downloader
=============================================================
Downloads ISTS Joint Coordination Meeting PDFs (Notice + Minutes) from CTUIL.
Source: https://ctuil.in/ists-joint-coordination-meeting

Downloads into: source/jcc_pdfs/
Merged from: ctuil-pdf-scraper-main/app/scrapers/source_02_ctuil_ists_joint_coordination_meeting_scraper.py
"""

from __future__ import annotations

import os
import re
import asyncio
import logging
from urllib.parse import urljoin
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

from pipeline.downloader.base import (
    safe_filename, download_file, download_batch,
    apply_download_limit, COMMON_HEADERS, make_connector,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://ctuil.in/ists-joint-coordination-meeting"

REGION_FOLDER_MAP = {
    "northern region":      "Northern Region",
    "western region":       "Western Region",
    "southern region":      "Southern Region",
    "eastern region":       "Eastern Region",
    "north eastern region": "North Eastern Region",
    "north-eastern region": "North Eastern Region",
    "ner": "North Eastern Region",
    "nr":  "Northern Region",
    "wr":  "Western Region",
    "sr":  "Southern Region",
    "er":  "Eastern Region",
}

PAGE_SEM = asyncio.Semaphore(3)  # Reduced for VM environments

# Per-request timeout for HTML fetching (seconds)
_HTML_TIMEOUT = aiohttp.ClientTimeout(total=90, connect=30, sock_connect=30, sock_read=60)


# ─── Filename Logic (from original scraper) ──────────────────────────────────

def _extract_meeting_label(stem: str, doc_type: str) -> str:
    stem = re.sub(r"^\d+[_\-\s]*", "", stem).strip()
    stem = stem.replace("_", " ")
    stem = re.sub(r"\s+", " ", stem).strip()
    stem = re.sub(r"\s*\(\d+\)\s*$", "", stem).strip()

    def region_code_from_text(text):
        t = text.lower()
        if re.search(r"\bner\b", t): return "NER"
        if re.search(r"\bnr\b", t): return "NR"
        if re.search(r"\ber\b", t): return "ER"
        if re.search(r"\bwr\b", t): return "WR"
        if re.search(r"\bsr\b", t): return "SR"
        if "north eastern" in t or "north-eastern" in t: return "NER"
        if "northern" in t: return "NR"
        if "western" in t: return "WR"
        if "southern" in t: return "SR"
        if "eastern" in t: return "ER"
        return None

    if doc_type == "Notice":
        stem = re.sub(r"(?i)^meeting\s+notice\s*&\s*agenda\s*(for)?\s*", "", stem).strip()
        stem = re.sub(r"(?i)^meeting\s+notice\s*(for)?\s*", "", stem).strip()
        stem = re.sub(r"(?i)^notice\s*(for)?\s*", "", stem).strip()
        stem = re.sub(r"(?i)^agenda\s*(for)?\s*", "", stem).strip()
    else:
        stem = re.sub(r"(?i)^minutes\s*(of)?\s*", "", stem).strip()
        stem = re.sub(r"(?i)^mom\s*(of)?\s*", "", stem).strip()

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

    if re.search(r"\bspecial\b", stem, re.IGNORECASE):
        multi = re.search(
            r"\b(NER|NR|ER|WR|SR)(?:\s*[-/]\s*(NER|NR|ER|WR|SR))+(?:\s*[-/]\s*(NER|NR|ER|WR|SR))?\b",
            stem, re.IGNORECASE,
        )
        if multi:
            codes = [c.upper() for c in multi.groups() if c]
            meeting_type = meeting_type or "JCC"
            return f"Special {meeting_type} " + "-".join(codes)
        if meeting_type and region_code:
            return f"Special {meeting_type}-{region_code}"

    return stem.strip()


def _formatted_filename(doc_type: str, url: str) -> str:
    original = safe_filename(url)
    if "." in original:
        stem, ext = original.rsplit(".", 1)
        ext = "." + ext.lower()
    else:
        stem, ext = original, ".pdf"
    label = _extract_meeting_label(stem, doc_type)
    return f"{doc_type}_{label}{ext}"


# ─── Parsing ──────────────────────────────────────────────────────────────────

async def _fetch_html(session, url):
    """Fetch HTML with retry for VM network instability."""
    async with PAGE_SEM:
        last_exc = None
        for attempt in range(4):
            try:
                async with session.get(url, timeout=_HTML_TIMEOUT) as resp:
                    return await resp.text()
            except (aiohttp.ClientConnectorError, aiohttp.ServerTimeoutError, OSError) as e:
                last_exc = e
                wait = 5 * (2 ** attempt)
                logger.warning("[JCC DL] Fetch attempt %d/4 failed (%s) — retry in %ds", attempt + 1, e, wait)
                await asyncio.sleep(wait)
            except Exception as e:
                last_exc = e
                logger.warning("[JCC DL] Fetch attempt %d/4 unexpected error: %s", attempt + 1, e)
                await asyncio.sleep(5)
        raise last_exc


def _get_total_pages(html: str) -> int:
    m = re.search(r"Displaying\s+\d+\s+to\s+\d+\s+of\s+(\d+)", html, re.I)
    if m:
        total = int(m.group(1))
        return (total + 9) // 10
    return 1


def _extract_rows(html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    header_row = table.find("tr")
    headers = [th.get_text(strip=True).lower()
               for th in header_row.find_all(["th", "td"])]

    region_col  = next((i for i, h in enumerate(headers) if h == "region"), 3)
    notice_col  = next((i for i, h in enumerate(headers) if "notice" in h), 4)
    minutes_col = next((i for i, h in enumerate(headers) if h in ("minutes", "mom")), 5)

    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue

        region_raw = cells[region_col].get_text(strip=True) if region_col < len(cells) else "Unknown"
        key = region_raw.strip().lower()
        region = REGION_FOLDER_MAP.get(key, region_raw.strip())

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


async def _collect_all(session) -> dict:
    first_url = f"{BASE_URL}?p=ajax&page=1&tab=0"
    first_html = await _fetch_html(session, first_url)
    total_pages = _get_total_pages(first_html)
    logger.info("[JCC DL] Total pages: %d", total_pages)

    all_html = [first_html]
    for page_num in range(2, total_pages + 1):
        url = f"{BASE_URL}?p=ajax&page={page_num}&tab=0"
        html = await _fetch_html(session, url)
        all_html.append(html)

    collected = {}
    for html in all_html:
        for row in _extract_rows(html):
            region = row["region"]
            if region not in collected:
                collected[region] = {"Notice": [], "Minutes": []}
            collected[region]["Notice"].extend(row["Notice"])
            collected[region]["Minutes"].extend(row["Minutes"])

    return collected


# ─── Public API ───────────────────────────────────────────────────────────────

def download_jcc_pdfs(
    dest_dir: str | Path,
    limit: int = 5,
    tracker=None,
) -> int:
    """
    Download JCC Meeting PDFs into dest_dir.

    Args:
        dest_dir: Target directory (e.g. source/jcc_pdfs/)
        limit: Max PDFs to download. -1 = all, default 5.
        tracker: PipelineTracker instance (optional)

    Returns:
        Number of PDFs downloaded.
    """
    dest = Path(dest_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    already_on_disk = len(list(dest.glob("*.pdf")))

    async def _run():
        headers = {**COMMON_HEADERS, "Referer": BASE_URL}
        connector = make_connector()
        session_timeout = aiohttp.ClientTimeout(total=300, connect=60, sock_connect=60, sock_read=120)
        async with aiohttp.ClientSession(
            headers=headers,
            connector=connector,
            timeout=session_timeout,
        ) as session:
            logger.info("[JCC DL] Collecting PDF links...")
            collected = await _collect_all(session)

            # Flatten all PDFs from all regions into one list
            all_pdfs = []
            for region, docs in collected.items():
                for doc_type in ("Notice", "Minutes"):
                    for url in docs[doc_type]:
                        all_pdfs.append({"region": region, "doc_type": doc_type, "url": url})

            logger.info("[JCC DL] Found %d total PDFs across all regions", len(all_pdfs))

            # Apply limit
            all_pdfs = apply_download_limit(all_pdfs, limit, already_on_disk)

            if not all_pdfs:
                logger.info("[JCC DL] No new PDFs to download.")
                return 0

            # Prepare download tasks — flat files into dest_dir
            tasks = []
            for idx, rec in enumerate(all_pdfs, start=1):
                clean_name = _formatted_filename(rec["doc_type"], rec["url"])
                numbered = f"{idx:02d}_{clean_name}"
                file_dest = str(dest / numbered)

                if os.path.exists(file_dest):
                    continue

                tasks.append((rec["url"], file_dest))

            if not tasks:
                logger.info("[JCC DL] All PDFs already on disk.")
                return 0

            logger.info("[JCC DL] Downloading %d PDFs...", len(tasks))
            downloaded = await download_batch(session, tasks)

            # Register in tracker
            if tracker:
                for url, file_path in tasks:
                    if os.path.exists(file_path):
                        fname = os.path.basename(file_path)
                        fsize = os.path.getsize(file_path)
                        tracker.register_download(
                            handler="jcc",
                            pdf_filename=fname,
                            pdf_path=file_path,
                            source_url=url,
                            file_size_bytes=fsize,
                        )

            return downloaded

    return asyncio.run(_run())
