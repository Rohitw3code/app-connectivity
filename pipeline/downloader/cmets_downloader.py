"""
pipeline/downloader/cmets_downloader.py — CMETS PDF Downloader
================================================================
Downloads ISTS Consultation Meeting PDFs (Agenda + Minutes) from CTUIL.
Source: https://ctuil.in/ists-consultation-meeting

Downloads into:
  source/cmets_pdfs/agenda/    — Agenda PDFs
  source/cmets_pdfs/minutes/   — Minutes PDFs  (used by extraction pipeline)
Merged from: ctuil-pdf-scraper-main/app/scrapers/source_01_ctuil_ists_consultation_meeting_scraper.py
"""

from __future__ import annotations

import os
import re
import asyncio
import logging
from collections import defaultdict
from urllib.parse import urljoin
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

from pipeline.downloader.base import (
    safe_filename, ensure_dir, download_file, download_batch,
    apply_download_limit, COMMON_HEADERS,
)

logger = logging.getLogger(__name__)

BASE_URL  = "https://ctuil.in/ists-consultation-meeting"
SITE_ROOT = "https://ctuil.in"

REGION_MAP = {
    "northern region":      "Northern Region",
    "western region":       "Western Region",
    "southern region":      "Southern Region",
    "eastern region":       "Eastern Region",
    "north eastern region": "North Eastern Region",
    "north-eastern region": "North Eastern Region",
}

PAGE_SEM = asyncio.Semaphore(10)


# ─── Filename Logic (from original scraper) ──────────────────────────────────

def _extract_meeting_label(stem: str, doc_type: str) -> str:
    stem = re.sub(r"\d{10,}", "", stem)
    stem = re.sub(r"^\d+[_\-\s]*", "", stem)
    stem = stem.replace("_", " ")
    stem = re.sub(r"\s+", " ", stem).strip()
    lower = stem.lower()

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
    async with PAGE_SEM:
        for attempt in range(3):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    r.raise_for_status()
                    return await r.text()
            except Exception:
                await asyncio.sleep(2 ** attempt)
    return ""


def _get_total_pages(html):
    m = re.search(r"Displaying\s+\d+\s+to\s+\d+\s+of\s+(\d+)", html, re.I)
    if m:
        n = int(m.group(1))
        return max(1, (n + 9) // 10)
    return 30


def _parse_rows(html):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        return []

    headers = [th.get_text(strip=True).lower()
               for th in table.find("tr").find_all(["th", "td"])]

    region_col  = next((i for i, h in enumerate(headers) if h == "region"),  3)
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
            links_from_cell(cells, agenda_col,  "Agenda") +
            links_from_cell(cells, minutes_col, "Minutes")
        ):
            entry["region"] = region
            records.append(entry)

    return records


async def _collect_all(session):
    first_html = await _fetch_html(session, f"{BASE_URL}?p=ajax&page=1&tab=0")
    if not first_html:
        return []
    total_pages = _get_total_pages(first_html)
    logger.info("[CMETS DL] Pages: %d", total_pages)

    urls = [f"{BASE_URL}?p=ajax&page={p}&tab=0" for p in range(2, total_pages + 1)]
    htmls = await asyncio.gather(*[_fetch_html(session, u) for u in urls])

    seen = set()
    all_records = []
    for html in [first_html, *htmls]:
        for rec in _parse_rows(html):
            if rec["url"] not in seen:
                seen.add(rec["url"])
                all_records.append(rec)
    return all_records


# ─── Public API ───────────────────────────────────────────────────────────────

def download_cmets_pdfs(
    dest_dir: str | Path,
    limit: int = 5,
    tracker=None,
) -> int:
    """
    Download CMETS PDFs into dest_dir, split by document type:

      dest_dir/agenda/   — Agenda PDFs
      dest_dir/minutes/  — Minutes PDFs  (used by extraction pipeline)

    Args:
        dest_dir: Root target directory (e.g. source/cmets_pdfs/)
        limit: Max PDFs to download total. -1 = all, default 5.
        tracker: PipelineTracker instance (optional)

    Returns:
        Number of PDFs downloaded.
    """
    dest = Path(dest_dir).resolve()

    agenda_dir  = dest / "agenda"
    minutes_dir = dest / "minutes"
    agenda_dir.mkdir(parents=True, exist_ok=True)
    minutes_dir.mkdir(parents=True, exist_ok=True)

    # Count already-downloaded PDFs across both subfolders
    already_on_disk = (
        len(list(agenda_dir.glob("*.pdf")))
        + len(list(minutes_dir.glob("*.pdf")))
    )

    async def _run():
        headers = {**COMMON_HEADERS, "Referer": BASE_URL}
        async with aiohttp.ClientSession(headers=headers) as session:
            logger.info("[CMETS DL] Collecting PDF links...")
            all_records = await _collect_all(session)

            filtered = [
                r for r in all_records
                if r["region"] and r["doc_type"] in ("Agenda", "Minutes")
            ]

            # Apply limit
            filtered = apply_download_limit(filtered, limit, already_on_disk)

            if not filtered:
                logger.info("[CMETS DL] No new PDFs to download.")
                return 0

            # Prepare download tasks — route into agenda/ or minutes/ subfolder
            tasks = []
            # Track per-type index for sequential numbering
            type_counters: dict[str, int] = {"Agenda": 0, "Minutes": 0}

            for rec in filtered:
                dtype = rec["doc_type"]          # "Agenda" or "Minutes"
                sub_dir = agenda_dir if dtype == "Agenda" else minutes_dir

                type_counters[dtype] += 1
                idx = type_counters[dtype]

                clean_name = _formatted_filename(dtype, rec["url"])
                numbered   = f"{idx:02d}_{clean_name}"
                file_dest  = str(sub_dir / numbered)

                if os.path.exists(file_dest):
                    continue

                tasks.append((rec["url"], file_dest))

            if not tasks:
                logger.info("[CMETS DL] All PDFs already on disk.")
                return 0

            logger.info("[CMETS DL] Downloading %d PDFs...", len(tasks))
            downloaded = await download_batch(session, tasks)

            # Register in tracker
            if tracker:
                for url, file_path in tasks:
                    if os.path.exists(file_path):
                        fname = os.path.basename(file_path)
                        fsize = os.path.getsize(file_path)
                        tracker.register_download(
                            handler="cmets",
                            pdf_filename=fname,
                            pdf_path=file_path,
                            source_url=url,
                            file_size_bytes=fsize,
                        )

            return downloaded

    return asyncio.run(_run())
