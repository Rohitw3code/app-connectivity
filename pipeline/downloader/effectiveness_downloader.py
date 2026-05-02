"""
pipeline/downloader/effectiveness_downloader.py — Effectiveness PDF Downloader
================================================================================
Downloads RE Generators (Regenerators) effective-date-wise connectivity PDFs.
Source: https://ctuil.in/regenerators

Downloads into: source/effectiveness_pdfs/
Merged from: ctuil-pdf-scraper-main/app/scrapers/source_03_ctuil_regenerators_scraper.py

NOTE: This scraper requires Playwright for JS-rendered pages.
"""

from __future__ import annotations

import os
import re
import asyncio
import logging
from urllib.parse import unquote
from pathlib import Path

import aiohttp
from bs4 import BeautifulSoup

from pipeline.downloader.base import (
    download_file, download_batch,
    apply_download_limit, COMMON_HEADERS,
)

logger = logging.getLogger(__name__)

PAGE_URL = "https://ctuil.in/regenerators"
BASE_URL = "https://ctuil.in"


def _safe_month(raw: str) -> str:
    return re.sub(r'[<>:"/\\|?*\x00-\x1f\s]', "_", raw.strip())


def _canonical_display_name(month: str) -> str:
    return f"{month}_RE effectiveness.pdf"


# ─── Fetch (Playwright) ──────────────────────────────────────────────────────

async def _fetch_rendered_html() -> str:
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("[EFF DL] Playwright not installed. Cannot scrape regenerators page.")
        return ""

    logger.info("[EFF DL] Launching headless browser...")
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(PAGE_URL, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_selector("table a[href]", timeout=15000)
        html = await page.content()
        await browser.close()

    logger.info("[EFF DL] Page rendered.")
    return html


# ─── Extract Links ────────────────────────────────────────────────────────────

def _extract_links(html: str) -> list:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        logger.warning("[EFF DL] No table found on page.")
        return []

    header_row = table.find("tr")
    headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]

    col_idx = 2
    for i, h in enumerate(headers):
        if "effective date" in h.lower() and "wise" in h.lower():
            col_idx = i
            break

    results = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue

        month = _safe_month(cells[0].get_text(strip=True)) if cells else "Unknown"

        if col_idx >= len(cells):
            continue

        for a in cells[col_idx].find_all("a", href=True):
            href = a["href"].strip()
            if not href or href.startswith("#") or "javascript" in href:
                continue
            full_url = href if href.startswith("http") else f"{BASE_URL}{href}"
            results.append((month, full_url))

    return results


# ─── Public API ───────────────────────────────────────────────────────────────

def download_effectiveness_pdfs(
    dest_dir: str | Path,
    limit: int = 5,
    tracker=None,
) -> int:
    """
    Download Effectiveness (RE Generators) PDFs into dest_dir.

    Args:
        dest_dir: Target directory (e.g. source/effectiveness_pdfs/)
        limit: Max PDFs to download. -1 = all, default 5.
        tracker: PipelineTracker instance (optional)

    Returns:
        Number of PDFs downloaded.
    """
    dest = Path(dest_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    already_on_disk = len(list(dest.glob("*.pdf")))

    async def _run():
        html = await _fetch_rendered_html()
        if not html:
            logger.error("[EFF DL] Failed to render page.")
            return 0

        links = _extract_links(html)
        if not links:
            logger.warning("[EFF DL] No PDF links found.")
            return 0

        logger.info("[EFF DL] Found %d PDF(s) on site.", len(links))

        # Apply limit
        links = apply_download_limit(links, limit, already_on_disk)

        if not links:
            logger.info("[EFF DL] No new PDFs to download.")
            return 0

        # Prepare download tasks
        tasks = []
        month_counts = {}
        for counter, (month, url) in enumerate(links, start=1):
            month_counts[month] = month_counts.get(month, 0) + 1
            display_name = _canonical_display_name(month)
            if month_counts[month] > 1:
                base, ext = display_name.rsplit(".", 1)
                display_name = f"{base}-{month_counts[month]}.{ext}"
            new_fname = f"{counter:02d}_{display_name}"
            new_path = str(dest / new_fname)

            if os.path.exists(new_path):
                continue

            tasks.append((url, new_path))

        if not tasks:
            logger.info("[EFF DL] All PDFs already on disk.")
            return 0

        logger.info("[EFF DL] Downloading %d PDFs...", len(tasks))

        async with aiohttp.ClientSession(headers={**COMMON_HEADERS, "Referer": PAGE_URL}) as session:
            downloaded = await download_batch(session, tasks)

        # Register in tracker
        if tracker:
            for url, file_path in tasks:
                if os.path.exists(file_path):
                    fname = os.path.basename(file_path)
                    fsize = os.path.getsize(file_path)
                    tracker.register_download(
                        handler="effectiveness",
                        pdf_filename=fname,
                        pdf_path=file_path,
                        source_url=url,
                        file_size_bytes=fsize,
                    )

        return downloaded

    return asyncio.run(_run())
