"""
pipeline/downloader/bayallocation_downloader.py — Bay Allocation PDF Downloader
=================================================================================
Downloads Renewable Energy / Bays Allocation PDFs from CTUIL.
Source: https://www.ctuil.in/renewable-energy

Downloads into: source/bayallocation/
Merged from: ctuil-pdf-scraper-main/app/scrapers/source_09_ctuil_renewable_energy_scraper.py

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

from pipeline.downloader.base import (
    download_file, download_batch,
    apply_download_limit, COMMON_HEADERS,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://www.ctuil.in/renewable-energy"


def _safe_filename(url: str) -> str:
    name = unquote(url.split("/")[-1].split("?")[0])
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)

    if "." in name:
        stem, ext = name.rsplit(".", 1)
        ext = "." + ext.lower()
    else:
        stem, ext = name, ".pdf"

    stem = re.sub(r"^\d{6,}", "", stem).lstrip("_- ").strip()
    stem = stem.replace("_", " ")
    stem = re.sub(r"\s+", " ", stem).strip()

    lower = stem.lower()

    if "allocation of bays" in lower:
        stem = re.sub(r"(?i)\b(approved|final|r\d+)\b", "", stem)
        stem = re.sub(r"\s+", " ", stem).strip()
        return f"{stem}{ext}"

    if "non re ss margin" in lower:
        stem = re.sub(r"(?i)\b(approved|final|rev[-\d]*)\b", "", stem)
        stem = re.sub(r"[-_]", " ", stem)
        stem = re.sub(r"\s+", " ", stem).strip()
        return f"{stem}{ext}"

    if "re ss margin" in lower:
        stem = re.sub(r"(?i)^re\s+", "", stem)
        stem = re.sub(r"(?i)\b(approved|final|rev[-\d]*)\b", "", stem)
        stem = re.sub(r"[-_]", " ", stem)
        stem = re.sub(r"\s+", " ", stem).strip()
        return f"{stem}{ext}"

    if "status of margins" in lower:
        stem = re.sub(r"\s+", " ", stem).strip()
        return f"{stem}{ext}"

    return f"{stem}{ext}" if stem else f"file{ext}"


# ─── Extract Links (Playwright) ──────────────────────────────────────────────

async def _extract_links():
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("[BAY DL] Playwright not installed.")
        return {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(BASE_URL)
        await page.wait_for_selector("table")

        data = await page.evaluate("""() => {
            const result = {
                bays: [],
                non_re: [],
                re_substations: [],
                proposed_re: []
            };

            const tables = Array.from(document.querySelectorAll('table'));

            tables.forEach(table => {
                const text = table.innerText.toLowerCase();

                if (text.includes("connectivity margin in ists substations")) {
                    const rows = table.querySelectorAll("tr");
                    rows.forEach(row => {
                        const cells = row.querySelectorAll("td");
                        if (cells.length >= 4) {
                            const nonRe = cells[2].querySelector("a");
                            if (nonRe && nonRe.href.toLowerCase().includes("pdf")) {
                                result.non_re.push(nonRe.href);
                            }
                            const reSub = cells[3].querySelector("a");
                            if (reSub && reSub.href.toLowerCase().includes("pdf")) {
                                result.re_substations.push(reSub.href);
                            }
                        }
                    });
                } else if (text.includes("proposed re integration")) {
                    const rows = table.querySelectorAll("tr");
                    rows.forEach(row => {
                        const link = row.querySelector("a");
                        if (link && link.href.toLowerCase().includes("pdf")) {
                            result.proposed_re.push(link.href);
                        }
                    });
                } else if (text.includes("allocation of bays")) {
                    const links = table.querySelectorAll("a[href]");
                    links.forEach(a => {
                        if (a.href.toLowerCase().includes("pdf")) {
                            result.bays.push(a.href);
                        }
                    });
                }
            });

            return result;
        }""")

        await browser.close()
        return data


# ─── Public API ───────────────────────────────────────────────────────────────

def download_bayallocation_pdfs(
    dest_dir: str | Path,
    limit: int = 5,
    tracker=None,
) -> int:
    """
    Download Bay Allocation (Renewable Energy) PDFs into dest_dir.

    Args:
        dest_dir: Target directory (e.g. source/bayallocation/)
        limit: Max PDFs to download. -1 = all, default 5.
        tracker: PipelineTracker instance (optional)

    Returns:
        Number of PDFs downloaded.
    """
    dest = Path(dest_dir).resolve()
    dest.mkdir(parents=True, exist_ok=True)

    already_on_disk = len(list(dest.glob("*.pdf")))

    async def _run():
        logger.info("[BAY DL] Extracting links from %s ...", BASE_URL)
        links_data = await _extract_links()

        if not links_data:
            logger.error("[BAY DL] No links extracted.")
            return 0

        # Focus on "bays" section for bay allocation
        # but collect all sections for completeness
        all_urls = []
        for section_key in ("bays", "non_re", "re_substations", "proposed_re"):
            for url in links_data.get(section_key, []):
                all_urls.append((section_key, url))

        logger.info("[BAY DL] Found %d total PDFs.", len(all_urls))

        # Apply limit
        all_urls = apply_download_limit(all_urls, limit, already_on_disk)

        if not all_urls:
            logger.info("[BAY DL] No new PDFs to download.")
            return 0

        # Prepare download tasks — flat files into dest_dir
        tasks = []
        for idx, (section, url) in enumerate(all_urls, start=1):
            name = _safe_filename(url)
            numbered = f"{idx:02d}_{section}_{name}"
            file_dest = str(dest / numbered)

            if os.path.exists(file_dest):
                continue

            tasks.append((url, file_dest))

        if not tasks:
            logger.info("[BAY DL] All PDFs already on disk.")
            return 0

        logger.info("[BAY DL] Downloading %d PDFs...", len(tasks))

        async with aiohttp.ClientSession(headers=COMMON_HEADERS) as session:
            downloaded = await download_batch(session, tasks)

        # Register in tracker
        if tracker:
            for url, file_path in tasks:
                if os.path.exists(file_path):
                    fname = os.path.basename(file_path)
                    fsize = os.path.getsize(file_path)
                    tracker.register_download(
                        handler="bayallocation",
                        pdf_filename=fname,
                        pdf_path=file_path,
                        source_url=url,
                        file_size_bytes=fsize,
                    )

        return downloaded

    return asyncio.run(_run())
