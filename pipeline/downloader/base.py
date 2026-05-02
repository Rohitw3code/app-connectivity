"""
pipeline/downloader/base.py — Shared download utilities
========================================================
Common helpers used by all downloader modules.
"""

from __future__ import annotations

import os
import re
import asyncio
import logging
from urllib.parse import unquote
from pathlib import Path
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

DOWNLOAD_SEM = asyncio.Semaphore(20)

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def safe_filename(url: str) -> str:
    """Extract a safe filename from a URL."""
    name = unquote(url.split("/")[-1].split("?")[0])
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    return name.strip("._") or "file.pdf"


def ensure_dir(*parts) -> str:
    """Create directory path and return it."""
    path = os.path.join(*parts)
    os.makedirs(path, exist_ok=True)
    return path


async def download_file(
    session: aiohttp.ClientSession,
    url: str,
    dest: str,
    timeout: int = 90,
) -> bool:
    """Download a single file. Returns True on success."""
    async with DOWNLOAD_SEM:
        for attempt in range(3):
            try:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=timeout)
                ) as resp:
                    if resp.status != 200:
                        logger.warning("[SKIP %d] %s", resp.status, url)
                        return False
                    data = await resp.read()

                os.makedirs(os.path.dirname(dest), exist_ok=True)
                with open(dest, "wb") as f:
                    f.write(data)

                logger.info("[OK] %s → %s", os.path.basename(dest), dest)
                return True

            except Exception as e:
                logger.warning("[RETRY %d] %s → %s", attempt + 1, url, e)
                await asyncio.sleep(2 ** attempt)

    logger.error("[FAILED] %s", url)
    return False


async def download_batch(
    session: aiohttp.ClientSession,
    tasks: list[tuple[str, str]],
    chunk_size: int = 15,
) -> int:
    """Download a batch of (url, dest) pairs. Returns count of successful downloads."""
    success = 0
    for i in range(0, len(tasks), chunk_size):
        results = await asyncio.gather(
            *[download_file(session, url, dest) for url, dest in tasks[i : i + chunk_size]]
        )
        success += sum(1 for r in results if r)
        await asyncio.sleep(0.5)
    return success


def apply_download_limit(
    urls: list[str | tuple],
    limit: int,
    already_downloaded: int = 0,
) -> list:
    """
    Apply download limit.
    -1 = download all
    N > 0 = download up to N total (including already downloaded)
    """
    if limit == -1:
        return urls

    remaining = max(0, limit - already_downloaded)
    return urls[:remaining]
