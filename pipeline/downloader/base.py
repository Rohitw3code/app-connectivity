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

# Reduced concurrency for VM environments (unstable / throttled networks)
DOWNLOAD_SEM = asyncio.Semaphore(5)

COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
}


def make_connector() -> aiohttp.TCPConnector:
    """Return a VM-friendly TCP connector with conservative settings."""
    return aiohttp.TCPConnector(
        limit=10,
        limit_per_host=3,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=False,
    )


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
    timeout: int = 180,
) -> bool:
    """Download a single file. Returns True on success."""
    async with DOWNLOAD_SEM:
        for attempt in range(5):
            try:
                async with session.get(
                    url,
                    timeout=aiohttp.ClientTimeout(
                        total=timeout,
                        connect=60,
                        sock_connect=60,
                        sock_read=120,
                    ),
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

            except (aiohttp.ClientConnectorError, aiohttp.ServerTimeoutError,
                    aiohttp.ClientPayloadError, OSError) as e:
                wait = min(5 * (2 ** attempt), 60)
                logger.warning("[RETRY %d/5] %s — %s (wait %ds)", attempt + 1, url, e, wait)
                await asyncio.sleep(wait)
            except Exception as e:
                logger.warning("[RETRY %d/5] %s — unexpected: %s", attempt + 1, url, e)
                await asyncio.sleep(5)

    logger.error("[FAILED] %s", url)
    return False


async def download_batch(
    session: aiohttp.ClientSession,
    tasks: list[tuple[str, str]],
    chunk_size: int = 3,
) -> int:
    """Download a batch of (url, dest) pairs. Returns count of successful downloads."""
    success = 0
    for i in range(0, len(tasks), chunk_size):
        results = await asyncio.gather(
            *[download_file(session, url, dest) for url, dest in tasks[i : i + chunk_size]]
        )
        success += sum(1 for r in results if r)
        await asyncio.sleep(1.5)  # polite pause between chunks on VM
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
