"""
jcc_handler/extraction.py — PDF table extraction logic
========================================================
Uses pdfplumber to detect and extract connectivity/pooling station
tables from JCC Meeting PDF pages.

Edit this file to change how tables are detected, headers are matched,
or data rows are parsed.
"""

from __future__ import annotations

from typing import Optional

import pdfplumber

from pipeline.jcc_handler.models import (
    REQUIRED_KEYWORDS,
    TARGET_COLUMN_FRAGMENTS,
    COLUMN_NAMES,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _clean(text) -> str:
    """Strip excess whitespace from a cell value."""
    if text is None:
        return ""
    return " ".join(str(text).split())


def _is_target_table(table: list) -> bool:
    """Return True if the table looks like the connectivity table."""
    if not table or len(table) < 2:
        return False
    header_text = " ".join(_clean(c).lower() for c in (table[0] or []) if c)
    hits = sum(1 for kw in TARGET_COLUMN_FRAGMENTS if kw in header_text)
    return hits >= 3


def _normalise_row(row: list, n_cols: int) -> list[str]:
    """Pad or trim a row to exactly *n_cols* cells."""
    cleaned = [_clean(c) for c in row]
    if len(cleaned) < n_cols:
        cleaned += [""] * (n_cols - len(cleaned))
    else:
        cleaned = cleaned[:n_cols]
    return cleaned


def _count_header_rows(table: list) -> int:
    """Count how many leading rows are header/sub-header rows."""
    count = 0
    for row in table:
        row_text = " ".join(_clean(c).lower() for c in row if c)
        if any(kw in row_text for kw in ["pooling", "grantee scope", "under ists"]):
            count += 1
        else:
            break
    return count


# ── Page-level extraction ─────────────────────────────────────────────────────

def page_passes_gate(text: str) -> bool:
    """Check whether *text* contains all required JCC keywords."""
    return all(kw in text for kw in REQUIRED_KEYWORDS)


def extract_page_data(page, page_number: int) -> Optional[dict]:
    """Extract the connectivity table from a single pdfplumber Page.

    Returns a dict with page metadata and a list of row dicts,
    or None if no target table is found.
    """
    tables = page.extract_tables()
    target = None
    for tbl in tables:
        if _is_target_table(tbl):
            target = tbl
            break

    if target is None:
        return None

    canon_n     = len(COLUMN_NAMES)
    header_rows = _count_header_rows(target)
    data_rows   = target[header_rows:]

    rows = []
    for raw_row in data_rows:
        norm = _normalise_row(raw_row, canon_n)
        if not any(norm):
            continue
        rows.append(dict(zip(COLUMN_NAMES, norm)))

    return {
        "page_number": page_number,
        "raw_text":    page.extract_text() or "",
        "rows":        rows,
    }


# ── Single-PDF extraction ────────────────────────────────────────────────────

def extract_jcc_pdf(pdf_path: str) -> list[dict]:
    """Extract all matching pages from one JCC PDF.

    Returns a list of page result dicts (same shape as extract_page_data).
    """
    all_pages: list[dict] = []
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        print(f"  [JCC] {total} pages — scanning for target tables …")

        for i, page in enumerate(pdf.pages):
            page_number = i + 1
            text = page.extract_text() or ""

            if not page_passes_gate(text):
                continue

            result = extract_page_data(page, page_number)
            if result is None:
                continue

            row_count = len(result["rows"])
            print(f"  ✓ Page {page_number:3d} → {row_count} data rows")
            all_pages.append(result)

    return all_pages
