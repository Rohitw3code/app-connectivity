"""
cmets_handler/voltage_extractor.py — Contextual Voltage Extraction (per row)
=============================================================================
Extracts the voltage level (e.g. 400 kV, 765 kV, 220 kV, 33 kV) from
individual row data and page context.

Voltage appears contextually:
  - In the substation column: "Aligarh 400kV", "Bhadla-V (765 kV)"
  - In the applicant/location text: "project at 220 kV substation"
  - In mode/criteria: "400kV connectivity"
  - As a page-level mention: table titled "List at 400 kV"

Common Indian power grid voltages (ISTS + state): 765, 400, 220, 132, 110, 66, 33 kV

Strategy
--------
1. LLM extracts "Voltage" per row (prompt-level — primary)
2. Regex fallback on row cell values (substation, location, developers, etc.)
3. Page-level fallback (table title / description)
4. Normalise to "<N> kV" format

Pipeline Integration
--------------------
Called from extraction.py after LLM normalization:

    from pipeline.cmets_handler.voltage_extractor import (
        extract_voltage_from_row,
        extract_voltage_from_page,
    )
    # Per-row fallback when LLM didn't fill Voltage
    voltage = extract_voltage_from_row(row_dict) or extract_voltage_from_page(page_text)
"""

from __future__ import annotations

import re
import logging
from collections import Counter
from typing import Optional

logger = logging.getLogger(__name__)


# ── Standard Indian power grid voltage levels ────────────────────────────────
# Ordered from highest to lowest so we prefer higher voltages when ambiguous.
_STANDARD_KV = ["765", "400", "220", "132", "110", "66", "33"]

# Regex: matches e.g. "400 kV", "400kV", "400KV",  "400 KVA" excluded
_KV_RE = re.compile(r"\b(\d{2,3})\s*kV\b", re.IGNORECASE)

# Row columns to scan (in priority order — substation most likely)
_ROW_SCAN_FIELDS = [
    "substaion",
    "Project Location",
    "Name of the developers",
    "Mode(Criteria for applying)",
    "Nature of Applicant",
    "type",
]

# Context keywords near which a voltage mention is most credible
_CONTEXT_WORDS = re.compile(
    r"\b(connectivity|substation|sub[\s\-]station|"
    r"pooling|injection|interconnection|voltage|level|kV)\b",
    re.IGNORECASE,
)


def _normalise(kv_str: str) -> str:
    """Return normalised voltage string, e.g. '400 kV'."""
    return f"{kv_str.strip()} kV"


def _best_voltage(candidates: list[str]) -> Optional[str]:
    """From a list of raw kV number strings, pick the best one.

    Priority:
    1. Standard voltage that appears most frequently
    2. Any standard voltage (highest first)
    3. Any candidate (most frequent)
    """
    if not candidates:
        return None

    # Filter to standard
    standard = [v for v in candidates if v in _STANDARD_KV]
    if standard:
        most_common, _ = Counter(standard).most_common(1)[0]
        return _normalise(most_common)

    # Non-standard: return most-frequent
    most_common, _ = Counter(candidates).most_common(1)[0]
    return _normalise(most_common)


def extract_voltage_from_row(row: dict) -> Optional[str]:
    """Extract voltage from a single row's cell values.

    Scans the most relevant columns (substation first, then location,
    developers, mode etc.) for voltage patterns like "400 kV".

    Parameters
    ----------
    row : dict
        Aliased row dict (keys = column display names).

    Returns
    -------
    str or None
        Voltage string like "400 kV", or None if not found.
    """
    # If the LLM already filled it in — just normalise and return
    existing = (row.get("Voltage") or "").strip()
    if existing:
        m = _KV_RE.search(existing)
        if m and m.group(1) in _STANDARD_KV:
            return _normalise(m.group(1))
        if m:
            return _normalise(m.group(1))

    candidates: list[str] = []

    for field in _ROW_SCAN_FIELDS:
        cell = str(row.get(field) or "").strip()
        if not cell:
            continue
        for m in _KV_RE.finditer(cell):
            candidates.append(m.group(1))

    return _best_voltage(candidates)


def extract_voltage_from_page(page_text: str) -> Optional[str]:
    """Page-level fallback: extract voltage from the page description.

    Used when all row-level extraction fails.  Scans the full page text,
    prioritising mentions near connectivity-related keywords.

    Parameters
    ----------
    page_text : str
        Full text extracted from a single PDF page.

    Returns
    -------
    str or None
    """
    if not page_text:
        return None

    # ── Context-aware: voltage within 100 chars of a context keyword ──
    context_candidates: list[str] = []
    for window_match in _CONTEXT_WORDS.finditer(page_text):
        start = max(0, window_match.start() - 80)
        end   = min(len(page_text), window_match.end() + 80)
        snippet = page_text[start:end]
        for m in _KV_RE.finditer(snippet):
            context_candidates.append(m.group(1))

    v = _best_voltage(context_candidates)
    if v:
        return v

    # ── Plain fallback: any kV on the page ────────────────────────────
    all_kv = [m.group(1) for m in _KV_RE.finditer(page_text)]
    return _best_voltage(all_kv)
