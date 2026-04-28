"""
cmets_handler/meeting_classifier.py — CMETS Meeting Number & Date Extraction
===============================================================================
Reads the FIRST PAGE of each CMETS PDF and extracts:

    1. Meeting number  — e.g. "42" from "42nd Consulting Meeting"
                         or from "Ref: CTU/N/00/CMETS_NR/42"
    2. Meeting date    — e.g. "11th November 2025 (Tuesday)"
                         parsed as dd.mm.yyyy

Then determines which columns the values go into based on GNA vs LTA
keyword dominance across the ENTIRE PDF:

    • If GNA keywords exist in comparable ratio to LTA (or GNA dominant):
        → CMETS GNA Approved  = meeting number
        → CMETS GNA Meeting Date = meeting date

    • If LTA keywords are clearly dominant:
        → CMETS LTA Approved  = meeting number
        → CMETS LTA Meeting Date = meeting date

This is a **pre-extraction** step — the 4 values produced here are
the same for EVERY row extracted from the same PDF.

Pipeline Integration
--------------------
Called from runner.py BEFORE page-by-page extraction.  The returned
dict is injected into every flattened row during the _flatten() step.

    from pipeline.cmets_handler.meeting_classifier import classify_meeting
    meta = classify_meeting(pdf_path)
"""

from __future__ import annotations

import re
import logging
from dataclasses import dataclass
from typing import Optional

import pdfplumber

logger = logging.getLogger(__name__)


# ── Result container ─────────────────────────────────────────────────────────

@dataclass
class MeetingMeta:
    """Metadata extracted from the first page of a CMETS PDF."""
    meeting_number:       Optional[str] = None
    meeting_date:         Optional[str] = None   # dd.mm.yyyy
    cmets_gna_approved:   Optional[str] = None   # meeting number (if GNA pathway)
    cmets_lta_approved:   Optional[str] = None   # meeting number (if LTA pathway)
    cmets_gna_meeting_date: Optional[str] = None # meeting date   (if GNA pathway)
    cmets_lta_meeting_date: Optional[str] = None # meeting date   (if LTA pathway)
    classification:       str = ""               # "GNA" or "LTA"
    gna_count:            int = 0
    lta_count:            int = 0

    def as_row_dict(self) -> dict:
        """Return the 4 columns to inject into every extracted row."""
        return {
            "CMETS GNA Approved":     self.cmets_gna_approved,
            "CMETS LTA Approved":     self.cmets_lta_approved,
            "CMETS GNA Meeting Date": self.cmets_gna_meeting_date,
            "CMETS LTA Meeting Date": self.cmets_lta_meeting_date,
        }


# ── Month name → number ─────────────────────────────────────────────────────

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    # Abbreviated
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9,
    "oct": 10, "nov": 11, "dec": 12,
}


# ── Meeting number extraction ───────────────────────────────────────────────

def _extract_meeting_number(text: str) -> Optional[str]:
    """Extract the consulting meeting number from the first page text.

    Looks for patterns like:
      - "42nd Consulting Meeting"
      - "42nd CMETS"
      - "Ref: CTU/N/00/CMETS_NR/42"
      - "CMETS_SR/35"
    """
    # Pattern 1: "NNth/st/nd/rd Consulting Meeting" or "NNth/st/nd/rd CMETS"
    m = re.search(
        r"(\d{1,3})\s*(?:st|nd|rd|th)\s+(?:consulting\s+meeting|cmets)",
        text, re.IGNORECASE,
    )
    if m:
        return m.group(1)

    # Pattern 2: Ref number like "CMETS_NR/42" or "CMETS_SR/35" or "CMETS/42"
    m = re.search(
        r"CMETS(?:_[A-Z]{1,3})?[/\\](\d{1,3})",
        text, re.IGNORECASE,
    )
    if m:
        return m.group(1)

    # Pattern 3: "Meeting No. 42" or "Meeting Number 42"
    m = re.search(
        r"meeting\s+(?:no\.?\s*|number\s*)(\d{1,3})",
        text, re.IGNORECASE,
    )
    if m:
        return m.group(1)

    return None


# ── Meeting date extraction ──────────────────────────────────────────────────

def _extract_meeting_date(text: str) -> Optional[str]:
    """Extract the meeting date from the first page text.

    Looks for natural language dates like:
      - "11th November 2025 (Tuesday)"
      - "3rd March 2026"
      - "25 April 2025"

    Returns date as dd.mm.yyyy string.
    Does NOT match numeric formats like 28-2025 or 11/2025.
    """
    # Pattern: "Nth Month YYYY" with optional day-of-week
    month_names = "|".join(_MONTHS.keys())
    pattern = (
        r"(\d{1,2})\s*(?:st|nd|rd|th)?\s+"
        rf"({month_names})\s+"
        r"(\d{{4}})"
    )
    m = re.search(pattern, text, re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = _MONTHS.get(month_name)
        if month and 1 <= day <= 31 and 2000 <= year <= 2099:
            return f"{day:02d}.{month:02d}.{year}"

    # Pattern 2: "Month Nth, YYYY" (American-ish)
    pattern2 = (
        rf"({month_names})\s+"
        r"(\d{1,2})\s*(?:st|nd|rd|th)?,?\s+"
        r"(\d{4})"
    )
    m = re.search(pattern2, text, re.IGNORECASE)
    if m:
        month_name = m.group(1).lower()
        day = int(m.group(2))
        year = int(m.group(3))
        month = _MONTHS.get(month_name)
        if month and 1 <= day <= 31 and 2000 <= year <= 2099:
            return f"{day:02d}.{month:02d}.{year}"

    return None


# ── GNA vs LTA keyword ratio ────────────────────────────────────────────────

# Patterns used to count GNA vs LTA presence.
# We scan the ENTIRE PDF (not just page 1) for a representative ratio.
_GNA_PATTERNS = [
    r"\bGNA\b",
    r"\bST[\s-]*II\b",
    r"\bStage[\s-]*II\b",
    r"\bGNA/ST\s*II\b",
]

_LTA_PATTERNS = [
    r"\bLTA\b",
    r"\bLong\s+Term\s+Access\b",
]


def _count_keywords(full_text: str) -> tuple[int, int]:
    """Count GNA-related and LTA-related keyword occurrences."""
    gna_count = sum(
        len(re.findall(pat, full_text, re.IGNORECASE))
        for pat in _GNA_PATTERNS
    )
    lta_count = sum(
        len(re.findall(pat, full_text, re.IGNORECASE))
        for pat in _LTA_PATTERNS
    )
    return gna_count, lta_count


def _classify(gna_count: int, lta_count: int) -> str:
    """Determine if this PDF is GNA-dominant or LTA-dominant.

    Logic:
      • If both GNA and LTA exist in comparable ratio (GNA >= LTA * 0.3)
        → classify as "GNA"  (GNA is default / dominant)
      • If LTA is clearly dominant (GNA < LTA * 0.3)
        → classify as "LTA"
      • If neither keyword exists → default "GNA"
    """
    if lta_count == 0 and gna_count == 0:
        return "GNA"  # default
    if lta_count == 0:
        return "GNA"
    if gna_count == 0:
        return "LTA"

    # If GNA count is at least 30% of LTA count → GNA (comparable or dominant)
    if gna_count >= lta_count * 0.3:
        return "GNA"
    return "LTA"


# ── Public API ───────────────────────────────────────────────────────────────

def classify_meeting(pdf_path: str) -> MeetingMeta:
    """Extract meeting metadata from a CMETS PDF.

    Steps:
      1. Read the first page → extract meeting number + date
      2. Read ALL pages → count GNA vs LTA keywords
      3. Classify as "GNA" or "LTA" based on keyword ratio
      4. Place meeting number + date into the appropriate columns

    Parameters
    ----------
    pdf_path : str
        Absolute path to the CMETS PDF.

    Returns
    -------
    MeetingMeta
        Contains the 4 column values to inject into every row.
    """
    meta = MeetingMeta()

    try:
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                logger.warning("[MeetingClassifier] PDF has no pages: %s", pdf_path)
                return meta

            # ── Step 1: First page — extract meeting number + date ─────────
            first_page_text = pdf.pages[0].extract_text(
                x_tolerance=3, y_tolerance=3,
            ) or ""

            meta.meeting_number = _extract_meeting_number(first_page_text)
            meta.meeting_date = _extract_meeting_date(first_page_text)

            if not meta.meeting_number:
                logger.info(
                    "[MeetingClassifier] Could not extract meeting number from: %s",
                    pdf_path,
                )

            if not meta.meeting_date:
                # Try page 2 as fallback for date (some PDFs have date on second page)
                if len(pdf.pages) > 1:
                    page2 = pdf.pages[1].extract_text(x_tolerance=3, y_tolerance=3) or ""
                    meta.meeting_date = _extract_meeting_date(page2)

            # ── Step 2: Full PDF — count GNA vs LTA keywords ──────────────
            all_text_parts: list[str] = []
            for page in pdf.pages:
                t = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
                if t.strip():
                    all_text_parts.append(t)
            full_text = "\n".join(all_text_parts)

            gna_count, lta_count = _count_keywords(full_text)
            meta.gna_count = gna_count
            meta.lta_count = lta_count

    except Exception as exc:
        logger.error("[MeetingClassifier] Failed to read PDF %s: %s", pdf_path, exc)
        return meta

    # ── Step 3: Classify ──────────────────────────────────────────────────
    meta.classification = _classify(meta.gna_count, meta.lta_count)

    # ── Step 4: Place values into the correct columns ─────────────────────
    if meta.classification == "GNA":
        meta.cmets_gna_approved = meta.meeting_number
        meta.cmets_gna_meeting_date = meta.meeting_date
    else:
        meta.cmets_lta_approved = meta.meeting_number
        meta.cmets_lta_meeting_date = meta.meeting_date

    logger.info(
        "[MeetingClassifier] %s — Meeting #%s, Date %s, "
        "GNA: %d, LTA: %d → %s",
        pdf_path, meta.meeting_number, meta.meeting_date,
        meta.gna_count, meta.lta_count, meta.classification,
    )

    return meta
