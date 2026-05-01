"""
effectiveness_handler/date_updater.py — GNA Operationalization Date Updater
=============================================================================
Compares the "expected date of connectivity / GNA to be made effective"
from effectiveness data with the "GNA Operationalization Date" from the
CMETS sheet.  When the effectiveness date is LATER than the CMETS date,
it updates:

    • GNA Operationalization Date       → the later (effectiveness) date
    • GNA Operationalization (Yes/No)   → recomputed based on the new date

Pipeline Integration
--------------------
Called from the mapping handler (Module 3) AFTER the main merge step,
or directly from the main pipeline after Module 3 completes.

    from pipeline.effectiveness_handler.date_updater import update_gna_dates
    df = update_gna_dates(cmets_df, effectiveness_lookup)
"""

from __future__ import annotations

import re
import logging
from datetime import datetime, date
from typing import Optional

import pandas as pd

from pipeline.shared_utils import (
    find_col,
    safe_str,
    ids_from_cell,
    lookup_first
)

from pipeline.shared_utils import (
    find_col,
    safe_str,
    ids_from_cell,
    lookup_first
)

logger = logging.getLogger(__name__)


# ── Date parsing ──────────────────────────────────────────────────────────────

# Supported formats:
#   dd.mm.yyyy  /  dd/mm/yyyy  /  dd-mm-yyyy
#   yyyy.mm.dd  /  yyyy/mm/dd  /  yyyy-mm-dd
#   d Month yyyy  (e.g. "15 March 2026")

_DATE_PATTERNS = [
    (r"\b(\d{2})[./-](\d{2})[./-](\d{4})\b", "%d.%m.%Y"),
    (r"\b(\d{4})[./-](\d{2})[./-](\d{2})\b", "%Y.%m.%d"),
]

_MONTH_NAMES = {
    "jan": 1, "january": 1, "feb": 2, "february": 2, "mar": 3, "march": 3,
    "apr": 4, "april": 4, "may": 5, "jun": 6, "june": 6,
    "jul": 7, "july": 7, "aug": 8, "august": 8, "sep": 9, "september": 9,
    "oct": 10, "october": 10, "nov": 11, "november": 11, "dec": 12, "december": 12,
}


def parse_date(raw: Optional[str]) -> Optional[date]:
    """Parse a date string into a Python ``date`` object.

    Returns None if the string is empty, unparseable, or clearly invalid.
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text.lower() in ("", "none", "null", "na", "n/a", "-", "--", "nan"):
        return None

    # Normalise separators to dots for consistent parsing
    normalised = text.replace("/", ".").replace("-", ".")

    # Attempt dd.mm.yyyy or yyyy.mm.dd
    for pattern, fmt in _DATE_PATTERNS:
        m = re.search(pattern, normalised)
        if m:
            try:
                return datetime.strptime(m.group(0), fmt).date()
            except ValueError:
                continue

    # Attempt "d Month yyyy" style
    m = re.search(r"\b(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})\b", text)
    if m:
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = _MONTH_NAMES.get(month_name)
        if month:
            try:
                return date(year, month, day)
            except ValueError:
                pass

    return None


def _yes_no(d: Optional[date]) -> Optional[str]:
    """Determine GNA Operationalization (Yes/No) from a date.

    • Yes — if the date is in the future (GNA not yet operationalized)
    • No  — if the date is today or in the past (already operationalized)
    """
    if d is None:
        return None
    return "Yes" if d > date.today() else "No"


def _format_date(d: date) -> str:
    """Format a date as dd.mm.yyyy (Indian convention)."""
    return d.strftime("%d.%m.%Y")





# ── Public API ────────────────────────────────────────────────────────────────

def update_gna_dates(
    df: pd.DataFrame,
    effectiveness_lookup: dict[str, dict],
) -> tuple[pd.DataFrame, dict]:
    """Compare effectiveness expected_date with CMETS GNA Operationalization
    Date and update to the later value when effectiveness is newer.

    Parameters
    ----------
    df : pd.DataFrame
        The CMETS DataFrame (or CMETS × Effectiveness merged DataFrame).
    effectiveness_lookup : dict
        ``application_id → record`` dict built by the mapping lookup module.
        Each record should have an ``expected_date`` key.

    Returns
    -------
    (updated_df, stats)
        stats keys: total_rows, matched, updated_date, kept_same, no_eff_date
    """
    col_gna_id   = find_col(df, "GNA/ST II Application ID")
    col_lta_id   = find_col(df, "LTA Application ID")
    col_gna_date = find_col(df, "GNA Operationalization Date")
    col_gna_yn   = find_col(df, "GNA Operationalization (Yes/No)")

    if not col_gna_date:
        logger.warning("[DateUpdater] 'GNA Operationalization Date' column not found — skipping.")
        return df, {"total_rows": len(df), "matched": 0, "updated_date": 0,
                     "kept_same": 0, "no_eff_date": 0, "error": "column_not_found"}

    # Ensure Yes/No column exists
    if not col_gna_yn:
        col_gna_yn = "GNA Operationalization (Yes/No)"
        if col_gna_yn not in df.columns:
            df[col_gna_yn] = None

    matched       = 0
    updated_date  = 0
    kept_same     = 0
    no_eff_date   = 0

    for idx, row in df.iterrows():
        # Find effectiveness record via GNA or LTA ID
        gna_ids = ids_from_cell(row.get(col_gna_id)) if col_gna_id else []
        eff_rec = lookup_first(gna_ids, effectiveness_lookup)

        if eff_rec is None and col_lta_id:
            lta_ids = ids_from_cell(row.get(col_lta_id))
            eff_rec = lookup_first(lta_ids, effectiveness_lookup)

        if eff_rec is None:
            continue

        matched += 1

        # Parse effectiveness expected_date
        eff_date_raw = eff_rec.get("expected_date")
        eff_date = parse_date(eff_date_raw)

        if eff_date is None:
            no_eff_date += 1
            continue

        # Parse CMETS GNA Operationalization Date
        cmets_date_raw = safe_str(row.get(col_gna_date))
        cmets_date = parse_date(cmets_date_raw)

        # Decide which date to use
        if cmets_date is None:
            # No existing CMETS date → use effectiveness date
            final_date = eff_date
            updated_date += 1
        elif eff_date > cmets_date:
            # Effectiveness date is later → update
            final_date = eff_date
            updated_date += 1
        else:
            # CMETS date is same or later → keep
            final_date = cmets_date
            kept_same += 1

        # Write the final date and recompute Yes/No
        df.at[idx, col_gna_date] = _format_date(final_date)
        df.at[idx, col_gna_yn]   = _yes_no(final_date)

    stats = {
        "total_rows":   len(df),
        "matched":      matched,
        "updated_date": updated_date,
        "kept_same":    kept_same,
        "no_eff_date":  no_eff_date,
    }

    logger.info(
        "[DateUpdater] Rows: %d | Matched: %d | Updated: %d | Kept same: %d | No eff date: %d",
        len(df), matched, updated_date, kept_same, no_eff_date,
    )

    return df, stats


# ── Additional Capacity Date Updater ─────────────────────────────────────────

def update_additional_capacity_dates(
    df: pd.DataFrame,
    effectiveness_lookup: dict[str, dict],
) -> tuple[pd.DataFrame, dict]:
    """Compare effectiveness ``expected_date`` with CMETS
    ``Date from which additional capacity is to be added`` and update
    to the later value when effectiveness is newer.

    This is the same future-date logic used in ``update_gna_dates`` but
    targets a different CMETS column.

    Parameters
    ----------
    df : pd.DataFrame
        The CMETS DataFrame (or merged DataFrame).
    effectiveness_lookup : dict
        ``application_id → record`` dict with an ``expected_date`` key.

    Returns
    -------
    (updated_df, stats)
        stats keys: total_rows, matched, updated_date, kept_same, no_eff_date
    """
    col_gna_id   = find_col(df, "GNA/ST II Application ID")
    col_lta_id   = find_col(df, "LTA Application ID")
    col_52_id    = find_col(df, "Application ID under Enhancement 5.2 or revision")
    col_add_date = find_col(df, "Date from which additional capacity is to be added")

    if not col_add_date:
        # Create the column if it doesn't exist
        col_add_date = "Date from which additional capacity is to be added"
        if col_add_date not in df.columns:
            df[col_add_date] = None

    matched       = 0
    updated_date  = 0
    kept_same     = 0
    no_eff_date   = 0

    for idx, row in df.iterrows():
        # ── Find effectiveness record via GNA → LTA → 5.2 cascade ────────
        eff_rec = None

        if col_gna_id:
            gna_ids = ids_from_cell(row.get(col_gna_id))
            eff_rec = lookup_first(gna_ids, effectiveness_lookup)

        if eff_rec is None and col_lta_id:
            lta_ids = ids_from_cell(row.get(col_lta_id))
            eff_rec = lookup_first(lta_ids, effectiveness_lookup)

        if eff_rec is None and col_52_id:
            enh_ids = ids_from_cell(row.get(col_52_id))
            eff_rec = lookup_first(enh_ids, effectiveness_lookup)

        if eff_rec is None:
            continue

        matched += 1

        # ── Parse effectiveness expected_date ─────────────────────────────
        eff_date_raw = eff_rec.get("expected_date")
        eff_date = parse_date(eff_date_raw)

        if eff_date is None:
            no_eff_date += 1
            continue

        # ── Parse CMETS additional capacity date ─────────────────────────
        cmets_date_raw = safe_str(row.get(col_add_date))
        cmets_date = parse_date(cmets_date_raw)

        # ── Decide which date to use (future-date logic) ─────────────────
        if cmets_date is None:
            # No existing CMETS date → use effectiveness date
            final_date = eff_date
            updated_date += 1
        elif eff_date > cmets_date:
            # Effectiveness date is later → update
            final_date = eff_date
            updated_date += 1
        else:
            # CMETS date is same or later → keep
            final_date = cmets_date
            kept_same += 1

        # ── Write the final date ─────────────────────────────────────────
        df.at[idx, col_add_date] = _format_date(final_date)

    stats = {
        "total_rows":   len(df),
        "matched":      matched,
        "updated_date": updated_date,
        "kept_same":    kept_same,
        "no_eff_date":  no_eff_date,
    }

    logger.info(
        "[AdditionalCapacityDateUpdater] Rows: %d | Matched: %d | Updated: %d | Kept same: %d | No eff date: %d",
        len(df), matched, updated_date, kept_same, no_eff_date,
    )

    return df, stats
