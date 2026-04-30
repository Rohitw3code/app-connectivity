"""
bayallocation_handler/extraction.py — PDF table extraction logic
=================================================================
Uses pdfplumber to extract the bay-allocation table from each page of the
Bay Allocation PDF.  Each page is treated as one independent extraction unit
and produces one JSON "page" object.

The table is a 20-column layout spanning substations, their bays, RE capacity
granted, margins, space provisions and remarks.
"""

from __future__ import annotations

import re
from typing import Optional

import pdfplumber

from pipeline.bayallocation_handler.models import (
    REQUIRED_KEYWORDS,
    TARGET_COLUMN_FRAGMENTS,
    COLUMN_NAMES,
    HEADER_ROW_COUNT,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _clean(text) -> str:
    """Collapse whitespace and strip a cell value."""
    if text is None:
        return ""
    return " ".join(str(text).split())


def _is_target_table(table: list) -> bool:
    """Return True if the table looks like the bay-allocation master table."""
    if not table or len(table) < 2:
        return False
    # Join first 3 rows to build a composite header string
    header_text = " ".join(
        _clean(c).lower()
        for row in table[:3]
        for c in (row or [])
        if c
    )
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


def _is_section_header(row: list[str]) -> bool:
    """Return True if this row is just a section label (e.g. 'Section-A')."""
    non_empty = [v for v in row if v]
    if len(non_empty) == 1:
        val = non_empty[0].strip()
        if re.match(r"^section[\s\-]*[a-zA-Z]", val, re.IGNORECASE):
            return True
    return False


def _is_total_row(row: list[str]) -> bool:
    """Return True if the row is just a subtotal / grand-total row."""
    non_empty = [v for v in row if v]
    # Pure numeric rows with no entity name suggest a subtotal
    if len(non_empty) == 1:
        try:
            float(non_empty[0].replace(",", ""))
            return True
        except ValueError:
            pass
    return False


# Sub-header tokens that indicate a repeated header row leaking past the skip
_SUB_HEADER_TOKENS = {
    "220kv", "400kv", "bay no.", "bay no", "name of entity",
    "connectivity", "quantum (mw)", "quantum", "margins available",
    "bay-wise margins", "available (mw)",
}

# Voltage-label tokens that appear in the sub-header voltage row
_VOLTAGE_LABELS = {"220kv", "400kv", "220kv*", "400kv*"}


def _is_sub_header_row(row: list[str]) -> bool:
    """Return True if this row consists entirely of sub-header label tokens.

    Catches two kinds of leaked header rows:
      1. The voltage row ("220kV", "400kV", "220kV*" …)
      2. The column-label row ("Bay No.", "Name of Entity", "Connectivity Quantum …")
    """
    non_empty = [v.strip().lower() for v in row if v.strip()]
    if not non_empty:
        return False

    # If every non-empty cell is a voltage label → voltage sub-header row
    if all(cell in _VOLTAGE_LABELS for cell in non_empty):
        return True

    # If every non-empty cell contains a known sub-header token → column label row
    return all(
        any(tok in cell for tok in _SUB_HEADER_TOKENS)
        for cell in non_empty
    )



# ── Page-level extraction ─────────────────────────────────────────────────────

def page_passes_gate(text: str) -> bool:
    """Check that the page text contains all required bay-allocation keywords."""
    return all(kw in text for kw in REQUIRED_KEYWORDS)


def extract_page_data(page, page_number: int) -> Optional[dict]:
    """Extract the bay-allocation table from a single pdfplumber Page.

    Returns
    -------
    dict
        {
          "page_number": int,
          "raw_text":    str,
          "entries":     [
              {
                "sl_no":                    str,
                "name_of_substation":       str,
                "substation_coordinates":   str,
                "region":                   str,
                "transformation_capacity_planned_mva":                   str,
                "transformation_capacity_existing_mva":                  str,
                "transformation_capacity_under_implementation_mva":      str,
                "section":                  str,          # current section label
                "bay_no_220kv":             str,
                "connectivity_quantum_mw_220kv": str,
                "name_of_entity_220kv":     str,
                "bay_no_400kv":             str,
                "connectivity_quantum_mw_400kv": str,
                "name_of_entity_400kv":     str,
                "margin_bay_no_220kv":      str,
                "margin_available_mw_220kv":str,
                "margin_bay_no_400kv":      str,
                "margin_available_mw_400kv":str,
                "space_provision_220kv":    str,
                "space_provision_400kv":    str,
                "remarks":                  str,
              },
              …
          ]
        }
    or None if no target table found.
    """
    tables = page.extract_tables()
    target = None
    for tbl in tables:
        if _is_target_table(tbl):
            target = tbl
            break

    if target is None:
        return None

    n_cols = len(COLUMN_NAMES)  # 20
    data_rows = target[HEADER_ROW_COUNT:]  # skip the 3 header rows

    entries: list[dict] = []
    current_section = ""
    current_substation_context: dict = {}  # carries sl_no, name, coords, region, capacity

    for raw_row in data_rows:
        norm = _normalise_row(raw_row, n_cols)

        # Skip completely empty rows
        if not any(norm):
            continue

        # Skip sub-header repeats that pdfplumber sometimes emits past row 3
        if _is_sub_header_row(norm):
            continue

        # Detect section header embedded in col 7 or col 10
        # pdfplumber sometimes places "Section-A" in col7 or col10
        possible_section_cols = [norm[7], norm[10], norm[3]]
        for sc in possible_section_cols:
            if re.match(r"^section[\s\-]*[a-zA-Z]", sc.strip(), re.IGNORECASE):
                current_section = sc.strip()
                break

        # Check if this is a pure section header row (nothing else of value)
        if _is_section_header(norm):
            continue

        # Skip pure subtotal rows
        if _is_total_row(norm):
            continue

        # If sl_no / substation columns are populated, this is a substation header row
        # Update the "current substation context" but also try to emit bay data if present
        sl_no = norm[0]
        substation_name = norm[1]
        coordinates = norm[2]
        region = norm[3]
        cap_planned = norm[4]
        cap_existing = norm[5]
        cap_under_impl = norm[6]

        if sl_no or substation_name:
            current_substation_context = {
                "sl_no":                                          sl_no,
                "name_of_substation":                             substation_name,
                "substation_coordinates":                         coordinates,
                "region":                                         region,
                "transformation_capacity_planned_mva":            cap_planned,
                "transformation_capacity_existing_mva":           cap_existing,
                "transformation_capacity_under_implementation_mva": cap_under_impl,
            }

        # Bay / entity columns
        bay_no_220     = norm[7]
        quantum_220    = norm[8]
        entity_220     = norm[9]
        bay_no_400     = norm[10]
        quantum_400    = norm[11]
        entity_400     = norm[12]
        margin_bay_220 = norm[13]
        margin_mw_220  = norm[14]
        margin_bay_400 = norm[15]
        margin_mw_400  = norm[16]
        space_220      = norm[17]
        space_400      = norm[18]
        remarks        = norm[19]

        # Skip section-header strings that leaked into bay columns
        def _is_section_str(v: str) -> bool:
            return bool(re.match(r"^section[\s\-]*[a-zA-Z]", v.strip(), re.IGNORECASE))

        for col_val in [bay_no_220, bay_no_400]:
            if _is_section_str(col_val):
                # just a section label, keep tracking but don't emit
                current_section = col_val
                break

        # Only emit a row if there is meaningful bay / entity data
        has_bay_data = any([
            bay_no_220, quantum_220, entity_220,
            bay_no_400, quantum_400, entity_400,
            margin_bay_220, margin_mw_220,
            margin_bay_400, margin_mw_400,
            remarks,
        ])

        if not has_bay_data:
            continue

        # Suppress pure section-label rows that slipped through
        if _is_section_str(bay_no_220) and not any([
                quantum_220, entity_220, bay_no_400, quantum_400, entity_400]):
            continue
        if _is_section_str(bay_no_400) and not any([
                quantum_400, entity_400, bay_no_220, quantum_220, entity_220]):
            continue

        entry = {
            # Substation meta (carried forward from last substation header row)
            **current_substation_context,
            # Section label
            "section": current_section,
            # 220 kV RE capacity granted
            "bay_no_220kv":                    bay_no_220,
            "connectivity_quantum_mw_220kv":   quantum_220,
            "name_of_entity_220kv":            entity_220,
            # 400 kV RE capacity granted
            "bay_no_400kv":                    bay_no_400,
            "connectivity_quantum_mw_400kv":   quantum_400,
            "name_of_entity_400kv":            entity_400,
            # Margins
            "margin_bay_no_220kv":             margin_bay_220,
            "margin_available_mw_220kv":       margin_mw_220,
            "margin_bay_no_400kv":             margin_bay_400,
            "margin_available_mw_400kv":       margin_mw_400,
            # Space provision (count of additional line bays)
            "space_provision_220kv":           space_220,
            "space_provision_400kv":           space_400,
            # Remarks
            "remarks":                         remarks,
        }
        entries.append(entry)

    if not entries:
        return None

    return {
        "page_number": page_number,
        "raw_text":    page.extract_text() or "",
        "entries":     entries,
    }


# ── Single-PDF extraction ─────────────────────────────────────────────────────

def extract_bayallocation_pdf(pdf_path: str) -> list[dict]:
    """Extract all pages from one Bay Allocation PDF.

    Each page is treated as one independent extraction unit.

    Returns
    -------
    list[dict]
        A list of page result dicts (same shape as extract_page_data returns).
        One element per page that contains a valid allocation table.
    """
    all_pages: list[dict] = []

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        print(f"  [BayAllocation] {total} pages — scanning …")

        for i, page in enumerate(pdf.pages):
            page_number = i + 1
            text = page.extract_text() or ""

            if not page_passes_gate(text):
                print(f"  ○ Page {page_number:3d} — skipped (keyword gate)")
                continue

            result = extract_page_data(page, page_number)
            if result is None:
                print(f"  ○ Page {page_number:3d} — no allocation table found")
                continue

            entry_count = len(result["entries"])
            print(f"  ✓ Page {page_number:3d} → {entry_count} bay entries")
            all_pages.append(result)

    return all_pages
