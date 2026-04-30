"""
bayallocation_handler/extraction.py -- PDF table extraction logic
=================================================================
Uses pdfplumber to extract the bay-allocation table from each page of the
Bay Allocation PDF.  Each page is treated as one independent extraction unit.

The handler receives a **single page** and extracts only the following columns
per row (each row is identified by its serial number):

JSON structure produced per page
---------------------------------
  {
    "page_number": int,
    "raw_text":    str,
    "substations": [
        {
          "sl_no":                    str,
          "name_of_substation":       str,
          "substation_coordinates":   str,
          "region":                   str,
          "220kv": {
              "bay_no": {
                  "204": "Name of Entity",
                  "34":  "",
                  "24":  "Name of Entity"
              }
          },
          "400kv": {
              "bay_no": {
                  "100": "Name of Entity",
                  "55":  ""
              }
          }
        },
        ...
    ]
  }

Each unique substation (sl_no) produces exactly ONE item in "substations".
Every bay number is mapped to its entity name; if the entity name is
missing the value is an empty string.
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


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _clean(text) -> str:
    """Collapse whitespace and strip a cell value."""
    if text is None:
        return ""
    return " ".join(str(text).split())


def _is_target_table(table: list) -> bool:
    """Return True if the table looks like the bay-allocation master table."""
    if not table or len(table) < 2:
        return False
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
    """Return True if the entire row is just one section label (e.g. 'Section-A')."""
    non_empty = [v for v in row if v]
    if len(non_empty) == 1:
        val = non_empty[0].strip()
        if re.match(r"^section[\s\-]*[a-zA-Z]", val, re.IGNORECASE):
            return True
    return False


def _is_total_row(row: list[str]) -> bool:
    """Return True if the row is just a subtotal / grand-total row."""
    non_empty = [v for v in row if v]
    if len(non_empty) == 1:
        try:
            float(non_empty[0].replace(",", ""))
            return True
        except ValueError:
            pass
    return False


# Tokens used to detect leaked sub-header rows
_SUB_HEADER_TOKENS = {
    "220kv", "400kv", "bay no.", "bay no", "name of entity",
    "connectivity", "quantum (mw)", "quantum", "margins available",
    "bay-wise margins", "available (mw)",
}
_VOLTAGE_LABELS = {"220kv", "400kv", "220kv*", "400kv*"}


def _is_sub_header_row(row: list[str]) -> bool:
    """Return True if every non-empty cell is a sub-header / voltage label.

    Catches two kinds of leaked header rows that pdfplumber sometimes emits
    past the fixed HEADER_ROW_COUNT skip:
      1. The voltage row  ('220kV', '400kV', '220kV*', '400kV*', ...)
      2. The column-label row ('Bay No.', 'Name of Entity', ...)
    """
    non_empty = [v.strip().lower() for v in row if v.strip()]
    if not non_empty:
        return False
    if all(cell in _VOLTAGE_LABELS for cell in non_empty):
        return True
    return all(
        any(tok in cell for tok in _SUB_HEADER_TOKENS)
        for cell in non_empty
    )


def _is_section_str(v: str) -> bool:
    """Return True if *v* looks like a section label ('Section-A', etc.)."""
    return bool(re.match(r"^section[\s\-]*[a-zA-Z]", v.strip(), re.IGNORECASE))


# ---------------------------------------------------------------------------
# Page-level gate
# ---------------------------------------------------------------------------

def page_passes_gate(text: str) -> bool:
    """Check that the page text contains all required bay-allocation keywords."""
    return all(kw in text for kw in REQUIRED_KEYWORDS)


# ---------------------------------------------------------------------------
# Helper: create an empty substation dict
# ---------------------------------------------------------------------------

def _new_substation(sl_no: str = "",
                    name: str = "",
                    coords: str = "",
                    region: str = "") -> dict:
    """Return a fresh substation dict with empty 220kV / 400kV bay dicts."""
    return {
        "sl_no":                  sl_no,
        "name_of_substation":     name,
        "substation_coordinates": coords,
        "region":                 region,
        "220kv": {
            "bay_no": {},   # {bay_number: entity_name_or_empty}
        },
        "400kv": {
            "bay_no": {},   # {bay_number: entity_name_or_empty}
        },
    }


# ---------------------------------------------------------------------------
# Core extraction:  page  ->  list of substations
# ---------------------------------------------------------------------------

def extract_page_data(page, page_number: int) -> Optional[dict]:
    """Extract all substations from one pdfplumber page.

    Each unique substation (identified by sl_no appearing in column 0)
    becomes **exactly one item** in the returned ``substations`` list.
    Each bay number is mapped to its entity name (or empty string) in
    the ``bay_no`` dict under the ``220kv`` and ``400kv`` keys.

    Returns None if no allocation table is found on this page.
    """
    tables = page.extract_tables()
    target = None
    for tbl in tables:
        if _is_target_table(tbl):
            target = tbl
            break

    if target is None:
        return None

    n_cols     = len(COLUMN_NAMES)          # 20 columns
    data_rows  = target[HEADER_ROW_COUNT:]  # skip header rows

    substations: list[dict] = []
    current_sub: dict | None = None

    for raw_row in data_rows:
        norm = _normalise_row(raw_row, n_cols)

        # ── Skip noise rows ────────────────────────────────────────────────
        if not any(norm):
            continue
        if _is_sub_header_row(norm):
            continue
        if _is_section_header(norm):
            continue
        if _is_total_row(norm):
            continue

        # ── Unpack only the columns we care about ──────────────────────────
        sl_no           = norm[0]
        substation_name = norm[1]
        coordinates     = norm[2]
        region          = norm[3]

        bay_no_220      = norm[7]
        entity_220      = norm[9]
        bay_no_400      = norm[10]
        entity_400      = norm[12]

        # ── Skip rows that are ONLY a section label leaked into bay cols ───
        for val in [bay_no_220, bay_no_400, region]:
            if _is_section_str(val):
                break
        else:
            # none of them were section labels — this is fine
            pass

        if _is_section_str(bay_no_220) and not any([entity_220, bay_no_400, entity_400]):
            continue
        if _is_section_str(bay_no_400) and not any([entity_400, bay_no_220, entity_220]):
            continue

        # ── New substation header row (identified by sl_no) ────────────────
        if sl_no or substation_name:
            # Flush the previous substation before starting a new one
            if current_sub is not None:
                substations.append(current_sub)

            current_sub = _new_substation(
                sl_no=sl_no,
                name=substation_name,
                coords=coordinates,
                region=region,
            )

        # ── Collect bay data into current substation's lists ───────────────
        has_bay_data = any([bay_no_220, entity_220, bay_no_400, entity_400])
        if not has_bay_data:
            continue

        # If a bay row appears before any substation header, create
        # an anonymous placeholder so no data is lost.
        if current_sub is None:
            current_sub = _new_substation()

        if bay_no_220:
            current_sub["220kv"]["bay_no"][bay_no_220] = entity_220
        if bay_no_400:
            current_sub["400kv"]["bay_no"][bay_no_400] = entity_400

    # Flush the last substation
    if current_sub is not None:
        substations.append(current_sub)

    if not substations:
        return None

    return {
        "page_number":  page_number,
        "raw_text":     page.extract_text() or "",
        "substations":  substations,
    }


# ---------------------------------------------------------------------------
# Single-PDF extraction
# ---------------------------------------------------------------------------

def extract_bayallocation_pdf(pdf_path: str) -> list[dict]:
    """Extract all pages from one Bay Allocation PDF.

    Each page is treated as one independent extraction unit.

    Returns
    -------
    list[dict]
        One element per page that contains a valid allocation table.
        Each element has 'page_number', 'raw_text', and 'substations'
        (a list of substation dicts with aggregated 220kV/400kV lists).
    """
    all_pages: list[dict] = []

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        print(f"  [BayAllocation] {total} pages -- scanning ...")

        for i, page in enumerate(pdf.pages):
            page_number = i + 1
            text = page.extract_text() or ""

            if not page_passes_gate(text):
                print(f"  o Page {page_number:3d} -- skipped (keyword gate)")
                continue

            result = extract_page_data(page, page_number)
            if result is None:
                print(f"  o Page {page_number:3d} -- no allocation table found")
                continue

            sub_count = len(result["substations"])
            print(f"  + Page {page_number:3d} -> {sub_count} substations")
            all_pages.append(result)

    return all_pages
