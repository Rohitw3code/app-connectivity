"""
bayallocation_handler/extraction.py -- PDF table extraction logic
=================================================================
Uses pdfplumber to extract the bay-allocation table from each page of the
Bay Allocation PDF. Each page is treated as one independent extraction unit.

The output keeps the old substation-level ``220kv.bay_no`` / ``400kv.bay_no``
dicts for compatibility, and also emits richer row-level JSON:

* ``substations[].allocations``: searchable allocation records containing
  voltage, bay number, entity name, connectivity quantum, substation name and
  coordinates together.
* ``pages[].table_rows``: formatted table rows with all canonical column names
  preserved for debugging/searching.
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


def _compact(text: str) -> str:
    """Lowercase text and remove non-alphanumerics for PDF header matching."""
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


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
    compact_header = _compact(header_text)
    hits = sum(
        1
        for kw in TARGET_COLUMN_FRAGMENTS
        if kw.lower() in header_text or _compact(kw) in compact_header
    )
    return hits >= 3


def _normalise_row(row: list, n_cols: int) -> list[str]:
    """Pad or trim a row to exactly *n_cols* cells."""
    cleaned = [_clean(c) for c in row]
    if len(cleaned) < n_cols:
        cleaned += [""] * (n_cols - len(cleaned))
    else:
        cleaned = cleaned[:n_cols]
    return cleaned


def _table_x_edges(table) -> list[float]:
    """Return column boundaries inferred from pdfplumber table cells."""
    return sorted({x for cell in table.cells for x in (cell[0], cell[2])})


def _cell_text(page, cell) -> str:
    """Extract text from a pdfplumber cell bbox with spacing preserved."""
    if not cell:
        return ""
    text = page.crop(cell).extract_text(x_tolerance=1, y_tolerance=3) or ""
    return _clean(text)


def _spanned_cell_text(page, table, row_index: int, col_index: int) -> str:
    """Return text from a row-spanned cell covering row_index/col_index.

    pdfplumber returns ``None`` for cells covered by a rowspan. The bay
    allocation PDFs use rowspans for quantum/entity cells when multiple bay
    numbers share the same grant. This helper looks upward in the same column
    and reuses the cell whose bbox still covers the current row midpoint.
    """
    row = table.rows[row_index]
    y_mid = (row.bbox[1] + row.bbox[3]) / 2

    for prev_index in range(row_index - 1, -1, -1):
        if col_index >= len(table.rows[prev_index].cells):
            continue
        prev_cell = table.rows[prev_index].cells[col_index]
        if not prev_cell:
            continue
        if prev_cell[1] <= y_mid <= prev_cell[3]:
            return _cell_text(page, prev_cell)
        if prev_cell[3] < y_mid:
            break
    return ""


def _extract_row_from_cells(page, table, row_index: int, n_cols: int) -> list[str]:
    """Extract one table row using cell bboxes and fill same-column rowspans."""
    cells = table.rows[row_index].cells
    values: list[str] = []
    for col_index in range(n_cols):
        cell = cells[col_index] if col_index < len(cells) else None
        value = _cell_text(page, cell)
        if not value and cell is None:
            value = _spanned_cell_text(page, table, row_index, col_index)
        values.append(value)
    return values


def _extract_left_column_values(page, table, row_index: int, n_cols: int) -> list[str]:
    """Extract left fixed columns by x/y crop.

    Some rows begin a new substation in left-side columns while pdfplumber's
    table extraction leaves those row-spanned cells empty. Cropping by inferred
    grid boundaries recovers the serial number, substation name and coordinates.
    """
    try:
        x_edges = _table_x_edges(table)
    except Exception:
        return [""] * min(7, n_cols)

    if len(x_edges) < n_cols + 1:
        return [""] * min(7, n_cols)

    row_bbox = table.rows[row_index].bbox
    values: list[str] = []
    for col_index in range(min(7, n_cols)):
        bbox = (x_edges[col_index], row_bbox[1], x_edges[col_index + 1], row_bbox[3])
        text = page.crop(bbox).extract_text(x_tolerance=1, y_tolerance=3) or ""
        values.append(_clean(text))
    return values


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


def _section_label(row: list[str]) -> str:
    """Return the first section label found in the row."""
    for value in row:
        if _is_section_str(value):
            return value.strip()
    return ""


def _is_number_like(value: str) -> bool:
    if not value:
        return False
    return bool(re.fullmatch(r"\d+(?:\.\d+)?", value.replace(",", "").strip()))


def _split_entities(entity_text: str) -> list[dict]:
    """Split an entity cell into searchable entity parts.

    The PDF often stores multiple developers in one cell separated by semicolon
    or plus signs. The raw cell is still authoritative; this list helps matching
    without losing the original wording.
    """
    if not entity_text:
        return []

    text = _clean(entity_text)
    pieces = re.split(r";|\s+\+\s+(?=[A-Z])", text)
    entities: list[dict] = []
    for piece in pieces:
        part = _clean(piece.strip(" ;,+"))
        if not part:
            continue
        capacity_match = re.search(r"\(([^)]*?(?:MW|\d)[^)]*)\)", part, flags=re.IGNORECASE)
        name = _clean(re.sub(r"\([^)]*\)", "", part).strip(" ;,+"))
        entities.append({
            "name": name or part,
            "raw": part,
            "capacity_text": capacity_match.group(1) if capacity_match else "",
        })
    return entities


# ---------------------------------------------------------------------------
# Page-level gate
# ---------------------------------------------------------------------------

def page_passes_gate(text: str) -> bool:
    """Check that the page text contains all required bay-allocation keywords."""
    compact_text = _compact(text)
    lower_text = (text or "").lower()
    return all(
        kw.lower() in lower_text or _compact(kw) in compact_text
        for kw in REQUIRED_KEYWORDS
    )


# ---------------------------------------------------------------------------
# Helper: create an empty substation dict
# ---------------------------------------------------------------------------

def _new_substation(sl_no: str = "",
                    name: str = "",
                    coords: str = "",
                    region: str = "",
                    planned: str = "",
                    existing: str = "",
                    under_implementation: str = "") -> dict:
    """Return a fresh substation dict with empty allocation containers."""
    return {
        "sl_no":                                          sl_no,
        "name_of_substation":                             name,
        "substation_coordinates":                         coords,
        "region":                                         region,
        "transformation_capacity_planned_mva":            planned,
        "transformation_capacity_existing_mva":           existing,
        "transformation_capacity_under_implementation_mva": under_implementation,
        "allocations": [],
        "220kv": {
            "bay_no": {},
            "entries": [],
        },
        "400kv": {
            "bay_no": {},
            "entries": [],
        },
    }


def _merge_substation_header(sub: dict, row: list[str]) -> None:
    """Fill missing substation header fields from a newly detected header row."""
    field_map = {
        "sl_no": 0,
        "name_of_substation": 1,
        "substation_coordinates": 2,
        "region": 3,
        "transformation_capacity_planned_mva": 4,
        "transformation_capacity_existing_mva": 5,
        "transformation_capacity_under_implementation_mva": 6,
    }
    for key, idx in field_map.items():
        if row[idx] and not sub.get(key):
            sub[key] = row[idx]


def _allocation_entry(
    *,
    page_number: int,
    table_row_index: int,
    section: str,
    voltage_key: str,
    bay_no: str,
    quantum: str,
    entity: str,
    margin_bay_no: str,
    margin_available: str,
    substation: dict,
) -> dict:
    voltage_label = "220 kV" if voltage_key == "220kv" else "400 kV"
    return {
        "page_number": page_number,
        "table_row_index": table_row_index,
        "section": section,
        "voltage": voltage_label,
        "voltage_key": voltage_key,
        "bay_no": bay_no,
        "connectivity_quantum_mw": quantum,
        "name_of_entity": entity,
        "entities": _split_entities(entity),
        "margin_bay_no": margin_bay_no,
        "margin_available_mw": margin_available,
        "name_of_substation": substation.get("name_of_substation", ""),
        "substation_coordinates": substation.get("substation_coordinates", ""),
        "region": substation.get("region", ""),
        "sl_no": substation.get("sl_no", ""),
        "search_fields": {
            "entity": entity,
            "substation": substation.get("name_of_substation", ""),
            "coordinates": substation.get("substation_coordinates", ""),
            "voltage": voltage_label,
            "connectivity_quantum_mw": quantum,
            "bay_no": bay_no,
        },
    }


def _table_row_record(
    *,
    page_number: int,
    table_row_index: int,
    section: str,
    row: list[str],
    substation: dict,
) -> dict:
    record = {
        "page_number": page_number,
        "table_row_index": table_row_index,
        "section": section,
    }
    for idx, column_name in enumerate(COLUMN_NAMES):
        record[column_name] = row[idx] if idx < len(row) else ""

    # Make every row directly searchable even if the PDF used row-spans.
    for key in (
        "sl_no",
        "name_of_substation",
        "substation_coordinates",
        "region",
        "transformation_capacity_planned_mva",
        "transformation_capacity_existing_mva",
        "transformation_capacity_under_implementation_mva",
    ):
        if not record.get(key):
            record[key] = substation.get(key, "")
    return record


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
    table_objects = page.find_tables()
    target_table = None
    target = None
    for tbl in table_objects:
        extracted = tbl.extract()
        if _is_target_table(extracted):
            target_table = tbl
            target = extracted
            break

    if target is None or target_table is None:
        return None

    n_cols = len(COLUMN_NAMES)

    substations: list[dict] = []
    current_sub: dict | None = None
    table_rows: list[dict] = []
    current_section = ""

    for table_row_index in range(HEADER_ROW_COUNT, len(target_table.rows)):
        raw_row = _extract_row_from_cells(page, target_table, table_row_index, n_cols)
        norm = _normalise_row(raw_row, n_cols)

        # Recover left fixed columns for rows where pdfplumber drops rowspans.
        left_values = _extract_left_column_values(page, target_table, table_row_index, n_cols)
        for idx, value in enumerate(left_values):
            if value and (idx <= 3 or not norm[idx]):
                norm[idx] = value

        # ── Skip noise rows ────────────────────────────────────────────────
        if not any(norm):
            continue
        if _is_sub_header_row(norm):
            continue

        section = _section_label(norm)
        if section:
            current_section = section

        # Pure section rows carry no allocation data but define context.
        if _is_section_header(norm):
            continue
        if _is_total_row(norm):
            continue

        # ── Unpack only the columns we care about ──────────────────────────
        sl_no           = norm[0]
        substation_name = norm[1]
        coordinates     = norm[2]
        region          = norm[3]
        planned         = norm[4]
        existing        = norm[5]
        under_impl      = norm[6]

        bay_no_220      = norm[7]
        quantum_220     = norm[8]
        entity_220      = norm[9]
        bay_no_400      = norm[10]
        quantum_400     = norm[11]
        entity_400      = norm[12]
        margin_bay_220  = norm[13]
        margin_220      = norm[14]
        margin_bay_400  = norm[15]
        margin_400      = norm[16]

        if _is_section_str(bay_no_220) and not any([entity_220, bay_no_400, entity_400]):
            continue
        if _is_section_str(bay_no_400) and not any([entity_400, bay_no_220, entity_220]):
            continue

        # ── New substation header row (identified by sl_no) ────────────────
        starts_substation = _is_number_like(sl_no) or (substation_name and coordinates)
        if starts_substation:
            # Flush the previous substation before starting a new one
            if current_sub is not None:
                substations.append(current_sub)

            current_sub = _new_substation(
                sl_no=sl_no,
                name=substation_name,
                coords=coordinates,
                region=region,
                planned=planned,
                existing=existing,
                under_implementation=under_impl,
            )
        elif current_sub is not None:
            _merge_substation_header(current_sub, norm)

        # ── Collect bay data into current substation's lists ───────────────
        has_bay_data = any([
            bay_no_220, quantum_220, entity_220,
            bay_no_400, quantum_400, entity_400,
            margin_bay_220, margin_220, margin_bay_400, margin_400,
        ])
        if not has_bay_data:
            continue

        # If a bay row appears before any substation header, create
        # an anonymous placeholder so no data is lost.
        if current_sub is None:
            current_sub = _new_substation()

        table_rows.append(
            _table_row_record(
                page_number=page_number,
                table_row_index=table_row_index,
                section=current_section,
                row=norm,
                substation=current_sub,
            )
        )

        if bay_no_220 and not _is_section_str(bay_no_220):
            current_sub["220kv"]["bay_no"][bay_no_220] = entity_220
            entry = _allocation_entry(
                page_number=page_number,
                table_row_index=table_row_index,
                section=current_section,
                voltage_key="220kv",
                bay_no=bay_no_220,
                quantum=quantum_220,
                entity=entity_220,
                margin_bay_no=margin_bay_220,
                margin_available=margin_220,
                substation=current_sub,
            )
            current_sub["220kv"]["entries"].append(entry)
            current_sub["allocations"].append(entry)

        if bay_no_400 and not _is_section_str(bay_no_400):
            current_sub["400kv"]["bay_no"][bay_no_400] = entity_400
            entry = _allocation_entry(
                page_number=page_number,
                table_row_index=table_row_index,
                section=current_section,
                voltage_key="400kv",
                bay_no=bay_no_400,
                quantum=quantum_400,
                entity=entity_400,
                margin_bay_no=margin_bay_400,
                margin_available=margin_400,
                substation=current_sub,
            )
            current_sub["400kv"]["entries"].append(entry)
            current_sub["allocations"].append(entry)

    # Flush the last substation
    if current_sub is not None:
        substations.append(current_sub)

    if not substations:
        return None

    return {
        "page_number":  page_number,
        "raw_text":     page.extract_text() or "",
        "columns":      COLUMN_NAMES,
        "table_rows":   table_rows,
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
