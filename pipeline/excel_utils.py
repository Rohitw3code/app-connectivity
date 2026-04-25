"""
pipeline/excel_utils.py — Generic JSON → Excel exporter
=========================================================
A single reusable function that converts any flat ``list[dict]``
(or a single ``dict``) into a formatted ``.xlsx`` workbook.

Usage
-----
    from pipeline.excel_utils import export_to_excel

    export_to_excel(
        rows        = my_flat_records,          # list[dict]
        output_path = "output/report.xlsx",
        sheet_name  = "Extracted Data",
        column_order= ["ID", "Name", "Value"],  # optional — auto-detected if omitted
        summary_rows= [                          # optional second sheet
            ("Run date", "2026-04-25"),
            ("Total rows", 42),
        ],
    )

All pipeline layers import this module; no layer-specific logic lives here.
"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_openpyxl():
    """Lazy import of openpyxl so the module is still importable without it."""
    try:
        return importlib.import_module("openpyxl")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Excel export requires 'openpyxl'. "
            "Run: pip install openpyxl"
        ) from exc


def _autosize_columns(sheet) -> None:
    """Set each column width to the max content length (capped at 60)."""
    for column in sheet.columns:
        max_len = 0
        col_letter = column[0].column_letter
        for cell in column:
            value = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(value))
        sheet.column_dimensions[col_letter].width = min(max(12, max_len + 2), 60)


def _apply_header_style(sheet, openpyxl_mod) -> None:
    """Bold, coloured header row with centre-alignment."""
    Font         = openpyxl_mod.styles.Font
    PatternFill  = openpyxl_mod.styles.PatternFill
    Alignment    = openpyxl_mod.styles.Alignment

    h_font  = Font(name="Arial", bold=True, size=10, color="FFFFFF")
    h_fill  = PatternFill("solid", start_color="1F4E79")
    h_align = Alignment(horizontal="center", vertical="center", wrap_text=True)

    sheet.row_dimensions[1].height = 36
    for cell in sheet[1]:
        cell.font      = h_font
        cell.fill      = h_fill
        cell.alignment = h_align


def _apply_data_style(sheet, openpyxl_mod) -> None:
    """Alternate row shading + body font for data rows."""
    Font        = openpyxl_mod.styles.Font
    PatternFill = openpyxl_mod.styles.PatternFill
    Alignment   = openpyxl_mod.styles.Alignment

    d_font   = Font(name="Arial", size=9)
    d_align  = Alignment(vertical="center", wrap_text=True)
    alt_fill = PatternFill("solid", start_color="EBF3FB")

    for row_idx in range(2, sheet.max_row + 1):
        sheet.row_dimensions[row_idx].height = 18
        fill = alt_fill if row_idx % 2 == 0 else None
        for cell in sheet[row_idx]:
            cell.font      = d_font
            cell.alignment = d_align
            if fill:
                cell.fill = fill


# ─── Public API ───────────────────────────────────────────────────────────────

def export_to_excel(
    rows: list[dict[str, Any]],
    output_path: str | Path,
    *,
    sheet_name: str = "Data",
    column_order: list[str] | None = None,
    summary_rows: list[tuple] | None = None,
    summary_sheet_name: str = "Run Summary",
) -> Path:
    """Convert a flat ``list[dict]`` to a formatted ``.xlsx`` workbook.

    Parameters
    ----------
    rows : list[dict]
        Records to write — one dict per spreadsheet row.
        All dicts should share the same keys; missing keys become blank cells.
    output_path : str | Path
        Destination ``.xlsx`` file.  Parent directories are created automatically.
    sheet_name : str
        Name of the main data worksheet.  Default: ``"Data"``.
    column_order : list[str] | None
        Explicit ordered list of column names.  When *None*, columns are
        derived from the union of all keys in *rows* (insertion order).
    summary_rows : list[tuple] | None
        Optional list of ``(label, value)`` tuples written to a second
        worksheet named *summary_sheet_name*.
    summary_sheet_name : str
        Name of the optional summary worksheet.  Default: ``"Run Summary"``.

    Returns
    -------
    Path
        Absolute path of the saved workbook.
    """
    opx = _get_openpyxl()
    wb  = opx.Workbook()

    # ── Determine column headers ───────────────────────────────────────────────
    if column_order:
        headers = column_order
    else:
        # Preserve insertion order; union of all keys across all rows
        seen: dict[str, None] = {}
        for row in rows:
            for key in row:
                seen[key] = None
        headers = list(seen)

    # ── Main data sheet ───────────────────────────────────────────────────────
    ws_data         = wb.active
    ws_data.title   = sheet_name
    ws_data.append(headers)

    for record in rows:
        ws_data.append([record.get(col) for col in headers])

    ws_data.freeze_panes       = "A2"
    ws_data.auto_filter.ref    = ws_data.dimensions

    _apply_header_style(ws_data, opx)
    _apply_data_style(ws_data, opx)
    _autosize_columns(ws_data)

    # ── Optional summary sheet ─────────────────────────────────────────────────
    if summary_rows:
        ws_summary       = wb.create_sheet(summary_sheet_name)
        for row in summary_rows:
            ws_summary.append(list(row))
        _autosize_columns(ws_summary)

    # ── Save ──────────────────────────────────────────────────────────────────
    out = Path(output_path).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out)
    return out
