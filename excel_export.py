from __future__ import annotations

from datetime import datetime
import importlib
from pathlib import Path
from typing import Any


DATA_COLUMNS = [
    "PDF",
    "Page Number",
    "Project Location",
    "State",
    "substaion",
    "Name of the developers",
    "GNA/ST II Application ID",
    "LTA Application ID",
    "Application ID under Enhancement 5.2 or revision",
    "Application Quantum (MW)(ST II)",
    "Nature of Applicant",
    "Mode(Criteria for applying)",
    "Applied Start of Connectivity sought by developer date",
    "Application/Submission Date",
]


def _flatten_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []

    for pdf_result in payload.get("results", []):
        pdf_path = pdf_result.get("pdf_path", "")
        for page in pdf_result.get("results", []):
            page_number = page.get("page_number")
            for row in page.get("rows", []):
                record = {
                    "PDF": pdf_path,
                    "Page Number": page_number,
                }
                for col in DATA_COLUMNS[2:]:
                    record[col] = row.get(col)
                records.append(record)

    return records


def _autosize_columns(sheet) -> None:
    for column in sheet.columns:
        max_len = 0
        col_letter = column[0].column_letter
        for cell in column:
            value = "" if cell.value is None else str(cell.value)
            max_len = max(max_len, len(value))
        sheet.column_dimensions[col_letter].width = min(max(12, max_len + 2), 60)


def export_results_to_excel(
    payload: dict[str, Any],
    excel_path: str | Path,
    *,
    runtime_seconds: float,
    started_at: datetime,
    finished_at: datetime,
) -> Path:
    """Export aggregated extraction output to an Excel workbook."""
    try:
        Workbook = importlib.import_module("openpyxl").Workbook
    except Exception as exc:  # pragma: no cover - environment-dependent import
        raise RuntimeError(
            "Excel export requires 'openpyxl'. Install dependencies from requirements.txt."
        ) from exc

    workbook = Workbook()

    # Data sheet
    ws_data = workbook.active
    ws_data.title = "Extracted Data"
    ws_data.append(DATA_COLUMNS)

    flat_records = _flatten_rows(payload)
    for rec in flat_records:
        ws_data.append([rec.get(col) for col in DATA_COLUMNS])

    ws_data.freeze_panes = "A2"
    ws_data.auto_filter.ref = ws_data.dimensions
    _autosize_columns(ws_data)

    # Summary sheet
    ws_summary = workbook.create_sheet("Run Summary")
    summary_rows = [
        ("Run started at", started_at.isoformat(timespec="seconds")),
        ("Run finished at", finished_at.isoformat(timespec="seconds")),
        ("Total runtime (seconds)", round(runtime_seconds, 2)),
        ("PDFs processed", payload.get("pdfs_processed", 0)),
        ("Total pages extracted", payload.get("total_pages_extracted", 0)),
        ("Total pages passed gate", payload.get("total_pages_passed_gate", 0)),
        ("Total pages skipped", payload.get("total_pages_skipped", 0)),
        ("Total rows", payload.get("total_rows", 0)),
    ]
    for row in summary_rows:
        ws_summary.append(row)

    _autosize_columns(ws_summary)

    out_path = Path(excel_path).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(out_path)
    return out_path
