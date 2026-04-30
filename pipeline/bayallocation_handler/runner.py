"""
bayallocation_handler/runner.py — Bay Allocation Orchestration (Module 5)
==========================================================================
Discovers all Bay Allocation PDFs in source/bayallocation/, checks a JSON
cache, extracts un-cached PDFs, and writes per-PDF and combined JSON output.

Each page of a PDF is treated as one independent extraction unit.

Usage (standalone):
    from pipeline.bayallocation_handler import run_bayallocation_extraction
    df = run_bayallocation_extraction()
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from config import RuntimeConfig, load_runtime_config
from pipeline.excel_utils import export_to_excel
from pipeline.bayallocation_handler.extraction import extract_bayallocation_pdf

logger = logging.getLogger(__name__)

# ─── Default I/O paths ────────────────────────────────────────────────────────
_START_DIR = Path(__file__).resolve().parent.parent.parent   # …/start/

BAY_SOURCE_DIR : Path = _START_DIR / "source" / "bayallocation"
BAY_OUTPUT_DIR : Path = _START_DIR / "output" / "bayallocation_cache"
BAY_EXCEL      : Path = _START_DIR / "excels" / "bayallocation_extracted.xlsx"

# Flat columns exported to Excel (in order)
EXCEL_COLUMNS = [
    "source_pdf",
    "page_number",
    "sl_no",
    "name_of_substation",
    "substation_coordinates",
    "region",
    "transformation_capacity_planned_mva",
    "transformation_capacity_existing_mva",
    "transformation_capacity_under_implementation_mva",
    "section",
    "bay_no_220kv",
    "connectivity_quantum_mw_220kv",
    "name_of_entity_220kv",
    "bay_no_400kv",
    "connectivity_quantum_mw_400kv",
    "name_of_entity_400kv",
    "margin_bay_no_220kv",
    "margin_available_mw_220kv",
    "margin_bay_no_400kv",
    "margin_available_mw_400kv",
    "space_provision_220kv",
    "space_provision_400kv",
    "remarks",
]


# ─── Cache helpers ────────────────────────────────────────────────────────────

def _cache_path(pdf_name: str, out_dir: Path) -> Path:
    return out_dir / f"{Path(pdf_name).stem}.json"


def _save_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)


def _load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _flatten(all_results: list[dict]) -> list[dict]:
    """Flatten per-PDF, per-page, per-substation, per-bay into flat rows for Excel."""
    flat: list[dict] = []
    for pdf_result in all_results:
        source = pdf_result.get("source", "")
        for page in pdf_result.get("pages", []):
            pnum = page.get("page_number")
            for sub in page.get("substations", []):
                sub_meta = {
                    "source_pdf":      source,
                    "page_number":     pnum,
                    "sl_no":           sub.get("sl_no", ""),
                    "name_of_substation":    sub.get("name_of_substation", ""),
                    "substation_coordinates":sub.get("substation_coordinates", ""),
                    "region":          sub.get("region", ""),
                    "transformation_capacity_planned_mva":
                        sub.get("transformation_capacity_planned_mva", ""),
                    "transformation_capacity_existing_mva":
                        sub.get("transformation_capacity_existing_mva", ""),
                    "transformation_capacity_under_implementation_mva":
                        sub.get("transformation_capacity_under_implementation_mva", ""),
                }
                for bay in sub.get("bays", []):
                    rec = {**sub_meta, **bay}
                    flat.append(rec)
    return flat


# ─── Public API ───────────────────────────────────────────────────────────────

def run_bayallocation_extraction(
    source_dir: Path | str | None = None,
    output_dir: Path | str | None = None,
    excel_path: Path | str | None = None,
    runtime:    Optional[RuntimeConfig] = None,
) -> pd.DataFrame:
    """Discover Bay Allocation PDFs → extract (skip cached) → dump JSON → write Excel.

    Parameters
    ----------
    source_dir : path to folder containing Bay Allocation PDFs
                 (default: source/bayallocation/)
    output_dir : path for per-PDF JSON cache files
                 (default: output/bayallocation_cache/)
    excel_path : path for the combined Excel report
                 (default: excels/bayallocation_extracted.xlsx)
    runtime    : RuntimeConfig instance (auto-loaded if None)

    Returns
    -------
    pd.DataFrame
        Flat DataFrame with one row per bay entry extracted.
    """
    src  = Path(source_dir).resolve() if source_dir else BAY_SOURCE_DIR
    out  = Path(output_dir).resolve() if output_dir else BAY_OUTPUT_DIR
    xlsx = Path(excel_path).resolve() if excel_path else BAY_EXCEL

    src.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    xlsx.parent.mkdir(parents=True, exist_ok=True)

    if runtime is None:
        runtime = load_runtime_config()

    print("\n" + "=" * 64)
    print("  MODULE 5 — BAY ALLOCATION PDF EXTRACTION")
    print("=" * 64)
    print(f"  Source dir  : {src}")
    print(f"  Output dir  : {out}")
    print(f"  Excel output: {xlsx}")

    # Recursive scan for PDFs
    pdf_files = sorted(src.rglob("*.pdf"))
    if not pdf_files:
        print(f"  [BayAllocation] No PDFs found in: {src}")
        print("=" * 64)
        return pd.DataFrame()

    cached_count = sum(1 for p in pdf_files if _cache_path(p.name, out).exists())
    print(f"  PDFs found  : {len(pdf_files)}")
    print(f"  Cached      : {cached_count}  (will be skipped)")
    print(f"  To extract  : {len(pdf_files) - cached_count}")
    print(f"  Mode        : {runtime.execution_target}")
    print("=" * 64)

    all_results: list[dict] = []

    for idx, pdf_path in enumerate(pdf_files, 1):
        cache = _cache_path(pdf_path.name, out)

        if cache.exists():
            print(f"\n  [{idx}/{len(pdf_files)}] SKIP    {pdf_path.name}")
            all_results.append(_load_json(cache))
            continue

        print(f"\n  [{idx}/{len(pdf_files)}] EXTRACT {pdf_path.name}")
        print("-" * 48)

        try:
            pages = extract_bayallocation_pdf(str(pdf_path))
        except Exception as exc:
            logger.error("[BayAllocation] Failed %s: %s", pdf_path.name, exc)
            print(f"  ERROR   {pdf_path.name}: {exc}")
            continue

        result = {
            "source":             pdf_path.name,
            "total_pages":        len(pages),
            "total_substations":  sum(len(p.get("substations", [])) for p in pages),
            "total_bays":         sum(
                len(s["bays"])
                for p in pages
                for s in p.get("substations", [])
            ),
            "pages":              pages,
        }

        _save_json(result, cache)
        print(f"  -> {len(pages)} pages, "
              f"{result['total_substations']} substations, "
              f"{result['total_bays']} bay rows saved -> {cache.name}")
        all_results.append(result)

    # -- Aggregate ─────────────────────────────────────────────────────────────
    total_pdfs         = len(all_results)
    total_pages        = sum(r.get("total_pages", 0) for r in all_results)
    total_substations  = sum(r.get("total_substations", 0) for r in all_results)
    flat_rows          = _flatten(all_results)
    total_bay_rows     = len(flat_rows)

    print("\n" + "=" * 64)
    print("  BAY ALLOCATION SUMMARY")
    print(f"    PDFs processed  : {total_pdfs}")
    print(f"    Pages matched   : {total_pages}")
    print(f"    Substations     : {total_substations}")
    print(f"    Bay rows total  : {total_bay_rows}")
    print("=" * 64)

    if not flat_rows:
        return pd.DataFrame()

    df = pd.DataFrame(flat_rows)

    # ── Excel export ─────────────────────────────────────────────────────────
    col_order = [c for c in EXCEL_COLUMNS if c in df.columns]
    export_to_excel(
        rows         = flat_rows,
        output_path  = xlsx,
        sheet_name   = "Bay Allocation Data",
        column_order = col_order,
        summary_rows = [
            ("PDFs processed",  total_pdfs),
            ("Pages matched",   total_pages),
            ("Substations",     total_substations),
            ("Bay rows total",  total_bay_rows),
        ],
    )
    print(f"\n[BayAllocation] Excel → {xlsx}")

    return df
