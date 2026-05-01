"""
bay_mapping_handler/runner.py — Bay Allocation Mapping Orchestration (Module 6)
================================================================================
Loads CMETS extracted data (from cmets.xlsx or the mapped Excel), loads the
Bay Allocation JSON cache, builds a lookup index, merges developer+voltage
against bay allocation entries, and writes an enriched output Excel.

Usage (standalone):
    from pipeline.bay_mapping_handler import run_bay_mapping
    result_path = run_bay_mapping()

Usage (in pipeline — after Modules 1–5):
    result_path = run_bay_mapping(
        cmets_excel_path = str(cmets_path),
        bay_output_dir   = args.bay_output_dir,
    )
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from pipeline.bay_mapping_handler.lookup import build_bay_lookup
from pipeline.bay_mapping_handler.merge import merge_bay_allocation, BAY_MAPPING_COLUMNS
from pipeline.mapping_handler.formatting import format_mapped_excel

logger = logging.getLogger(__name__)

# ─── Default I/O paths ────────────────────────────────────────────────────────
_START_DIR = Path(__file__).resolve().parent.parent.parent   # …/start/

# Input: CMETS data (prefer the fully-enriched mapped file, fall back to raw)
CMETS_EXCEL_PATH  : Path = _START_DIR / "excels" / "cmets_extracted.xlsx"
# Input: Bay allocation JSON cache
BAY_OUTPUT_DIR    : Path = _START_DIR / "output" / "bayallocation_cache"
# Output: Enriched Excel
BAY_MAPPED_EXCEL  : Path = _START_DIR / "excels" / "cmets_bay_mapped.xlsx"
# Output: Enriched JSON
BAY_MAPPED_JSON   : Path = _START_DIR / "excels" / "cmets_bay_mapped.json"


# ─── Public API ───────────────────────────────────────────────────────────────

def run_bay_mapping(
    cmets_excel_path:  Path | str | None = None,
    bay_output_dir:    Path | str | None = None,
    output_excel_path: Path | str | None = None,
    output_json_path:  Path | str | None = None,
) -> Path:
    """Merge CMETS + Bay Allocation data → write enriched Excel + JSON.

    Parameters
    ----------
    cmets_excel_path  : Path to the CMETS extracted Excel file.
                        (default: excels/cmets_extracted.xlsx)
    bay_output_dir    : Path to bay allocation JSON cache directory.
                        (default: output/bayallocation_cache/)
    output_excel_path : Path for the output enriched Excel.
                        (default: excels/cmets_bay_mapped.xlsx)
    output_json_path  : Path for the output enriched JSON.
                        (default: excels/cmets_bay_mapped.json)

    Returns
    -------
    Path
        Absolute path to the generated Excel file.
    """
    cmets_path = Path(cmets_excel_path).resolve() if cmets_excel_path else CMETS_EXCEL_PATH
    bay_dir    = Path(bay_output_dir).resolve()    if bay_output_dir    else BAY_OUTPUT_DIR
    xlsx_out   = Path(output_excel_path).resolve()  if output_excel_path  else BAY_MAPPED_EXCEL
    json_out   = Path(output_json_path).resolve()   if output_json_path   else BAY_MAPPED_JSON

    xlsx_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.parent.mkdir(parents=True, exist_ok=True)
    bay_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 64)
    print("  MODULE 6 — CMETS × BAY ALLOCATION MAPPING")
    print("=" * 64)
    print(f"  CMETS Excel         : {cmets_path}")
    print(f"  Bay Allocation cache : {bay_dir}")
    print(f"  Output Excel        : {xlsx_out}")
    print(f"  Output JSON         : {json_out}")
    print("=" * 64)

    # ── Step 1: Build bay allocation lookup ────────────────────────────────
    bay_index = build_bay_lookup(bay_dir)
    total_220 = len(bay_index.get("220kv", []))
    total_400 = len(bay_index.get("400kv", []))
    print(f"[BayMapping] Bay index: 220kV={total_220} entries, 400kV={total_400} entries")

    if total_220 + total_400 == 0:
        print("[BayMapping] WARNING: No bay allocation data found.")
        print("[BayMapping]  → Run Module 5 (Bay Allocation Extraction) first.")

    # ── Step 2: Load CMETS Excel ──────────────────────────────────────────
    if not cmets_path.exists():
        raise FileNotFoundError(
            f"[BayMapping] CMETS Excel not found: {cmets_path}. "
            "Run Module 1 (CMETS extraction) first."
        )
    cmets_df = pd.read_excel(cmets_path, sheet_name=0)
    print(f"[BayMapping] CMETS rows loaded: {len(cmets_df)}")

    if cmets_df.empty:
        print("[BayMapping] WARNING: CMETS DataFrame is empty — nothing to map.")
        cmets_df.to_excel(str(xlsx_out), index=False, sheet_name="Bay Mapped Data")
        return xlsx_out

    # ── Step 3: Merge ─────────────────────────────────────────────────────
    enriched_df, stats = merge_bay_allocation(cmets_df, bay_index)
    print(
        f"[BayMapping] Results: "
        f"Matched={stats['matched']} | "
        f"Multi-match={stats['multi_match']} | "
        f"No voltage={stats['no_voltage']} | "
        f"No developer={stats['no_developer']} | "
        f"Unmatched={stats['unmatched']} | "
        f"Total={stats['total_rows']}"
    )

    # ── Step 4: Dump JSON ─────────────────────────────────────────────────
    mapped_records = enriched_df.to_dict(orient="records")
    with open(json_out, "w", encoding="utf-8") as fh:
        json.dump(
            {"match_stats": stats, "records": mapped_records},
            fh, indent=2, ensure_ascii=False, default=str,
        )
    print(f"[BayMapping] JSON saved → {json_out}")

    # ── Step 5: Write Excel ───────────────────────────────────────────────
    enriched_df.to_excel(str(xlsx_out), index=False, sheet_name="Bay Mapped Data")
    format_mapped_excel(str(xlsx_out))
    print(f"[BayMapping] Excel saved → {xlsx_out}")
    print("=" * 64)

    return xlsx_out
