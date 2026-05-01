"""
mapping_handler/runner.py — Mapping Orchestration
===================================================
Loads cmets.xlsx, builds effectiveness lookup, merges, writes
effectiveness_mapped.json + effectiveness_mapped.xlsx.

This is the only file that performs I/O orchestration for Module 3.
Edit merge.py to change the merge/enrichment logic.
Edit formatting.py to change the Excel styling.
Edit lookup.py to change how the effectiveness lookup is built.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from pipeline.mapping_handler.lookup import build_lookup
from pipeline.mapping_handler.merge import merge_rows
from pipeline.mapping_handler.formatting import format_mapped_excel
from pipeline.effectiveness_handler.date_updater import update_gna_dates

logger = logging.getLogger(__name__)

# ─── Default I/O paths ────────────────────────────────────────────────────────
_START_DIR = Path(__file__).resolve().parent.parent.parent

EFFECTIVENESS_OUTPUT_DIR : Path = _START_DIR / "output" / "effectiveness_cache"
CMETS_EXCEL_PATH         : Path = _START_DIR / "excels" / "cmets.xlsx"
MAPPED_JSON_PATH         : Path = _START_DIR / "output" / "cmets_effectiveness_mapped.json"
MAPPED_EXCEL_PATH        : Path = _START_DIR / "excels" / "effectiveness_mapped.xlsx"


# ─── Public API ───────────────────────────────────────────────────────────────

def run_mapping(
    cmets_excel_path:         Path | str | None = None,
    effectiveness_df:         pd.DataFrame | None = None,
    effectiveness_output_dir: Path | str | None = None,
    mapped_json_path:         Path | str | None = None,
    mapped_excel_path:        Path | str | None = None,
) -> Path:
    """Merge CMETS + effectiveness data → write JSON + Excel.

    Returns the absolute path to effectiveness_mapped.xlsx.
    """
    cmets_path  = Path(cmets_excel_path).resolve()         if cmets_excel_path         else CMETS_EXCEL_PATH
    eff_out_dir = Path(effectiveness_output_dir).resolve()  if effectiveness_output_dir  else EFFECTIVENESS_OUTPUT_DIR
    json_path   = Path(mapped_json_path).resolve()          if mapped_json_path          else MAPPED_JSON_PATH
    xlsx_path   = Path(mapped_excel_path).resolve()         if mapped_excel_path         else MAPPED_EXCEL_PATH

    if effectiveness_df is None:
        effectiveness_df = pd.DataFrame()

    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    eff_out_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 64)
    print("  MODULE 3 — CMETS × EFFECTIVENESS MAPPING")
    print("=" * 64)
    print(f"  CMETS Excel         : {cmets_path}")
    print(f"  Effectiveness cache : {eff_out_dir}")
    print(f"  Output JSON         : {json_path}")
    print(f"  Output Excel        : {xlsx_path}")
    print("=" * 64)

    # Build lookup
    lookup = build_lookup(effectiveness_df, eff_out_dir)
    if not lookup:
        logger.warning("[Mapping] No effectiveness data — output mirrors cmets.xlsx.")
        print("[Mapping] WARNING: No effectiveness data found.")
    print(f"[Mapping] Lookup: {len(lookup)} unique application IDs")

    # Load CMETS Excel
    if not cmets_path.exists():
        raise FileNotFoundError(
            f"[Mapping] cmets.xlsx not found: {cmets_path}. "
            "Run Module 1 (CMETS extraction) first."
        )
    cmets_df = pd.read_excel(cmets_path, sheet_name=0)
    print(f"[Mapping] CMETS rows loaded: {len(cmets_df)}")

    # Merge
    enriched_df, stats = merge_rows(cmets_df, lookup)
    print(
        f"[Mapping] Matched GNA: {stats['matched_gna']} | "
        f"LTA: {stats['matched_lta']} | "
        f"Unmatched: {stats['unmatched']} | "
        f"Total: {stats['total_rows']}"
    )

    # GNA Operationalization Date update
    # Compare effectiveness expected_date with CMETS GNA Operationalization Date
    # and update to the later value when effectiveness is newer.
    if lookup:
        enriched_df, date_stats = update_gna_dates(enriched_df, lookup)
        print(
            f"[Mapping] GNA Date Update: "
            f"Matched: {date_stats['matched']} | "
            f"Updated: {date_stats['updated_date']} | "
            f"Kept same: {date_stats['kept_same']} | "
            f"No eff date: {date_stats['no_eff_date']}"
        )

    # Dump JSON
    mapped_records = enriched_df.to_dict(orient="records")
    with open(json_path, "w", encoding="utf-8") as fh:
        json.dump(
            {"match_stats": stats, "records": mapped_records},
            fh, indent=2, ensure_ascii=False, default=str,
        )
    print(f"[Mapping] JSON saved → {json_path}")

    # Write Excel
    enriched_df.to_excel(str(xlsx_path), index=False, sheet_name="Extracted Data")
    format_mapped_excel(str(xlsx_path))
    print(f"[Mapping] Excel saved → {xlsx_path}")
    print("=" * 64)

    return xlsx_path
