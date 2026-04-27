"""
jcc_handler/runner.py — JCC Orchestration (Module 4)
======================================================
Discovers all JCC Meeting PDFs in the source folder (recursive scan),
checks JSON cache, extracts un-cached PDFs, writes per-PDF and combined
JSON + Excel output.

After extraction, runs the **JCC Output Layer** which cross-references
with effectiveness data to compute GNA / TGNA values.

This is the only file that performs I/O orchestration for Module 4.
Edit extraction.py to change how tables are detected or parsed.
Edit models.py to change column names or keyword filters.
Edit jcc_output_layer.py to change the GNA/TGNA computation logic.
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
from pipeline.jcc_handler.models import COLUMN_NAMES
from pipeline.jcc_handler.extraction import extract_jcc_pdf
from pipeline.jcc_handler.jcc_output_layer import run_jcc_output_layer, run_layer4_excel

logger = logging.getLogger(__name__)

# ─── Default I/O paths ────────────────────────────────────────────────────────
_START_DIR = Path(__file__).resolve().parent.parent.parent   # …/start/

JCC_SOURCE_DIR  : Path = _START_DIR / "source" / "jcc_pdfs"
JCC_OUTPUT_DIR  : Path = _START_DIR / "output" / "jcc_cache"
JCC_EXCEL       : Path = _START_DIR / "excels" / "jcc_extracted.xlsx"


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
    """Flatten per-PDF results into flat rows for Excel."""
    flat: list[dict] = []
    for pdf_result in all_results:
        source = pdf_result.get("source", "")
        for page in pdf_result.get("pages", []):
            pnum = page.get("page_number")
            for row in page.get("rows", []):
                rec = {"source_pdf": source, "page_number": pnum}
                rec.update(row)
                flat.append(rec)
    return flat


# ─── Public API ───────────────────────────────────────────────────────────────

def run_jcc_extraction(
    source_dir:  Path | str | None = None,
    output_dir:  Path | str | None = None,
    excel_path:  Path | str | None = None,
    runtime:     Optional[RuntimeConfig] = None,
    *,
    effectiveness_df: pd.DataFrame | None = None,
    effectiveness_excel_path: Path | str | None = None,
    effectiveness_output_dir: Path | str | None = None,
    jcc_output_excel_path: Path | str | None = None,
    mapped_excel_path: Path | str | None = None,
    mapped_df: pd.DataFrame | None = None,
    layer4_excel_path: Path | str | None = None,
) -> pd.DataFrame:
    """Discover JCC Meeting PDFs → extract (skip cached) → dump JSON → write Excel.

    After extraction, runs the JCC Output Layer to cross-reference with
    effectiveness data and compute GNA / TGNA values.

    Parameters
    ----------
    effectiveness_df : DataFrame, optional
        Pre-loaded effectiveness data (from Module 2).
    effectiveness_excel_path : Path, optional
        Path to effectiveness_combined.xlsx (fallback).
    effectiveness_output_dir : Path, optional
        Folder with effectiveness JSON caches (fallback).
    jcc_output_excel_path : Path, optional
        Output path for the 4-column JCC Output Excel.
    mapped_excel_path : Path, optional
        Path to effectiveness_mapped.xlsx (Module 3 output) for Layer 4.
    mapped_df : DataFrame, optional
        Pre-loaded Module 3 mapped DataFrame for Layer 4.
    layer4_excel_path : Path, optional
        Output path for the Layer 4 Excel (all mapped data + GNA/TGNA).

    Returns pd.DataFrame with all extracted JCC rows.
    """
    src  = Path(source_dir).resolve()  if source_dir else JCC_SOURCE_DIR
    out  = Path(output_dir).resolve()  if output_dir else JCC_OUTPUT_DIR
    xlsx = Path(excel_path).resolve()  if excel_path else JCC_EXCEL

    src.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)
    xlsx.parent.mkdir(parents=True, exist_ok=True)

    if runtime is None:
        runtime = load_runtime_config()

    print("\n" + "=" * 64)
    print("  MODULE 4 — JCC MEETING PDF EXTRACTION")
    print("=" * 64)
    print(f"  Source dir  : {src}")
    print(f"  Output dir  : {out}")
    print(f"  Excel output: {xlsx}")

    # Recursive scan for PDFs
    pdf_files = sorted(src.rglob("*.pdf"))
    if not pdf_files:
        print(f"  [JCC] No PDFs found in: {src}")
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
            pages = extract_jcc_pdf(str(pdf_path))
        except Exception as exc:
            logger.error("[JCC] Failed %s: %s", pdf_path.name, exc)
            print(f"  ERROR   {pdf_path.name}: {exc}")
            continue

        result = {
            "source": pdf_path.name,
            "total_matching_pages": len(pages),
            "pages": pages,
        }

        _save_json(result, cache)
        total_rows = sum(len(p.get("rows", [])) for p in pages)
        print(f"  → {len(pages)} pages, {total_rows} rows saved → {cache.name}")
        all_results.append(result)

    # Aggregate stats
    total_pdfs   = len(all_results)
    total_pages  = sum(r.get("total_matching_pages", 0) for r in all_results)
    flat_rows    = _flatten(all_results)
    total_rows   = len(flat_rows)

    print("\n" + "=" * 64)
    print("  JCC SUMMARY")
    print(f"    PDFs processed  : {total_pdfs}")
    print(f"    Pages matched   : {total_pages}")
    print(f"    Total data rows : {total_rows}")
    print("=" * 64)

    if not flat_rows:
        return pd.DataFrame()

    df = pd.DataFrame(flat_rows)

    # Excel column order
    col_order = ["source_pdf", "page_number"] + [c for c in COLUMN_NAMES if c in df.columns]
    export_to_excel(
        rows         = flat_rows,
        output_path  = xlsx,
        sheet_name   = "JCC Extracted Data",
        column_order = col_order,
        summary_rows = [
            ("PDFs processed",  total_pdfs),
            ("Pages matched",   total_pages),
            ("Total data rows", total_rows),
        ],
    )
    print(f"\n[JCC] Excel → {xlsx}")

    # ── JCC Output Layer — GNA / TGNA cross-reference ─────────────────────
    try:
        jcc_output_df = run_jcc_output_layer(
            jcc_results              = all_results,
            effectiveness_excel_path = effectiveness_excel_path,
            effectiveness_df         = effectiveness_df,
            effectiveness_output_dir = effectiveness_output_dir,
            output_excel_path        = jcc_output_excel_path,
        )
        print(f"\n[JCC] ✓ Output Layer complete — {len(jcc_output_df)} rows")
    except Exception as exc:
        logger.error("[JCC] Output Layer failed: %s", exc)
        print(f"\n[JCC] ⚠ Output Layer failed: {exc}")

    # ── Layer 4 — Full mapped data + GNA / TGNA ──────────────────────────
    try:
        layer4_df = run_layer4_excel(
            jcc_results       = all_results,
            mapped_excel_path = mapped_excel_path,
            mapped_df         = mapped_df,
            output_excel_path = layer4_excel_path,
        )
        print(f"\n[JCC] ✓ Layer 4 complete — {len(layer4_df)} rows")
    except Exception as exc:
        logger.error("[JCC] Layer 4 failed: %s", exc)
        print(f"\n[JCC] ⚠ Layer 4 failed: {exc}")

    return df
