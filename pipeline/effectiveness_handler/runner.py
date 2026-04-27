"""
effectiveness_handler/runner.py — Effectiveness Orchestration
================================================================
Discovers effectiveness PDFs, checks JSON cache, extracts un-cached
PDFs, writes effectiveness_combined.xlsx.

This is the only file that performs I/O orchestration for Module 2.
Edit extraction.py to change how data is extracted from PDFs.
Edit models.py to change the schema or column order.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from config import RuntimeConfig, load_runtime_config
from pipeline.excel_utils import export_to_excel
from pipeline.effectiveness_handler.models import RERecord, safe_record, dedup_records, EFF_COLUMNS
from pipeline.effectiveness_handler.extraction import extract_with_llm, extract_with_tables

logger = logging.getLogger(__name__)

# ─── Default I/O paths ────────────────────────────────────────────────────────
_START_DIR = Path(__file__).resolve().parent.parent.parent

EFFECTIVE_SOURCE_DIR    : Path = _START_DIR / "source" / "effectiveness_pdfs"
EFFECTIVENESS_OUTPUT_DIR: Path = _START_DIR / "output" / "effectiveness_cache"
EFFECTIVENESS_EXCEL     : Path = _START_DIR / "excels" / "effectiveness_combined.xlsx"


# ─── Cache helpers ────────────────────────────────────────────────────────────

def _cache_path(pdf_name: str, out_dir: Path) -> Path:
    return out_dir / f"{Path(pdf_name).stem}.json"


def _save_json(records: list[RERecord], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump([r.model_dump() for r in records], fh, indent=2, ensure_ascii=False)


def _load_all_jsons(out_dir: Path) -> list[RERecord]:
    records: list[RERecord] = []
    for jf in sorted(out_dir.glob("*.json")):
        try:
            with open(jf, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            records.extend(r for r in (safe_record(item) for item in data) if r)
        except Exception as exc:
            logger.warning("[Effectiveness] Could not read %s: %s", jf.name, exc)
    return records


# ─── Public API ───────────────────────────────────────────────────────────────

def run_effectiveness_extraction(
    source_dir:  Path | str | None = None,
    output_dir:  Path | str | None = None,
    excel_path:  Path | str | None = None,
    runtime:     Optional[RuntimeConfig] = None,
) -> pd.DataFrame:
    """Discover effectiveness PDFs → extract (skip cached) → dump JSON → write Excel.

    Returns pd.DataFrame with all effectiveness records.
    """
    src  = Path(source_dir).resolve()  if source_dir else EFFECTIVE_SOURCE_DIR
    out  = Path(output_dir).resolve()  if output_dir else EFFECTIVENESS_OUTPUT_DIR
    xlsx = Path(excel_path).resolve()  if excel_path else EFFECTIVENESS_EXCEL

    src.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)

    if runtime is None:
        runtime = load_runtime_config()

    use_llm = bool(runtime.api_key)

    print("\n" + "=" * 64)
    print("  MODULE 2 — EFFECTIVENESS PDF EXTRACTION")
    print("=" * 64)
    print(f"  Source dir  : {src}")
    print(f"  Output dir  : {out}")
    print(f"  Excel output: {xlsx}")

    if not src.exists() or not any(src.rglob("*.pdf")):
        print(f"  [Effectiveness] No PDFs found in: {src}")
        print("=" * 64)
        return pd.DataFrame()

    pdf_files    = sorted(src.rglob("*.pdf"))
    cached_count = sum(1 for p in pdf_files if _cache_path(p.name, out).exists())

    print(f"  PDFs found  : {len(pdf_files)}")
    print(f"  Cached      : {cached_count}  (will be skipped)")
    print(f"  To extract  : {len(pdf_files) - cached_count}")
    print(f"  Mode        : {runtime.execution_target}")
    print("=" * 64)

    for pdf_path in pdf_files:
        cache = _cache_path(pdf_path.name, out)
        if cache.exists():
            print(f"  SKIP    {pdf_path.name}")
            continue

        print(f"\n  EXTRACT {pdf_path.name}")
        try:
            records = extract_with_llm(str(pdf_path), pdf_path.name, runtime) \
                      if use_llm else \
                      extract_with_tables(str(pdf_path), pdf_path.name)
        except Exception as exc:
            logger.error("[Effectiveness] Failed %s: %s", pdf_path.name, exc)
            print(f"  ERROR   {pdf_path.name}: {exc}")
            continue

        records = dedup_records(records)
        _save_json(records, cache)
        print(f"  → {len(records)} records saved → {cache.name}")

    all_records = dedup_records(_load_all_jsons(out))
    print(f"\n  Total effectiveness records: {len(all_records)}")
    print("=" * 64)

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame([r.model_dump() for r in all_records])
    col_order = [c for c in EFF_COLUMNS if c in df.columns]
    export_to_excel(
        rows         = df.to_dict(orient="records"),
        output_path  = xlsx,
        sheet_name   = "Effectiveness Data",
        column_order = col_order,
        summary_rows = [("Total records", len(all_records))],
    )
    print(f"\n[Effectiveness] effectiveness_combined.xlsx → {xlsx}")
    return df
