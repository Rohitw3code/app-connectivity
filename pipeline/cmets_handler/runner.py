"""
cmets_handler/runner.py — CMETS Orchestration (discovery, cache, Excel)
=========================================================================
Discovers all PDFs in source_1/, checks JSON cache in source_1_output/,
extracts only un-cached PDFs, flattens results, writes cmets.xlsx.

This is the only file that performs I/O orchestration for Module 1.
Edit extraction.py or normalization.py to change how data is extracted
or cleaned — this file only handles discovery, caching, and output.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Optional

from config import RuntimeConfig, load_runtime_config
from pipeline.excel_utils import export_to_excel
from pipeline.cmets_handler.models import PipelineResult, CMETS_COLUMNS
from pipeline.cmets_handler.extraction import run_single_pdf

logger = logging.getLogger(__name__)

# ─── Default I/O paths ────────────────────────────────────────────────────────
_START_DIR  = Path(__file__).resolve().parent.parent.parent   # …/start/
SOURCE_DIR  : Path = _START_DIR / "source_1"
OUTPUT_DIR  : Path = _START_DIR / "source_1_output"
CMETS_EXCEL : Path = _START_DIR / "excels" / "cmets.xlsx"


# ─── Serialisation ────────────────────────────────────────────────────────────

def _serialize(result: PipelineResult) -> dict:
    out = result.model_dump()
    for i, pr in enumerate(out["results"]):
        pr["rows"] = [r.model_dump(by_alias=True) for r in result.results[i].rows]
    return out


def _save_json(data: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)


def _load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _flatten(all_serialized: list[dict]) -> list[dict]:
    """Flatten nested per-PDF results into flat rows for Excel."""
    records = []
    for pr in all_serialized:
        pdf_path = pr.get("pdf_path", "")
        for page in pr.get("results", []):
            pnum = page.get("page_number")
            for row in page.get("rows", []):
                rec = {"PDF": pdf_path, "Page Number": pnum}
                for col in CMETS_COLUMNS[2:]:
                    rec[col] = row.get(col)
                rec["Applied Start of Connectivity sought by developer date"
                    "( start date of connectivity as per the application)"] = (
                    row.get("Applied Start of Connectivity sought by developer date")
                    or row.get("Start Date of Connectivity (As per Application)")
                )
                records.append(rec)
    return records


def _agg_stats(all_serialized: list[dict]) -> dict:
    return {
        "pdfs_processed":          len(all_serialized),
        "total_pages_extracted":   sum(p.get("total_pages_extracted",   0) for p in all_serialized),
        "total_pages_passed_gate": sum(p.get("pages_passed_gate",       0) for p in all_serialized),
        "total_pages_skipped":     sum(p.get("pages_skipped",           0) for p in all_serialized),
        "total_rows":              sum(p.get("total_rows",              0) for p in all_serialized),
    }


# ─── Public API ───────────────────────────────────────────────────────────────

def run_cmets_extraction(
    source_dir:  Path | str | None = None,
    output_dir:  Path | str | None = None,
    excel_path:  Path | str | None = None,
    single_pdf:  Optional[str] = None,
    runtime:     Optional[RuntimeConfig] = None,
) -> Path:
    """Discover CMETS PDFs → extract (skip cached) → dump JSON → write cmets.xlsx.

    Returns the absolute path to cmets.xlsx.
    """
    src   = Path(source_dir).resolve()  if source_dir else SOURCE_DIR
    out   = Path(output_dir).resolve()  if output_dir else OUTPUT_DIR
    xlsx  = Path(excel_path).resolve()  if excel_path else CMETS_EXCEL

    src.mkdir(parents=True, exist_ok=True)
    out.mkdir(parents=True, exist_ok=True)

    if runtime is None:
        runtime = load_runtime_config()

    # Discover PDFs
    if single_pdf:
        p = Path(single_pdf).resolve()
        if not p.is_file():
            raise FileNotFoundError(f"[CMETS] PDF not found: {p}")
        pdf_paths = [p]
    else:
        pdf_paths = sorted(p for p in src.glob("*.pdf") if p.is_file())
        if not pdf_paths:
            raise SystemExit(f"[CMETS] No PDFs found in '{src}'.")

    cached = sum(1 for p in pdf_paths if (out / f"{p.stem}.json").exists())

    print("\n" + "=" * 64)
    print("  MODULE 1 — CMETS PDF EXTRACTION")
    print("=" * 64)
    print(f"  Source dir     : {src}")
    print(f"  Output dir     : {out}")
    print(f"  Excel output   : {xlsx}")
    print(f"  PDFs found     : {len(pdf_paths)}")
    print(f"  Cached (skip)  : {cached}")
    print(f"  To extract     : {len(pdf_paths) - cached}")
    print(f"  Mode           : {runtime.execution_target}")
    print("=" * 64)

    started_at = datetime.now()
    t0         = perf_counter()
    all_data:  list[dict] = []

    for idx, pdf_path in enumerate(pdf_paths, 1):
        cache = out / f"{pdf_path.stem}.json"

        if cache.exists():
            print(f"\n[{idx}/{len(pdf_paths)}] SKIP    {pdf_path.name}")
            all_data.append(_load_json(cache))
            continue

        print(f"\n[{idx}/{len(pdf_paths)}] EXTRACT {pdf_path.name}")
        print("-" * 64)
        result = run_single_pdf(
            pdf_path=str(pdf_path),
            api_key=runtime.api_key or None,
            vm_mode=runtime.vm_mode,
            llm_script_path=runtime.llm_script_path,
        )
        data = _serialize(result)
        _save_json(data, cache)
        print(f"  → JSON: {cache.name}")
        all_data.append(data)

    runtime_s   = perf_counter() - t0
    finished_at = datetime.now()
    stats       = _agg_stats(all_data)

    print("\n" + "=" * 64)
    print("  CMETS SUMMARY")
    print(f"    PDFs          : {len(pdf_paths)}  (skipped {cached})")
    print(f"    Pages ext.    : {stats['total_pages_extracted']}")
    print(f"    Pages passed  : {stats['total_pages_passed_gate']}")
    print(f"    Total rows    : {stats['total_rows']}")
    print(f"    Runtime (s)   : {runtime_s:.1f}")
    print("=" * 64)

    flat_rows = _flatten(all_data)
    out_path  = export_to_excel(
        rows         = flat_rows,
        output_path  = xlsx,
        sheet_name   = "Extracted Data",
        column_order = CMETS_COLUMNS,
        summary_rows = [
            ("Run started at",           started_at.isoformat(timespec="seconds")),
            ("Run finished at",          finished_at.isoformat(timespec="seconds")),
            ("Total runtime (seconds)",  round(runtime_s, 2)),
            ("PDFs processed",           stats["pdfs_processed"]),
            ("Total pages extracted",    stats["total_pages_extracted"]),
            ("Total pages passed gate",  stats["total_pages_passed_gate"]),
            ("Total pages skipped",      stats["total_pages_skipped"]),
            ("Total rows",               stats["total_rows"]),
        ],
    )
    print(f"\n[CMETS] cmets.xlsx → {out_path}")
    return out_path
