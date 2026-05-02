"""
main.py — Pipeline Entry Point (Download + Extract + Map)
============================================================
Run the complete pipeline: download PDFs → extract data → map → export.

    python main.py                        # full run (download 5 each → extract → map)
    python main.py --download-limit -1    # download ALL PDFs
    python main.py --download-limit 10    # download 10 per handler
    python main.py --skip-download        # skip download, use existing PDFs
    python main.py --skip-effectiveness   # Module 1 only
    python main.py --pdf path/to/file.pdf # single PDF (Module 1 only)
    python main.py --export               # export all DB records to Excel
    python main.py --status               # show tracker status
    python main.py --mode laptop --api-key sk-...

Default execution mode: vm  (set EXECUTION_TARGET in config.py to change)
Default download limit: 5   (set DOWNLOAD_LIMIT in config.py or use --download-limit)
Missing input/output folders are created automatically.

Pipeline Flow
-------------
  Phase 0: DOWNLOAD — Download PDFs for each handler type
  Phase 1: EXTRACT  — Extract data from downloaded PDFs
  Phase 2: MAP      — Merge and cross-reference extracted data

Handler Mapping
---------------
  CMETS (ISTS Consultation Meeting)     → source_01 scraper → cmets_handler
  JCC (Joint Coordination Meeting)      → source_02 scraper → jcc_handler
  Effectiveness (Regenerators)          → source_03 scraper → effectiveness_handler
  Bay Allocation (Renewable Energy)     → source_09 scraper → bayallocation_handler

Excel outputs (in excels/ folder)
----------------------------------
  01_cmets_extracted.xlsx             Module 1 — CMETS PDF extraction
  02_effectiveness_extracted.xlsx     Module 2 — Effectiveness PDF extraction
  03_cmets_effectiveness_mapped.xlsx  Module 3 — CMETS x Effectiveness merge
  04_jcc_extracted.xlsx               Module 4 — JCC extraction (main)
  04_jcc_output_layer.xlsx            Module 4 — JCC output layer
  04_jcc_extracted_mapped.xlsx        Module 4 — JCC mapped
  04_cmets_jcc_mapped.xlsx            Module 4 — Layer 4 merged
  05_bayallocation_extracted.xlsx     Module 5 — Bay Allocation extraction
  06_cmets_bay_mapped.xlsx            Module 6 — CMETS x Bay Allocation mapping
  00_full_export.xlsx                 Full DB export (all records)

SQLite Database: pipeline_tracker.db
"""

from __future__ import annotations

import argparse
import logging
import traceback
from pathlib import Path

import pandas as pd

from config import load_runtime_config
from pipeline.tracker import PipelineTracker
from pipeline.downloader import (
    download_cmets_pdfs,
    download_jcc_pdfs,
    download_effectiveness_pdfs,
    download_bayallocation_pdfs,
)
from pipeline.cmets_handler         import run_cmets_extraction
from pipeline.effectiveness_handler import run_effectiveness_extraction
from pipeline.mapping_handler       import run_mapping
from pipeline.jcc_handler           import run_jcc_extraction
from pipeline.bayallocation_handler import run_bayallocation_extraction
from pipeline.bay_mapping_handler   import run_bay_mapping
from pipeline.excel_utils           import export_to_excel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

# --- Required folders — auto-created on startup ------------------------------
_START_DIR = Path(__file__).resolve().parent

REQUIRED_FOLDERS = [
    # (path, description)
    (_START_DIR / "source" / "cmets_pdfs",                  "CMETS PDF root"),
    (_START_DIR / "source" / "cmets_pdfs" / "agenda",        "CMETS Agenda PDFs"),
    (_START_DIR / "source" / "cmets_pdfs" / "minutes",       "CMETS Minutes PDFs (extraction input)"),
    (_START_DIR / "source" / "effectiveness_pdfs",           "Effectiveness PDF input"),
    (_START_DIR / "source" / "jcc_pdfs",                     "JCC PDF input"),
    (_START_DIR / "output" / "cmets_cache",                  "CMETS JSON cache"),
    (_START_DIR / "output" / "effectiveness_cache",          "Effectiveness JSON cache"),
    (_START_DIR / "output" / "jcc_cache",                    "JCC JSON cache"),
    (_START_DIR / "source" / "bayallocation",                "Bay Allocation PDF input"),
    (_START_DIR / "output" / "bayallocation_cache",          "Bay Allocation JSON cache"),
    (_START_DIR / "excels",                                  "Generated Excel reports"),
]


def _ensure_folders() -> None:
    """Create all required input/output folders if they don't exist."""
    for folder, desc in REQUIRED_FOLDERS:
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
            print(f"  [Created] {folder.relative_to(_START_DIR)}  ({desc})")
        else:
            print(f"  [OK]      {folder.relative_to(_START_DIR)}")


def _skip(label: str, path: str | Path) -> bool:
    """Return True and print a skip notice if *path* already exists."""
    p = Path(path)
    if p.exists():
        print(f"[Pipeline] SKIP  {label} — {p.name} already exists\n")
        return True
    return False


# --- CLI ---------------------------------------------------------------------

def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="CMETS / GNI PDF Download + Extraction Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--pdf",              default=None, metavar="FILE",
                   help="Process a single PDF only (Module 1 only)")
    p.add_argument("--source-dir",       default=None, metavar="DIR",
                   help="CMETS PDF folder               (default: source/cmets_pdfs/)")
    p.add_argument("--output-dir",       default=None, metavar="DIR",
                   help="CMETS JSON cache folder        (default: output/cmets_cache/)")
    p.add_argument("--cmets-excel",      default=None, metavar="FILE",
                   help="CMETS Excel output             (default: excels/01_cmets_extracted.xlsx)")
    p.add_argument("--effective-dir",    default=None, metavar="DIR",
                   help="Effectiveness PDF folder       (default: source/effectiveness_pdfs/)")
    p.add_argument("--eff-output-dir",   default=None, metavar="DIR",
                   help="Effectiveness JSON cache folder(default: output/effectiveness_cache/)")
    p.add_argument("--mapped-excel",     default=None, metavar="FILE",
                   help="Mapped output Excel            (default: excels/03_cmets_effectiveness_mapped.xlsx)")
    p.add_argument("--jcc-source-dir",   default=None, metavar="DIR",
                   help="JCC PDF folder                 (default: source/jcc_pdfs/)")
    p.add_argument("--jcc-output-dir",   default=None, metavar="DIR",
                   help="JCC JSON cache folder          (default: output/jcc_cache/)")
    p.add_argument("--bay-source-dir",   default=None, metavar="DIR",
                   help="Bay Allocation PDF folder      (default: source/bayallocation/)")
    p.add_argument("--bay-output-dir",   default=None, metavar="DIR",
                   help="Bay Allocation JSON cache      (default: output/bayallocation_cache/)")
    p.add_argument("--skip-effectiveness", action="store_true",
                   help="Run Module 1 only; skip Modules 2-6")
    p.add_argument("--skip-download",    action="store_true",
                   help="Skip PDF download phase, use existing PDFs only")
    p.add_argument("--download-limit",   type=int, default=None,
                   help="Max PDFs to download per handler (-1=all, default=5)")
    p.add_argument("--mode",             choices=["vm", "laptop"], default=None,
                   help="Override execution mode (default: vm)")
    p.add_argument("--api-key",          default=None,
                   help="OpenAI API key (laptop mode override)")
    p.add_argument("--llm-script",       default=None,
                   help="Path to llm_client.bat (vm mode override)")
    p.add_argument("--export",           action="store_true",
                   help="Export all DB records to Excel and exit")
    p.add_argument("--status",           action="store_true",
                   help="Show tracker status and exit")
    return p.parse_args()


# --- Export & Status ---------------------------------------------------------

def _export_all_records(tracker: PipelineTracker) -> None:
    """Export all records from SQLite to an Excel file."""
    excels_dir = _START_DIR / "excels"
    excels_dir.mkdir(parents=True, exist_ok=True)

    records = tracker.export_records_as_rows()
    if not records:
        print("[Export] No records in database to export.")
        return

    out_path = export_to_excel(
        rows=records,
        output_path=excels_dir / "00_full_export.xlsx",
        sheet_name="All Records",
    )
    print(f"[Export] {len(records)} records exported → {out_path}")
    tracker.register_excel("full_export", str(out_path), "All Records", len(records))


def _show_status(tracker: PipelineTracker) -> None:
    """Print tracker status for all handlers."""
    summary = tracker.summary()
    print("\n" + "=" * 64)
    print("  PIPELINE TRACKER STATUS")
    print("=" * 64)
    for table, count in summary.items():
        print(f"  {table:<20}: {count}")
    print("-" * 64)

    for handler in ("cmets", "jcc", "effectiveness", "bayallocation"):
        status = tracker.handler_status(handler)
        print(f"\n  [{handler.upper()}]")
        print(f"    Downloads  : {status['downloads']}")
        print(f"    Extracted  : {status['extracted']}")
        print(f"    Pending    : {status['pending']}")
        print(f"    Failed     : {status['failed']}")
        print(f"    Records    : {status['records']}")

    print("=" * 64 + "\n")


# --- Download Phase ----------------------------------------------------------

def _run_downloads(runtime, tracker: PipelineTracker, args) -> dict:
    """Phase 0: Download PDFs for each handler type."""
    limit = runtime.download_limit

    print("\n" + "=" * 64)
    print("  PHASE 0 — PDF DOWNLOAD")
    print(f"  Download limit: {limit} per handler ('–1' = all)")
    print("=" * 64)

    results = {}

    # CMETS
    cmets_dir = args.source_dir or str(_START_DIR / "source" / "cmets_pdfs")
    print(f"\n  [CMETS] Downloading to {cmets_dir} ...")
    try:
        count = download_cmets_pdfs(cmets_dir, limit=limit, tracker=tracker)
        results["cmets"] = count
        print(f"  [CMETS] Downloaded: {count} PDFs")
    except Exception:
        logging.error("CMETS download failed.")
        traceback.print_exc()
        results["cmets"] = 0

    # JCC
    jcc_dir = args.jcc_source_dir or str(_START_DIR / "source" / "jcc_pdfs")
    print(f"\n  [JCC] Downloading to {jcc_dir} ...")
    try:
        count = download_jcc_pdfs(jcc_dir, limit=limit, tracker=tracker)
        results["jcc"] = count
        print(f"  [JCC] Downloaded: {count} PDFs")
    except Exception:
        logging.error("JCC download failed.")
        traceback.print_exc()
        results["jcc"] = 0

    # Effectiveness
    eff_dir = args.effective_dir or str(_START_DIR / "source" / "effectiveness_pdfs")
    print(f"\n  [EFFECTIVENESS] Downloading to {eff_dir} ...")
    try:
        count = download_effectiveness_pdfs(eff_dir, limit=limit, tracker=tracker)
        results["effectiveness"] = count
        print(f"  [EFFECTIVENESS] Downloaded: {count} PDFs")
    except Exception:
        logging.error("Effectiveness download failed.")
        traceback.print_exc()
        results["effectiveness"] = 0

    # Bay Allocation
    bay_dir = args.bay_source_dir or str(_START_DIR / "source" / "bayallocation")
    print(f"\n  [BAY ALLOCATION] Downloading to {bay_dir} ...")
    try:
        count = download_bayallocation_pdfs(bay_dir, limit=limit, tracker=tracker)
        results["bayallocation"] = count
        print(f"  [BAY ALLOCATION] Downloaded: {count} PDFs")
    except Exception:
        logging.error("Bay Allocation download failed.")
        traceback.print_exc()
        results["bayallocation"] = 0

    total = sum(results.values())
    print(f"\n  DOWNLOAD PHASE COMPLETE — {total} PDFs downloaded total")
    print("=" * 64 + "\n")

    return results


# --- Extraction Phase --------------------------------------------------------

def _run_extraction(runtime, tracker: PipelineTracker, args) -> None:
    """Phase 1+2: Extract data from PDFs → map → Excel."""
    excels_dir = _START_DIR / "excels"
    output_dir = _START_DIR / "output"

    # ── Module 1: CMETS extraction ────────────────────────────────────────────
    cmets_excel = args.cmets_excel or str(excels_dir / "01_cmets_extracted.xlsx")
    cmets_path  = Path(cmets_excel).resolve()

    # Check if Excel exists; if tracked but missing on disk, regenerate
    if not Path(cmets_excel).exists():
        print("\n[Pipeline] Module 1 — CMETS extraction (Minutes only)")

        # Use only the minutes/ subfolder for extraction
        cmets_src = Path(
            args.source_dir or str(_START_DIR / "source" / "cmets_pdfs" / "minutes")
        )
        for pdf in sorted(cmets_src.glob("*.pdf")):
            if not tracker.is_extracted("cmets", pdf.name):
                dl_id = tracker.get_download_id("cmets", pdf.name)
                ext_id = tracker.register_extraction("cmets", pdf.name, dl_id)

        cmets_path = run_cmets_extraction(
            source_dir = str(cmets_src),
            output_dir = args.output_dir,
            excel_path = cmets_excel,
            single_pdf = args.pdf,
            runtime    = runtime,
        )

        # Mark extractions as completed & register records
        for pdf in sorted(cmets_src.glob("*.pdf")):
            cache_path = Path(args.output_dir or str(_START_DIR / "output" / "cmets_cache")) / f"{pdf.stem}.json"
            if cache_path.exists():
                _register_extraction_complete(tracker, "cmets", pdf.name, str(cache_path))

        tracker.register_excel("cmets", str(cmets_path), "Extracted Data")
        print(f"\n[Pipeline] Module 1 complete -> {cmets_path.name}\n")
    else:
        print(f"[Pipeline] SKIP  Module 1 — {Path(cmets_excel).name} already exists\n")

    if args.skip_effectiveness:
        print("[Pipeline] --skip-effectiveness set. Done.\n")
        return

    # ── Module 2: Effectiveness extraction ────────────────────────────────────
    eff_excel = str(excels_dir / "02_effectiveness_extracted.xlsx")
    eff_df    = pd.DataFrame()

    if _skip("Module 2", eff_excel):
        try:
            eff_df = pd.read_excel(eff_excel, sheet_name=0)
        except Exception:
            pass
    else:
        try:
            eff_df = run_effectiveness_extraction(
                source_dir = args.effective_dir,
                output_dir = args.eff_output_dir,
                excel_path = eff_excel,
                runtime    = runtime,
            )
            tracker.register_excel("effectiveness", eff_excel, "Extracted Data", len(eff_df))
            print(f"\n[Pipeline] Module 2 complete — {len(eff_df)} records\n")
        except Exception:
            logging.error("Module 2 failed — Module 3 will use cached JSONs only.")
            traceback.print_exc()

    # ── Module 3: Mapping ─────────────────────────────────────────────────────
    mapped_excel = args.mapped_excel or str(excels_dir / "03_cmets_effectiveness_mapped.xlsx")
    mapped_path  = Path(mapped_excel).resolve()

    if not _skip("Module 3", mapped_excel):
        try:
            mapped_path = run_mapping(
                cmets_excel_path         = cmets_path,
                effectiveness_df         = eff_df,
                effectiveness_output_dir = args.eff_output_dir,
                mapped_json_path         = str(output_dir / "cmets_effectiveness_mapped.json"),
                mapped_excel_path        = mapped_excel,
            )
            tracker.register_excel("mapping", str(mapped_path), "Mapped Data")
            print(f"\n[Pipeline] Module 3 complete -> {mapped_path.name}\n")
        except Exception:
            logging.error("Module 3 failed.")
            traceback.print_exc()

    # ── Module 4: JCC extraction + Output Layer + Layer 4 ─────────────────────
    jcc_excel    = str(excels_dir / "04_jcc_extracted.xlsx")
    layer4_excel = str(excels_dir / "04_cmets_jcc_mapped.xlsx")

    if Path(jcc_excel).exists() and Path(layer4_excel).exists():
        print("[Pipeline] SKIP  Module 4 — 04_jcc_extracted.xlsx / 04_cmets_jcc_mapped.xlsx already exist\n")
    else:
        try:
            jcc_df = run_jcc_extraction(
                source_dir               = args.jcc_source_dir,
                output_dir               = args.jcc_output_dir,
                excel_path               = jcc_excel,
                runtime                  = runtime,
                effectiveness_df         = eff_df,
                effectiveness_excel_path = eff_excel,
                effectiveness_output_dir = args.eff_output_dir,
                jcc_output_excel_path    = str(excels_dir / "04_jcc_output_layer.xlsx"),
                jcc_mapped_excel_path    = str(excels_dir / "04_jcc_extracted_mapped.xlsx"),
                mapped_excel_path        = str(mapped_path),
                layer4_excel_path        = layer4_excel,
                cmets_excel_path         = str(cmets_path),
            )
            tracker.register_excel("jcc", jcc_excel, "Extracted Data", len(jcc_df))
            tracker.register_excel("jcc_layer4", layer4_excel, "Layer 4 Mapped")
            print(f"\n[Pipeline] Module 4 complete — {len(jcc_df)} rows\n")
        except Exception:
            logging.error("Module 4 failed.")
            traceback.print_exc()

    # ── Module 5: Bay Allocation extraction ───────────────────────────────────
    bay_excel = str(excels_dir / "05_bayallocation_extracted.xlsx")

    if _skip("Module 5", bay_excel):
        bay_df = pd.DataFrame()
    else:
        try:
            bay_df = run_bayallocation_extraction(
                source_dir = args.bay_source_dir,
                output_dir = args.bay_output_dir,
                excel_path = bay_excel,
                runtime    = runtime,
            )
            tracker.register_excel("bayallocation", bay_excel, "Extracted Data", len(bay_df))
            print(f"\n[Pipeline] Module 5 complete — {len(bay_df)} entries\n")
        except Exception:
            logging.error("Module 5 failed.")
            traceback.print_exc()

    # ── Module 6: CMETS x Bay Allocation mapping ──────────────────────────────
    bay_mapped_excel = str(excels_dir / "06_cmets_bay_mapped.xlsx")

    if not _skip("Module 6", bay_mapped_excel):
        try:
            bay_mapped_path = run_bay_mapping(
                cmets_excel_path  = cmets_path,
                bay_output_dir    = args.bay_output_dir,
                output_excel_path = bay_mapped_excel,
                output_json_path  = str(output_dir / "cmets_bay_mapped.json"),
            )
            tracker.register_excel("bay_mapping", str(bay_mapped_path), "Bay Mapped")
            print(f"\n[Pipeline] Module 6 complete -> {bay_mapped_path.name}\n")
        except Exception:
            logging.error("Module 6 failed.")
            traceback.print_exc()


def _register_extraction_complete(tracker: PipelineTracker, handler: str, pdf_filename: str, cache_path: str):
    """Mark an extraction as completed in the tracker."""
    import json
    rows = 0
    try:
        with open(cache_path) as f:
            data = json.load(f)
        rows = data.get("total_rows", 0)

        # Register extracted rows as records
        for page in data.get("results", []):
            pnum = page.get("page_number", 0)
            for row in page.get("rows", []):
                gna_id = ""
                lta_id = ""
                if isinstance(row, dict):
                    gna_id = str(row.get("GNA Application No", row.get("Application No. GNA", ""))).strip()
                    lta_id = str(row.get("LTA Application No", row.get("Application No. LTA", ""))).strip()
                tracker.upsert_record(
                    handler=handler,
                    gna_id=gna_id,
                    lta_id=lta_id,
                    pdf_filename=pdf_filename,
                    page_number=pnum,
                    data=row if isinstance(row, dict) else {},
                )
    except Exception:
        pass

    # Find and update the extraction record
    pending = tracker.get_pending_extractions(handler)
    for ext in pending:
        if ext["pdf_filename"] == pdf_filename:
            tracker.complete_extraction(ext["id"], rows, cache_path)
            break


# --- Main --------------------------------------------------------------------

def main() -> None:
    args    = _build_args()
    runtime = load_runtime_config(
        mode_override          = args.mode,
        api_key_override       = args.api_key,
        llm_script_override    = args.llm_script,
        download_limit_override= args.download_limit,
    )

    tracker = PipelineTracker()

    # Quick commands
    if args.export:
        _export_all_records(tracker)
        tracker.close()
        return

    if args.status:
        _show_status(tracker)
        tracker.close()
        return

    print("\n" + "=" * 64)
    print("  CMETS / GNI  DOWNLOAD + EXTRACTION PIPELINE")
    print("=" * 64)
    print(f"  Mode             : {runtime.execution_target}")
    print(f"  Download limit   : {runtime.download_limit}")
    print(f"  Skip download    : {args.skip_download}")
    print(f"  Skip Module 2-6  : {args.skip_effectiveness}")
    print(f"  Tracker DB       : {tracker.db_path}")
    print("-" * 64)
    print("  Checking required folders...")
    _ensure_folders()
    print("=" * 64 + "\n")

    # Start pipeline run tracking
    run_id = tracker.start_run(runtime.download_limit)

    try:
        total_downloaded = 0
        download_results = {}

        # ── Phase 0: Download ─────────────────────────────────────────────────
        if not args.skip_download and not args.pdf:
            download_results = _run_downloads(runtime, tracker, args)
            total_downloaded = sum(download_results.values())

        # ── Phase 1+2: Extract + Map ──────────────────────────────────────────
        _run_extraction(runtime, tracker, args)

        # Count excels
        total_excels = len(list((_START_DIR / "excels").glob("*.xlsx")))
        total_extracted = sum(
            tracker.count_extractions(h, "completed")
            for h in ("cmets", "jcc", "effectiveness", "bayallocation")
        )

        tracker.finish_run(run_id, total_downloaded, total_extracted, total_excels)

        print("=" * 64)
        print("  ALL PHASES COMPLETE")
        print(f"  Downloaded : {total_downloaded}")
        print(f"  Extracted  : {total_extracted}")
        print(f"  Excels     : {total_excels}")
        print(f"  DB records : {tracker.count_records()}")
        print("=" * 64 + "\n")

    except Exception:
        tracker.fail_run(run_id)
        raise
    finally:
        tracker.close()


if __name__ == "__main__":
    main()
