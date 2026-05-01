"""
main.py — Pipeline Entry Point
================================
Run the six-module extraction & mapping pipeline:

    python main.py                        # full run (Modules 1 -> 2 -> 3 -> 4 -> 5 -> 6)
    python main.py --skip-effectiveness   # Module 1 only
    python main.py --pdf path/to/file.pdf # single PDF (Module 1 only)
    python main.py --mode laptop --api-key sk-...

Default execution mode: vm  (set EXECUTION_TARGET in config.py to change)
Missing input/output folders are created automatically.

Excel outputs (in excels/ folder)
----------------------------------
Each Excel is prefixed with its module execution number.
If the file already exists it is SKIPPED — delete it to re-run that module.

  01_cmets_extracted.xlsx             Module 1 — CMETS PDF extraction
  02_effectiveness_extracted.xlsx     Module 2 — Effectiveness PDF extraction
  03_cmets_effectiveness_mapped.xlsx  Module 3 — CMETS x Effectiveness merge
  04_jcc_extracted.xlsx               Module 4 — JCC extraction (main)
  04_jcc_output_layer.xlsx            Module 4 — JCC output layer
  04_jcc_extracted_mapped.xlsx        Module 4 — JCC mapped
  04_cmets_jcc_mapped.xlsx            Module 4 — Layer 4 merged
  05_bayallocation_extracted.xlsx     Module 5 — Bay Allocation extraction
  06_cmets_bay_mapped.xlsx            Module 6 — CMETS x Bay Allocation mapping

JSON outputs (in output/ folder — NOT in excels/)
--------------------------------------------------
  output/cmets_effectiveness_mapped.json
  output/cmets_bay_mapped.json

Modules
-------
  Module 1 (pipeline/cmets_handler/)              - CMETS PDF extraction
  Module 2 (pipeline/effectiveness_handler/)      - Effectiveness PDF extraction
  Module 3 (pipeline/mapping_handler/)            - CMETS x Effectiveness merge
  Module 4 (pipeline/jcc_handler/)                - JCC Meeting PDF extraction
  Module 5 (pipeline/bayallocation_handler/)      - Bay Allocation PDF extraction
  Module 6 (pipeline/bay_mapping_handler/)        - CMETS x Bay Allocation mapping
"""

from __future__ import annotations

import argparse
import logging
import traceback
from pathlib import Path

import pandas as pd

from config import load_runtime_config
from pipeline.cmets_handler         import run_cmets_extraction
from pipeline.effectiveness_handler import run_effectiveness_extraction
from pipeline.mapping_handler       import run_mapping
from pipeline.jcc_handler           import run_jcc_extraction
from pipeline.bayallocation_handler import run_bayallocation_extraction
from pipeline.bay_mapping_handler   import run_bay_mapping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

# --- Required folders — auto-created on startup ------------------------------
_START_DIR = Path(__file__).resolve().parent

REQUIRED_FOLDERS = [
    # (path, description)
    (_START_DIR / "source" / "cmets_pdfs",           "CMETS PDF input"),
    (_START_DIR / "source" / "effectiveness_pdfs",   "Effectiveness PDF input"),
    (_START_DIR / "source" / "jcc_pdfs",             "JCC PDF input"),
    (_START_DIR / "output" / "cmets_cache",           "CMETS JSON cache"),
    (_START_DIR / "output" / "effectiveness_cache",   "Effectiveness JSON cache"),
    (_START_DIR / "output" / "jcc_cache",             "JCC JSON cache"),
    (_START_DIR / "source" / "bayallocation",          "Bay Allocation PDF input"),
    (_START_DIR / "output" / "bayallocation_cache",    "Bay Allocation JSON cache"),
    (_START_DIR / "excels",                           "Generated Excel reports"),
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
        description="CMETS / GNI six-module extraction pipeline",
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
    p.add_argument("--mode",             choices=["vm", "laptop"], default=None,
                   help="Override execution mode (default: vm)")
    p.add_argument("--api-key",          default=None,
                   help="OpenAI API key (laptop mode override)")
    p.add_argument("--llm-script",       default=None,
                   help="Path to llm_client.bat (vm mode override)")
    return p.parse_args()


# --- Main --------------------------------------------------------------------

def main() -> None:
    args    = _build_args()
    runtime = load_runtime_config(
        mode_override       = args.mode,
        api_key_override    = args.api_key,
        llm_script_override = args.llm_script,
    )

    print("\n" + "=" * 64)
    print("  CMETS / GNI  EXTRACTION PIPELINE")
    print("=" * 64)
    print(f"  Mode             : {runtime.execution_target}")
    print(f"  Skip Module 2-6  : {args.skip_effectiveness}")
    print("-" * 64)
    print("  Checking required folders...")
    _ensure_folders()
    print("=" * 64 + "\n")

    excels_dir = _START_DIR / "excels"
    output_dir = _START_DIR / "output"

    # ── Module 1: CMETS extraction ────────────────────────────────────────────
    # Prefix 01_  |  Skip if Excel already exists
    cmets_excel = args.cmets_excel or str(excels_dir / "01_cmets_extracted.xlsx")
    cmets_path  = Path(cmets_excel).resolve()

    if not _skip("Module 1", cmets_excel):
        cmets_path = run_cmets_extraction(
            source_dir = args.source_dir,
            output_dir = args.output_dir,
            excel_path = cmets_excel,
            single_pdf = args.pdf,
            runtime    = runtime,
        )
        print(f"\n[Pipeline] Module 1 complete -> {cmets_path.name}\n")

    if args.skip_effectiveness:
        print("[Pipeline] --skip-effectiveness set. Done.\n")
        return

    # ── Module 2: Effectiveness extraction ────────────────────────────────────
    # Prefix 02_  |  Skip if Excel already exists
    eff_excel = str(excels_dir / "02_effectiveness_extracted.xlsx")
    eff_df    = pd.DataFrame()

    if _skip("Module 2", eff_excel):
        # Still load the data so Module 3 / 4 can use it if needed
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
            print(f"\n[Pipeline] Module 2 complete — {len(eff_df)} records\n")
        except Exception:
            logging.error("Module 2 failed — Module 3 will use cached JSONs only.")
            traceback.print_exc()

    # ── Module 3: Mapping ─────────────────────────────────────────────────────
    # Prefix 03_  |  Skip if Excel already exists
    # JSON -> output/ (NOT excels/)
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
            print(f"\n[Pipeline] Module 3 complete -> {mapped_path.name}\n")
        except Exception:
            logging.error("Module 3 failed.")
            traceback.print_exc()

    # ── Module 4: JCC extraction + Output Layer + Layer 4 ─────────────────────
    # Prefix 04_  |  Skip if both primary Excels already exist
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
            print(f"\n[Pipeline] Module 4 complete — {len(jcc_df)} rows\n")
        except Exception:
            logging.error("Module 4 failed.")
            traceback.print_exc()

    # ── Module 5: Bay Allocation extraction ───────────────────────────────────
    # Prefix 05_  |  Skip if Excel already exists
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
            print(f"\n[Pipeline] Module 5 complete — {len(bay_df)} entries\n")
        except Exception:
            logging.error("Module 5 failed.")
            traceback.print_exc()

    # ── Module 6: CMETS x Bay Allocation mapping ──────────────────────────────
    # Prefix 06_  |  Skip if Excel already exists
    # JSON -> output/ (NOT excels/)
    bay_mapped_excel = str(excels_dir / "06_cmets_bay_mapped.xlsx")

    if not _skip("Module 6", bay_mapped_excel):
        try:
            bay_mapped_path = run_bay_mapping(
                cmets_excel_path  = cmets_path,
                bay_output_dir    = args.bay_output_dir,
                output_excel_path = bay_mapped_excel,
                output_json_path  = str(output_dir / "cmets_bay_mapped.json"),
            )
            print(f"\n[Pipeline] Module 6 complete -> {bay_mapped_path.name}\n")
        except Exception:
            logging.error("Module 6 failed.")
            traceback.print_exc()

    print("=" * 64)
    print("  ALL MODULES COMPLETE")
    print("=" * 64 + "\n")


if __name__ == "__main__":
    main()
