"""
main.py — Pipeline Entry Point
================================
Run the three-module extraction & mapping pipeline:

    python main.py                        # full run (Modules 1 → 2 → 3)
    python main.py --skip-effectiveness   # Module 1 only
    python main.py --pdf path/to/file.pdf # single PDF (Module 1 only)
    python main.py --mode laptop --api-key sk-...

Default execution mode: vm  (set EXECUTION_TARGET in config.py to change)
Missing input/output folders are created automatically.

Modules
-------
  Module 1 (pipeline/cmets_handler/)         — CMETS PDF extraction
  Module 2 (pipeline/effectiveness_handler/) — Effectiveness PDF extraction
  Module 3 (pipeline/mapping_handler/)       — CMETS × Effectiveness merge
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)

# ─── Required folders — auto-created on startup ──────────────────────────────
_START_DIR = Path(__file__).resolve().parent
_APP_DIR   = _START_DIR.parent

REQUIRED_FOLDERS = [
    # (path, description)
    (_START_DIR / "source_1",                               "CMETS PDF input folder"),
    (_START_DIR / "source_1_output",                        "CMETS JSON cache folder"),
    (_START_DIR / "effectiveness_output",                   "Effectiveness JSON cache folder"),
    (_APP_DIR   / "CTUIL-Regenerators-Effective-Date-wise", "Effectiveness PDF input folder"),
]


def _ensure_folders() -> None:
    """Create all required input/output folders if they don't exist."""
    for folder, desc in REQUIRED_FOLDERS:
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
            print(f"  [Created] {folder}  ({desc})")
        else:
            print(f"  [OK]      {folder}")


# ─── CLI ──────────────────────────────────────────────────────────────────────

def _build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="CMETS / GNI three-module extraction pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--pdf",              default=None, metavar="FILE",
                   help="Process a single PDF only (Module 1 only)")
    p.add_argument("--source-dir",       default=None, metavar="DIR",
                   help="CMETS PDF folder               (default: source_1/)")
    p.add_argument("--output-dir",       default=None, metavar="DIR",
                   help="CMETS JSON cache folder        (default: source_1_output/)")
    p.add_argument("--cmets-excel",      default=None, metavar="FILE",
                   help="CMETS Excel output             (default: cmets.xlsx)")
    p.add_argument("--effective-dir",    default=None, metavar="DIR",
                   help="Effectiveness PDF folder       (default: auto-resolved)")
    p.add_argument("--eff-output-dir",   default=None, metavar="DIR",
                   help="Effectiveness JSON cache folder(default: effectiveness_output/)")
    p.add_argument("--mapped-excel",     default=None, metavar="FILE",
                   help="Mapped output Excel            (default: effectiveness_mapped.xlsx)")
    p.add_argument("--skip-effectiveness", action="store_true",
                   help="Run Module 1 only; skip Modules 2 & 3")
    p.add_argument("--mode",             choices=["vm", "laptop"], default=None,
                   help="Override execution mode (default: vm)")
    p.add_argument("--api-key",          default=None,
                   help="OpenAI API key (laptop mode override)")
    p.add_argument("--llm-script",       default=None,
                   help="Path to llm_client.bat (vm mode override)")
    return p.parse_args()


# ─── Main ─────────────────────────────────────────────────────────────────────

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
    print(f"  Skip Module 2+3  : {args.skip_effectiveness}")
    print("-" * 64)
    print("  Checking required folders…")
    _ensure_folders()
    print("=" * 64 + "\n")

    # ── Module 1: CMETS extraction ────────────────────────────────────────────
    cmets_path = run_cmets_extraction(
        source_dir = args.source_dir,
        output_dir = args.output_dir,
        excel_path = args.cmets_excel,
        single_pdf = args.pdf,
        runtime    = runtime,
    )
    print(f"\n[Pipeline] ✓ Module 1 complete → {cmets_path.name}\n")

    if args.skip_effectiveness:
        print("[Pipeline] --skip-effectiveness set. Done.\n")
        return

    # ── Module 2: Effectiveness extraction ────────────────────────────────────
    eff_df = pd.DataFrame()
    try:
        eff_df = run_effectiveness_extraction(
            source_dir = args.effective_dir,
            output_dir = args.eff_output_dir,
            runtime    = runtime,
        )
        print(f"\n[Pipeline] ✓ Module 2 complete — {len(eff_df)} records\n")
    except Exception:
        logging.error("Module 2 failed — Module 3 will use cached JSONs only.")
        traceback.print_exc()

    # ── Module 3: Mapping ─────────────────────────────────────────────────────
    try:
        mapped_path = run_mapping(
            cmets_excel_path         = cmets_path,
            effectiveness_df         = eff_df,
            effectiveness_output_dir = args.eff_output_dir,
            mapped_excel_path        = args.mapped_excel,
        )
        print(f"\n[Pipeline] ✓ Module 3 complete → {mapped_path.name}\n")
    except Exception:
        logging.error("Module 3 failed.")
        traceback.print_exc()

    print("=" * 64)
    print("  ALL MODULES COMPLETE")
    print("=" * 64 + "\n")


if __name__ == "__main__":
    main()
