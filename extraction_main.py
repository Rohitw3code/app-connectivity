"""Run only the extraction phase for PDFs not yet marked as extracted."""

from __future__ import annotations

import argparse
from pathlib import Path

from config import load_runtime_config
from pipeline.extraction_orchestrator import run_pending_extractions

_START_DIR = Path(__file__).resolve().parent


def _build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extraction-only pipeline")
    parser.add_argument("--mode", choices=["vm", "laptop"], default=None,
                        help="Override execution mode (default: vm)")
    parser.add_argument("--api-key", default=None,
                        help="OpenAI API key (laptop mode override)")
    parser.add_argument("--llm-script", default=None,
                        help="Path to llm_client.bat (vm mode override)")
    parser.add_argument("--sources", default=None, metavar="LIST",
                        help="Comma-separated source keys/names to extract (default: all)")
    return parser.parse_args()


def main() -> None:
    args = _build_args()
    runtime = load_runtime_config(
        mode_override=args.mode,
        api_key_override=args.api_key,
        llm_script_override=args.llm_script,
    )

    only_sources = None
    if args.sources:
        only_sources = [part.strip() for part in args.sources.split(",") if part.strip()]

    results = run_pending_extractions(runtime, only_sources=only_sources)
    print("\n" + "=" * 64)
    print("  EXTRACTION COMPLETE")
    for item in results:
        print(f"  {item['source']:<35} pending={item['pending']:<4} extracted={item['extracted']}")
    print("=" * 64 + "\n")


if __name__ == "__main__":
    main()
