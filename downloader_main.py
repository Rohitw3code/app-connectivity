"""Run only the download sub-pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path

from config import load_runtime_config
from pipeline.downloader import run_download_subpipeline
from pipeline.tracker import PipelineTracker

_START_DIR = Path(__file__).resolve().parent


def _build_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PDF download-only pipeline")
    parser.add_argument("--download-limit", type=int, default=None,
                        help="Max PDFs to download per scraper/type (-1=all, default=5)")
    parser.add_argument("--download-all", action="store_true", default=None,
                        help="Download every available PDF from each scraper")
    parser.add_argument("--download-output-dir", default=None, metavar="DIR",
                        help="PDF download root (default: output/)")
    parser.add_argument("--download-scrapers", default=None, metavar="LIST",
                        help="Comma-separated scraper keys to run (default: all)")
    parser.add_argument("--pfccl-query", default=None, metavar="TEXT",
                        help="Tender title substring for the PFCCLINDIA scraper")
    return parser.parse_args()


def main() -> None:
    args = _build_args()
    runtime = load_runtime_config(
        download_limit_override=args.download_limit,
        download_all_override=args.download_all,
    )

    output_root = Path(args.download_output_dir).resolve() if args.download_output_dir else _START_DIR / "output"
    selected = None
    if args.download_scrapers:
        selected = [part.strip() for part in args.download_scrapers.split(",") if part.strip()]

    with PipelineTracker() as tracker:
        results = run_download_subpipeline(
            output_root=output_root,
            limit=runtime.download_limit,
            download_all=runtime.download_all,
            tracker=tracker,
            scrapers=selected,
            pfccl_query=args.pfccl_query,
        )

    total = sum(results.values()) if results else 0
    print("\n" + "=" * 64)
    print("  DOWNLOAD COMPLETE")
    print(f"  Downloaded: {total}")
    print("=" * 64 + "\n")


if __name__ == "__main__":
    main()
