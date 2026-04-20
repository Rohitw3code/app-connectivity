from __future__ import annotations

import argparse
import json
from pathlib import Path

from config import DEFAULT_CHUNKS_PER_PAGE, DEFAULT_PAGE_CHUNK_OVERLAP, load_runtime_config
from extractor import PipelineResult, run_pipeline


def _resolve_pdf_path(source_dir: str, pdf: str | None) -> Path:
    if pdf:
        return Path(pdf).resolve()

    src_dir = Path(source_dir).resolve()
    pdfs = sorted(p for p in src_dir.glob("*.pdf") if p.is_file())
    if not pdfs:
        raise SystemExit(f"ERROR: No PDF found in {src_dir}. Use --pdf to specify a file.")
    return pdfs[0]


def _serialize_result(result: PipelineResult) -> dict:
    output = result.model_dump()
    for i, page_res in enumerate(output["results"]):
        page_res["rows"] = [
            r.model_dump(by_alias=True)
            for r in result.results[i].rows
        ]
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="PDF mapped extraction pipeline")
    parser.add_argument("--source-dir", default="source_1", help="Folder containing PDF files")
    parser.add_argument("--pdf", default=None, help="Path to PDF file (optional; defaults to first PDF in source-dir)")

    parser.add_argument("--mode", choices=["vm", "laptop"], default=None, help="Execution target from config: vm or laptop")
    parser.add_argument("--vm", default=None, help="Legacy bool override for VM mode (true/false)")
    parser.add_argument("--api-key", default=None, help="OpenAI API key override")
    parser.add_argument("--llm-script", default=None, help="Path to llm_client.bat for VM mode")

    parser.add_argument("--output", default="output.json", help="Output JSON file")
    parser.add_argument("--chunks-dir", default="page_chunks", help="Folder to save per-page chunk JSON files")
    parser.add_argument("--chunks-per-page", type=int, default=DEFAULT_CHUNKS_PER_PAGE, help="Number of chunks to split each page into")
    parser.add_argument("--page-chunk-overlap", type=int, default=DEFAULT_PAGE_CHUNK_OVERLAP, help="Character overlap between adjacent page chunks")
    args = parser.parse_args()

    runtime = load_runtime_config(
        mode_override=args.mode,
        vm_override=args.vm,
        api_key_override=args.api_key,
        llm_script_override=args.llm_script,
    )
    pdf_path = _resolve_pdf_path(args.source_dir, args.pdf)

    print("=" * 64)
    print("  PDF MAPPED EXTRACTION PIPELINE")
    print("=" * 64)
    print(f"PDF selected     : {pdf_path}")
    print(f"Execution target : {runtime.execution_target}")
    print(f"VM mode          : {runtime.vm_mode}")

    chunks_dir = Path(args.chunks_dir).resolve()
    result = run_pipeline(
        pdf_path=str(pdf_path),
        api_key=runtime.api_key or None,
        chunks_dir=chunks_dir,
        chunks_per_page=args.chunks_per_page,
        page_chunk_overlap_chars=max(0, args.page_chunk_overlap),
        vm_mode=runtime.vm_mode,
        llm_script_path=runtime.llm_script_path,
    )

    print("=" * 64)
    print("SUMMARY")
    print(f"  Pages extracted  : {result.total_pages_extracted}")
    print(f"  Pages passed gate: {result.pages_passed_gate}")
    print(f"  Pages skipped    : {result.pages_skipped}")
    print(f"  Total rows parsed: {result.total_rows}")
    print("=" * 64)

    output = _serialize_result(result)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\nOutput saved → {args.output}")
    print(f"Chunk files saved → {chunks_dir}")

    if result.results:
        first = result.results[0]
        print(f"\nPreview — Page {first.page_number}:")
        for row in first.rows[:3]:
            print(json.dumps(row.model_dump(by_alias=True), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
