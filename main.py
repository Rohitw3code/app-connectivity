from __future__ import annotations

import argparse
import json
from pathlib import Path
from datetime import datetime
from time import perf_counter

from config import DEFAULT_CHUNKS_PER_PAGE, DEFAULT_PAGE_CHUNK_OVERLAP, load_runtime_config
from excel_export import export_results_to_excel
from extractor import PipelineResult, run_pipeline


def _resolve_pdf_paths(source_dir: str, pdf: str | None) -> list[Path]:
    if pdf:
        return [Path(pdf).resolve()]

    src_dir = Path(source_dir).resolve()
    pdfs = sorted(p for p in src_dir.glob("*.pdf") if p.is_file())
    if not pdfs:
        raise SystemExit(f"ERROR: No PDF found in {src_dir}. Use --pdf to specify a file.")
    return pdfs


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
    parser.add_argument("--pdf", default=None, help="Path to a single PDF file (optional; by default all PDFs in source-dir are processed)")

    parser.add_argument("--mode", choices=["vm", "laptop"], default=None, help="Optional override for config EXECUTION_TARGET")
    parser.add_argument("--api-key", default=None, help="OpenAI API key override")
    parser.add_argument("--llm-script", default=None, help="Path to llm_client.bat for VM mode")

    parser.add_argument("--output", default="output.json", help="Output JSON file")
    parser.add_argument("--excel-output", default="output.xlsx", help="Output Excel file")
    parser.add_argument("--chunks-dir", default="page_chunks", help="Folder to save per-page chunk JSON files")
    parser.add_argument("--chunks-per-page", type=int, default=DEFAULT_CHUNKS_PER_PAGE, help="Number of chunks to split each page into")
    parser.add_argument("--page-chunk-overlap", type=int, default=DEFAULT_PAGE_CHUNK_OVERLAP, help="Character overlap between adjacent page chunks")
    args = parser.parse_args()

    runtime = load_runtime_config(
        mode_override=args.mode,
        api_key_override=args.api_key,
        llm_script_override=args.llm_script,
    )
    started_at = datetime.now()
    run_start = perf_counter()
    pdf_paths = _resolve_pdf_paths(args.source_dir, args.pdf)

    print("=" * 64)
    print("  PDF MAPPED EXTRACTION PIPELINE")
    print("=" * 64)
    print(f"PDFs selected    : {len(pdf_paths)}")
    print(f"Execution target : {runtime.execution_target}")
    print(f"VM mode          : {runtime.vm_mode}")

    chunks_dir = Path(args.chunks_dir).resolve()
    all_results: list[PipelineResult] = []
    for idx, pdf_path in enumerate(pdf_paths, start=1):
        print("-" * 64)
        print(f"[{idx}/{len(pdf_paths)}] Processing: {pdf_path}")
        result = run_pipeline(
            pdf_path=str(pdf_path),
            api_key=runtime.api_key or None,
            chunks_dir=chunks_dir,
            chunks_per_page=args.chunks_per_page,
            page_chunk_overlap_chars=max(0, args.page_chunk_overlap),
            vm_mode=runtime.vm_mode,
            llm_script_path=runtime.llm_script_path,
        )
        all_results.append(result)

    total_pages_extracted = sum(r.total_pages_extracted for r in all_results)
    total_pages_passed_gate = sum(r.pages_passed_gate for r in all_results)
    total_pages_skipped = sum(r.pages_skipped for r in all_results)
    total_rows = sum(r.total_rows for r in all_results)

    print("=" * 64)
    print("SUMMARY")
    print(f"  PDFs processed   : {len(all_results)}")
    print(f"  Pages extracted  : {total_pages_extracted}")
    print(f"  Pages passed gate: {total_pages_passed_gate}")
    print(f"  Pages skipped    : {total_pages_skipped}")
    print(f"  Total rows parsed: {total_rows}")
    print("=" * 64)

    output = {
        "pdfs_processed": len(all_results),
        "total_pages_extracted": total_pages_extracted,
        "total_pages_passed_gate": total_pages_passed_gate,
        "total_pages_skipped": total_pages_skipped,
        "total_rows": total_rows,
        "results": [_serialize_result(result) for result in all_results],
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    finished_at = datetime.now()
    runtime_seconds = perf_counter() - run_start
    excel_path = export_results_to_excel(
        output,
        args.excel_output,
        runtime_seconds=runtime_seconds,
        started_at=started_at,
        finished_at=finished_at,
    )

    print(f"\nOutput saved → {args.output}")
    print(f"Excel saved  → {excel_path}")
    print(f"Chunk files saved → {chunks_dir}")
    print(f"Runtime (s) → {runtime_seconds:.2f}")

    if all_results and all_results[0].results:
        first_page = all_results[0].results[0]
        print(f"\nPreview — Page {first_page.page_number} (first PDF):")
        for row in first_page.rows[:3]:
            print(json.dumps(row.model_dump(by_alias=True), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
