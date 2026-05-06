"""Helpers for extraction-by-source using per-source cache tables."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Callable, Iterable
import shutil

from config import RuntimeConfig
from pipeline.downloader.pdf_cache import get_pdf_cache
from pipeline.cmets_handler import run_cmets_extraction
from pipeline.effectiveness_handler import run_effectiveness_extraction
from pipeline.jcc_handler import run_jcc_extraction
from pipeline.bayallocation_handler import run_bayallocation_extraction

_START_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class ExtractionSource:
    name: str
    key: str
    handler: str
    source_dir: Path
    fallback_dir: Path
    output_dir: Path
    excel_path: Path
    runner: Callable[..., object]
    allow_types: tuple[str, ...] | None = None
    flatten: bool = False


@dataclass(frozen=True)
class PendingPdf:
    pdf_name: str
    pdf_type: str
    pdf_path: Path


def _infer_type(source_name: str, pdf_path: Path, pdf_type: str | None) -> str:
    if pdf_type and str(pdf_type).strip():
        return str(pdf_type).strip()
    parts = pdf_path.parts
    jcc_regions = {
        "Eastern Region",
        "North Eastern Region",
        "Northern Region",
        "Southern Region",
        "Western Region",
    }
    if source_name in parts:
        idx = parts.index(source_name)
        if idx + 1 < len(parts) - 1:
            first = parts[idx + 1]
            if first in jcc_regions and idx + 2 < len(parts) - 1:
                return parts[idx + 2]
            return first
    parent = pdf_path.parent.name
    if parent and parent != source_name:
        return parent
    return ""


def _resolve_source_dir(primary: Path, fallback: Path) -> Path:
    if primary.exists() and any(primary.rglob("*.pdf")):
        return primary
    return fallback


def _resolve_pdf_path(source_dir: Path, pdf_name: str) -> Path | None:
    for path in source_dir.rglob(pdf_name):
        if path.is_file():
            return path
    return None


def _seed_cache_from_source(cache, source_name: str, source_dir: Path) -> None:
    entries = []
    for pdf in source_dir.rglob("*.pdf"):
        if not pdf.is_file():
            continue
        pdf_type = _infer_type(source_name, pdf, "")
        entries.append((pdf.name, pdf_type, str(pdf)))
    cache.record_existing_pdfs(entries)


def _collect_pending(source: ExtractionSource, db_path: Path) -> list[PendingPdf]:
    cache = get_pdf_cache(db_path, source.key, source.name)
    active_dir = _resolve_source_dir(source.source_dir, source.fallback_dir)
    _seed_cache_from_source(cache, source.name, active_dir)

    pending = []
    for row in cache.get_pending_extractions():
        pdf_name = row.get("pdf_name", "")
        if not pdf_name:
            continue
        pdf_type = row.get("pdf_type", "")
        pdf_path_str = row.get("pdf_path", "")
        pdf_path = Path(pdf_path_str) if pdf_path_str else None
        if pdf_path is None or not pdf_path.exists():
            pdf_path = _resolve_pdf_path(active_dir, pdf_name)
        if pdf_path is None:
            continue
        inferred_type = _infer_type(source.name, pdf_path, pdf_type)
        if source.allow_types and inferred_type not in source.allow_types:
            continue
        pending.append(PendingPdf(pdf_name=pdf_name, pdf_type=inferred_type, pdf_path=pdf_path))
    return pending


def _prepare_temp_dir(pdfs: Iterable[PendingPdf], flatten: bool) -> tuple[TemporaryDirectory, Path]:
    tmp = TemporaryDirectory()
    root = Path(tmp.name)
    for item in pdfs:
        if flatten or not item.pdf_type:
            dest_dir = root
        else:
            dest_dir = root / item.pdf_type
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest = dest_dir / item.pdf_name
        if not dest.exists():
            shutil.copy2(item.pdf_path, dest)
    return tmp, root


def _cache_path_for(output_dir: Path, pdf_name: str) -> Path:
    return output_dir / f"{Path(pdf_name).stem}.json"


def extract_pending_for_source(source: ExtractionSource, runtime: RuntimeConfig, db_path: Path) -> dict:
    pending = _collect_pending(source, db_path)
    if not pending:
        return {"source": source.name, "pending": 0, "extracted": 0}

    tmp_ctx, temp_dir = _prepare_temp_dir(pending, source.flatten)
    try:
        if source.runner is run_cmets_extraction:
            source.runner(
                source_dir=str(temp_dir),
                output_dir=str(source.output_dir),
                excel_path=str(source.excel_path),
                runtime=runtime,
            )
        elif source.runner is run_jcc_extraction:
            source.runner(
                source_dir=str(temp_dir),
                output_dir=str(source.output_dir),
                excel_path=str(source.excel_path),
                runtime=runtime,
            )
        else:
            source.runner(
                source_dir=str(temp_dir),
                output_dir=str(source.output_dir),
                excel_path=str(source.excel_path),
                runtime=runtime,
            )
    finally:
        tmp_ctx.cleanup()

    cache = get_pdf_cache(db_path, source.key, source.name)
    extracted = 0
    for item in pending:
        cache_path = _cache_path_for(source.output_dir, item.pdf_name)
        if cache_path.exists():
            cache.mark_extracted(item.pdf_name, pdf_type=item.pdf_type, pdf_path=item.pdf_path)
            extracted += 1
    return {"source": source.name, "pending": len(pending), "extracted": extracted}


def get_extraction_sources(start_dir: Path | None = None) -> list[ExtractionSource]:
    root = start_dir or _START_DIR
    output_root = root / "output" / "source_output"
    source_root = root / "source"
    output_cache = root / "output"
    excel_root = root / "excels"

    return [
        ExtractionSource(
            name="CTUIL-ISTS-CMETS",
            key="cmets",
            handler="cmets",
            source_dir=output_root / "CTUIL-ISTS-CMETS" / "minutes",
            fallback_dir=source_root / "cmets_pdfs" / "minutes",
            output_dir=output_cache / "cmets_cache",
            excel_path=excel_root / "01_cmets_extracted.xlsx",
            runner=run_cmets_extraction,
            allow_types=None,
            flatten=True,
        ),
        ExtractionSource(
            name="CTUIL-GNA-Connectivity-Fresh",
            key="effectiveness",
            handler="effectiveness",
            source_dir=output_root / "CTUIL-GNA-Connectivity-Fresh",
            fallback_dir=source_root / "effectiveness_pdfs",
            output_dir=output_cache / "effectiveness_cache",
            excel_path=excel_root / "02_effectiveness_extracted.xlsx",
            runner=run_effectiveness_extraction,
            flatten=False,
        ),
        ExtractionSource(
            name="CTUIL-ISTS-JCC",
            key="jcc",
            handler="jcc",
            source_dir=output_root / "CTUIL-ISTS-JCC",
            fallback_dir=source_root / "jcc_pdfs",
            output_dir=output_cache / "jcc_cache",
            excel_path=excel_root / "04_jcc_extracted.xlsx",
            runner=run_jcc_extraction,
            allow_types=("Minutes",),
            flatten=False,
        ),
        ExtractionSource(
            name="CTUIL-Renewable-Energy",
            key="bayallocation",
            handler="bayallocation",
            source_dir=output_root / "CTUIL-Renewable-Energy" / "Bays Allocation",
            fallback_dir=source_root / "bayallocation",
            output_dir=output_cache / "bayallocation_cache",
            excel_path=excel_root / "05_bayallocation_extracted.xlsx",
            runner=run_bayallocation_extraction,
            allow_types=None,
            flatten=False,
        ),
    ]


def run_pending_extractions(runtime: RuntimeConfig, only_sources: Iterable[str] | None = None) -> list[dict]:
    db_path = _START_DIR / "pipeline_tracker.db"
    sources = get_extraction_sources(_START_DIR)
    if only_sources:
        selected = {name.strip() for name in only_sources if name.strip()}
        sources = [s for s in sources if s.name in selected or s.key in selected or s.handler in selected]
    return [extract_pending_for_source(source, runtime, db_path) for source in sources]
