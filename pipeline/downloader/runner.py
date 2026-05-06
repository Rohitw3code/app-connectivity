"""Download sub-pipeline for the copied CTUIL/CEA/PFCCL scrapers."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import threading
import time
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path
from types import ModuleType
from typing import Iterable

from pipeline.downloader.catalog import SCRAPER_SPECS, SPECS_BY_KEY, ScraperSpec

_START_DIR = Path(__file__).resolve().parent.parent.parent
DEFAULT_DOWNLOAD_ROOT = _START_DIR / "output"


def _effective_limit(limit: int | None, download_all: bool = False) -> int:
    if download_all:
        return -1
    if limit is None:
        return 5
    return limit


def _limit_list(items, limit: int):
    if limit is None or limit < 0:
        return items
    return items[:limit]


def _count_pdfs(root: Path) -> set[Path]:
    if not root.exists():
        return set()
    return {p.resolve() for p in root.rglob("*.pdf") if p.is_file()}


def _register_new_downloads(tracker, handler: str, before: set[Path], after: set[Path]) -> None:
    if tracker is None:
        return
    for pdf in sorted(after - before):
        try:
            tracker.register_download(
                handler=handler,
                pdf_filename=pdf.name,
                pdf_path=str(pdf),
                file_size_bytes=pdf.stat().st_size,
            )
        except Exception:
            pass


@contextmanager
def _pushd(path: Path):
    old = Path.cwd()
    path.mkdir(parents=True, exist_ok=True)
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old)


@contextmanager
def _patched_attrs(module: ModuleType, patches: dict[str, object]):
    originals = {}
    missing = object()
    for name, value in patches.items():
        originals[name] = getattr(module, name, missing)
        setattr(module, name, value)
    try:
        yield
    finally:
        for name, value in originals.items():
            if value is missing:
                delattr(module, name)
            else:
                setattr(module, name, value)


def _limit_mapping_values(mapping: dict, limit: int) -> dict:
    if limit < 0:
        return mapping
    trimmed = {}
    for key, value in mapping.items():
        if isinstance(value, dict):
            trimmed[key] = {inner_key: _limit_list(inner_value, limit) for inner_key, inner_value in value.items()}
        else:
            trimmed[key] = _limit_list(value, limit)
    return trimmed


def _build_limit_patches(module: ModuleType, spec: ScraperSpec, limit: int) -> dict[str, object]:
    if limit < 0:
        return {}

    patches: dict[str, object] = {}

    if spec.limit_strategy == "collect_records_by_doc_type":
        original = module.collect_all

        async def limited_collect_all(*args, **kwargs):
            records = await original(*args, **kwargs)
            counts: dict[str, int] = defaultdict(int)
            limited = []
            for record in records:
                doc_type = record.get("doc_type", "pdf")
                if counts[doc_type] >= limit:
                    continue
                counts[doc_type] += 1
                limited.append(record)
            return limited

        patches["collect_all"] = limited_collect_all

    elif spec.limit_strategy == "collect_mapping_values":
        original = module.collect_all

        async def limited_collect_all(*args, **kwargs):
            return _limit_mapping_values(await original(*args, **kwargs), limit)

        patches["collect_all"] = limited_collect_all

    elif spec.limit_strategy == "plan_sequence":
        original = module.reorder_and_plan

        def limited_reorder_and_plan(dest_dir, items, *args, **kwargs):
            return original(dest_dir, _limit_list(items, limit), *args, **kwargs)

        patches["reorder_and_plan"] = limited_reorder_and_plan

    elif spec.limit_strategy == "plan_urls":
        original = module.reorder_and_plan

        def limited_reorder_and_plan(dest_dir, urls, *args, **kwargs):
            return original(dest_dir, _limit_list(urls, limit), *args, **kwargs)

        patches["reorder_and_plan"] = limited_reorder_and_plan

    elif spec.limit_strategy == "apply_incremental_urls":
        original = module.apply_incremental_update

        def limited_apply_incremental_update(dest_dir, urls, *args, **kwargs):
            return original(dest_dir, _limit_list(urls, limit), *args, **kwargs)

        patches["apply_incremental_update"] = limited_apply_incremental_update

    elif spec.limit_strategy == "download_by_category":
        original = module.download_pdf
        lock = threading.Lock()
        counts: dict[str, int] = defaultdict(int)

        def limited_download_pdf(session, item, *args, **kwargs):
            category = item.get("category", "pdf") if isinstance(item, dict) else "pdf"
            with lock:
                if counts[category] >= limit:
                    return None
                counts[category] += 1
            return original(session, item, *args, **kwargs)

        patches["download_pdf"] = limited_download_pdf

    elif spec.limit_strategy == "download_counter":
        original = module.download_pdf
        lock = threading.Lock()
        count = {"value": 0}

        def limited_download_pdf(*args, **kwargs):
            with lock:
                if count["value"] >= limit:
                    return False
                count["value"] += 1
            return original(*args, **kwargs)

        patches["download_pdf"] = limited_download_pdf

    return patches


def _build_output_patches(module: ModuleType, spec: ScraperSpec) -> dict[str, object]:
    patches: dict[str, object] = {}
    if spec.output_attr:
        patches[spec.output_attr] = spec.output_dir
    if spec.key == "cmets":
        patches["INDEX_FILE"] = str(Path(spec.output_dir) / "download_index.txt")
    if spec.key == "monitoring_connectivity":
        patches["TARGETS"] = [
            {**target, "dest_dir": str(Path("uploads/CTUIL-Revocations-PDFs") / Path(target["dest_dir"]).name)}
            for target in module.TARGETS
        ]
    return patches


def _run_module_main(module: ModuleType, spec: ScraperSpec, pfccl_query: str | None) -> None:
    if spec.requires_query:
        if not pfccl_query:
            print(f"  [{spec.label}] skipped: query required")
            return
        module.run(pfccl_query, Path(spec.output_dir))
        return

    result = module.main()
    if inspect.isawaitable(result):
        asyncio.run(result)


def run_scraper(
    spec: ScraperSpec,
    *,
    output_root: Path | str | None = None,
    limit: int | None = 5,
    download_all: bool = False,
    tracker=None,
    pfccl_query: str | None = None,
) -> int:
    """Run one copied scraper and return the number of newly saved PDFs."""
    root = Path(output_root).resolve() if output_root else DEFAULT_DOWNLOAD_ROOT
    limit = _effective_limit(limit, download_all)
    module_name = f"pipeline.downloader.scrapers.{spec.module}"
    output_dir = root / spec.output_dir

    with _pushd(root):
        module = importlib.import_module(module_name)
        before = _count_pdfs(output_dir)
        patches = {
            **_build_output_patches(module, spec),
            **_build_limit_patches(module, spec, limit),
        }
        started = time.time()
        print(f"\n  [{spec.label}] -> {output_dir}")
        with _patched_attrs(module, patches):
            _run_module_main(module, spec, pfccl_query)
        after = _count_pdfs(output_dir)

    _register_new_downloads(tracker, spec.handler, before, after)
    count = len(after - before)
    print(f"  [{spec.label}] new PDFs: {count} ({time.time() - started:.1f}s)")
    return count


def run_download_subpipeline(
    *,
    output_root: Path | str | None = None,
    limit: int | None = 5,
    download_all: bool = False,
    tracker=None,
    scrapers: Iterable[str] | None = None,
    pfccl_query: str | None = None,
) -> dict[str, int]:
    """Run the download phase for every selected scraper."""
    selected = list(scrapers) if scrapers else [spec.key for spec in SCRAPER_SPECS]
    results: dict[str, int] = {}
    for key in selected:
        spec = SPECS_BY_KEY.get(key)
        if spec is None:
            print(f"  [download] unknown scraper key skipped: {key}")
            results[key] = 0
            continue
        try:
            results[key] = run_scraper(
                spec,
                output_root=output_root,
                limit=limit,
                download_all=download_all,
                tracker=tracker,
                pfccl_query=pfccl_query,
            )
        except Exception as exc:
            print(f"  [{spec.label}] FAILED: {exc}")
            results[key] = 0
    return results


def download_cmets_pdfs(output_root=None, limit: int | None = 5, tracker=None, download_all: bool = False) -> int:
    return run_scraper(SPECS_BY_KEY["cmets"], output_root=output_root, limit=limit, tracker=tracker, download_all=download_all)


def download_jcc_pdfs(output_root=None, limit: int | None = 5, tracker=None, download_all: bool = False) -> int:
    return run_scraper(SPECS_BY_KEY["jcc"], output_root=output_root, limit=limit, tracker=tracker, download_all=download_all)


def download_effectiveness_pdfs(output_root=None, limit: int | None = 5, tracker=None, download_all: bool = False) -> int:
    return run_scraper(SPECS_BY_KEY["effectiveness"], output_root=output_root, limit=limit, tracker=tracker, download_all=download_all)


def download_bayallocation_pdfs(output_root=None, limit: int | None = 5, tracker=None, download_all: bool = False) -> int:
    return run_scraper(SPECS_BY_KEY["bayallocation"], output_root=output_root, limit=limit, tracker=tracker, download_all=download_all)
