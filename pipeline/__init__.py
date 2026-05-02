"""
pipeline — PDF Download, Extraction & Mapping Pipeline
========================================================
Sub-packages:
    downloader/                → PDF Download Module (merged from ctuil-pdf-scraper)
    cmets_handler/             → Module 1: CMETS PDF extraction
    effectiveness_handler/     → Module 2: Effectiveness PDF extraction
    mapping_handler/           → Module 3: CMETS × Effectiveness merge
    jcc_handler/               → Module 4: JCC Meeting PDF extraction
    bayallocation_handler/     → Module 5: Bay Allocation PDF extraction
    bay_mapping_handler/       → Module 6: CMETS × Bay Allocation mapping

Shared utilities:
    tracker.py                 → SQLite pipeline tracker
    excel_utils.py             → Generic JSON → Excel exporter
"""

from pipeline.cmets_handler            import run_cmets_extraction
from pipeline.effectiveness_handler    import run_effectiveness_extraction
from pipeline.mapping_handler          import run_mapping
from pipeline.jcc_handler              import run_jcc_extraction
from pipeline.bayallocation_handler    import run_bayallocation_extraction
from pipeline.bay_mapping_handler      import run_bay_mapping
from pipeline.tracker                  import PipelineTracker
from pipeline.downloader               import (
    download_cmets_pdfs,
    download_jcc_pdfs,
    download_effectiveness_pdfs,
    download_bayallocation_pdfs,
)

__all__ = [
    # Downloaders
    "download_cmets_pdfs",
    "download_jcc_pdfs",
    "download_effectiveness_pdfs",
    "download_bayallocation_pdfs",
    # Extractors
    "run_cmets_extraction",
    "run_effectiveness_extraction",
    "run_mapping",
    "run_jcc_extraction",
    "run_bayallocation_extraction",
    "run_bay_mapping",
    # Tracker
    "PipelineTracker",
]
