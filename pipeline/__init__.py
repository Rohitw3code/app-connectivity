"""
pipeline — Three-module PDF extraction & mapping pipeline
==========================================================
Sub-packages:
    cmets_handler/          → Module 1: CMETS PDF extraction
    effectiveness_handler/  → Module 2: Effectiveness PDF extraction
    mapping_handler/        → Module 3: CMETS × Effectiveness merge

Shared utilities:
    excel_utils.py          → Generic JSON → Excel exporter
"""

from pipeline.cmets_handler         import run_cmets_extraction
from pipeline.effectiveness_handler import run_effectiveness_extraction
from pipeline.mapping_handler       import run_mapping

__all__ = [
    "run_cmets_extraction",
    "run_effectiveness_extraction",
    "run_mapping",
]
