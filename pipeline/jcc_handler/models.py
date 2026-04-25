"""
jcc_handler/models.py — Schema & constants for JCC table extraction
=====================================================================
Edit this file to change the target columns, keyword gate, or
canonical column names for JCC meeting PDFs.
"""

from __future__ import annotations

# Keywords that must ALL appear on a page for it to be considered a target page
REQUIRED_KEYWORDS = ["Pooling", "Quantum", "Connectivity"]

# Fragments (lower-cased) that identify the connectivity table header row
TARGET_COLUMN_FRAGMENTS = [
    "pooling",
    "applicant",
    "quantum",
    "gen comm",
    "schedule as per",
    "connectivity start",
]

# Canonical column names exposed in JSON output
COLUMN_NAMES = [
    "sr_no",
    "pooling_station",
    "connectivity_applicant",
    "connectivity_quantum_mw",
    "gen_comm_schedule_prev_jcc",
    "schedule_as_per_current_jcc",
    "schedule_current_jcc_ists_scope",
    "connectivity_start_date_under_gna",
    "remarks",
]
