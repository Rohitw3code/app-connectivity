"""
bayallocation_handler/models.py — Schema & constants for Bay Allocation PDF extraction
========================================================================================
The Bay Allocation PDF contains one large table per page (spanning all substations
on that page).  Each page is treated as one extraction unit.

Column layout (20-column table):
  col  0  → sl_no
  col  1  → name_of_substation
  col  2  → substation_coordinates
  col  3  → region
  col  4  → transformation_capacity_planned_mva
  col  5  → transformation_capacity_existing_mva
  col  6  → transformation_capacity_under_implementation_mva
  col  7  → bay_no_220kv                    (RE Capacity Granted – 220 kV bay number)
  col  8  → connectivity_quantum_mw_220kv   (RE Capacity Granted – 220 kV quantum)
  col  9  → name_of_entity_220kv            (RE Capacity Granted – 220 kV entity)
  col 10  → bay_no_400kv                    (RE Capacity Granted – 400 kV bay number)
  col 11  → connectivity_quantum_mw_400kv   (RE Capacity Granted – 400 kV quantum)
  col 12  → name_of_entity_400kv            (RE Capacity Granted – 400 kV entity)
  col 13  → margin_bay_no_220kv             (Margin – 220 kV bay number)
  col 14  → margin_available_mw_220kv       (Margin – 220 kV margins available)
  col 15  → margin_bay_no_400kv             (Margin – 400 kV bay number)
  col 16  → margin_available_mw_400kv       (Margin – 400 kV margins available)
  col 17  → space_provision_220kv           (Space Provision – 220 kV, No. of line bays)
  col 18  → space_provision_400kv           (Space Provision – 400 kV, No. of line bays)
  col 19  → remarks
"""

from __future__ import annotations

# ── Keyword gate ───────────────────────────────────────────────────────────────
# All these strings must appear somewhere in the page text for the page to be
# considered a Bay Allocation data page.
REQUIRED_KEYWORDS = [
    "Name of Substation",
    "RE Capacity Granted",
    "Margin on Existing",
]

# ── Header fragments used to identify the main allocation table ────────────────
# At least 3 must appear (case-insensitive) in the joined first-row text.
TARGET_COLUMN_FRAGMENTS = [
    "sl. no",
    "name of substation",
    "re capacity granted",
    "margin",
    "space provision",
    "remarks",
]

# ── Canonical column names (20 columns) ───────────────────────────────────────
COLUMN_NAMES: list[str] = [
    "sl_no",
    "name_of_substation",
    "substation_coordinates",
    "region",
    "transformation_capacity_planned_mva",
    "transformation_capacity_existing_mva",
    "transformation_capacity_under_implementation_mva",
    "bay_no_220kv",
    "connectivity_quantum_mw_220kv",
    "name_of_entity_220kv",
    "bay_no_400kv",
    "connectivity_quantum_mw_400kv",
    "name_of_entity_400kv",
    "margin_bay_no_220kv",
    "margin_available_mw_220kv",
    "margin_bay_no_400kv",
    "margin_available_mw_400kv",
    "space_provision_220kv",
    "space_provision_400kv",
    "remarks",
]

# Number of leading table rows that are header / sub-header rows to skip
HEADER_ROW_COUNT = 5
