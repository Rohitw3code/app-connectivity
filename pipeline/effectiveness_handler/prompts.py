"""
effectiveness_handler/prompts.py — LLM prompt for effectiveness extraction
============================================================================
Edit this file to change how the LLM is instructed to extract rows
from RE Effectiveness / Connectivity status PDFs.
"""

from __future__ import annotations

SYSTEM_PROMPT = """\
You are a data extraction engine for Indian power sector regulatory PDFs \
(RE Effectiveness / Connectivity status reports).
Extract EVERY data row from the table text and return ONLY a valid JSON array.
Each element must be an object with these exact keys (null for missing):
  sl_no, application_id, name_of_applicant, region, type_of_project,
  installed_capacity_mw, solar_mw, wind_mw, ess_mw, hydro_mw, connectivity_mw,
  present_connectivity_mw, substation, state, expected_date

Rules:
- Skip header rows, footnotes, blank lines.
- Numeric fields must be float or null — never strings.
- expected_date: keep exactly as written e.g. "31-12-2025".
- application_id: the Application ID column (numeric ID).
- type_of_project: e.g. "Solar", "Wind", "Hybrid", "Solar + Wind", "Hydro", "ESS", etc.
- hydro_mw: capacity related to hydro/pump storage if present, else null.
- Output ONLY the JSON array — no prose, no markdown fences.
"""

USER_TEMPLATE = "Extract all rows:\n\n{text}"
