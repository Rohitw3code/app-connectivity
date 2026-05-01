"""
mapping_handler/merge.py — Row-by-row CMETS × Effectiveness merge
===================================================================
Takes a CMETS DataFrame and an effectiveness lookup dict, enriches
each row by matching Application IDs (GNA primary, LTA fallback, 5.2 GNA).

Edit this file to change:
  • Which CMETS columns are updated from effectiveness data
  • Which new enrichment columns are added
  • The matching / fallback strategy
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd

from pipeline.shared_utils import (
    find_col,
    ids_from_cell,
    lookup_first,
    safe_float,
    classify_project_type
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_valid(val) -> bool:
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    return str(val).strip().lower() not in ("", "none", "null", "na", "n/a", "-", "--", "nan")




# ── Enrichment column definitions ────────────────────────────────────────────

NEW_COLUMNS = [
    "Region", "Type of Project",
    "Installed capacity (MW) solar",  "Installed capacity (MW) wind",
    "Installed capacity (MW) ess",    "Installed capacity (MW) hydro",
    "Installed capacity (MW) hybrid",
]

_EFF_FIELD_TO_COL: list[tuple[str, str]] = [
    ("region",          "Region"),
    ("type_of_project", "Type of Project"),
    ("solar_mw",        "Installed capacity (MW) solar"),
    ("wind_mw",         "Installed capacity (MW) wind"),
    ("ess_mw",          "Installed capacity (MW) ess"),
    ("hydro_mw",        "Installed capacity (MW) hydro"),
]


# ── Columns to update from effectiveness when matched ────────────────────────
# These are effectiveness columns that overlap with CMETS columns.
# When a match is found via GNA/LTA/5.2, the effectiveness value
# overwrites the CMETS value (if the effectiveness value is valid).

_OVERLAPPING_UPDATES: list[tuple[str, list[str]]] = [
    # (effectiveness_key, [cmets_column_candidates...])
    ("name_of_applicant",       ["Name of Developers", "Name of the developers"]),
    ("substation",              ["Substation", "substaion"]),
    ("state",                   ["State"]),
    ("installed_capacity_mw",   ["Application Quantum (MW)(ST II)"]),
    ("connectivity_mw",         ["Application Quantum (MW)(ST II)"]),
    ("region",                  ["Region"]),
]


# ── Main merge function ──────────────────────────────────────────────────────

def merge_rows(df: pd.DataFrame, lookup: dict) -> tuple[pd.DataFrame, dict]:
    """Apply effectiveness data to CMETS DataFrame rows.

    Returns (enriched_df, match_stats).
    """
    for col in NEW_COLUMNS:
        if col not in df.columns:
            df[col] = None

    gna_col      = find_col(df, "GNA/ST II Application ID")
    lta_col      = find_col(df, "LTA Application ID")
    col_52       = find_col(df, "Application ID under Enhancement 5.2 or revision")
    col_dev_name = find_col(df, "Name of the developers", "Name of developers", "Name of Developers")
    col_subst    = find_col(df, "substaion", "Substation")
    col_state    = find_col(df, "State")
    col_quantum  = find_col(df, "Application Quantum (MW)(ST II)")

    matched_gna = matched_lta = matched_52 = unmatched = 0

    for idx, row in df.iterrows():
        # ── Find effectiveness record using ID cascade ────────────────
        eff       = None
        match_via = None

        # Try GNA first
        if gna_col:
            gna_ids = ids_from_cell(row.get(gna_col))
            eff     = lookup_first(gna_ids, lookup)
            if eff:
                match_via = "GNA"

        # Fallback to LTA
        if eff is None and lta_col:
            eff = lookup_first(ids_from_cell(row.get(lta_col)), lookup)
            if eff:
                match_via = "LTA"

        # Fallback to 5.2 GNA
        if eff is None and col_52:
            eff = lookup_first(ids_from_cell(row.get(col_52)), lookup)
            if eff:
                match_via = "5.2"

        if eff is None:
            unmatched += 1
            continue

        if match_via == "GNA":
            matched_gna += 1
        elif match_via == "LTA":
            matched_lta += 1
        else:
            matched_52 += 1

        # ── Update overlapping CMETS columns from effectiveness ───────
        if col_dev_name and _is_valid(eff.get("name_of_applicant")):
            df.at[idx, col_dev_name] = eff["name_of_applicant"]
        if col_subst and _is_valid(eff.get("substation")):
            df.at[idx, col_subst] = eff["substation"]
        if col_state and _is_valid(eff.get("state")):
            df.at[idx, col_state] = eff["state"]
        if col_quantum and _is_valid(eff.get("installed_capacity_mw")):
            df.at[idx, col_quantum] = eff["installed_capacity_mw"]

        # ── Populate new enrichment columns ───────────────────────────
        for eff_key, col_name in _EFF_FIELD_TO_COL:
            if _is_valid(eff.get(eff_key)):
                df.at[idx, col_name] = eff[eff_key]

        # ── Hybrid total ──────────────────────────────────────────────
        cats = classify_project_type(eff.get("type_of_project") or "")
        if "hybrid" in cats:
            total = sum(
                float(eff.get(k) or 0)
                for k in ("solar_mw", "wind_mw", "ess_mw", "hydro_mw")
                if _is_valid(eff.get(k))
            )
            if total > 0:
                df.at[idx, "Installed capacity (MW) hybrid"] = total

    stats = {
        "matched_gna": matched_gna,
        "matched_lta": matched_lta,
        "matched_52":  matched_52,
        "unmatched":   unmatched,
        "total_rows":  len(df),
    }
    return df, stats
