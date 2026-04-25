"""
mapping_handler/merge.py — Row-by-row CMETS × Effectiveness merge
===================================================================
Takes a CMETS DataFrame and an effectiveness lookup dict, enriches
each row by matching Application IDs (GNA primary, LTA fallback).

Edit this file to change:
  • Which CMETS columns are updated from effectiveness data
  • Which new enrichment columns are added
  • The matching / fallback strategy
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd

# ── Helpers ───────────────────────────────────────────────────────────────────

def _is_valid(val) -> bool:
    if val is None:
        return False
    if isinstance(val, float) and pd.isna(val):
        return False
    return str(val).strip().lower() not in ("", "none", "null", "na", "n/a", "-", "--", "nan")


def _classify_project_type(type_str: Optional[str]) -> dict:
    if not type_str:
        return {}
    t = type_str.lower().strip()
    cats: list[str] = []
    if "solar"  in t: cats.append("solar")
    if "wind"   in t: cats.append("wind")
    if "ess"    in t or "energy storage" in t or "bess" in t: cats.append("ess")
    if "hydro"  in t or "pump" in t or "psp" in t: cats.append("hydro")
    if len(cats) > 1 or "hybrid" in t: cats.append("hybrid")
    return {cat: True for cat in cats}


def _find_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    for c in candidates:
        for col in df.columns:
            if c.lower() == col.lower():
                return col
    return None


def _ids_from_cell(cell_val) -> list[str]:
    raw = str(cell_val or "").strip()
    return [x.strip() for x in re.split(r"[,;\s]+", raw) if x.strip()] if raw else []


def _lookup_first(ids: list[str], lookup: dict) -> Optional[dict]:
    for id_ in ids:
        if id_ in lookup:
            return lookup[id_]
    return None


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


# ── Main merge function ──────────────────────────────────────────────────────

def merge_rows(df: pd.DataFrame, lookup: dict) -> tuple[pd.DataFrame, dict]:
    """Apply effectiveness data to CMETS DataFrame rows.

    Returns (enriched_df, match_stats).
    """
    for col in NEW_COLUMNS:
        if col not in df.columns:
            df[col] = None

    gna_col      = _find_col(df, "GNA/ST II Application ID")
    lta_col      = _find_col(df, "LTA Application ID")
    col_dev_name = _find_col(df, "Name of the developers", "Name of developers")
    col_subst    = _find_col(df, "substaion", "Substation")
    col_state    = _find_col(df, "State")
    col_quantum  = _find_col(df, "Application Quantum (MW)(ST II)")

    matched_gna = matched_lta = unmatched = 0

    for idx, row in df.iterrows():
        gna_ids   = _ids_from_cell(row.get(gna_col)) if gna_col else []
        eff       = _lookup_first(gna_ids, lookup)
        match_via = "GNA"

        if eff is None and lta_col:
            eff       = _lookup_first(_ids_from_cell(row.get(lta_col)), lookup)
            match_via = "LTA"

        if eff is None:
            unmatched += 1
            continue

        if match_via == "GNA":
            matched_gna += 1
        else:
            matched_lta += 1

        # Update existing columns
        if col_dev_name and _is_valid(eff.get("name_of_applicant")):
            df.at[idx, col_dev_name] = eff["name_of_applicant"]
        if col_subst and _is_valid(eff.get("substation")):
            df.at[idx, col_subst] = eff["substation"]
        if col_state and _is_valid(eff.get("state")):
            df.at[idx, col_state] = eff["state"]
        if col_quantum and _is_valid(eff.get("installed_capacity_mw")):
            df.at[idx, col_quantum] = eff["installed_capacity_mw"]

        # Populate new enrichment columns
        for eff_key, col_name in _EFF_FIELD_TO_COL:
            if _is_valid(eff.get(eff_key)):
                df.at[idx, col_name] = eff[eff_key]

        # Hybrid total
        cats = _classify_project_type(eff.get("type_of_project") or "")
        if cats.get("hybrid"):
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
        "unmatched":   unmatched,
        "total_rows":  len(df),
    }
    return df, stats
