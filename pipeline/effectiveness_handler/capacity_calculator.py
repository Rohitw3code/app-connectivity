"""
effectiveness_handler/capacity_calculator.py — Installed/Break-up Capacity Calculator
========================================================================================
Cross-references CMETS rows with effectiveness data to compute the
**Installed/Break-up Capacity (MW)** sub-columns: [Solar, Wind, Hybrid, Hydro].

Logic
-----
For each CMETS row:
1. Find the matching effectiveness record using the ID cascade:
   GNA/ST II Application ID  →  LTA Application ID  →  5.2 GNA Application ID
2. Parse the CMETS ``Type`` column to extract individual energy types and their
   MW values (e.g. ``"Wind (12) + BESS (44)"`` → ``{"wind": 12, "bess": 44}``).
3. Read the effectiveness record's ``type_of_project`` to know the project category
   and read the per-technology MW columns (``solar_mw``, ``wind_mw``, ``ess_mw``,
   ``hydro_mw``).
4. Sum matching values: for each technology found in the CMETS Type column that
   matches the effectiveness type_of_project, compute:
       ``Installed capacity = effectiveness_value + cmets_type_value``
5. Store the result in the corresponding sub-column.

Called from :mod:`pipeline.mapping_handler.merge` or directly from the mapping runner.

Usage
-----
    from pipeline.effectiveness_handler.capacity_calculator import compute_installed_capacity
    df = compute_installed_capacity(cmets_df, effectiveness_lookup)
"""

from __future__ import annotations

import re
import logging
from typing import Optional

import pandas as pd

from pipeline.shared_utils import (
    safe_str,
    safe_float,
    ids_from_cell,
    lookup_first,
    find_col,
    classify_project_type
)

logger = logging.getLogger(__name__)


# ── Output column names ──────────────────────────────────────────────────────

CAPACITY_COLUMNS = [
    "Installed/Break-up Capacity (MW) Solar",
    "Installed/Break-up Capacity (MW) Wind",
    "Installed/Break-up Capacity (MW) Hybrid",
    "Installed/Break-up Capacity (MW) Hydro",
]


# ── Helpers ───────────────────────────────────────────────────────────────────



# ── CMETS Type parser ────────────────────────────────────────────────────────

# Matches patterns like: "Wind (12)", "Solar(300)", "BESS (44)", "Hydro(150)"
_TYPE_VALUE_RE = re.compile(
    r"(solar|wind|bess|ess|hydro|hybrid|thermal|psp|pump\s*storage)"
    r"\s*\(\s*([\d,.]+)\s*\)",
    re.IGNORECASE,
)

# Category normalisation map
_CAT_MAP = {
    "solar":        "solar",
    "wind":         "wind",
    "bess":         "ess",
    "ess":          "ess",
    "hydro":        "hydro",
    "psp":          "hydro",
    "pump storage": "hydro",
    "hybrid":       "hybrid",
    "thermal":      "thermal",
}


def parse_cmets_type(type_str: Optional[str]) -> dict[str, float]:
    """Parse the CMETS Type column into a category → MW mapping.

    Examples
    --------
    >>> parse_cmets_type("Wind (12) + BESS (44)")
    {'wind': 12.0, 'ess': 44.0}

    >>> parse_cmets_type("Solar")
    {'solar': 0.0}   # present but no MW value in the string

    >>> parse_cmets_type(None)
    {}
    """
    text = safe_str(type_str)
    if not text:
        return {}

    result: dict[str, float] = {}

    # Try to match explicit "(MW)" patterns first
    for match in _TYPE_VALUE_RE.finditer(text):
        raw_cat = match.group(1).lower().strip()
        mw_val  = safe_float(match.group(2))
        cat     = _CAT_MAP.get(raw_cat, raw_cat)
        result[cat] = result.get(cat, 0.0) + mw_val

    # If no explicit MW values found, detect category presence from text
    if not result:
        lower = text.lower()
        for raw_cat, cat in _CAT_MAP.items():
            if raw_cat in lower:
                result[cat] = 0.0

    return result



# ── Category → output column mapping ────────────────────────────────────────

_CAT_TO_OUTPUT_COL = {
    "solar":  "Installed/Break-up Capacity (MW) Solar",
    "wind":   "Installed/Break-up Capacity (MW) Wind",
    "hybrid": "Installed/Break-up Capacity (MW) Hybrid",
    "hydro":  "Installed/Break-up Capacity (MW) Hydro",
}

# Effectiveness record key → normalised category
_EFF_KEY_TO_CAT = {
    "solar_mw": "solar",
    "wind_mw":  "wind",
    "ess_mw":   "ess",
    "hydro_mw": "hydro",
}


# ── Public API ────────────────────────────────────────────────────────────────

def compute_installed_capacity(
    df: pd.DataFrame,
    effectiveness_lookup: dict[str, dict],
) -> tuple[pd.DataFrame, dict]:
    """Compute Installed/Break-up Capacity (MW) sub-columns for each CMETS row.

    For each CMETS row matched to an effectiveness record:
    1. Parse the CMETS ``Type`` column for per-technology MW values.
    2. Read the effectiveness per-technology columns (solar_mw, wind_mw, etc.).
    3. Sum matching categories.
    4. Store the results in the output sub-columns.

    Parameters
    ----------
    df : pd.DataFrame
        The CMETS DataFrame (possibly already enriched by Module 3 merge).
    effectiveness_lookup : dict
        ``application_id → record`` dictionary.

    Returns
    -------
    (updated_df, stats)
        stats: total_rows, matched, computed, skipped
    """
    # Ensure output columns exist
    for col in CAPACITY_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Find ID columns in CMETS
    col_gna = find_col(df, "GNA/ST II Application ID")
    col_lta = find_col(df, "LTA Application ID")
    col_52  = find_col(df, "Application ID under Enhancement 5.2 or revision")
    col_type = find_col(df, "Type")

    matched  = 0
    computed = 0
    skipped  = 0

    for idx, row in df.iterrows():
        # ── Find effectiveness record using ID cascade ────────────────────
        eff_rec = None

        # Try GNA first
        if col_gna:
            gna_ids = ids_from_cell(row.get(col_gna))
            eff_rec = lookup_first(gna_ids, effectiveness_lookup)

        # Fallback to LTA
        if eff_rec is None and col_lta:
            lta_ids = ids_from_cell(row.get(col_lta))
            eff_rec = lookup_first(lta_ids, effectiveness_lookup)

        # Fallback to 5.2 GNA
        if eff_rec is None and col_52:
            enh_ids = ids_from_cell(row.get(col_52))
            eff_rec = lookup_first(enh_ids, effectiveness_lookup)

        if eff_rec is None:
            skipped += 1
            continue

        matched += 1

        # ── Parse CMETS Type column values ────────────────────────────────
        cmets_type_str = safe_str(row.get(col_type)) if col_type else ""
        cmets_type_values = parse_cmets_type(cmets_type_str)

        # ── Read effectiveness per-technology MW values ───────────────────
        eff_type_of_project = safe_str(eff_rec.get("type_of_project"))
        eff_cats = classify_project_type(eff_type_of_project)

        # Build effectiveness MW dict: category → value
        eff_mw: dict[str, float] = {}
        for eff_key, cat in _EFF_KEY_TO_CAT.items():
            val = safe_float(eff_rec.get(eff_key))
            if val > 0:
                eff_mw[cat] = val

        # ── Compute sums for each output category ────────────────────────
        # For each output category (solar, wind, hybrid, hydro):
        #   sum = effectiveness_value (from type_of_project columns)
        #       + cmets_type_value (from Type column parsed values)
        # The CMETS type value is only added if it matches the effectiveness
        # type_of_project category.

        row_has_value = False

        for cat, output_col in _CAT_TO_OUTPUT_COL.items():
            eff_val   = eff_mw.get(cat, 0.0)
            cmets_val = cmets_type_values.get(cat, 0.0)

            # ESS values contribute to the matching effectiveness type
            # e.g., if type_of_project is "wind" and CMETS has "BESS (44)",
            # the ESS doesn't go into wind; only the wind values sum.
            # But if the effectiveness itself has ESS, it stays separate.

            total = eff_val + cmets_val

            if cat == "hybrid":
                # Hybrid = sum of ALL effectiveness MW columns
                # + sum of ALL cmets type values
                if eff_cats and ("hybrid" in eff_cats or len(eff_cats) > 1):
                    hybrid_eff_total = sum(eff_mw.values())
                    hybrid_cmets_total = sum(cmets_type_values.values())
                    total = hybrid_eff_total + hybrid_cmets_total
                elif cmets_type_values.get("hybrid", None) is not None:
                    # CMETS explicitly says hybrid
                    total = sum(eff_mw.values()) + cmets_type_values.get("hybrid", 0.0)
                else:
                    total = 0.0

            if total > 0:
                df.at[idx, output_col] = total
                row_has_value = True

        if row_has_value:
            computed += 1

    stats = {
        "total_rows": len(df),
        "matched":    matched,
        "computed":   computed,
        "skipped":    skipped,
    }

    logger.info(
        "[CapacityCalc] Rows: %d | Matched: %d | Computed: %d | Skipped: %d",
        len(df), matched, computed, skipped,
    )

    return df, stats
