"""
bay_mapping_handler/merge.py — CMETS × Bay Allocation Merge Logic
===================================================================
For each row in the CMETS DataFrame, looks up the developer name in the
bay allocation index under the matching voltage level (220kV / 400kV).

Matching strategy
-----------------
1. Normalise the CMETS ``Voltage`` field to ``"220kv"`` or ``"400kv"``.
2. Normalise the CMETS ``Name of the developers`` to a lowercase search token.
3. Scan all bay allocation entries under that voltage for a **substring match**
   (the CMETS developer name appears inside the bay allocation entity text,
   OR the bay allocation entity name appears inside the CMETS developer name).
4. If a match is found → enrich the row with:
     • ``Bay No (Bay Allocation)``
     • ``Substation Name (Bay Allocation)``
     • ``Substation Coordinates (Bay Allocation)``
5. If multiple bays match, all are concatenated with `` | ``.

Edit this file to change matching logic, enrichment columns, or
the fuzzy-matching threshold.
"""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd


# ---------------------------------------------------------------------------
# Normalisation helpers
# ---------------------------------------------------------------------------

def _normalise_voltage(voltage_str: Optional[str]) -> Optional[str]:
    """Convert a free-text voltage like '220 kV', '220kV', '400kv' to
    the canonical key ``'220kv'`` or ``'400kv'``.  Returns None if
    the voltage is unrecognised.
    """
    if not voltage_str:
        return None
    v = re.sub(r"[\s\-_]+", "", str(voltage_str).strip().lower())
    if "220" in v:
        return "220kv"
    if "400" in v:
        return "400kv"
    return None


def _normalise_name(name: Optional[str]) -> str:
    """Lowercase, collapse whitespace, strip punctuation for matching."""
    if not name:
        return ""
    s = str(name).strip().lower()
    # Remove common suffixes that may differ between sources
    s = re.sub(r"\s*(pvt\.?\s*ltd\.?|ltd\.?|llp|private\s+limited|limited)\s*$", "", s)
    s = re.sub(r"[.\-,;:()]+", " ", s)
    s = " ".join(s.split())
    return s.strip()


def _names_match(cmets_name: str, bay_entity: str) -> bool:
    """Return True if the two names are a plausible match.

    Uses bidirectional substring containment after normalisation.
    """
    cn = _normalise_name(cmets_name)
    bn = _normalise_name(bay_entity)
    if not cn or not bn:
        return False

    # Extract individual entity names from the bay entity text
    # (e.g. "Powerica Ltd. (50.6); IndianOil NTPC (147)" → two entities)
    # Split on semicolons to get individual entities in the bay cell
    bay_entities = [x.strip() for x in re.split(r"[;]", bay_entity) if x.strip()]

    for single_entity in bay_entities:
        sn = _normalise_name(single_entity)
        if not sn:
            continue
        # Exact match after normalisation
        if cn == sn:
            return True
        # Substring containment
        if cn in sn or sn in cn:
            return True
        # Try matching the core entity name (before parenthetical capacity info)
        core_bay = re.sub(r"\(.*?\)", "", single_entity).strip()
        core_bay_n = _normalise_name(core_bay)
        core_cmets = re.sub(r"\(.*?\)", "", cmets_name).strip()
        core_cmets_n = _normalise_name(core_cmets)
        if core_cmets_n and core_bay_n:
            if core_cmets_n in core_bay_n or core_bay_n in core_cmets_n:
                return True

    return False


# ---------------------------------------------------------------------------
# Enrichment column names
# ---------------------------------------------------------------------------

BAY_MAPPING_COLUMNS = [
    "Bay No (Bay Allocation)",
    "Substation Name (Bay Allocation)",
    "Substation Coordinates (Bay Allocation)",
]


# ---------------------------------------------------------------------------
# Main merge function
# ---------------------------------------------------------------------------

def merge_bay_allocation(
    df: pd.DataFrame,
    bay_index: dict[str, list[dict]],
) -> tuple[pd.DataFrame, dict]:
    """Enrich a CMETS DataFrame with bay allocation data.

    Parameters
    ----------
    df : pd.DataFrame
        CMETS data with at least ``Voltage`` and
        ``Name of the developers`` columns.
    bay_index : dict
        Output of ``build_bay_lookup()``:
        ``{"220kv": [...], "400kv": [...]}``

    Returns
    -------
    (enriched_df, match_stats)
    """
    # Ensure enrichment columns exist
    for col in BAY_MAPPING_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Locate the relevant source columns
    voltage_col = None
    dev_col     = None
    for c in df.columns:
        cl = c.lower()
        if "voltage" in cl and voltage_col is None:
            voltage_col = c
        if "name of the developers" in cl and dev_col is None:
            dev_col = c
        if dev_col is None and "name of developers" in cl:
            dev_col = c

    if voltage_col is None:
        print("[BayMapping] WARNING: No 'Voltage' column found in CMETS data.")
    if dev_col is None:
        print("[BayMapping] WARNING: No 'Name of the developers' column found in CMETS data.")

    matched = 0
    multi_match = 0
    no_voltage = 0
    no_developer = 0
    unmatched = 0

    for idx, row in df.iterrows():
        raw_voltage = str(row.get(voltage_col, "")) if voltage_col else ""
        raw_dev     = str(row.get(dev_col, ""))     if dev_col     else ""

        voltage_key = _normalise_voltage(raw_voltage)
        if not voltage_key:
            no_voltage += 1
            continue

        if not raw_dev or raw_dev.lower() in ("nan", "none", ""):
            no_developer += 1
            continue

        entries = bay_index.get(voltage_key, [])
        hits: list[dict] = []
        for entry in entries:
            if _names_match(raw_dev, entry["entity_name"]):
                hits.append(entry)

        if not hits:
            unmatched += 1
            continue

        # Deduplicate by bay_no (same bay can appear if entity repeats)
        seen_bays: set[str] = set()
        unique_hits: list[dict] = []
        for h in hits:
            if h["bay_no"] not in seen_bays:
                seen_bays.add(h["bay_no"])
                unique_hits.append(h)

        if len(unique_hits) > 1:
            multi_match += 1

        matched += 1

        # Populate enrichment columns
        bay_nos    = " | ".join(h["bay_no"] for h in unique_hits)
        sub_names  = " | ".join(
            h["name_of_substation"]
            for h in unique_hits if h["name_of_substation"]
        )
        sub_coords = " | ".join(
            h["substation_coordinates"]
            for h in unique_hits if h["substation_coordinates"]
        )

        # Deduplicate substation values (multiple bays often share the same substation)
        sub_names  = " | ".join(dict.fromkeys(sub_names.split(" | ")))
        sub_coords = " | ".join(dict.fromkeys(sub_coords.split(" | ")))

        df.at[idx, "Bay No (Bay Allocation)"]                  = bay_nos
        df.at[idx, "Substation Name (Bay Allocation)"]         = sub_names
        df.at[idx, "Substation Coordinates (Bay Allocation)"]  = sub_coords

    stats = {
        "total_rows":   len(df),
        "matched":      matched,
        "multi_match":  multi_match,
        "no_voltage":   no_voltage,
        "no_developer": no_developer,
        "unmatched":    unmatched,
    }
    return df, stats
