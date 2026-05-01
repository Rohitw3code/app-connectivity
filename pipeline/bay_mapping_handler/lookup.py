"""
bay_mapping_handler/lookup.py — Bay Allocation Lookup Index Builder
=====================================================================
Builds a lookup structure from bay allocation extracted data (JSON cache
or re-extracted from PDFs) that enables fast matching by voltage level
and developer/entity name.

Index structure
---------------
{
    "220kv": [
        {
            "entity_name":            str,   # raw entity text from bay allocation
            "bay_no":                 str,   # bay number
            "name_of_substation":     str,
            "substation_coordinates": str,
            "region":                 str,
            "sl_no":                  str,
        },
        ...
    ],
    "400kv": [ ... ]
}

Usage:
    from pipeline.bay_mapping_handler.lookup import build_bay_lookup
    index = build_bay_lookup(bay_output_dir)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Index entry factory
# ---------------------------------------------------------------------------

def _entry(entity_name: str, bay_no: str, sub: dict) -> dict:
    """Create a single lookup entry from a bay allocation substation record."""
    return {
        "entity_name":            entity_name,
        "bay_no":                 bay_no,
        "name_of_substation":     sub.get("name_of_substation", ""),
        "substation_coordinates": sub.get("substation_coordinates", ""),
        "region":                 sub.get("region", ""),
        "sl_no":                  sub.get("sl_no", ""),
    }


# ---------------------------------------------------------------------------
# Public: build the lookup index
# ---------------------------------------------------------------------------

def build_bay_lookup(bay_output_dir: Path) -> dict[str, list[dict]]:
    """Scan all bay-allocation JSON cache files and build the lookup index.

    Parameters
    ----------
    bay_output_dir : Path
        Directory containing per-PDF JSON cache files produced by
        Module 5 (bayallocation_handler).

    Returns
    -------
    dict with keys ``"220kv"`` and ``"400kv"``, each mapping to a list
    of lookup entry dicts.
    """
    index: dict[str, list[dict]] = {
        "220kv": [],
        "400kv": [],
    }

    json_files = sorted(bay_output_dir.glob("*.json"))
    if not json_files:
        logger.warning("[BayMapping] No JSON cache files found in %s", bay_output_dir)
        return index

    for jf in json_files:
        try:
            with open(jf, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:
            logger.warning("[BayMapping] Could not read %s: %s", jf.name, exc)
            continue

        for page in data.get("pages", []):
            for sub in page.get("substations", []):
                # Process 220kV bays
                bay_dict_220 = sub.get("220kv", {}).get("bay_no", {})
                for bay_no, entity_name in bay_dict_220.items():
                    if entity_name:  # skip empty entities
                        index["220kv"].append(_entry(entity_name, bay_no, sub))

                # Process 400kV bays
                bay_dict_400 = sub.get("400kv", {}).get("bay_no", {})
                for bay_no, entity_name in bay_dict_400.items():
                    if entity_name:  # skip empty entities
                        index["400kv"].append(_entry(entity_name, bay_no, sub))

    logger.info(
        "[BayMapping] Lookup built: 220kV=%d entries, 400kV=%d entries",
        len(index["220kv"]), len(index["400kv"]),
    )
    return index
