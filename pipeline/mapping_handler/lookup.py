"""
mapping_handler/lookup.py — Effectiveness lookup builder
==========================================================
Builds an ``application_id → record`` dictionary from:
  1. An in-memory DataFrame (from Module 2's current run)
  2. On-disk JSON cache files (from previous runs)

Disk data takes precedence over in-memory (authoritative cache).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


def build_lookup(effectiveness_df: pd.DataFrame, eff_output_dir: Path) -> dict[str, dict]:
    """Build application_id → record dict.

    Parameters
    ----------
    effectiveness_df  : in-memory DataFrame from Module 2
    eff_output_dir    : folder with effectiveness JSON cache files

    Returns
    -------
    dict mapping application_id string → record dict
    """
    lookup: dict[str, dict] = {}

    # Seed from in-memory DataFrame (current run)
    if not effectiveness_df.empty:
        for _, row in effectiveness_df.iterrows():
            app_id = str(row.get("application_id", "") or "").strip()
            if app_id:
                lookup[app_id] = row.to_dict()

    # Supplement / override with all on-disk JSONs
    for jf in sorted(eff_output_dir.glob("*.json")):
        try:
            with open(jf, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            for item in data:
                app_id = str(item.get("application_id", "") or "").strip()
                if app_id:
                    lookup[app_id] = item
        except Exception as exc:
            logger.warning("[Mapping] Could not read %s: %s", jf.name, exc)

    return lookup
