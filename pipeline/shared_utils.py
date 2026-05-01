"""
pipeline/shared_utils.py
========================
Centralized utility functions shared across multiple handlers
(CMETS, Effectiveness, Mapping) to reduce redundant logic.
"""

from __future__ import annotations

import json
import re
from typing import Optional
import pandas as pd

def parse_json(text: str) -> dict | list:
    """Safely parse JSON from a raw LLM response string."""
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}|\[.*\]", raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
    return {}

def safe_str(val) -> str:
    """Convert a value to a clean string, handling None/NaN."""
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    return str(val).strip()

def safe_float(val) -> float:
    """Convert a value to float, returning 0.0 on failure."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return 0.0 if pd.isna(val) else float(val)
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0

def find_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """Return the first matching column name (case-insensitive) from a DataFrame."""
    for c in candidates:
        for col in df.columns:
            if c.lower() == col.lower():
                return col
    return None

def ids_from_cell(cell_val) -> list[str]:
    """Split a cell value into individual application IDs."""
    raw = safe_str(cell_val)
    if not raw:
        return []
    return [x.strip() for x in re.split(r"[,;\s]+", raw) if x.strip()]

def lookup_first(ids: list[str], lookup: dict) -> Optional[dict]:
    """Return the first matching record from *lookup* for any ID in *ids*."""
    for id_ in ids:
        if id_ in lookup:
            return lookup[id_]
    return None

def classify_project_type(type_str: Optional[str]) -> set[str]:
    """Classify a project type string into normalized categories (solar, wind, ess, hydro, hybrid)."""
    text = safe_str(type_str).lower()
    if not text:
        return set()

    cats: set[str] = set()
    if "solar"  in text: cats.add("solar")
    if "wind"   in text: cats.add("wind")
    if "ess"    in text or "energy storage" in text or "bess" in text: cats.add("ess")
    if "hydro"  in text or "pump" in text or "psp" in text: cats.add("hydro")
    if "hybrid" in text: cats.add("hybrid")

    return cats
