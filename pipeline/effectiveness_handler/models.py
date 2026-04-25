"""
effectiveness_handler/models.py — Pydantic schema & helpers
=============================================================
RERecord schema for effectiveness data. Edit this file to
add / remove / rename columns.
"""

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, field_validator


class RERecord(BaseModel):
    """One extracted record from an effectiveness PDF."""

    sl_no:                   Optional[str]   = None
    application_id:          Optional[str]   = None
    name_of_applicant:       Optional[str]   = None
    region:                  Optional[str]   = None
    type_of_project:         Optional[str]   = None
    installed_capacity_mw:   Optional[float] = None
    solar_mw:                Optional[float] = None
    wind_mw:                 Optional[float] = None
    ess_mw:                  Optional[float] = None
    hydro_mw:                Optional[float] = None
    connectivity_mw:         Optional[float] = None
    present_connectivity_mw: Optional[float] = None
    substation:              Optional[str]   = None
    state:                   Optional[str]   = None
    expected_date:           Optional[str]   = None
    source_file:             Optional[str]   = None

    @field_validator(
        "installed_capacity_mw", "solar_mw", "wind_mw", "ess_mw",
        "hydro_mw", "connectivity_mw", "present_connectivity_mw", mode="before",
    )
    @classmethod
    def _coerce_num(cls, v):
        if v in (None, "", "N/A", "-", "null", "—"):
            return None
        try:
            return float(str(v).replace(",", "").strip())
        except (ValueError, TypeError):
            return None

    @field_validator(
        "sl_no", "application_id", "name_of_applicant", "region",
        "type_of_project", "substation", "state", "expected_date",
        "source_file", mode="before",
    )
    @classmethod
    def _coerce_str(cls, v):
        if v in (None, "", "N/A", "null"):
            return None
        s = str(v).replace("\n", " ").strip()
        return s or None


def safe_record(raw: dict) -> Optional[RERecord]:
    """Construct a RERecord from *raw*, returning None on failure."""
    try:
        return RERecord(**raw)
    except Exception:
        try:
            return RERecord(**{k: raw.get(k) for k in RERecord.model_fields})
        except Exception:
            return None


def dedup_records(records: list[RERecord]) -> list[RERecord]:
    """Remove duplicate records by application_id + name."""
    seen: set = set()
    out: list[RERecord] = []
    for r in records:
        key = (r.application_id or "").strip() + "||" + (r.name_of_applicant or "").strip()
        if key == "||" or key not in seen:
            seen.add(key)
            out.append(r)
    return out


# Excel column order for effectiveness_combined.xlsx
EFF_COLUMNS = [
    "source_file", "sl_no", "application_id", "name_of_applicant",
    "region", "type_of_project", "installed_capacity_mw",
    "solar_mw", "wind_mw", "ess_mw", "hydro_mw",
    "connectivity_mw", "present_connectivity_mw",
    "substation", "state", "expected_date",
]
