"""
cmets_handler/normalization.py — Row validation & normalisation
================================================================
Post-LLM cleaning: value coercion, state extraction, date parsing,
deduplication, and Pydantic model construction.

Each column's normalisation function is referenced by name in
column_registry.py.  The mapping from name → callable is in
NORM_FUNCTIONS at the bottom of this file.

Edit this file to change how extracted raw values are cleaned /
normalised before being written to JSON and Excel.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Optional

from pipeline.cmets_handler.models import MappedRow
from pipeline.cmets_handler.column_registry import (
    COLUMN_DEFS,
    get_llm_key_to_column_map,
)
from pipeline.cmets_handler.battery_extractor import extract_battery_values

# ── Indian states / UTs ──────────────────────────────────────────────────────
INDIA_STATES_UTS = [
    "andhra pradesh", "arunachal pradesh", "assam", "bihar", "chhattisgarh",
    "goa", "gujarat", "haryana", "himachal pradesh", "jharkhand", "karnataka",
    "kerala", "madhya pradesh", "maharashtra", "manipur", "meghalaya", "mizoram",
    "nagaland", "odisha", "punjab", "rajasthan", "sikkim", "tamil nadu", "telangana",
    "tripura", "uttar pradesh", "uttarakhand", "west bengal",
    "andaman and nicobar islands", "chandigarh",
    "dadra and nagar haveli and daman and diu", "delhi",
    "jammu and kashmir", "ladakh", "lakshadweep", "puducherry",
]


# ═══════════════════════════════════════════════════════════════════════════════
# NORM FUNCTIONS — each column references one of these by name
# ═══════════════════════════════════════════════════════════════════════════════

# ── clean ────────────────────────────────────────────────────────────────────
def clean(v: Optional[str]) -> Optional[str]:
    """Basic cleaning: strip whitespace, convert null-like strings to None."""
    if v is None:
        return None
    v = str(v).strip()
    return None if v.lower() in {"null", "none", "na", "n/a", "-", "--"} else (v or None)


# ── extract_state ────────────────────────────────────────────────────────────
def extract_state(loc: Optional[str]) -> Optional[str]:
    """Derive Indian state/UT name from Project Location text."""
    loc = clean(loc)
    if not loc:
        return None
    lower = loc.lower()
    for state in sorted(INDIA_STATES_UTS, key=len, reverse=True):
        if state in lower:
            return state
    if "," in loc:
        tail = loc.split(",")[-1].strip(" .")
        return tail.lower() or None
    return None


# ── norm_num_ids ─────────────────────────────────────────────────────────────
def norm_num_ids(v: Optional[str], strip_zeros: bool = False) -> Optional[str]:
    """Normalise numeric application IDs (6+ digit numbers)."""
    v = clean(v)
    if not v:
        return None
    ids = re.findall(r"\b\d{6,}\b", v)
    if not ids:
        return v
    return ", ".join(i.lstrip("0") or "0" for i in ids) if strip_zeros else ", ".join(ids)


def norm_num_ids_strip(v: Optional[str]) -> Optional[str]:
    """Normalise numeric IDs with leading-zero stripping (for LTA IDs)."""
    return norm_num_ids(v, strip_zeros=True)


# ── extract_ids ──────────────────────────────────────────────────────────────
def extract_ids(v: Optional[str]) -> list[str]:
    """Extract all 6+ digit IDs from a string."""
    v = clean(v)
    return re.findall(r"\b\d{6,}\b", v) if v else []


def _is_lta(v: str) -> bool:
    return str(v).startswith("04")


def _has_ctx(pattern: str, *vals: Optional[str]) -> bool:
    text = " ".join(str(x or "") for x in vals)
    return bool(re.search(pattern, text, re.IGNORECASE))


def _pick_gna(ids: list[str], prefer_st2: bool) -> Optional[str]:
    if not ids:
        return None
    if prefer_st2:
        return next((i for i in ids if not _is_lta(i)), None)
    non_lta = [i for i in ids if not _is_lta(i)]
    return non_lta[0] if non_lta else ids[0]


# ── derive_enhancement_id ────────────────────────────────────────────────────
def derive_enhancement_id(enh, gna, lta, mode) -> Optional[str]:
    """Derive Enhancement 5.2 application ID from context fields."""
    if not _has_ctx(r"\b(5\.?2|regulation\s*5\.?2|enhancement|revision)\b", enh, gna, lta, mode):
        return None
    st2 = _has_ctx(r"\b(stage\s*ii|st\s*ii|gna/st\s*ii)\b", enh, gna, lta, mode)
    for ids in (extract_ids(enh), extract_ids(gna)):
        c = _pick_gna(ids, st2)
        if c:
            return c
    lta_ids = extract_ids(lta)
    if len(lta_ids) == 1:
        return None if _is_lta(lta_ids[0]) else lta_ids[0]
    return _pick_gna(lta_ids, st2)


# ── extract_date ─────────────────────────────────────────────────────────────
def extract_date(v: Optional[str]) -> Optional[str]:
    """Extract and normalise a date string from text."""
    v = clean(v)
    if not v:
        return None
    for pat in (r"\b\d{2}[./-]\d{2}[./-]\d{4}\b",
                r"\b\d{4}[./-]\d{2}[./-]\d{2}\b",
                r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b"):
        m = re.search(pat, v)
        if m:
            return m.group(0)
    return None


# ── gna_yes_no ───────────────────────────────────────────────────────────────
def gna_yes_no(date_str: Optional[str]) -> Optional[str]:
    """Determine Yes/No based on whether GNA date is in the future."""
    d = extract_date(date_str)
    if not d:
        return None
    norm = d.replace("/", ".").replace("-", ".")
    for fmt in ("%d.%m.%Y", "%Y.%m.%d"):
        try:
            dt = datetime.strptime(norm, fmt)
            return "Yes" if dt.date() > datetime.now().date() else "No"
        except ValueError:
            pass
    return None


# ── norm_status ──────────────────────────────────────────────────────────────
def norm_status(v: Optional[str]) -> Optional[str]:
    """Normalise application status to canonical values."""
    v = clean(v)
    if not v:
        return None
    lower = v.lower()
    if "withdraw" in lower:
        return "Withdrawn"
    if "revoke" in lower:
        return "Revoked"
    if "grant" in lower:
        return "granted"
    return v


# ── PSP columns ─────────────────────────────────────────────────────────────
def psp_cols(*vals: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract PSP MWh/Injection/Drawl from combined text (only when PSP context present)."""
    text = " ".join(str(v or "") for v in vals)
    if not re.search(r"\b(pump\s*storage|psp)\b", text, re.IGNORECASE):
        return None, None, None
    mwh_m   = re.search(r"for\s+(\d+(?:\.\d+)?)\s*(?:MWh|MW)", text, re.IGNORECASE) or \
              re.search(r"(\d+(?:\.\d+)?)\s*MWh", text, re.IGNORECASE)
    inj_m   = re.search(r"(?:max\s*)?injection\s*[:\-]?\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
    drawl_m = re.search(r"(?:max\s*)?(?:drawl|drawal)\s*[:\-]?\s*(\d+(?:\.\d+)?)", text, re.IGNORECASE)
    return (mwh_m.group(1) if mwh_m else None,
            inj_m.group(1) if inj_m else None,
            drawl_m.group(1) if drawl_m else None)


def norm_psp_mwh(v: Optional[str]) -> Optional[str]:
    """Normalise PSP MWh value."""
    return clean(v)


def norm_psp_injection(v: Optional[str]) -> Optional[str]:
    """Normalise PSP Injection (MW) value."""
    return clean(v)


def norm_psp_drawl(v: Optional[str]) -> Optional[str]:
    """Normalise PSP Drawl (MW) value."""
    return clean(v)


# ── Battery (BESS) columns ──────────────────────────────────────────────────

def norm_battery_mwh(v: Optional[str]) -> Optional[str]:
    """Normalise Battery MWh value."""
    return clean(v)


def norm_battery_injection(v: Optional[str]) -> Optional[str]:
    """Normalise Battery Injection (MW) value."""
    return clean(v)


def norm_battery_drawl(v: Optional[str]) -> Optional[str]:
    """Normalise Battery Drawl (MW) value."""
    return clean(v)


# ── norm_dev ─────────────────────────────────────────────────────────────────
def norm_dev(v: Optional[str]) -> Optional[str]:
    """Clean developer name: remove LOA/CRITERION artefacts."""
    v = clean(v)
    if not v:
        return None
    return None if any(t in v.upper() for t in (" LOA", "CRITERION", "APPLYING")) else v


# ── norm_type ────────────────────────────────────────────────────────────────
# Strict keyword set for the type column.
# Only these keywords are allowed: Solar, BESS, Wind, Solar+Wind, Solar+BESS
_KEYWORD_CANON: dict[str, str] = {
    "solar":  "Solar",
    "wind":   "Wind",
    "bess":   "BESS",
    "hybrid": "Solar+Wind",   # map hybrid → Solar+Wind
}

# Regex: captures a keyword optionally followed by a parenthetical value
# e.g. "Solar (300)" → ("Solar", "(300)")
#      "BESS"        → ("BESS", "")
_TYPE_TOKEN_RE = re.compile(
    r"(solar|wind|bess|hybrid)"       # keyword
    r"(?:\s*\(([^)]*)\))?",           # optional (value)
    re.IGNORECASE,
)


def norm_type(v: Optional[str]) -> Optional[str]:
    """Normalise the type column to strict keywords (Solar, BESS, Wind,
    Solar+Wind, Solar+BESS) while preserving associated MW values.

    Examples:
        "solar (300)"              → "Solar (300)"
        "Wind (12) + BESS (19)"    → "Wind (12) + BESS (19)"
        "Hybrid (500)"             → "Solar+Wind (500)"
        "Hybrid + BESS"            → "Solar+Wind + BESS"
        "Generator (Solar)"        → "Solar"
        "Thermal"                  → None  (not in allowed set)
    """
    v = clean(v)
    if not v:
        return None

    tokens = _TYPE_TOKEN_RE.finditer(v)
    parts: list[str] = []
    for m in tokens:
        raw_kw = m.group(1).lower()
        canon = _KEYWORD_CANON.get(raw_kw)
        if canon is None:
            continue  # skip unrecognised keywords
        val = (m.group(2) or "").strip()
        parts.append(f"{canon} ({val})" if val else canon)

    if not parts:
        return None

    return " + ".join(parts)


# ── norm_voltage ─────────────────────────────────────────────────────────────
def norm_voltage(v: Optional[str]) -> Optional[str]:
    """Normalise voltage string to '<N> kV' format."""
    v = clean(v)
    if not v:
        return None
    m = re.search(r"(\d{2,3})\s*kV", v, re.IGNORECASE)
    if m:
        return f"{m.group(1)} kV"
    return v


# ═══════════════════════════════════════════════════════════════════════════════
# NORM FUNCTION REGISTRY — maps function name (str) → callable
# ═══════════════════════════════════════════════════════════════════════════════
# Each column in column_registry.py references a norm_func by NAME.
# This dict resolves those names to actual functions.

NORM_FUNCTIONS: dict[str, callable] = {
    "clean":                 clean,
    "extract_state":         extract_state,
    "norm_num_ids":          norm_num_ids,
    "norm_num_ids_strip":    norm_num_ids_strip,
    "derive_enhancement_id": derive_enhancement_id,
    "extract_date":          extract_date,
    "gna_yes_no":            gna_yes_no,
    "norm_status":           norm_status,
    "norm_dev":              norm_dev,
    "norm_type":             norm_type,
    "norm_voltage":          norm_voltage,
    "norm_psp_mwh":          norm_psp_mwh,
    "norm_psp_injection":    norm_psp_injection,
    "norm_psp_drawl":        norm_psp_drawl,
    "norm_battery_mwh":      norm_battery_mwh,
    "norm_battery_injection": norm_battery_injection,
    "norm_battery_drawl":    norm_battery_drawl,
}


# ── Row-level blocklist values for Nature of Applicant ────────────────────────
# Rows with these values are skipped entirely (not connectivity generators).
_NATURE_BLOCKLIST = [
    "bulk consumer",
    "drawee entity",
    "drawee entity connected",
]


def _has_any_primary_key(row: dict) -> bool:
    """Return True if the row has at least one primary ID field.

    Primary keys: GNA/ST II Application ID, LTA Application ID,
    or Application ID under Enhancement 5.2 or revision.
    """
    return bool(
        clean(row.get("GNA/ST II Application ID"))
        or clean(row.get("LTA Application ID"))
        or clean(row.get("Application ID under Enhancement 5.2 or revision"))
    )


def _is_nature_blocklisted(row: dict) -> bool:
    """Return True if the Nature of Applicant value is in the blocklist."""
    nature = clean(row.get("Nature of Applicant"))
    if not nature:
        return False
    lower = nature.lower()
    return any(blocked in lower for blocked in _NATURE_BLOCKLIST)


def remap_llm_keys(row: dict) -> dict:
    """Remap LLM response keys to final column names using the registry.

    The LLM returns keys like 'substaion', 'type', 'Voltage' etc.
    This function maps them to canonical column names like 'Substation',
    'Type', 'Voltage level'.
    """
    key_map = get_llm_key_to_column_map()
    remapped = {}
    for k, v in row.items():
        final_col = key_map.get(k, k)  # map to final name, or keep as-is
        remapped[final_col] = v
    return remapped


def validate_rows(raw_rows: list[dict]) -> list[MappedRow]:
    """Filter raw dicts → valid MappedRow objects.

    A row must have at least ONE primary key ID (GNA/ST-II, LTA,
    or Enhancement 5.2) and must NOT have a blocklisted Nature of
    Applicant value.
    """
    out = []
    for row in raw_rows:
        # Remap LLM keys → final column names
        row = remap_llm_keys(row)

        # Primary key check: need at least one ID
        if not _has_any_primary_key(row):
            continue
        # Nature of Applicant blocklist
        if _is_nature_blocklisted(row):
            print(f"      [SKIP] Blocklisted Nature of Applicant: {row.get('Nature of Applicant')}")
            continue
        try:
            out.append(MappedRow.model_validate(row))
        except Exception as e:
            print(f"      [Pydantic skip] {e}")
    return out


def normalize(rows: list[MappedRow]) -> list[MappedRow]:
    """Apply full normalisation to a list of validated rows.

    Each column is normalised according to its norm_func defined in
    column_registry.py.  Special handling for multi-field derived
    columns (Enhancement ID, PSP, Battery, State, GNA Yes/No).
    """
    out = []
    for row in rows:
        p = row.model_dump(by_alias=True)

        # ── Raw values needed for multi-field derivations ─────────────────
        raw_gna  = p.get("GNA/ST II Application ID")
        raw_lta  = p.get("LTA Application ID")
        raw_mode = p.get("Mode(Criteria for applying)")
        raw_enh  = p.get("Application ID under Enhancement 5.2 or revision")
        raw_opd  = p.get("GNA Operationalization Date")
        raw_stat = p.get("Status of application(Withdrawn / granted. Revoked.)")

        # ── PSP raw values ────────────────────────────────────────────────
        raw_psp_mwh = p.get("PSP MWh")
        raw_psp_inj = p.get("PSP Injection (MW)")
        raw_psp_drw = p.get("PSP Drawl (MW)")

        # ── Battery raw values ────────────────────────────────────────────
        raw_bat_mwh = p.get("Battery MWh")
        raw_bat_inj = p.get("Battery Injection (MW)")
        raw_bat_drw = p.get("Battery Drawl (MW)")

        # ── Simple single-column normalisations ──────────────────────────
        p["Substation"]                  = clean(p.get("Substation"))
        p["Project Location"]            = clean(p.get("Project Location"))
        p["Name of Developers"]          = norm_dev(p.get("Name of Developers"))
        p["GNA/ST II Application ID"]    = norm_num_ids(raw_gna, strip_zeros=False)
        p["Application Quantum (MW)(ST II)"] = clean(p.get("Application Quantum (MW)(ST II)"))
        p["Granted Quantum GNA/LTA(MW)"] = clean(p.get("Granted Quantum GNA/LTA(MW)"))
        p["Mode(Criteria for applying)"] = clean(p.get("Mode(Criteria for applying)"))
        p["Nature of Applicant"]         = clean(p.get("Nature of Applicant"))

        # ── Primary key check after normalisation ─────────────────────────
        has_gna = bool(clean(p["GNA/ST II Application ID"]))
        p["LTA Application ID"]          = norm_num_ids(raw_lta, strip_zeros=True)
        has_lta = bool(clean(p["LTA Application ID"]))
        p["Application ID under Enhancement 5.2 or revision"] = derive_enhancement_id(
            raw_enh, raw_gna, raw_lta, raw_mode
        )
        has_enh = bool(clean(p["Application ID under Enhancement 5.2 or revision"]))

        if not (has_gna or has_lta or has_enh):
            continue

        # ── Date columns ─────────────────────────────────────────────────
        p["Application/Submission Date"] = extract_date(p.get("Application/Submission Date"))
        p["Applied Start of Connectivity sought by developer date"
          "( start date of connectivity as per the application)"] = extract_date(
            p.get("Applied Start of Connectivity sought by developer date"
                  "( start date of connectivity as per the application)")
        )
        p["Date from which additional capacity is to be added"] = extract_date(
            p.get("Date from which additional capacity is to be added")
        )
        p["GNA Operationalization Date"] = extract_date(raw_opd)

        # ── Calculated: GNA Yes/No ───────────────────────────────────────
        p["GNA Operationalization (Yes/No)"] = gna_yes_no(p["GNA Operationalization Date"])

        # ── Status ───────────────────────────────────────────────────────
        p["Status of application(Withdrawn / granted. Revoked.)"] = norm_status(raw_stat)

        # ── PSP columns (multi-field derivation) ─────────────────────────
        mwh, inj, drw = psp_cols(raw_psp_mwh, raw_psp_inj, raw_psp_drw, raw_mode)
        p["PSP MWh"]            = clean(mwh or raw_psp_mwh)
        p["PSP Injection (MW)"] = clean(inj or raw_psp_inj)
        p["PSP Drawl (MW)"]     = clean(drw or raw_psp_drw)

        # ── Battery (BESS) columns (multi-field derivation) ──────────────
        bat_mwh, bat_inj, bat_drw = extract_battery_values(
            raw_bat_mwh, raw_bat_inj, raw_bat_drw, raw_mode,
            p.get("Type", ""),
        )
        p["Battery MWh"]            = clean(bat_mwh or raw_bat_mwh)
        p["Battery Injection (MW)"] = clean(bat_inj or raw_bat_inj)
        p["Battery Drawl (MW)"]     = clean(bat_drw or raw_bat_drw)

        # ── Derived: State from Project Location ─────────────────────────
        p["State"] = extract_state(p.get("Project Location"))

        # ── Derived: Type ────────────────────────────────────────────────
        p["Type"] = norm_type(p.get("Type"))

        # ── Voltage level ────────────────────────────────────────────────
        p["Voltage level"] = norm_voltage(p.get("Voltage level"))

        out.append(MappedRow.model_validate(p))
    return out


def dedup_dicts(rows: list[dict]) -> list[dict]:
    """Remove duplicate row dicts by composite key."""
    seen: set = set()
    unique: list[dict] = []
    for row in rows:
        gna = str(row.get("GNA/ST II Application ID") or "").strip()
        lta = str(row.get("LTA Application ID") or "").strip()
        loc = str(row.get("Project Location") or "").strip().lower()
        dev = str(row.get("Name of the developers") or row.get("Name of Developers") or "").strip().lower()
        key = (gna, lta, loc, dev) if any([gna, lta, loc, dev]) else json.dumps(row, sort_keys=True)
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique
