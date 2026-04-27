"""
cmets_handler/normalization.py — Row validation & normalisation
================================================================
Post-LLM cleaning: value coercion, state extraction, date parsing,
deduplication, and Pydantic model construction.

Edit this file to change how extracted raw values are cleaned /
normalised before being written to JSON and Excel.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Optional

from pipeline.cmets_handler.models import MappedRow

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


# ── Atomic helpers ────────────────────────────────────────────────────────────

def clean(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    v = str(v).strip()
    return None if v.lower() in {"null", "none", "na", "n/a", "-", "--"} else (v or None)


def extract_state(loc: Optional[str]) -> Optional[str]:
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


def norm_num_ids(v: Optional[str], strip_zeros: bool = False) -> Optional[str]:
    v = clean(v)
    if not v:
        return None
    ids = re.findall(r"\b\d{6,}\b", v)
    if not ids:
        return v
    return ", ".join(i.lstrip("0") or "0" for i in ids) if strip_zeros else ", ".join(ids)


def extract_ids(v: Optional[str]) -> list[str]:
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


def derive_enhancement_id(enh, gna, lta, mode) -> Optional[str]:
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


def extract_date(v: Optional[str]) -> Optional[str]:
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


def gna_yes_no(date_str: Optional[str]) -> Optional[str]:
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


def norm_status(v: Optional[str]) -> Optional[str]:
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


def psp_cols(*vals: Optional[str]) -> tuple[Optional[str], Optional[str], Optional[str]]:
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


def norm_dev(v: Optional[str]) -> Optional[str]:
    v = clean(v)
    if not v:
        return None
    return None if any(t in v.upper() for t in (" LOA", "CRITERION", "APPLYING")) else v


# Valid energy source types (normalised lowercase -> display form)
_VALID_TYPES: dict[str, str] = {
    "solar":          "Solar",
    "wind":           "Wind",
    "hybrid":         "Hybrid",
    "bess":           "BESS",
    "solar + bess":   "Solar + BESS",
    "solar+bess":     "Solar + BESS",
    "hybrid + bess":  "Hybrid + BESS",
    "hybrid+bess":    "Hybrid + BESS",
    "hydro":          "Hydro",
    "hydro + bess":   "Hydro+BESS",
    "hydro+bess":     "Hydro+BESS",
    "thermal":        "Thermal",
}


def norm_type(v: Optional[str]) -> Optional[str]:
    """Normalise the energy source type to a canonical value."""
    v = clean(v)
    if not v:
        return None
    key = re.sub(r"\s+", " ", v.strip().lower())
    if key in _VALID_TYPES:
        return _VALID_TYPES[key]
    # Fuzzy match: check if any valid type is a substring
    for k, canonical in _VALID_TYPES.items():
        if k in key:
            return canonical
    return v  # return as-is if not recognised


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


def validate_rows(raw_rows: list[dict]) -> list[MappedRow]:
    """Filter raw dicts → valid MappedRow objects.

    A row must have at least ONE primary key ID (GNA/ST-II, LTA,
    or Enhancement 5.2) and must NOT have a blocklisted Nature of
    Applicant value.
    """
    out = []
    for row in raw_rows:
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
    """Apply full normalisation to a list of validated rows."""
    out = []
    for row in rows:
        p = row.model_dump(by_alias=True)
        raw_gna  = p.get("GNA/ST II Application ID")
        raw_lta  = p.get("LTA Application ID")
        raw_mode = p.get("Mode(Criteria for applying)")
        raw_enh  = p.get("Application ID under Enhancement 5.2 or revision")
        raw_opd  = p.get("GNA Operationalization Date")
        raw_stat = p.get("Status of application(Withdrawn / granted. Revoked.)")
        raw_mwh  = p.get("PSP MWh")
        raw_inj  = p.get("PSP Injection (MW)")
        raw_drw  = p.get("PSP Drawl (MW)")

        p["Project Location"]               = clean(p.get("Project Location"))
        p["State"]                           = extract_state(p.get("Project Location"))
        p["substaion"]                       = clean(p.get("substaion"))
        p["Name of the developers"]          = norm_dev(p.get("Name of the developers"))
        p["type"]                            = norm_type(p.get("type"))
        p["GNA/ST II Application ID"]        = norm_num_ids(raw_gna, strip_zeros=False)

        # Primary key check: need at least one ID after normalisation
        has_gna = bool(clean(p["GNA/ST II Application ID"]))
        p["LTA Application ID"]              = norm_num_ids(raw_lta, strip_zeros=True)
        has_lta = bool(clean(p["LTA Application ID"]))
        p["Application ID under Enhancement 5.2 or revision"] = derive_enhancement_id(raw_enh, raw_gna, raw_lta, raw_mode)
        has_enh = bool(clean(p["Application ID under Enhancement 5.2 or revision"]))

        if not (has_gna or has_lta or has_enh):
            continue
        p["Application Quantum (MW)(ST II)"] = clean(p.get("Application Quantum (MW)(ST II)"))
        p["Nature of Applicant"]             = clean(p.get("Nature of Applicant"))
        p["Mode(Criteria for applying)"]     = clean(p.get("Mode(Criteria for applying)"))
        p["Applied Start of Connectivity sought by developer date"] = extract_date(
            p.get("Applied Start of Connectivity sought by developer date"))
        p["Application/Submission Date"]     = extract_date(p.get("Application/Submission Date"))
        p["GNA Operationalization Date"]     = extract_date(raw_opd)
        p["GNA Operationalization (Yes/No)"] = gna_yes_no(p["GNA Operationalization Date"])
        p["Status of application(Withdrawn / granted. Revoked.)"] = norm_status(raw_stat)

        mwh, inj, drw = psp_cols(raw_mwh, raw_inj, raw_drw, raw_mode)
        p["PSP MWh"]            = clean(mwh or raw_mwh)
        p["PSP Injection (MW)"] = clean(inj or raw_inj)
        p["PSP Drawl (MW)"]     = clean(drw or raw_drw)

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
        dev = str(row.get("Name of the developers") or "").strip().lower()
        key = (gna, lta, loc, dev) if any([gna, lta, loc, dev]) else json.dumps(row, sort_keys=True)
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique
