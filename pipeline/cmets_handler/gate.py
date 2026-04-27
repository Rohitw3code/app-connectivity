"""
cmets_handler/gate.py — Dual-Strategy Column Detection Gate (Sub-layer B)
=========================================================================
Determines whether a PDF page likely contains a CMETS data table using
two strategies:

  1. **Header-based** — regex patterns that match known column header text
  2. **Value-based**  — characteristic cell values that fingerprint a column
     even when the header wording changes across PDF editions

Edit ``TARGET_COLUMN_VARIANTS`` for header patterns and
``VALUE_FINGERPRINTS`` for value-based detection.
"""

from __future__ import annotations

import re

# ── Strategy 1: Header-based regex patterns ──────────────────────────────────
# Each key is the canonical column name used downstream.
# Each value is a list of "variant" patterns — a variant is a list of regex
# keywords that must ALL match (AND logic). The column matches if ANY variant
# matches (OR logic).

TARGET_COLUMN_VARIANTS: dict[str, list[list[str]]] = {

    "Project Location": [
        [r"\bProject\b", r"\bLocation\b"],
        [r"\bProject\s+Location\b"],
    ],

    "substaion": [
        # original
        [r"\bConnectivity\b", r"\blocation\b", r"\bApplication\b"],
        # variant: "Nearest Pooling Station (As per Application)"
        [r"\bNearest\b", r"\bPooling\b", r"\bStation\b"],
        [r"\bPooling\b", r"\bStation\b", r"\bApplication\b"],
        # variant: "Connectivity Granted at"
        [r"\bConnectivity\b", r"\bGranted\s+at\b"],
        [r"\bConnectivity\b", r"\bGranted\b"],
        # variant: "Location requested for Grant of Stage-II Connectivity"
        [r"\bLocation\b", r"\bGrant\b", r"\bStage\b", r"\bConnectivity\b"],
        [r"\bLocation\s+requested\b", r"\bGrant\b"],
        # variant: "Connectivity Injection Point"
        [r"\bConnectivity\b", r"\bInjection\s+Point\b"],
        [r"\bInjection\s+Point\b"],
        # variant: substation / sub-station
        [r"\bsub[\s-]?station\b"],
    ],

    "Name of the developers": [
        [r"\bApplicant\b"],
        [r"\bName\b", r"\bApplicant\b"],
        [r"\bName\b", r"\bDeveloper\b"],
        [r"\bDeveloper\b"],
    ],

    "type": [
        [r"\bType\b", r"\b(?:Source|Generation|Energy|Plant)\b"],
        [r"\bSource\s+Type\b"],
        [r"\bGeneration\s+Type\b"],
        [r"\bEnergy\s+Source\b"],
    ],

    "GNA/ST II Application ID": [
        [r"\bApplication\b", r"\bID\b"],
        [r"\bApplication\b", r"\bNo\b", r"\bDate\b"],
        [r"\bGNA\b", r"\bApplication\b"],
        [r"\bST[\s-]*II\b", r"\bApplication\b"],
        [r"\bST[\s-]*II\b", r"\bID\b"],
    ],

    "LTA Application ID": [
        [r"\bApp\b", r"\bNo\b", r"\bConn\b", r"\bQuantum\b", r"\bConnectivity\b"],
        [r"\bLTA\b", r"\bApplication\b"],
        [r"\bLTA\b", r"\bID\b"],
        [r"\bLTA\b", r"\bApp\b"],
    ],

    "Application ID under Enhancement 5.2 or revision": [
        [r"\bApplication\b", r"\bID\b", r"\b5\.?2\b"],
        [r"\bApplication\b", r"\bNo\b", r"\bDate\b", r"\b5\.?2\b"],
        [r"\benhancement\b", r"\b5\.?2\b"],
        [r"\brevision\b", r"\bapplication\b", r"\bID\b"],
    ],

    "Application Quantum (MW)(ST II)": [
        [r"\bInstalled\b", r"\bCapacity\b", r"\bMW\b"],
        [r"\bConnectivity\b", r"\bQuantum\b", r"\bMW\b"],
        [r"\bApplication\b", r"\bQuantum\b", r"\bMW\b"],
        [r"\bCapacity\b", r"\bMW\b"],
    ],

    "Nature of Applicant": [
        [r"\bNature\b", r"\bApplicant\b"],
    ],

    "Mode(Criteria for applying)": [
        [r"\bCriterion\b", r"\bapplying\b"],
        [r"\bCriteria\b", r"\bapplying\b"],
        [r"\bMode\b", r"\bCriteria\b"],
        [r"\bMode\b", r"\bapplying\b"],
    ],

    "Applied Start of Connectivity sought by developer date": [
        [r"\bStart\b", r"\bDate\b", r"\bConnectivity\b", r"\bApplication\b"],
        [r"\bStart\b", r"\bDate\b", r"\bConnectivity\b"],
        [r"\bConnectivity\b", r"\bsought\b", r"\bdate\b"],
    ],

    "Application/Submission Date": [
        [r"\bApplication\b", r"\bNo\b", r"\bDate\b"],
        [r"\bSubmission\b", r"\bDate\b"],
        [r"\bApplication\b", r"\bDate\b"],
    ],

    "GNA Operationalization Date": [
        [r"\bGNA\b", r"\bOperationalization\b"],
        [r"\bSCoD\b"],
        [r"\bSCOD\b"],
    ],

    "Status of application(Withdrawn / granted. Revoked.)": [
        [r"\bWithdrawn\b"],
        [r"\bgrant(?:ed)?\b"],
        [r"\bRevoked\b"],
        [r"\bStatus\b", r"\bApplication\b"],
    ],

    "PSP MWh": [
        [r"\bpump\s*storage\b"],
        [r"\bPSP\b"],
    ],
}


# ── Strategy 2: Value-based fingerprints ─────────────────────────────────────
# Each key is a canonical column name.
# Each value is a list of regex patterns that match CHARACTERISTIC CELL VALUES.
# If enough fingerprint values appear on a page, the column is considered
# present even if the header wording is unrecognised.
#
# The threshold (MIN_VALUE_HITS) controls how many distinct fingerprint
# matches are needed to trigger value-based detection.

MIN_VALUE_HITS = 2   # need at least 2 distinct value matches per column

VALUE_FINGERPRINTS: dict[str, list[str]] = {

    "substaion": [
        # Substation names: <Name>-<Roman/Number> or <Name> (PG/PGCIL/PS)
        r"\b(?:Aligarh|Amargarh|Barmer|Bhadla|Bikaner|Fatehgarh|Kishtwar|"
        r"Merta|Pali|Ramgarh|Sanchore|Ajmer|Jodhpur|Jaipur|Bhilwara|"
        r"Kota|Udaipur|Sikar|Tonk|Chittorgarh|Nagaur|Jhunjhunu|"
        r"Alwar|Bhiwani|Hisar|Mahendragarh|Narnaul|Rewari|"
        r"Moga|Bathinda|Ludhiana|Amritsar|Jalandhar|"
        r"Anta|Gwalior|Indore|Ujjain|Bhopal|Rewa|Raipur|"
        r"Bareilly|Agra|Kanpur|Lucknow|Gorakhpur|"
        r"Dharamshala|Nalagarh|Parwanoo|Shimla|"
        r"Jammu|Srinagar|Leh|Kargil|"
        r"Ranchi|Dhanbad|Bokaro|Deoghar|"
        r"Patna|Muzaffarpur|Bhagalpur|"
        r"Kolkata|Durgapur|Kharagpur|"
        r"Hyderabad|Warangal|Vijayawada|Kurnool|Raichur|"
        r"Chennai|Madurai|Salem|Tirunelveli|"
        r"Bengaluru|Mysuru|Hubballi|Bellary|"
        r"Ahmedabad|Gandhinagar|Surat|Vadodara|"
        r"Pune|Mumbai|Nagpur|Nashik|Aurangabad|"
        r"Bhubaneswar|Rourkela|Cuttack"
        r")(?:\s*[-‐–]\s*(?:I{1,4}V?|V?I{0,3}|[0-9]+))?\b",
        # (PG) / (PGCIL) / (PS) suffix pattern
        r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s*\((?:PG|PGCIL|PS)\)",
    ],

    "Name of the developers": [
        # Company suffixes: Private Limited, Ltd, LLP, etc.
        r"\b(?:Private\s+Limited|Pvt\.?\s*Ltd|Limited|LLP|Corporation)\b",
        # Known developer name prefixes
        r"\b(?:ACME|Adani|Tata|NTPC|NHPC|SJVN|Greenko|ReNew|"
        r"Azure|Hero\s+Future|JSW|Torrent|Sembcorp|Sprng|"
        r"Ayana|SB\s+Energy|Avaada|CleanMax|Fourth\s+Partner|"
        r"Engie|O2\s+Power|Amp\s+Energy|Vikram\s+Solar|"
        r"Sterling\s+Wilson|Waaree|Suzlon|Inox|"
        r"THDC|CESC|BSES|AM\s+Green|Altra\s+Xergi)\b",
    ],

    "type": [
        # Energy source types
        r"\bSolar\s*\+?\s*BESS\b",
        r"\bHybrid\s*\+?\s*BESS\b",
        r"\bHydro\s*\+?\s*BESS\b",
        r"\b(?:Solar|Wind|Hybrid|BESS|Hydro|Thermal)\b",
    ],

    "GNA/ST II Application ID": [
        # 10-digit numeric IDs starting with 12, 22, 11
        r"\b(?:12|22|11)\d{8}\b",
        # ST-II / ST II prefix context
        r"\bST[\s-]*II\b",
    ],

    "LTA Application ID": [
        # IDs prefixed with 04 or 41
        r"\b(?:04|41)\d{7,8}\b",
        # LTA keyword context
        r"\bLTA\s*:",
    ],

    "Mode(Criteria for applying)": [
        # Known mode/criteria values
        r"\bLand\s+(?:BG\s+)?Route\b",
        r"\bLOA\s+or\s+PPA\b",
        r"\b(?:NHPC|NTPC|SJVN|REMCL|SECI|IREDA)\s+LOA\b",
        r"\bLand\s+Route\b",
    ],
}


def _header_detect(text: str) -> dict[str, bool]:
    """Strategy 1: detect columns by matching header keywords."""
    return {
        col: any(
            all(re.search(kw, text, re.IGNORECASE) for kw in variant)
            for variant in variants
        )
        for col, variants in TARGET_COLUMN_VARIANTS.items()
    }


def _value_detect(text: str) -> dict[str, bool]:
    """Strategy 2: detect columns by characteristic cell values."""
    results: dict[str, bool] = {}
    for col, patterns in VALUE_FINGERPRINTS.items():
        hits = 0
        for pat in patterns:
            matches = re.findall(pat, text, re.IGNORECASE)
            hits += len(matches)
        results[col] = hits >= MIN_VALUE_HITS
    return results


def page_passes_gate(text: str) -> tuple[bool, list[str]]:
    """Check whether *text* contains a CMETS data table.

    Uses a dual strategy:
      1. Header-based regex matching (existing logic)
      2. Value-based fingerprint matching (fallback for variant headers)

    Returns
    -------
    (passed, active_fields)
        ``passed`` is True if at least one column is detected.
        ``active_fields`` lists the detected column names (union of both
        strategies, deduplicated).
    """
    header_hits = _header_detect(text)
    value_hits  = _value_detect(text)

    # Merge: a column is active if EITHER strategy detects it
    all_columns = set(header_hits.keys()) | set(value_hits.keys())
    active = sorted(
        col for col in all_columns
        if header_hits.get(col, False) or value_hits.get(col, False)
    )

    return bool(active), active
