"""
cmets_handler/gate.py — Regex Column-Header Gate (Sub-layer B)
================================================================
Determines whether a PDF page likely contains a CMETS data table by
checking for known column header patterns (regex variants).

Edit the ``TARGET_COLUMN_VARIANTS`` dictionary to support new table
formats or column name variations.
"""

from __future__ import annotations

import re

TARGET_COLUMN_VARIANTS: dict[str, list[list[str]]] = {
    "Project Location": [
        [r"\bProject\b", r"\bLocation\b"],
    ],
    "substaion": [
        [r"\bConnectivity\b", r"\blocation\b", r"\bApplication\b"],
    ],
    "Name of the developers": [
        [r"\bApplicant\b"],
        [r"\bName\b", r"\bApplicant\b"],
    ],
    "GNA/ST II Application ID": [
        [r"\bApplication\b", r"\bID\b"],
        [r"\bApplication\b", r"\bNo\b", r"\bDate\b"],
    ],
    "LTA Application ID": [
        [r"\bApp\b", r"\bNo\b", r"\bConn\b", r"\bQuantum\b", r"\bConnectivity\b"],
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
    ],
    "Nature of Applicant": [
        [r"\bNature\b", r"\bApplicant\b"],
    ],
    "Mode(Criteria for applying)": [
        [r"\bCriterion\b", r"\bapplying\b"],
    ],
    "Applied Start of Connectivity sought by developer date": [
        [r"\bStart\b", r"\bDate\b", r"\bConnectivity\b", r"\bApplication\b"],
    ],
    "Application/Submission Date": [
        [r"\bApplication\b", r"\bNo\b", r"\bDate\b"],
        [r"\bSubmission\b", r"\bDate\b"],
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
    ],
    "PSP MWh": [
        [r"\bpump\s*storage\b"],
        [r"\bPSP\b"],
    ],
}


def page_passes_gate(text: str) -> tuple[bool, list[str]]:
    """Check whether *text* contains any known CMETS column-header patterns.

    Returns
    -------
    (passed, active_fields)
        ``passed`` is True if at least one column variant matches.
        ``active_fields`` lists the matched column names.
    """
    hits = {
        col: any(
            all(re.search(kw, text, re.IGNORECASE) for kw in variant)
            for variant in variants
        )
        for col, variants in TARGET_COLUMN_VARIANTS.items()
    }
    active = [col for col, ok in hits.items() if ok]
    return bool(active), active
