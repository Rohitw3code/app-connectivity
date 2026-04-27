"""
cmets_handler/table_extraction.py — Per-page pdfplumber table extraction engine
=================================================================================
Extracts tables from each PDF page using pdfplumber, maps columns to canonical
names using the same variant logic from gate.py, and applies regex-based rules
for context-dependent columns (Enhancement 5.2, PSP, status, etc.).

The LLM is only used as a targeted enrichment step for columns that genuinely
need semantic understanding of the page description text.

Extraction strategy per page:
  1. pdfplumber.extract_tables() → structured rows
  2. Header row detection → column mapping to canonical names
  3. Regex context rules on page text (Enhancement 5.2, PSP, etc.)
  4. Rule-based enrichment (state from location, type normalisation etc.)
  5. LLM reasoning ONLY when table rows need semantic enrichment from the
     surrounding description text
"""

from __future__ import annotations

import json
import re
from typing import Optional

import pdfplumber

from config import MODEL
from llm_client import call_llm, extract_text_from_response


# ══════════════════════════════════════════════════════════════════════════════
# GENERIC TABLE EXTRACTOR — works on any pdfplumber page
# ══════════════════════════════════════════════════════════════════════════════

# Multiple strategies for extracting tables, tried in order of reliability
_TABLE_STRATEGIES = [
    # Strategy 1: line-based (best for formal bordered tables)
    {"vertical_strategy": "lines", "horizontal_strategy": "lines",
     "snap_tolerance": 3, "join_tolerance": 3},
    # Strategy 2: text-based (for borderless / partially bordered tables)
    {"vertical_strategy": "text", "horizontal_strategy": "text",
     "snap_tolerance": 5, "join_tolerance": 5},
    # Strategy 3: mixed (lines vertical, text horizontal)
    {"vertical_strategy": "lines", "horizontal_strategy": "text",
     "snap_tolerance": 4, "join_tolerance": 4},
]


def extract_all_tables_from_page(page: pdfplumber.page.Page) -> list[list[list]]:
    """Extract ALL tables from a pdfplumber page using multiple strategies.

    Tries line-based, text-based, and mixed strategies. Returns the result
    from the first strategy that successfully finds tables.

    Returns list of tables, where each table is a list of rows (list of cells).
    """
    for strategy in _TABLE_STRATEGIES:
        try:
            tables = page.extract_tables(strategy) or []
            # Filter out empty tables
            tables = [t for t in tables if t and len(t) >= 2]
            if tables:
                return tables
        except Exception:
            continue

    # Final fallback: single-table extraction
    try:
        tbl = page.extract_table()
        if tbl and len(tbl) >= 2:
            return [tbl]
    except Exception:
        pass

    return []


# ══════════════════════════════════════════════════════════════════════════════
# HEADER MAPPING — maps raw header text → canonical CMETS column name
# ══════════════════════════════════════════════════════════════════════════════
# Uses the same column variants as gate.py but adapted for header cell matching.
# Each entry:  (regex_pattern, canonical_column_name)
# Order matters — first match wins. More specific patterns MUST come first.

CMETS_HEADER_PATTERNS: list[tuple[str, str]] = [
    # ── Project Location ──
    (r"project\s*location",                                              "Project Location"),

    # ── Substation / Connectivity Point ──
    (r"connectivity\s*location.*application",                            "substaion"),
    (r"connectivity\s*granted\s*at",                                     "substaion"),
    (r"nearest\s*pooling\s*station",                                     "substaion"),
    (r"location\s*requested\s*for\s*grant.*stage.*connectivity",         "substaion"),
    (r"connectivity\s*injection\s*point",                                "substaion"),
    (r"injection\s*point",                                               "substaion"),
    (r"sub[\s-]?station",                                                "substaion"),
    (r"pooling\s*station",                                               "substaion"),

    # ── Developer / Applicant ──
    (r"name\s*of\s*(?:the\s*)?(?:applicant|developer)",                  "Name of the developers"),
    (r"\bapplicant\b(?!.*type)(?!.*nature)",                             "Name of the developers"),
    (r"\bdeveloper\b",                                                   "Name of the developers"),

    # ── Type / Source ──
    (r"type\s*of\s*(?:source|generation|energy|plant)",                  "type"),
    (r"source\s*type",                                                   "type"),
    (r"generation\s*type",                                               "type"),
    (r"energy\s*source",                                                 "type"),

    # ── Application IDs — Enhancement 5.2 MUST be checked BEFORE generic ──
    (r"(?:application|app).*(?:5\.?\s*2|enhancement|revision)",          "Application ID under Enhancement 5.2 or revision"),
    (r"enhancement.*5\.?\s*2",                                           "Application ID under Enhancement 5.2 or revision"),
    (r"revision.*application.*(?:id|no)",                                "Application ID under Enhancement 5.2 or revision"),
    # LTA before generic (more specific)
    (r"lta\s*(?:application|id|app)",                                    "LTA Application ID"),
    (r"app.*no.*conn.*quantum.*connectivity",                            "LTA Application ID"),
    (r"already\s*granted\s*connectivity",                                "LTA Application ID"),
    # GNA / ST-II (generic application ID)
    (r"(?:gna|st[\s-]*ii).*application",                                 "GNA/ST II Application ID"),
    (r"application\s*(?:no|id).*(?:date)?",                              "GNA/ST II Application ID"),

    # ── Quantum / Capacity ──
    (r"installed\s*capacity.*mw",                                        "Application Quantum (MW)(ST II)"),
    (r"connectivity\s*quantum.*mw",                                      "Application Quantum (MW)(ST II)"),
    (r"application\s*quantum.*mw",                                       "Application Quantum (MW)(ST II)"),
    (r"capacity\s*\(?mw\)?",                                             "Application Quantum (MW)(ST II)"),

    # ── Nature of Applicant ──
    (r"nature\s*of\s*applicant",                                         "Nature of Applicant"),

    # ── Mode / Criteria ──
    (r"criteri(?:on|a)\s*(?:for\s*)?applying",                           "Mode(Criteria for applying)"),
    (r"mode.*criteri",                                                   "Mode(Criteria for applying)"),
    (r"mode\s*(?:of\s*)?applying",                                       "Mode(Criteria for applying)"),

    # ── Start date of connectivity ──
    (r"start\s*(?:date\s*(?:of\s*)?)?connectivity.*(?:application|sought)", "Applied Start of Connectivity sought by developer date"),
    (r"connectivity\s*sought.*date",                                      "Applied Start of Connectivity sought by developer date"),

    # ── Application/Submission Date ──
    (r"submission\s*date",                                               "Application/Submission Date"),

    # ── GNA Operationalization / SCoD ──
    (r"gna\s*operationali[sz]ation",                                     "GNA Operationalization Date"),
    (r"\bscod\b",                                                        "GNA Operationalization Date"),

    # ── Status ──
    (r"status\s*of\s*application",                                       "Status of application(Withdrawn / granted. Revoked.)"),

    # ── PSP ──
    (r"psp\s*mwh|pump\s*storage\s*mwh",                                 "PSP MWh"),
    (r"psp\s*injection|pump\s*storage\s*injection",                      "PSP Injection (MW)"),
    (r"psp\s*draw[lw]|pump\s*storage\s*draw[lw]",                       "PSP Drawl (MW)"),

    # ── State (sometimes explicit column) ──
    (r"^\s*state\s*$",                                                   "State"),

    # ── Skip serial number columns ──
    (r"s[il]\.?\s*no",                                                   "_skip_"),
    (r"^\s*(?:sr|no)\.?\s*$",                                            "_skip_"),
]


def _map_single_header(cell_text: str) -> Optional[str]:
    """Map a single header cell to a canonical column name. Returns None if unrecognised."""
    if not cell_text:
        return None
    cleaned = re.sub(r"\s+", " ", cell_text.strip())
    if not cleaned:
        return None
    for pat, canonical in CMETS_HEADER_PATTERNS:
        if re.search(pat, cleaned, re.IGNORECASE):
            return canonical
    return None


def _build_column_mapping(header_row: list) -> dict[int, str]:
    """Build {column_index: canonical_name} from a table header row."""
    mapping: dict[int, str] = {}
    for i, cell in enumerate(header_row):
        canonical = _map_single_header(str(cell or ""))
        if canonical and canonical != "_skip_":
            mapping[i] = canonical
    return mapping


def _is_likely_header(row: list) -> bool:
    """Heuristic: a row is a header if ≥ 2 cells map to known canonical columns."""
    hits = sum(1 for cell in row if _map_single_header(str(cell or "")) is not None)
    return hits >= 2


def _is_data_row(row: list) -> bool:
    """Heuristic: a valid data row has ≥ 2 non-empty cells."""
    if not row:
        return False
    non_empty = sum(1 for c in row if c is not None and str(c).strip())
    return non_empty >= 2


def _clean_cell(val) -> Optional[str]:
    """Clean a single cell value."""
    if val is None:
        return None
    s = re.sub(r"\s+", " ", str(val).strip())
    return s if s else None


# ══════════════════════════════════════════════════════════════════════════════
# PAGE-LEVEL TABLE EXTRACTION → canonical row dicts
# ══════════════════════════════════════════════════════════════════════════════

def extract_table_rows_from_page(page: pdfplumber.page.Page) -> list[dict]:
    """Extract structured row dicts from a pdfplumber page.

    Steps:
      1. Try multiple table extraction strategies
      2. Detect header rows and build column mapping
      3. Map data cells to canonical column names
      4. Return list of {canonical_column: value} dicts

    Returns empty list if no tables or no recognisable headers found.
    """
    tables = extract_all_tables_from_page(page)
    if not tables:
        return []

    all_rows: list[dict] = []
    col_mapping: dict[int, str] = {}

    for table in tables:
        if not table:
            continue

        # Reset mapping per table (different tables on same page may
        # have different column layouts)
        table_mapping: dict[int, str] = {}

        for row in table:
            if not row or all(c is None for c in row):
                continue

            # Check if this is a header row
            if _is_likely_header(row):
                new_mapping = _build_column_mapping(row)
                if new_mapping:
                    table_mapping = new_mapping
                    col_mapping = table_mapping
                continue

            # Use table-level mapping, fall back to persistent mapping
            active_mapping = table_mapping or col_mapping
            if not active_mapping:
                continue

            if not _is_data_row(row):
                continue

            # Build row dict from mapping
            row_dict: dict = {}
            for col_idx, canonical in active_mapping.items():
                if col_idx < len(row):
                    val = _clean_cell(row[col_idx])
                    if val:
                        row_dict[canonical] = val

            if row_dict:
                all_rows.append(row_dict)

    return all_rows


# ══════════════════════════════════════════════════════════════════════════════
# REGEX CONTEXT DETECTORS — page-level signals from raw text
# ══════════════════════════════════════════════════════════════════════════════

def detect_enhancement_52(page_text: str) -> bool:
    """Return True if page mentions Enhancement 5.2 / regulation 5.2 / revision."""
    return bool(re.search(
        r"\b(?:enhancement|regulation)\s*5\.?\s*2\b"
        r"|\brevision\b.*\b(?:application|connectivity)\b",
        page_text, re.IGNORECASE,
    ))


def detect_psp(page_text: str) -> bool:
    """Return True if page mentions pump storage / PSP."""
    return bool(re.search(r"\b(?:pump\s*storage|PSP)\b", page_text, re.IGNORECASE))


def extract_application_ids_from_text(page_text: str) -> list[str]:
    """Extract application number(s) from the page description text."""
    # Standard 10-digit IDs starting with 12, 22, 11
    ids = re.findall(r"\b(?:12|22|11)\d{8}\b", page_text)
    if ids:
        return ids
    # Fallback: any long numeric ID (8-12 digits)
    return re.findall(r"\b\d{8,12}\b", page_text)


# ══════════════════════════════════════════════════════════════════════════════
# BLOCKLIST CHECK — applied on extracted table rows (not just page text)
# ══════════════════════════════════════════════════════════════════════════════

# GNARE column names that indicate the table is NOT a connectivity table
_GNARE_COLUMN_PATTERNS = [
    r"gnare\s*within\s*region",
    r"gnare\s*outside\s*region",
    r"total\s*gnare\s*required",
    r"start\s*date\s*of\s*gnare",
    r"end\s*date\s*of\s*gnare",
    r"gnare.*mw",
]

# Nature of Applicant values to skip
_NATURE_BLOCKLIST = ["bulk consumer", "drawee entity", "drawee entity connected"]


def table_is_gnare(table: list[list]) -> bool:
    """Return True if the table contains GNARE columns (should be skipped)."""
    if not table:
        return False
    # Check all rows (especially headers) for GNARE keywords
    for row in table[:3]:  # check first 3 rows (headers + sub-headers)
        row_text = " ".join(str(c or "") for c in row).lower()
        for pat in _GNARE_COLUMN_PATTERNS:
            if re.search(pat, row_text, re.IGNORECASE):
                return True
    return False


def row_is_blocklisted(row_dict: dict) -> bool:
    """Return True if the row has a blocklisted Nature of Applicant value."""
    nature = row_dict.get("Nature of Applicant")
    if not nature:
        return False
    lower = nature.lower().strip()
    return any(blocked in lower for blocked in _NATURE_BLOCKLIST)


# ══════════════════════════════════════════════════════════════════════════════
# CONTEXT ENRICHMENT — apply regex rules to enrich extracted rows
# ══════════════════════════════════════════════════════════════════════════════

def enrich_rows_with_context(rows: list[dict], page_text: str) -> list[dict]:
    """Enrich extracted table rows with page-level context signals.

    Applies rules for:
    - Enhancement 5.2: if page context mentions 5.2, take the application no.
      from the GNA/ST II column and ALSO put it in the Enhancement 5.2 column
    - PSP: only fill PSP columns if page context mentions pump storage/PSP
    - State: can often be derived from project location in the page description
    """
    is_enh52 = detect_enhancement_52(page_text)
    is_psp   = detect_psp(page_text)

    enriched: list[dict] = []
    for row in rows:
        row = dict(row)  # shallow copy

        # ── Enhancement 5.2 logic ──
        enh_col = "Application ID under Enhancement 5.2 or revision"
        if is_enh52:
            if not row.get(enh_col):
                # Take application ID from the existing columns
                app_id = (
                    row.get("GNA/ST II Application ID")
                    or row.get("LTA Application ID")
                )
                if app_id:
                    row[enh_col] = app_id
                else:
                    # Try extracting from page text
                    ids = extract_application_ids_from_text(page_text)
                    if ids:
                        row[enh_col] = ", ".join(ids)

        # ── PSP logic ──
        if not is_psp:
            # Clear PSP fields when page is NOT about pump storage
            for psp_col in ("PSP MWh", "PSP Injection (MW)", "PSP Drawl (MW)"):
                row.pop(psp_col, None)

        enriched.append(row)

    return enriched


# ══════════════════════════════════════════════════════════════════════════════
# LLM TARGETED REASONING — only for semantic enrichment of table-extracted rows
# ══════════════════════════════════════════════════════════════════════════════

_LLM_ENRICH_SYSTEM = """\
You are a precise data verification assistant for Indian energy/power connectivity applications.

You will receive:
1. Structured table rows ALREADY EXTRACTED from a PDF page (JSON)
2. The full page text for additional context

Your task: REVIEW and ENRICH each row using context from the page text.
The table data is already extracted — do NOT discard it. Only ADD or CORRECT values.

For each row:
- Verify column values look correct against the page text
- Fill "State" from "Project Location" if empty (extract state name)
- Fill "type" if missing — must be one of: Solar, Wind, Hybrid, BESS, Solar + BESS, Hybrid + BESS, Hydro, Hydro+BESS, Thermal
- Fill "GNA Operationalization Date" from SCoD/SCOD mentions in description
- Fill "Status of application" from context: Withdrawn / granted / Revoked
- "Name of the developers" must be a company name, NOT criteria like "SECI LOA"
- "GNA Operationalization (Yes/No)" — return null (computed in post-processing)
- Ensure "Application/Submission Date" is the date portion only

CRITICAL: Keep ALL existing values that are correct. Return ALL rows.

Return JSON: {"rows": [ {row with all 19 columns} ]}
If a value is unknown, use null.

The 19 columns:
Project Location, State, substaion, Name of the developers, type,
GNA/ST II Application ID, LTA Application ID,
Application ID under Enhancement 5.2 or revision,
Application Quantum (MW)(ST II), Nature of Applicant,
Mode(Criteria for applying),
Applied Start of Connectivity sought by developer date,
Application/Submission Date, GNA Operationalization Date,
GNA Operationalization (Yes/No),
Status of application(Withdrawn / granted. Revoked.),
PSP MWh, PSP Injection (MW), PSP Drawl (MW)
"""

_LLM_ENRICH_USER = """\
Here are the rows already extracted from the table on this page:
```json
{table_rows_json}
```

Full page text for context:
{page_text}

Review, verify, and enrich each row. Fill missing values from the context.
Return JSON with "rows" array."""


def _parse_json(text: str) -> dict | list:
    """Parse JSON from LLM response, handling markdown fences."""
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
    return {}


def llm_enrich_rows(
    table_rows: list[dict],
    page_text: str,
    vm_mode: bool,
    api_key: Optional[str],
    llm_script_path: Optional[str],
) -> list[dict]:
    """Send extracted table rows + page context to LLM for semantic enrichment.

    This is MUCH cheaper than full-page LLM extraction since the structured
    data is already extracted — LLM only needs to verify and fill gaps.
    """
    if not table_rows:
        return []

    prompt = {
        "messages": [
            {"role": "system", "content": _LLM_ENRICH_SYSTEM},
            {"role": "user",   "content": _LLM_ENRICH_USER.format(
                table_rows_json=json.dumps(table_rows, indent=2, ensure_ascii=False),
                page_text=page_text,
            )},
        ],
        "temperature": 0,
        "max_tokens":  4000,
    }
    try:
        resp    = call_llm(prompt, vm=vm_mode, api_key=api_key, model=MODEL, script_path=llm_script_path)
        content = extract_text_from_response(resp)
        result  = _parse_json(content)
        rows    = result.get("rows", []) if isinstance(result, dict) else []
        return rows if isinstance(rows, list) else table_rows
    except Exception as exc:
        print(f"      [LLM enrich error] {exc}")
        return table_rows  # fallback: return original rows un-enriched


# ══════════════════════════════════════════════════════════════════════════════
# LLM FULL EXTRACTION — fallback when NO tables found on a page
# ══════════════════════════════════════════════════════════════════════════════

_LLM_FULL_SYSTEM = """\
You are a precise data extraction assistant for Indian energy/power connectivity applications.

You will receive the FULL TEXT of a PDF page. No structured table could be extracted
programmatically from this page. Your task: scan the text and extract EVERY data row.

Output keys for each row:
Project Location, State, substaion, Name of the developers, type,
GNA/ST II Application ID, LTA Application ID,
Application ID under Enhancement 5.2 or revision,
Application Quantum (MW)(ST II), Nature of Applicant,
Mode(Criteria for applying),
Applied Start of Connectivity sought by developer date,
Application/Submission Date, GNA Operationalization Date,
GNA Operationalization (Yes/No),
Status of application(Withdrawn / granted. Revoked.),
PSP MWh, PSP Injection (MW), PSP Drawl (MW)

CRITICAL RULES:
- A row MUST have at least ONE of: GNA/ST II Application ID, LTA Application ID,
  or Application ID under Enhancement 5.2 or revision
- Skip rows with Nature of Applicant = "Bulk consumer" / "Drawee entity"
- Skip any GNARE-related data
- "type" must be: Solar, Wind, Hybrid, BESS, Solar + BESS, Hybrid + BESS,
  Hydro, Hydro+BESS, Thermal, or null
- Use null for missing values

Return JSON: {"rows": [ {...} ]}
If no data: {"rows": []}
"""


def llm_full_extract(
    page_text: str,
    active_fields: list[str],
    vm_mode: bool,
    api_key: Optional[str],
    llm_script_path: Optional[str],
) -> list[dict]:
    """Full LLM extraction fallback when pdfplumber finds no tables."""
    prompt = {
        "messages": [
            {"role": "system", "content": _LLM_FULL_SYSTEM},
            {"role": "user",   "content":
                f"Detected column labels: {', '.join(active_fields)}\n\n"
                f"Full page text:\n{page_text}"
            },
        ],
        "temperature": 0,
        "max_tokens":  4000,
    }
    try:
        resp    = call_llm(prompt, vm=vm_mode, api_key=api_key, model=MODEL, script_path=llm_script_path)
        content = extract_text_from_response(resp)
        result  = _parse_json(content)
        rows    = result.get("rows", []) if isinstance(result, dict) else []
        return rows if isinstance(rows, list) else []
    except Exception as exc:
        print(f"      [LLM full-extract error] {exc}")
        return []
