"""
effectiveness_handler/extraction.py — Per-page table extraction
=================================================================
Uses per-page pdfplumber table extraction instead of chunked LLM extraction.

Two strategies:
  1. Primary: pdfplumber table extraction per page → header mapping → rows
     + optional LLM enrichment for semantic gaps
  2. Fallback: full LLM extraction per page (when no tables found)

When no API key is set, uses pure pdfplumber table extraction (no LLM).
"""

from __future__ import annotations

import json
import re
import time
from typing import Optional

import pdfplumber

from config import MODEL
from llm_client import call_llm, extract_text_from_response
from pipeline.effectiveness_handler.prompts import SYSTEM_PROMPT, USER_TEMPLATE
from pipeline.effectiveness_handler.models import RERecord, safe_record


# ── JSON parser ───────────────────────────────────────────────────────────────

def _parse_json(text: str) -> list | dict:
    raw = (text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\[.*\]", raw, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
    return {}


# ── Header mapping for effectiveness tables ──────────────────────────────────

_HEADER_MAP: dict[str, str] = {
    "si no": "sl_no", "sl. no.": "sl_no", "sl no": "sl_no",
    "application id": "application_id",
    "name of applicant": "name_of_applicant",
    "region": "region",
    "type of project": "type_of_project",
    "installed capacity (mw)": "installed_capacity_mw",
    "installed capacity":      "installed_capacity_mw",
    "solar": "solar_mw", "wind": "wind_mw", "ess": "ess_mw", "hydro": "hydro_mw",
    "connectivity (mw)": "connectivity_mw", "connectivity": "connectivity_mw",
    "present connectivity /deemed gna": "present_connectivity_mw",
    "present connectivity":             "present_connectivity_mw",
    "substation": "substation", "state": "state",
    "expected date of connectivity/ gna to be made effective": "expected_date",
    "expected date of connectivity/gna to be made effective":  "expected_date",
    "expected date": "expected_date",
}


def _map_headers(raw: list) -> dict:
    mapping: dict = {}
    for i, h in enumerate(raw):
        key = re.sub(r"\s+", " ", (h or "").lower().strip())
        if key in _HEADER_MAP:
            mapping[i] = _HEADER_MAP[key]
        else:
            for pat, field in _HEADER_MAP.items():
                if key and pat in key:
                    mapping[i] = field
                    break
    return mapping


def _is_header_row(row: list) -> bool:
    text = " ".join(str(c or "") for c in row).lower()
    return "application" in text or "sl. no" in text or "si no" in text


# ── Multi-strategy table extraction ──────────────────────────────────────────

_TABLE_STRATEGIES = [
    {"vertical_strategy": "lines", "horizontal_strategy": "lines",
     "snap_tolerance": 3, "join_tolerance": 3},
    {"vertical_strategy": "text", "horizontal_strategy": "text",
     "snap_tolerance": 5, "join_tolerance": 5},
    {"vertical_strategy": "lines", "horizontal_strategy": "text",
     "snap_tolerance": 4, "join_tolerance": 4},
]


def _extract_tables_from_page(page) -> list[list]:
    """Extract tables from a single pdfplumber page using multiple strategies."""
    for strategy in _TABLE_STRATEGIES:
        try:
            tables = page.extract_tables(strategy) or []
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


# ── Per-page table extraction → RERecord list ────────────────────────────────

def _extract_page_tables(page, source_name: str) -> list[RERecord]:
    """Extract records from a single page using pdfplumber table detection."""
    records: list[RERecord] = []
    tables = _extract_tables_from_page(page)
    if not tables:
        return records

    mapping: dict = {}
    for table in tables:
        if not table:
            continue
        for row in table:
            if not row or all(c is None for c in row):
                continue
            if _is_header_row(row):
                mapping = _map_headers(row)
                continue
            if not mapping:
                continue
            raw = {field: row[ci] for ci, field in mapping.items() if ci < len(row)}
            if not raw.get("application_id") and not raw.get("name_of_applicant"):
                continue
            raw["source_file"] = source_name
            rec = safe_record(raw)
            if rec:
                records.append(rec)

    return records


# ── Strategy 1: Per-page table extraction + optional LLM enrichment ──────────

def extract_with_llm(pdf_path: str, source_name: str, runtime) -> list[RERecord]:
    """Extract records from one effectiveness PDF using per-page table extraction.

    For each page:
      1. Try pdfplumber table extraction first
      2. If tables found → use them directly (no LLM needed for structured data)
      3. If NO tables found and page has text → fallback to LLM per-page extraction
    """
    records: list[RERecord] = []

    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        print(f"    {total} pages — extracting per page …")

        for i, page in enumerate(pdf.pages):
            page_num = i + 1
            text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""

            if not text.strip():
                continue

            # Try table extraction first
            page_records = _extract_page_tables(page, source_name)

            if page_records:
                print(f"    Page {page_num:>3}: {len(page_records)} rows (table)")
                records.extend(page_records)
            else:
                # Fallback: LLM per-page extraction (no chunking)
                print(f"    Page {page_num:>3}: no tables → LLM …", end=" ", flush=True)
                for attempt in range(3):
                    try:
                        resp = call_llm(
                            prompt_payload={"messages": [
                                {"role": "system", "content": SYSTEM_PROMPT},
                                {"role": "user",   "content": USER_TEMPLATE.format(text=text)},
                            ], "temperature": 0, "max_tokens": 4000},
                            vm=runtime.vm_mode,
                            api_key=runtime.api_key or None,
                            model=MODEL,
                            script_path=runtime.llm_script_path,
                        )
                        raw_text = extract_text_from_response(resp)
                        result = _parse_json(raw_text)
                        rows = result if isinstance(result, list) else (
                            next((v for v in result.values() if isinstance(v, list)), [])
                            if isinstance(result, dict) else []
                        )
                        batch = [r for r in (safe_record({**row, "source_file": source_name}) for row in rows) if r]
                        records.extend(batch)
                        print(f"{len(batch)} rows")
                        break
                    except Exception as exc:
                        if attempt < 2:
                            time.sleep(10)
                        else:
                            print(f"FAILED ({exc})")

    return records


# ── Strategy 2: pdfplumber table-only (no LLM) ──────────────────────────────

def extract_with_tables(pdf_path: str, source_name: str) -> list[RERecord]:
    """Pure pdfplumber table extraction (when no API key is set).

    Uses per-page extraction with multi-strategy table detection.
    """
    records: list[RERecord] = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            page_records = _extract_page_tables(page, source_name)
            if page_records:
                print(f"    Page {i + 1:>3}: {len(page_records)} rows")
                records.extend(page_records)
    return records
