"""
effectiveness_handler/extraction.py — PDF extraction sub-layers
=================================================================
Contains two extraction strategies:
  1. LLM-based extraction (primary) — chunked text → GPT → parsed rows
  2. pdfplumber table extraction (fallback) — when no API key is set

Edit this file to change how data is extracted from effectiveness PDFs.
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


# ── Strategy 1: LLM extraction ───────────────────────────────────────────────

def extract_with_llm(pdf_path: str, source_name: str, runtime) -> list[RERecord]:
    """Extract all records from one effectiveness PDF via LLM (with retry)."""
    pages_text: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
            if t.strip():
                pages_text.append(t)

    # Batch into ~10 000-char chunks
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for text in pages_text:
        if cur_len + len(text) > 10_000 and cur:
            chunks.append("\n\n".join(cur))
            cur, cur_len = [], 0
        cur.append(text)
        cur_len += len(text)
    if cur:
        chunks.append("\n\n".join(cur))

    records: list[RERecord] = []
    for i, chunk in enumerate(chunks, 1):
        print(f"    LLM chunk {i}/{len(chunks)} …", end=" ", flush=True)
        for attempt in range(3):
            try:
                resp     = call_llm(
                    prompt_payload={"messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user",   "content": USER_TEMPLATE.format(text=chunk)},
                    ], "temperature": 0, "max_tokens": 4000},
                    vm=runtime.vm_mode,
                    api_key=runtime.api_key or None,
                    model=MODEL,
                    script_path=runtime.llm_script_path,
                )
                raw_text = extract_text_from_response(resp)
                result   = _parse_json(raw_text)
                rows     = result if isinstance(result, list) else (
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


# ── Strategy 2: pdfplumber table fallback ─────────────────────────────────────

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


def extract_with_tables(pdf_path: str, source_name: str) -> list[RERecord]:
    """pdfplumber table-detection fallback (when no API key is set)."""
    records: list[RERecord] = []
    mapping: dict = {}
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables({
                "vertical_strategy": "lines", "horizontal_strategy": "lines",
                "snap_tolerance": 3, "join_tolerance": 3,
            }) or []
            if not tables:
                tbl = page.extract_table()
                tables = [tbl] if tbl else []
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
