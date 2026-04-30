"""
cmets_handler/extraction.py — PDF reading & LLM extraction sub-layers
=======================================================================
Sub-layer A: pdfplumber page text extraction
Sub-layer B: regex column-header gate (delegates to gate.py)
Sub-layer C: LLM row extraction (sends page text → GPT → parsed rows)

Edit this file to change how pages are read from PDFs or how the LLM
response is parsed.
"""

from __future__ import annotations

import json
import re
from typing import Optional

import pdfplumber

from config import MAX_PAGES, MODEL
from llm_client import call_llm, extract_text_from_response
from pipeline.cmets_handler.prompts import SYSTEM_PROMPT, USER_TEMPLATE
from pipeline.cmets_handler.gate import page_passes_gate
from pipeline.cmets_handler.models import MappedRow, PageResult, PipelineResult
from pipeline.cmets_handler.normalization import validate_rows, normalize, dedup_dicts
from pipeline.cmets_handler.voltage_extractor import (
    extract_voltage_from_row,
    extract_voltage_from_page,
)


# ── Sub-layer A: PDF page extraction ──────────────────────────────────────────

def extract_pages(pdf_path: str) -> list[dict]:
    """Read page text from *pdf_path* using pdfplumber.

    Returns a list of ``{"page_number": int, "text": str}`` dicts.
    """
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        limit = total if MAX_PAGES == -1 else min(MAX_PAGES, total)
        label = "all" if MAX_PAGES == -1 else f"first {limit}"
        print(f"  [A] {total} pages total — processing {label}")
        for i in range(limit):
            text = pdf.pages[i].extract_text() or ""
            pages.append({"page_number": i + 1, "text": text})
    return pages


# ── Sub-layer C: LLM row extraction ──────────────────────────────────────────

def _parse_json(text: str) -> dict | list:
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


def llm_extract_rows(
    page_text: str,
    active_fields: list[str],
    vm_mode: bool,
    api_key: Optional[str],
    llm_script_path: Optional[str],
) -> list[dict]:
    """Send a single page to the LLM and return raw row dicts."""
    prompt = {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": USER_TEMPLATE.format(
                active_fields=", ".join(active_fields),
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
        return rows if isinstance(rows, list) else []
    except Exception as exc:
        print(f"      [LLM error] {exc}")
        return []


# ── Combined: run all sub-layers for one PDF ──────────────────────────────────

def run_single_pdf(
    pdf_path: str,
    api_key: Optional[str],
    vm_mode: bool = False,
    llm_script_path: Optional[str] = None,
) -> PipelineResult:
    """Run sub-layers A→B→C for a single PDF and return PipelineResult."""
    pages         = extract_pages(pdf_path)
    results       = []
    pages_passed  = 0
    pages_skipped = 0

    for page in pages:
        pnum = page["page_number"]
        text = page["text"]

        # Sub-layer B: regex gate
        passed, active_fields = page_passes_gate(text)
        if not passed:
            print(f"  [B] Page {pnum:>3}: SKIP")
            pages_skipped += 1
            continue

        print(f"  [B] Page {pnum:>3}: PASS ✓  fields={active_fields}")
        pages_passed += 1

        # Sub-layer C: LLM extraction
        print(f"  [C] Page {pnum} ({len(text)} chars) → LLM …", end="", flush=True)
        raw_rows   = llm_extract_rows(text, active_fields, vm_mode, api_key, llm_script_path)
        print(f" {len(raw_rows)} raw")

        raw_rows   = dedup_dicts(raw_rows)
        validated  = validate_rows(raw_rows)
        normalized = normalize(validated)
        print(f"         → {len(normalized)} normalised rows")

        # Sub-layer V: Contextual voltage extraction (per row)
        # Primary: LLM-provided Voltage field + row cell scan (substation, location…)
        # Fallback: page-level voltage if row gives nothing
        page_voltage = extract_voltage_from_page(text)
        injected: list[MappedRow] = []
        for row in normalized:
            d = row.model_dump(by_alias=True)
            row_voltage = extract_voltage_from_row(d) or page_voltage
            d["Voltage"] = row_voltage
            injected.append(MappedRow.model_validate(d))
        normalized = injected

        if page_voltage:
            print(f"  [V] Page {pnum}: page-level voltage → {page_voltage}")

        results.append(PageResult(page_number=pnum, rows_found=len(normalized), rows=normalized))

    return PipelineResult(
        pdf_path=pdf_path,
        total_pages_extracted=len(pages),
        pages_passed_gate=pages_passed,
        pages_skipped=pages_skipped,
        total_rows=sum(r.rows_found for r in results),
        results=results,
    )
