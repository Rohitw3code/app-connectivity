"""
cmets_handler/extraction.py — PDF reading & per-page table extraction
=======================================================================
Sub-layer A: pdfplumber page text + table extraction (per page)
Sub-layer B: regex column-header gate (delegates to gate.py)
Sub-layer C: table extraction → regex enrichment → LLM reasoning

Strategy for each page:
  1. Extract page text AND tables via pdfplumber
  2. Gate check (regex detection) — does page contain CMETS data?
  3. If tables found:
     a) Check for GNARE tables (skip them)
     b) Map headers → canonical columns (header variant matching)
     c) Apply regex-based context enrichment (Enhancement 5.2, PSP, etc.)
     d) Filter blocklisted rows (Nature of Applicant)
     e) Send to LLM for semantic enrichment of remaining gaps
  4. If NO tables found but gate passed → full LLM extraction fallback

This replaces the old approach of sending raw page text to LLM for full
extraction. Now LLM is only used for targeted enrichment or as a fallback.
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
from pipeline.cmets_handler.table_extraction import (
    extract_all_tables_from_page,
    extract_table_rows_from_page,
    enrich_rows_with_context,
    table_is_gnare,
    row_is_blocklisted,
    llm_enrich_rows,
    llm_full_extract,
)


# ── Sub-layer A: PDF page extraction with tables ────────────────────────────

def extract_pages(pdf_path: str) -> list[dict]:
    """Read page text AND extract tables from each page using pdfplumber.

    Returns a list of:
        {"page_number": int, "text": str, "table_rows": list[dict]}
    """
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        limit = total if MAX_PAGES == -1 else min(MAX_PAGES, total)
        label = "all" if MAX_PAGES == -1 else f"first {limit}"
        print(f"  [A] {total} pages total — processing {label}")

        for i in range(limit):
            page = pdf.pages[i]
            text = page.extract_text() or ""

            # Extract structured table rows from this page
            table_rows = extract_table_rows_from_page(page)

            # Check if any table on this page is a GNARE table (skip)
            raw_tables = extract_all_tables_from_page(page)
            is_gnare = any(table_is_gnare(tbl) for tbl in raw_tables)

            if is_gnare:
                print(f"  [A] Page {i + 1:>3}: GNARE table detected — skipping table rows")
                table_rows = []  # clear extracted rows for GNARE tables

            pages.append({
                "page_number": i + 1,
                "text": text,
                "table_rows": table_rows,
            })

            if table_rows:
                print(f"  [A] Page {i + 1:>3}: {len(table_rows)} table rows extracted")
            else:
                print(f"  [A] Page {i + 1:>3}: no usable tables")

    return pages


# ── Sub-layer C helpers ───────────────────────────────────────────────────────

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
    """Send a single page to the LLM for full extraction (fallback only)."""
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
    """Run sub-layers A→B→C for a single PDF and return PipelineResult.

    Flow per page:
      A) Extract text + tables via pdfplumber
      B) Gate check (regex column detection + blocklist)
      C) If table rows found:
           → Enrich with regex context (Enhancement 5.2, PSP)
           → Filter blocklisted rows
           → LLM semantic enrichment
         If NO table rows found (but gate passed):
           → Full LLM extraction fallback
    """
    pages         = extract_pages(pdf_path)
    results       = []
    pages_passed  = 0
    pages_skipped = 0

    for page in pages:
        pnum       = page["page_number"]
        text       = page["text"]
        table_rows = page["table_rows"]

        # Sub-layer B: regex gate
        passed, active_fields = page_passes_gate(text)
        if not passed:
            print(f"  [B] Page {pnum:>3}: SKIP")
            pages_skipped += 1
            continue

        print(f"  [B] Page {pnum:>3}: PASS ✓  fields={active_fields}")
        pages_passed += 1

        # Sub-layer C: Extraction
        if table_rows:
            # ── Table-based path ──

            # C1: Apply regex-based context enrichment
            print(f"  [C] Page {pnum}: {len(table_rows)} table rows → context enrichment …")
            enriched = enrich_rows_with_context(table_rows, text)

            # C2: Filter blocklisted rows
            filtered = []
            for row in enriched:
                if row_is_blocklisted(row):
                    nature = row.get("Nature of Applicant", "")
                    print(f"      [SKIP] Blocklisted: {nature}")
                    continue
                filtered.append(row)

            # C3: LLM semantic enrichment (targeted — sends table data + page text)
            if filtered:
                print(f"  [C] Page {pnum}: {len(filtered)} rows → LLM enrichment …",
                      end="", flush=True)
                raw_rows = llm_enrich_rows(
                    filtered, text, vm_mode, api_key, llm_script_path
                )
                print(f" {len(raw_rows)} enriched")
            else:
                raw_rows = []
        else:
            # ── Full LLM fallback (no tables found) ──
            print(f"  [C] Page {pnum}: no tables → LLM full extraction ({len(text)} chars) …",
                  end="", flush=True)
            raw_rows = llm_full_extract(
                text, active_fields, vm_mode, api_key, llm_script_path
            )
            print(f" {len(raw_rows)} raw")

        # Post-processing: dedup → validate → normalize (same as before)
        raw_rows   = dedup_dicts(raw_rows)
        validated  = validate_rows(raw_rows)
        normalized = normalize(validated)
        print(f"         → {len(normalized)} normalised rows")

        results.append(PageResult(page_number=pnum, rows_found=len(normalized), rows=normalized))

    return PipelineResult(
        pdf_path=pdf_path,
        total_pages_extracted=len(pages),
        pages_passed_gate=pages_passed,
        pages_skipped=pages_skipped,
        total_rows=sum(r.rows_found for r in results),
        results=results,
    )
