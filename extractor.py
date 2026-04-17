"""
PDF Table Extraction Pipeline
==============================
Layer 1 : pdfplumber  — extract first 10 pages
Layer 2 : Regex gate  — detect required column headers; skip page if absent
Layer 3 : LangChain + GPT-4o-mini + Pydantic — chunk each page, extract rows

Usage:
    python pdf_pipeline.py --pdf test.pdf --api-key sk-...
    (or set OPENAI_API_KEY env variable)
"""

from __future__ import annotations

import re
import json
import argparse
import os
from pathlib import Path
from typing import Optional, List

import pdfplumber
from pydantic import BaseModel, ConfigDict, Field
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import JsonOutputParser

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────
MAX_PAGES     = 10
CHUNK_SIZE    = 1800   # characters — wide enough to capture a few rows
CHUNK_OVERLAP = 300    # overlap so rows split across chunk boundaries are recovered
MODEL         = "gpt-4o-mini"


# ──────────────────────────────────────────────────────────────
# LAYER 2 — REGEX COLUMN GATE
#
# pdfplumber renders multi-column table headers row-by-row so a
# two-line header like "Connectivity Quantum (MW)" is split across
# lines with other column fragments in between.  Phrase matching
# fails in this case.  Solution: check each required column as a
# SET of keywords that must ALL appear anywhere in the page text.
# ──────────────────────────────────────────────────────────────
COLUMN_KEYWORDS: dict[str, list[str]] = {
    "Sl.No.":                                           [r"\bSl\b",          r"\bNo\b"],
    "Application ID":                                   [r"\bApplication\b", r"\bID\b"],
    "Applicant":                                        [r"\bApplicant\b"],
    "Project Location":                                 [r"\bProject\b",     r"\bLocation\b"],
    "Submission Date":                                  [r"\bSubmission\b",  r"\bDate\b"],
    "Nature of Applicant":                              [r"\bNature\b",      r"\bApplicant\b"],
    "Criterion for applying":                           [r"\bCriterion\b",   r"\bapplying\b"],
    "Connectivity Quantum (MW)":                        [r"\bConnectivi",     r"\bQuantum\b",  r"\bMW\b"],
    "Start Date for Connectivity (As per Application)": [r"\bStart\b",       r"\bDate\b",     r"\bConnectivi"],
    "Connectivity location (As per Application)":       [r"\bConnectivi",     r"\blocation\b"],
}


def check_columns(text: str) -> tuple[bool, dict[str, bool]]:
    """
    For each required column verify every keyword is found somewhere
    in the page text (case-insensitive).
    Returns (all_present, per-column hit map).
    """
    hits: dict[str, bool] = {}
    for col, keywords in COLUMN_KEYWORDS.items():
        hits[col] = all(re.search(kw, text, re.IGNORECASE) for kw in keywords)
    return all(hits.values()), hits


# ──────────────────────────────────────────────────────────────
# PYDANTIC MODELS
# ──────────────────────────────────────────────────────────────
class TableRow(BaseModel):
    """One data row extracted from the PDF table."""
    model_config = ConfigDict(populate_by_name=True)

    sl_no:                   Optional[str] = Field(None, alias="Sl.No.")
    application_id:          Optional[str] = Field(None, alias="Application ID")
    applicant:               Optional[str] = Field(None, alias="Applicant")
    project_location:        Optional[str] = Field(None, alias="Project Location")
    submission_date:         Optional[str] = Field(None, alias="Submission Date")
    nature_of_applicant:     Optional[str] = Field(None, alias="Nature of Applicant")
    criterion_for_applying:  Optional[str] = Field(None, alias="Criterion for applying")
    connectivity_quantum_mw: Optional[str] = Field(None, alias="Connectivity Quantum (MW)")
    start_date:              Optional[str] = Field(
        None, alias="Start Date for Connectivity (As per Application)"
    )
    connectivity_location:   Optional[str] = Field(
        None, alias="Connectivity location (As per Application)"
    )

class PageResult(BaseModel):
    page_number:  int
    rows_found:   int
    rows:         List[TableRow]


class PipelineResult(BaseModel):
    pdf_path:              str
    total_pages_extracted: int
    pages_passed_gate:     int
    pages_skipped:         int
    total_rows:            int
    results:               List[PageResult]


# ──────────────────────────────────────────────────────────────
# LAYER 3 — LANGCHAIN EXTRACTION CHAIN
# ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a precise data extraction assistant for PDF tables.
The user sends a TEXT CHUNK extracted from a PDF page that contains one or more tables \
with these columns:
  1. Sl.No.
  2. Application ID
  3. Applicant
  4. Project Location
  5. Submission Date
  6. Nature of Applicant
  7. Criterion for applying
  8. Connectivity Quantum (MW)
  9. Start Date for Connectivity (As per Application)
  10. Connectivity location (As per Application)

RULES:
- Extract EVERY data row visible in the chunk. A page may have multiple tables — extract ALL.
- Use the EXACT column names listed above as JSON keys.
- Use null for any field not present or not readable.
- Numbers and dates: keep as strings exactly as they appear in the text.
- Ignore header rows, section headings, footnotes, and body paragraphs.
- Return a JSON object in this exact shape — no markdown, no explanation:

{{
  "rows": [
    {{
      "Sl.No.": "1",
      "Application ID": "2200000001",
      "Applicant": "ABC Solar Pvt Ltd",
      "Project Location": "Rajasthan",
      "Submission Date": "15.02.2024",
      "Nature of Applicant": "Generator (Solar)",
      "Criterion for applying": "SECI LOA",
      "Connectivity Quantum (MW)": "200",
      "Start Date for Connectivity (As per Application)": "16.04.2026",
      "Connectivity location (As per Application)": "Fatehgarh-IV PS"
    }}
  ]
}}

If no data rows are present in this chunk return: {{"rows": []}}
"""

USER_TEMPLATE = "Extract all table rows from this PDF chunk:\n\n{chunk}"


def build_chain(llm: ChatOpenAI) -> object:
    """Build a LangChain LCEL chain: prompt | llm | json_parser."""
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("human",  USER_TEMPLATE),
    ])
    parser = JsonOutputParser()
    return prompt | llm | parser


def build_splitter() -> RecursiveCharacterTextSplitter:
    return RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", " ", ""],  # prefer splitting on blank lines first
    )


def extract_rows_from_chunk(chain, chunk: str) -> list[dict]:
    """Invoke the LangChain chain on one chunk; return list of raw row dicts."""
    try:
        result = chain.invoke({"chunk": chunk})
        if isinstance(result, dict):
            rows = result.get("rows", [])
        elif isinstance(result, list):
            rows = result
        else:
            rows = []
        return rows if isinstance(rows, list) else []
    except Exception as e:
        print(f"      [Chain error] {e}")
        return []


def deduplicate(rows: list[dict]) -> list[dict]:
    """
    Remove rows duplicated by chunk overlap.
    Key = (Sl.No., Application ID) — both must be non-empty.
    Falls back to full-row fingerprint when keys are missing.
    """
    seen, unique = set(), []
    for row in rows:
        sl  = str(row.get("Sl.No.")         or "").strip()
        aid = str(row.get("Application ID") or "").strip()
        key = (sl, aid) if (sl or aid) else json.dumps(row, sort_keys=True)
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique


def validate_rows(raw_rows: list[dict]) -> list[TableRow]:
    """Run each raw dict through Pydantic; skip invalid rows with a warning."""
    validated = []
    for row in raw_rows:
        try:
            validated.append(TableRow.model_validate(row))
        except Exception as e:
            print(f"      [Pydantic skip] {e}")
    return validated


# ──────────────────────────────────────────────────────────────
# LAYER 1 — PDF EXTRACTION
# ──────────────────────────────────────────────────────────────
def extract_pages(pdf_path: str) -> list[dict]:
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        total = len(pdf.pages)
        limit = min(MAX_PAGES, total)
        print(f"\n[Layer 1] PDF → {total} pages total. Processing first {limit}.\n")
        for i in range(limit):
            text = pdf.pages[i].extract_text() or ""
            pages.append({"page_number": i + 1, "text": text})
    return pages


# ──────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────────────────────
def run_pipeline(pdf_path: str, api_key: str) -> PipelineResult:
    llm   = ChatOpenAI(model=MODEL, temperature=0, api_key=api_key)
    chain = build_chain(llm)
    splitter = build_splitter()

    pages        = extract_pages(pdf_path)
    results      = []
    pages_passed = 0
    pages_skipped= 0

    for page in pages:
        pnum = page["page_number"]
        text = page["text"]

        # ── Layer 2: Gate ───────────────────────────────────────
        passed, col_hits = check_columns(text)
        if not passed:
            missing = [c for c, hit in col_hits.items() if not hit]
            print(f"[Layer 2] Page {pnum:>3}: SKIP  — missing columns: {missing}")
            pages_skipped += 1
            continue

        print(f"[Layer 2] Page {pnum:>3}: PASS ✓")
        pages_passed += 1

        # ── Layer 3: Chunk → LangChain → Pydantic ───────────────
        chunks   = splitter.split_text(text)
        all_raw  = []

        print(f"  [Layer 3] {len(chunks)} chunk(s) → GPT-4o-mini")
        for idx, chunk in enumerate(chunks):
            print(f"    Chunk {idx+1}/{len(chunks)} ({len(chunk)} chars) … ", end="", flush=True)
            raw = extract_rows_from_chunk(chain, chunk)
            print(f"{len(raw)} row(s)")
            all_raw.extend(raw)

        all_raw   = deduplicate(all_raw)
        validated = validate_rows(all_raw)

        print(f"  → {len(validated)} unique validated row(s) on page {pnum}\n")
        results.append(PageResult(
            page_number=pnum,
            rows_found=len(validated),
            rows=validated,
        ))

    total_rows = sum(r.rows_found for r in results)
    return PipelineResult(
        pdf_path=pdf_path,
        total_pages_extracted=len(pages),
        pages_passed_gate=pages_passed,
        pages_skipped=pages_skipped,
        total_rows=total_rows,
        results=results,
    )


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)

    parser = argparse.ArgumentParser(description="PDF Table Extraction — LangChain + GPT-4o-mini + Pydantic")
    parser.add_argument("--source-dir", default="source_1", help="Folder containing PDF files")
    parser.add_argument("--pdf",     default=None,           help="Path to PDF file (optional; defaults to first PDF in source-dir)")
    parser.add_argument("--api-key", default=None,           help="OpenAI API key (or set OPENAI_API_KEY)")
    parser.add_argument("--output",  default="output.json",  help="Output JSON file")
    args = parser.parse_args()

    if args.pdf:
        pdf_path = Path(args.pdf).resolve()
    else:
        source_dir = Path(args.source_dir).resolve()
        pdfs = sorted(p for p in source_dir.glob("*.pdf") if p.is_file())
        if not pdfs:
            raise SystemExit(f"ERROR: No PDF found in {source_dir}. Use --pdf to specify a file.")
        pdf_path = pdfs[0]

    key = args.api_key or os.environ.get("OPENAI_API_KEY", "")
    if not key:
        raise SystemExit("ERROR: OpenAI API key required. Use --api-key or set OPENAI_API_KEY in .env.")

    print("=" * 62)
    print("  PDF EXTRACTION PIPELINE  (LangChain + GPT-4o-mini + Pydantic)")
    print("=" * 62)

    print(f"PDF selected: {pdf_path}")

    result = run_pipeline(str(pdf_path), key)

    print("=" * 62)
    print("SUMMARY")
    print(f"  Pages extracted  : {result.total_pages_extracted}")
    print(f"  Pages passed gate: {result.pages_passed_gate}")
    print(f"  Pages skipped    : {result.pages_skipped}")
    print(f"  Total rows parsed: {result.total_rows}")
    print("=" * 62)

    # Serialise with by_alias=True so JSON keys match the original column names
    output = result.model_dump()
    for i, page_res in enumerate(output["results"]):
        page_res["rows"] = [
            r.model_dump(by_alias=True)
            for r in result.results[i].rows
        ]

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    print(f"\nOutput saved → {args.output}")

    # Preview first 3 rows
    if result.results:
        first = result.results[0]
        print(f"\nPreview — Page {first.page_number}:")
        for row in first.rows[:3]:
            print(json.dumps(row.model_dump(by_alias=True), indent=2, ensure_ascii=False))