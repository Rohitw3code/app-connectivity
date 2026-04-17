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
from typing import Optional

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
MAX_PAGES = 10
CHUNK_SIZE = 1800
CHUNK_OVERLAP = 300
MODEL = "gpt-4o-mini"


# ──────────────────────────────────────────────────────────────
# LAYER 2 — REGEX VARIANT GATE
# ──────────────────────────────────────────────────────────────
# Each output field maps to one or more header variants.
# Every regex keyword in a variant must be found somewhere in the text.
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
    "Application Quantum (MW)(ST II)": [
        [r"\bInstalled\b", r"\bCapacity\b", r"\bMW\b"],
        [r"\bConnectivity\b", r"\bQuantum\b", r"\bMW\b"],
    ],
}


INDIA_STATES_UTS = [
    "andhra pradesh", "arunachal pradesh", "assam", "bihar", "chhattisgarh",
    "goa", "gujarat", "haryana", "himachal pradesh", "jharkhand", "karnataka",
    "kerala", "madhya pradesh", "maharashtra", "manipur", "meghalaya", "mizoram",
    "nagaland", "odisha", "punjab", "rajasthan", "sikkim", "tamil nadu", "telangana",
    "tripura", "uttar pradesh", "uttarakhand", "west bengal", "andaman and nicobar islands",
    "chandigarh", "dadra and nagar haveli and daman and diu", "delhi", "jammu and kashmir",
    "ladakh", "lakshadweep", "puducherry",
]


def _contains_any_variant(text: str, variants: list[list[str]]) -> bool:
    for variant_keywords in variants:
        if all(re.search(keyword, text, re.IGNORECASE) for keyword in variant_keywords):
            return True
    return False


def check_page_for_variants(text: str) -> tuple[bool, dict[str, bool]]:
    hits = {
        col: _contains_any_variant(text, variants)
        for col, variants in TARGET_COLUMN_VARIANTS.items()
    }
    return any(hits.values()), hits


def check_chunk_for_variants(chunk: str) -> tuple[bool, dict[str, bool]]:
    hits = {
        col: _contains_any_variant(chunk, variants)
        for col, variants in TARGET_COLUMN_VARIANTS.items()
    }
    return any(hits.values()), hits


# ──────────────────────────────────────────────────────────────
# PYDANTIC MODELS
# ──────────────────────────────────────────────────────────────
class MappedRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    project_location: Optional[str] = Field(None, alias="Project Location")
    state: Optional[str] = Field(None, alias="State")
    substaion: Optional[str] = Field(None, alias="substaion")
    name_of_the_developers: Optional[str] = Field(None, alias="Name of the developers")
    gna_st_ii_application_id: Optional[str] = Field(None, alias="GNA/ST II Application ID")
    lta_application_id: Optional[str] = Field(None, alias="LTA Application ID")
    application_quantum_mw_st_ii: Optional[str] = Field(None, alias="Application Quantum (MW)(ST II)")

class PageResult(BaseModel):
    page_number:  int
    rows_found:   int
    rows:         list[MappedRow]


class PipelineResult(BaseModel):
    pdf_path:              str
    total_pages_extracted: int
    pages_passed_gate:     int
    pages_skipped:         int
    total_rows:            int
    results:               list[PageResult]


# ──────────────────────────────────────────────────────────────
# LAYER 3 — LANGCHAIN EXTRACTION CHAIN
# ──────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a precise data extraction assistant.
You will receive a TEXT CHUNK from a PDF page and a list of column labels detected in this chunk.

Extract table data and return ONLY these output keys:
1) Project Location
2) State
3) substaion
4) Name of the developers
5) GNA/ST II Application ID
6) LTA Application ID
7) Application Quantum (MW)(ST II)

Column-name mapping rules:
- Project Location <- Project Location
- State <- derive from Project Location (state name only)
- substaion <- Connectivity Location (As per Application)
- Name of the developers <- Applicant OR Name of Applicant
- GNA/ST II Application ID <- Application No. & Date OR Application ID
- LTA Application ID <- App. No. & Conn. Quantum (MW) of already granted Connectivity
- Application Quantum (MW)(ST II) <- Installed Capacity (MW) OR Connectivity Quantum (MW)

Extraction rules:
- Extract every visible data row in the chunk.
- Use null if a value is not available.
- Keep values as strings exactly as seen (except LTA leading-zero cleanup is done later).
- Ignore headers, footnotes, and explanatory paragraphs.
- "Name of the developers" must be the applicant/developer company name, not criterion values like "SECI LOA" or "SJVN LOA".

Return JSON only in this exact shape:
{{
    "rows": [
        {{
            "Project Location": "bulandshahr distt, uttar pradesh",
            "State": "uttar pradesh",
            "substaion": "Aligarh (PG)",
            "Name of the developers": "THDC India Limited",
            "GNA/ST II Application ID": "1200003683",
            "LTA Application ID": "0412100008",
            "Application Quantum (MW)(ST II)": "300"
        }}
    ]
}}

If there is no data row in the chunk: {{"rows": []}}
"""

USER_TEMPLATE = "Detected column labels in this chunk: {active_fields}\n\nChunk text:\n{chunk}"


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


def extract_rows_from_chunk(chain, chunk: str, active_fields: list[str]) -> list[dict]:
    try:
        result = chain.invoke({"chunk": chunk, "active_fields": ", ".join(active_fields)})
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
    """Remove overlap duplicates using stable keys and fallback fingerprint."""
    seen, unique = set(), []
    for row in rows:
        gna = str(row.get("GNA/ST II Application ID") or "").strip()
        lta = str(row.get("LTA Application ID") or "").strip()
        loc = str(row.get("Project Location") or "").strip().lower()
        dev = str(row.get("Name of the developers") or "").strip().lower()
        key = (gna, lta, loc, dev) if (gna or lta or loc or dev) else json.dumps(row, sort_keys=True)
        if key not in seen:
            seen.add(key)
            unique.append(row)
    return unique


def _clean_value(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    if value.lower() in {"null", "none", "na", "n/a", "-", "--"}:
        return None
    return value if value else None


def _extract_state(project_location: Optional[str]) -> Optional[str]:
    project_location = _clean_value(project_location)
    if not project_location:
        return None

    lower_text = project_location.lower()
    for state in sorted(INDIA_STATES_UTS, key=len, reverse=True):
        if state in lower_text:
            return state

    if "," in project_location:
        tail = project_location.split(",")[-1].strip(" .")
        return tail.lower() if tail else None

    return None


def _normalize_numeric_id_list(value: Optional[str], strip_leading_zeros: bool = False) -> Optional[str]:
    value = _clean_value(value)
    if not value:
        return None

    ids = re.findall(r"\b\d{6,}\b", value)
    if not ids:
        return value

    if strip_leading_zeros:
        cleaned = [item.lstrip("0") or "0" for item in ids]
    else:
        cleaned = ids

    return ", ".join(cleaned)


def _normalize_developer_name(value: Optional[str]) -> Optional[str]:
    value = _clean_value(value)
    if not value:
        return None

    upper = value.upper()
    blocked_tokens = [" LOA", "CRITERION", "APPLYING"]
    if any(token in upper for token in blocked_tokens):
        return None

    return value


def normalize_rows(rows: list[MappedRow]) -> list[MappedRow]:
    normalized = []
    for row in rows:
        payload = row.model_dump(by_alias=True)
        payload["Project Location"] = _clean_value(payload.get("Project Location"))
        payload["State"] = _extract_state(payload.get("Project Location"))
        payload["substaion"] = _clean_value(payload.get("substaion"))
        payload["Name of the developers"] = _normalize_developer_name(payload.get("Name of the developers"))
        payload["GNA/ST II Application ID"] = _normalize_numeric_id_list(
            payload.get("GNA/ST II Application ID"), strip_leading_zeros=False
        )
        payload["LTA Application ID"] = _normalize_numeric_id_list(
            payload.get("LTA Application ID"), strip_leading_zeros=True
        )
        payload["Application Quantum (MW)(ST II)"] = _clean_value(
            payload.get("Application Quantum (MW)(ST II)")
        )
        normalized.append(MappedRow.model_validate(payload))
    return normalized


def validate_rows(raw_rows: list[dict]) -> list[MappedRow]:
    validated = []
    for row in raw_rows:
        try:
            validated.append(MappedRow.model_validate(row))
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
    llm = ChatOpenAI(model=MODEL, temperature=0, api_key=api_key)
    chain = build_chain(llm)
    splitter = build_splitter()

    pages = extract_pages(pdf_path)
    results = []
    pages_passed = 0
    pages_skipped = 0

    for page in pages:
        pnum = page["page_number"]
        text = page["text"]

        # ── Layer 2: Page gate ─────────────────────────────────
        passed, page_hits = check_page_for_variants(text)
        if not passed:
            print(f"[Layer 2] Page {pnum:>3}: SKIP  — no target column variants found")
            pages_skipped += 1
            continue

        page_fields = [name for name, ok in page_hits.items() if ok]
        print(f"[Layer 2] Page {pnum:>3}: PASS ✓ — found variants for: {page_fields}")
        pages_passed += 1

        # ── Layer 3: Chunk gate → extraction → normalize ──────
        chunks = splitter.split_text(text)
        all_raw = []

        print(f"  [Layer 3] {len(chunks)} chunk(s) scanned with regex gate")
        for idx, chunk in enumerate(chunks):
            chunk_ok, chunk_hits = check_chunk_for_variants(chunk)
            if not chunk_ok:
                print(f"    Chunk {idx+1}/{len(chunks)} ({len(chunk)} chars) … skipped (no variants)")
                continue

            active_fields = [name for name, ok in chunk_hits.items() if ok]
            print(
                f"    Chunk {idx+1}/{len(chunks)} ({len(chunk)} chars) … extracting for {active_fields}",
                end="",
                flush=True,
            )
            raw = extract_rows_from_chunk(chain, chunk, active_fields)
            print(f" → {len(raw)} row(s)")
            all_raw.extend(raw)

        all_raw = deduplicate(all_raw)
        validated = validate_rows(all_raw)
        normalized = normalize_rows(validated)

        print(f"  → {len(normalized)} unique normalized row(s) on page {pnum}\n")
        results.append(PageResult(
            page_number=pnum,
            rows_found=len(normalized),
            rows=normalized,
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

    parser = argparse.ArgumentParser(description="PDF mapped extraction — chunk regex gate + GPT")
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

    print("=" * 64)
    print("  PDF MAPPED EXTRACTION PIPELINE (Chunk Regex Gate + GPT-4o-mini)")
    print("=" * 64)

    print(f"PDF selected: {pdf_path}")

    result = run_pipeline(str(pdf_path), key)

    print("=" * 64)
    print("SUMMARY")
    print(f"  Pages extracted  : {result.total_pages_extracted}")
    print(f"  Pages passed gate: {result.pages_passed_gate}")
    print(f"  Pages skipped    : {result.pages_skipped}")
    print(f"  Total rows parsed: {result.total_rows}")
    print("=" * 64)

    # Serialise with by_alias=True so JSON keys match the mapped output column names
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