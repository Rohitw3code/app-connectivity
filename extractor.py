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
    nature_of_applicant: Optional[str] = Field(None, alias="Nature of Applicant")
    mode_criteria_for_applying: Optional[str] = Field(None, alias="Mode(Criteria for applying)")
    applied_start_connectivity_date: Optional[str] = Field(
        None, alias="Applied Start of Connectivity sought by developer date"
    )
    application_submission_date: Optional[str] = Field(None, alias="Application/Submission Date")

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
8) Nature of Applicant
9) Mode(Criteria for applying)
10) Applied Start of Connectivity sought by developer date
11) Application/Submission Date

Column-name mapping rules:
- Project Location <- Project Location
- State <- derive from Project Location (state name only)
- substaion <- Connectivity Location (As per Application)
- Name of the developers <- Applicant OR Name of Applicant
- GNA/ST II Application ID <- Application No. & Date OR Application ID
- LTA Application ID <- App. No. & Conn. Quantum (MW) of already granted Connectivity
- Application Quantum (MW)(ST II) <- Installed Capacity (MW) OR Connectivity Quantum (MW)
- Nature of Applicant <- Nature of Applicant
- Mode(Criteria for applying) <- Criterion for applying
- Applied Start of Connectivity sought by developer date <- Start Date of Connectivity (As per Application)
- Application/Submission Date <- Application No. & Date OR Submission Date (extract only date)

Extraction rules:
- Extract every visible data row in the chunk.
- Use null if a value is not available.
- It is not required that all columns exist in each row; extract what is present and keep others as null.
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
            "Application Quantum (MW)(ST II)": "300",
            "Nature of Applicant": "Generator (Solar)",
            "Mode(Criteria for applying)": "SECI LOA",
            "Applied Start of Connectivity sought by developer date": "16.04.2026",
            "Application/Submission Date": "15.02.2024"
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


def build_fallback_splitter() -> RecursiveCharacterTextSplitter:
    return RecursiveCharacterTextSplitter(
        chunk_size=900,
        chunk_overlap=150,
        separators=["\n\n", "\n", " ", ""],
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


def extract_rows_with_fallback(
    chain,
    chunk: str,
    active_fields: list[str],
    fallback_splitter: RecursiveCharacterTextSplitter,
) -> list[dict]:
    """Try extraction on full chunk first, then retry with smaller sub-chunks if needed."""
    primary_rows = extract_rows_from_chunk(chain, chunk, active_fields)
    if primary_rows or len(chunk) < 700:
        return primary_rows

    fallback_rows: list[dict] = []
    sub_chunks = fallback_splitter.split_text(chunk)
    for sub_chunk in sub_chunks:
        sub_rows = extract_rows_from_chunk(chain, sub_chunk, active_fields)
        fallback_rows.extend(sub_rows)

    return fallback_rows


def save_page_chunks(chunks_dir: Path, pdf_path: str, page_number: int, chunks: list[str]) -> None:
    chunks_dir.mkdir(parents=True, exist_ok=True)
    pdf_stem = Path(pdf_path).stem
    chunk_file = chunks_dir / f"{pdf_stem}_page_{page_number}.json"
    payload = {
        "pdf": str(pdf_path),
        "page_number": page_number,
        "chunks_count": len(chunks),
        "chunks": [
            {
                "chunk_index": idx + 1,
                "char_length": len(chunk),
                "text": chunk,
            }
            for idx, chunk in enumerate(chunks)
        ],
    }
    with open(chunk_file, "w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2, ensure_ascii=False)


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


def _extract_date(value: Optional[str]) -> Optional[str]:
    value = _clean_value(value)
    if not value:
        return None

    patterns = [
        r"\b\d{2}[./-]\d{2}[./-]\d{4}\b",
        r"\b\d{4}[./-]\d{2}[./-]\d{2}\b",
        r"\b\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4}\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, value)
        if match:
            return match.group(0)

    return None


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
        payload["Nature of Applicant"] = _clean_value(payload.get("Nature of Applicant"))
        payload["Mode(Criteria for applying)"] = _clean_value(payload.get("Mode(Criteria for applying)"))
        payload["Applied Start of Connectivity sought by developer date"] = _extract_date(
            payload.get("Applied Start of Connectivity sought by developer date")
        )
        payload["Application/Submission Date"] = _extract_date(
            payload.get("Application/Submission Date")
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
def run_pipeline(pdf_path: str, api_key: str, chunks_dir: Path) -> PipelineResult:
    llm = ChatOpenAI(model=MODEL, temperature=0, api_key=api_key)
    chain = build_chain(llm)
    splitter = build_splitter()
    fallback_splitter = build_fallback_splitter()

    pages = extract_pages(pdf_path)
    results = []
    pages_passed = 0
    pages_skipped = 0

    for page in pages:
        pnum = page["page_number"]
        text = page["text"]
        chunks = splitter.split_text(text)
        save_page_chunks(chunks_dir, pdf_path, pnum, chunks)

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
            raw = extract_rows_with_fallback(chain, chunk, active_fields, fallback_splitter)
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
    parser.add_argument("--chunks-dir", default="page_chunks", help="Folder to save per-page chunk JSON files")
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

    chunks_dir = Path(args.chunks_dir).resolve()
    result = run_pipeline(str(pdf_path), key, chunks_dir)

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
    print(f"Chunk files saved → {chunks_dir}")

    # Preview first 3 rows
    if result.results:
        first = result.results[0]
        print(f"\nPreview — Page {first.page_number}:")
        for row in first.rows[:3]:
            print(json.dumps(row.model_dump(by_alias=True), indent=2, ensure_ascii=False))