"""
PDF Table Extraction Pipeline
==============================
Layer 1 : pdfplumber  — extract first 10 pages
Layer 2 : Regex gate  — detect required column headers; skip page if absent
Layer 3 : LangChain + GPT-4o-mini + Pydantic — chunk each page, extract rows

Usage:
    python pdf_pipeline.py --pdf test.pdf --api-key sk-...
    (or set OPENAI_API_KEY env variable)
    Set VM=true to use llm_client.bat path instead of direct OpenAI API.
"""

from __future__ import annotations

import re
import json
from pathlib import Path
from typing import Optional

import pdfplumber
from pydantic import BaseModel, ConfigDict, Field
from langchain_text_splitters import RecursiveCharacterTextSplitter

from config import CHUNK_OVERLAP, CHUNK_SIZE, MAX_PAGES
from data_extraction import extract_rows_with_fallback

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
    application_id_under_enhancement_5_2_or_revision: Optional[str] = Field(
        None, alias="Application ID under Enhancement 5.2 or revision"
    )
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


def build_splitter() -> RecursiveCharacterTextSplitter:
    return RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=["\n\n", "\n", " ", ""],  # prefer splitting on blank lines first
    )


def split_page_into_fixed_chunks(
    text: str,
    num_chunks: int = 4,
    overlap_chars: int = 180,
) -> list[dict]:
    """Split a page into exactly num_chunks chunks with explicit overlap metadata."""
    src = text or ""
    if num_chunks <= 0:
        return [{"chunk_index": 1, "start": 0, "end": len(src), "char_length": len(src), "text": src}]

    if not src:
        return [
            {
                "chunk_index": i + 1,
                "start": 0,
                "end": 0,
                "char_length": 0,
                "text": "",
                "overlap_with_previous": 0,
            }
            for i in range(num_chunks)
        ]

    n = len(src)
    chunk_size = max(1, (n + num_chunks - 1) // num_chunks)
    chunks: list[dict] = []

    for i in range(num_chunks):
        nominal_start = i * chunk_size
        nominal_end = n if i == num_chunks - 1 else min(n, (i + 1) * chunk_size)

        start = max(0, nominal_start - (overlap_chars if i > 0 else 0))
        end = min(n, nominal_end + (overlap_chars if i < num_chunks - 1 else 0))

        if chunks and start >= chunks[-1]["end"]:
            # Ensure adjacent chunks overlap by at least one char.
            start = max(0, chunks[-1]["end"] - 1)

        chunk_text = src[start:end].strip()
        chunks.append(
            {
                "chunk_index": i + 1,
                "start": start,
                "end": end,
                "char_length": len(chunk_text),
                "text": chunk_text,
            }
        )

    for i, chunk in enumerate(chunks):
        if i == 0:
            chunk["overlap_with_previous"] = 0
        else:
            prev = chunks[i - 1]
            chunk["overlap_with_previous"] = max(0, prev["end"] - chunk["start"])

    return chunks

def build_fallback_splitter() -> RecursiveCharacterTextSplitter:
    return RecursiveCharacterTextSplitter(
        chunk_size=900,
        chunk_overlap=150,
        separators=["\n\n", "\n", " ", ""],
    )


def save_page_chunks(chunks_dir: Path, pdf_path: str, page_number: int, chunks: list[dict]) -> None:
    chunks_dir.mkdir(parents=True, exist_ok=True)
    pdf_stem = Path(pdf_path).stem
    chunk_file = chunks_dir / f"{pdf_stem}_page_{page_number}.json"
    payload = {
        "pdf": str(pdf_path),
        "page_number": page_number,
        "chunks_count": len(chunks),
        "chunks": chunks,
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


def _extract_numeric_ids(value: Optional[str]) -> list[str]:
    value = _clean_value(value)
    if not value:
        return []
    return re.findall(r"\b\d{6,}\b", value)


def _is_lta_id(value: str) -> bool:
    return str(value).startswith("04")


def _has_stage_ii_context(*values: Optional[str]) -> bool:
    text = " ".join(str(v or "") for v in values).lower()
    return bool(re.search(r"\b(stage\s*ii|st\s*ii|gna/st\s*ii|gna\s*st\s*ii)\b", text, flags=re.IGNORECASE))


def _has_enhancement_context(*values: Optional[str]) -> bool:
    text = " ".join(str(v or "") for v in values).lower()
    return bool(re.search(r"\b(5\.?2|regulation\s*5\.?2|enhancement|revision)\b", text, flags=re.IGNORECASE))


def _pick_gna_preferred_id(candidates: list[str], prefer_stage_ii: bool) -> Optional[str]:
    if not candidates:
        return None

    if prefer_stage_ii:
        for candidate in candidates:
            if not _is_lta_id(candidate):
                return candidate
        return None

    non_lta = [candidate for candidate in candidates if not _is_lta_id(candidate)]
    if non_lta:
        return non_lta[0]
    return candidates[0]


def _derive_enhancement_application_id(
    enhancement_value: Optional[str],
    gna_value: Optional[str],
    lta_value: Optional[str],
    mode_value: Optional[str],
) -> Optional[str]:
    if not _has_enhancement_context(enhancement_value, gna_value, lta_value, mode_value):
        return None

    stage_ii_context = _has_stage_ii_context(enhancement_value, gna_value, lta_value, mode_value)

    enhancement_ids = _extract_numeric_ids(enhancement_value)
    gna_ids = _extract_numeric_ids(gna_value)
    lta_ids = _extract_numeric_ids(lta_value)

    # Prefer explicit enhancement/application-id column values for regulation 5.2 rows.
    candidate = _pick_gna_preferred_id(enhancement_ids, prefer_stage_ii=stage_ii_context)
    if candidate:
        return candidate

    # Fallback to GNA/ST-II column.
    candidate = _pick_gna_preferred_id(gna_ids, prefer_stage_ii=stage_ii_context)
    if candidate:
        return candidate

    # Last fallback from App. No. & Conn. Quantum (already granted):
    # if single ID starts with 04 treat as LTA (skip); else treat as GNA.
    if len(lta_ids) == 1:
        only = lta_ids[0]
        return None if _is_lta_id(only) else only

    return _pick_gna_preferred_id(lta_ids, prefer_stage_ii=stage_ii_context)


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
        raw_gna = payload.get("GNA/ST II Application ID")
        raw_lta = payload.get("LTA Application ID")
        raw_mode = payload.get("Mode(Criteria for applying)")
        raw_enhancement = payload.get("Application ID under Enhancement 5.2 or revision")

        payload["Project Location"] = _clean_value(payload.get("Project Location"))
        payload["State"] = _extract_state(payload.get("Project Location"))
        payload["substaion"] = _clean_value(payload.get("substaion"))
        payload["Name of the developers"] = _normalize_developer_name(payload.get("Name of the developers"))
        payload["GNA/ST II Application ID"] = _normalize_numeric_id_list(
            raw_gna, strip_leading_zeros=False
        )
        payload["LTA Application ID"] = _normalize_numeric_id_list(
            raw_lta, strip_leading_zeros=True
        )
        payload["Application ID under Enhancement 5.2 or revision"] = _derive_enhancement_application_id(
            enhancement_value=raw_enhancement,
            gna_value=raw_gna,
            lta_value=raw_lta,
            mode_value=raw_mode,
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
        limit = total if MAX_PAGES == -1 else min(MAX_PAGES, total)
        if MAX_PAGES == -1:
            print(f"\n[Layer 1] PDF → {total} pages total. Processing all pages.\n")
        else:
            print(f"\n[Layer 1] PDF → {total} pages total. Processing first {limit}.\n")
        for i in range(limit):
            text = pdf.pages[i].extract_text() or ""
            pages.append({"page_number": i + 1, "text": text})
    return pages


# ──────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ──────────────────────────────────────────────────────────────
def run_pipeline(
    pdf_path: str,
    api_key: Optional[str],
    chunks_dir: Path,
    chunks_per_page: int = 4,
    page_chunk_overlap_chars: int = 180,
    vm_mode: bool = False,
    llm_script_path: Optional[str] = None,
) -> PipelineResult:
    fallback_splitter = build_fallback_splitter()

    pages = extract_pages(pdf_path)
    results = []
    pages_passed = 0
    pages_skipped = 0

    for page in pages:
        pnum = page["page_number"]
        text = page["text"]
        chunk_entries = split_page_into_fixed_chunks(
            text,
            num_chunks=chunks_per_page,
            overlap_chars=page_chunk_overlap_chars,
        )
        save_page_chunks(chunks_dir, pdf_path, pnum, chunk_entries)
        chunks = [entry.get("text", "") for entry in chunk_entries]

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
            raw = extract_rows_with_fallback(
                chunk,
                active_fields,
                fallback_splitter,
                vm_mode=vm_mode,
                api_key=api_key,
                llm_script_path=llm_script_path,
            )
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
