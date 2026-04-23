"""
PDF Table Extractor - Extracts renewable energy project data into JSON
Source: 01_Dec-25_RE_effectiveness.pdf
"""

import json
import re
import sys
from pathlib import Path

# Try to use pdfplumber for best table extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

# Fallback: pypdf
try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False


# ── Column definitions (in order they appear in the PDF) ─────────────────────
COLUMNS = [
    "si_no",
    "application_id",
    "name_of_applicant",
    "region",
    "type_of_project",
    "installed_capacity_mw",
    "solar_mw",
    "wind_mw",
    "ess_mw",
    "connectivity_mw",
    "substation",
    "state",
    "expected_date_of_connectivity",
]


# ── pdfplumber-based extractor (preferred) ───────────────────────────────────
def extract_with_pdfplumber(pdf_path: str) -> list[dict]:
    """Extract table rows using pdfplumber's layout engine."""
    records = []

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()

            for table in tables:
                for row in table:
                    # Skip header rows
                    if not row or not row[0]:
                        continue
                    if str(row[0]).strip().lower() in ("si no", "sl no", "s.no", "#"):
                        continue

                    # Clean all cells
                    cleaned = [cell.strip().replace("\n", " ") if cell else "" for cell in row]

                    # Must start with a numeric SI No
                    si_val = cleaned[0] if cleaned else ""
                    if not si_val.isdigit():
                        continue

                    # Pad / trim to expected column count
                    while len(cleaned) < len(COLUMNS):
                        cleaned.append("")
                    cleaned = cleaned[: len(COLUMNS)]

                    record = dict(zip(COLUMNS, cleaned))
                    # Type-cast numeric fields
                    record = coerce_types(record)
                    records.append(record)

    return records


# ── pypdf fallback extractor ─────────────────────────────────────────────────
def extract_with_pypdf(pdf_path: str) -> list[dict]:
    """
    Fallback: extract raw text and parse line-by-line.
    Less reliable for multi-line cells but works without pdfplumber.
    """
    reader = PdfReader(pdf_path)
    all_text = "\n".join(page.extract_text() or "" for page in reader.pages)
    return parse_text_lines(all_text)


def parse_text_lines(text: str) -> list[dict]:
    """
    Parse raw extracted text into structured records.
    Handles the specific column layout of the RE effectiveness PDF.
    """
    records = []
    lines = text.splitlines()

    # Regex: line starting with an integer SI No followed by application ID
    row_start = re.compile(r"^\s*(\d{1,4})\s+(9[12]\d{8}|2[12]\d{8})\s+(.+)")

    i = 0
    while i < len(lines):
        m = row_start.match(lines[i])
        if not m:
            i += 1
            continue

        # Gather continuation lines until next row or blank
        raw = lines[i]
        j = i + 1
        while j < len(lines) and not row_start.match(lines[j]):
            next_line = lines[j].strip()
            if next_line:
                raw += " " + next_line
            else:
                break
            j += 1
        i = j

        record = parse_single_row(raw)
        if record:
            records.append(record)

    return records


def parse_single_row(raw: str) -> dict | None:
    """
    Parse one concatenated row string into a dict.
    The PDF columns after the name are: Region | Type | Installed | Solar | Wind | ESS | Connectivity | Substation | State | Date
    """
    # Split on 2+ consecutive spaces or tab-like gaps
    tokens = re.split(r"\s{2,}", raw.strip())
    if len(tokens) < 5:
        return None

    # Identify SI No (first numeric token)
    try:
        si_no = int(tokens[0])
    except ValueError:
        return None

    record: dict = {col: "" for col in COLUMNS}
    record["si_no"] = si_no

    # Application ID (always starts with 9 or 2 followed by 9 digits)
    app_id_match = re.search(r"\b([92][12]\d{8})\b", raw)
    if app_id_match:
        record["application_id"] = app_id_match.group(1)

    # Date at the end (dd-mm-yyyy)
    date_match = re.search(r"(\d{2}-\d{2}-\d{4})\s*$", raw)
    if date_match:
        record["expected_date_of_connectivity"] = date_match.group(1)
        raw = raw[: date_match.start()]

    # Region codes
    region_match = re.search(r"\b(NR|SR|WR|ER|NER)\b", raw)
    if region_match:
        record["region"] = region_match.group(1)

    # Project type
    type_match = re.search(
        r"\b(Solar|Wind|Hydro|Hybrid|Thermal|PSP|Gas|Nuclear|Bulk Consumer|Standalone ESS)\b",
        raw, re.IGNORECASE
    )
    if type_match:
        record["type_of_project"] = type_match.group(1).title()

    # States
    state_match = re.search(
        r"(Rajasthan|Gujarat|Maharashtra|Karnataka|Andhra Pradesh|Tamil Nadu|"
        r"Madhya Pradesh|Uttar Pradesh|Himachal Pradesh|Uttarakhand|"
        r"Arunachal Pradesh|Chhattisgarh|Odisha|Sikkim|West Bengal|Bihar|"
        r"Jammu and Kashmir|Assam|Kerala|Telangana)",
        raw
    )
    if state_match:
        record["state"] = state_match.group(1)

    # Numeric columns: look for sequences of numbers
    numbers = re.findall(r"\b(\d+(?:\.\d+)?)\b", raw)
    numeric_fields = [
        "installed_capacity_mw", "solar_mw", "wind_mw",
        "ess_mw", "connectivity_mw"
    ]
    # Skip si_no (already parsed)
    nums_to_use = [n for n in numbers if not n == str(si_no)][:5]
    for field, val in zip(numeric_fields, nums_to_use):
        try:
            record[field] = float(val) if "." in val else int(val)
        except ValueError:
            record[field] = val

    return record


# ── Type coercion ─────────────────────────────────────────────────────────────
def coerce_types(record: dict) -> dict:
    """Convert numeric strings to int/float where appropriate."""
    numeric_cols = [
        "si_no", "installed_capacity_mw", "solar_mw",
        "wind_mw", "ess_mw", "connectivity_mw"
    ]
    for col in numeric_cols:
        val = record.get(col, "")
        if val == "" or val is None:
            record[col] = None
            continue
        try:
            as_float = float(str(val).replace(",", ""))
            record[col] = int(as_float) if as_float.is_integer() else as_float
        except (ValueError, TypeError):
            pass  # keep as string
    return record


# ── Per-column summary ────────────────────────────────────────────────────────
def build_column_summary(records: list[dict]) -> dict:
    """Build a per-column dict: unique values + basic stats for numerics."""
    summary: dict = {}

    for col in COLUMNS:
        values = [r.get(col) for r in records if r.get(col) not in (None, "")]
        numeric_vals = [v for v in values if isinstance(v, (int, float))]

        if numeric_vals:
            summary[col] = {
                "type": "numeric",
                "count": len(numeric_vals),
                "min": min(numeric_vals),
                "max": max(numeric_vals),
                "total": round(sum(numeric_vals), 2),
            }
        else:
            unique = sorted(set(str(v) for v in values))
            summary[col] = {
                "type": "categorical",
                "count": len(values),
                "unique_count": len(unique),
                "unique_values": unique[:50],  # cap at 50 for readability
            }

    return summary


# ── Main ──────────────────────────────────────────────────────────────────────
def main(pdf_path: str, output_path: str = "output.json"):
    print(f"[INFO] Reading: {pdf_path}")
    pdf_path = Path(pdf_path)

    if not pdf_path.exists():
        print(f"[ERROR] File not found: {pdf_path}")
        sys.exit(1)

    # Extract records
    if HAS_PDFPLUMBER:
        print("[INFO] Using pdfplumber (preferred)")
        records = extract_with_pdfplumber(str(pdf_path))
    elif HAS_PYPDF:
        print("[INFO] pdfplumber not available — falling back to pypdf text extraction")
        records = extract_with_pypdf(str(pdf_path))
    else:
        print("[ERROR] Neither pdfplumber nor pypdf is installed.")
        print("        Run: pip install pdfplumber   (recommended)")
        sys.exit(1)

    print(f"[INFO] Extracted {len(records)} records")

    # Build output structure
    output = {
        "metadata": {
            "source_file": pdf_path.name,
            "total_records": len(records),
            "columns": COLUMNS,
        },
        "column_summary": build_column_summary(records),
        "records": records,
    }

    # Write JSON
    out_path = Path(output_path)
    out_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[INFO] JSON written to: {out_path}")

    # Quick preview
    print("\n── Column Summary ──────────────────────────────────────")
    for col, info in output["column_summary"].items():
        if info["type"] == "numeric":
            print(f"  {col}: total={info['total']} MW, min={info['min']}, max={info['max']}")
        else:
            print(f"  {col}: {info['unique_count']} unique values")


if __name__ == "__main__":
    pdf_file = sys.argv[1] if len(sys.argv) > 1 else "01_Dec-25_RE_effectiveness.pdf"
    out_file = sys.argv[2] if len(sys.argv) > 2 else "output.json"
    main(pdf_path="01_Dec-25_RE effectiveness.pdf")