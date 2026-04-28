# CMETS Handler — Module 1

## Purpose
Extracts structured data from **CMETS / GNI connectivity** PDF documents
published by Indian power sector regulators (CTUIL/PGCIL).

## Data Flow

```
source/cmets_pdfs/                    ← Input: CMETS PDF files
    ├── CMETS_Report_2024.pdf
    └── CMETS_Report_2025.pdf

          ↓  (pdfplumber + GPT-4o-mini)

output/cmets_cache/                   ← Cache: per-PDF JSON (skip if exists)
    ├── CMETS_Report_2024.json
    └── CMETS_Report_2025.json

          ↓  (flatten + format)

excels/cmets_extracted.xlsx           ← Output: consolidated Excel report
```

## Sub-Layer Architecture

| File               | Role | Type |
|--------------------|------|------|
| `prompts.py`       | LLM system & user prompt templates | Extraction |
| `models.py`        | Pydantic schemas (`MappedRow`, `PageResult`, etc.) | Schema |
| `gate.py`          | Regex column-header gate — filters relevant pages | Extraction |
| `normalization.py` | Value cleaning, state extraction, date parsing, dedup | Logic |
| `extraction.py`    | PDF reading (pdfplumber) + LLM row extraction | Extraction |
| `runner.py`        | Orchestration: discover → cache → extract → Excel | I/O |

## How Extraction Works

1. **Sub-layer A** (`extraction.py → extract_pages()`):
   Read all pages from the PDF using pdfplumber.

2. **Sub-layer B** (`gate.py → page_passes_gate()`):
   Dual strategy — regex header matching + value-based fingerprinting.
   Also applies blocklist rejection (GNARE tables, bulk consumers).

3. **Sub-layer C** (`extraction.py → llm_extract_rows()`):
   Send the page text + detected column names to GPT-4o-mini.
   The LLM returns a JSON object with extracted rows.

4. **Normalisation** (`normalization.py`):
   Clean values, extract Indian state from location, parse dates,
   derive GNA operationalization status, and deduplicate.

5. **Caching** (`runner.py`):
   Each PDF's extraction result is saved as `cmets_cache/<stem>.json`.
   On re-run, PDFs with existing JSON cache are skipped.

## Logic Operations in `normalization.py`

These are **post-extraction** operations (not extraction itself):

| Operation | Function | What it does |
|-----------|----------|-------------|
| State extraction | `extract_state()` | Derives Indian state from Project Location text |
| Date parsing | `extract_date()` | Normalises date strings (dd.mm.yyyy, yyyy/mm/dd, etc.) |
| GNA Yes/No | `gna_yes_no()` | Compares GNA Operationalization Date with today → "Yes"/"No" |
| Enhancement 5.2 ID | `derive_enhancement_id()` | Derives Enhancement 5.2 application ID from context |
| Status normalisation | `norm_status()` | Maps "withdraw"→"Withdrawn", "grant"→"granted", etc. |
| Nature blocklist | `_is_nature_blocklisted()` | Skips "Bulk consumer" / "Drawee entity" rows |
| Type normalisation | `norm_type()` | Normalises energy source type to canonical values |
| PSP columns | `psp_cols()` | Extracts pump-storage MWh/Injection/Drawl values |
| Developer name | `norm_dev()` | Cleans developer name (removes LOA/CRITERION artefacts) |

## How to Change

- **Add/remove output columns**: Edit `models.py → MappedRow`
- **Change LLM instructions**: Edit `prompts.py`
- **Support new table formats**: Edit `gate.py → TARGET_COLUMN_VARIANTS`
- **Support new value fingerprints**: Edit `gate.py → VALUE_FINGERPRINTS`
- **Change value cleaning rules**: Edit `normalization.py`
- **Change page-reading strategy**: Edit `extraction.py → extract_pages()`
