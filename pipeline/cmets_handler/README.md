# CMETS Handler — Module 1

## Purpose
Extracts structured data from **CMETS / GNI connectivity** PDF documents
published by Indian power sector regulators (CTUIL/PGCIL).

## Data Flow

```
source_1/                         ← Input: CMETS PDF files
    ├── CMETS_Report_2024.pdf
    └── CMETS_Report_2025.pdf

          ↓  (pdfplumber + GPT-4o-mini)

source_1_output/                  ← Cache: per-PDF JSON (skip if exists)
    ├── CMETS_Report_2024.json
    └── CMETS_Report_2025.json

          ↓  (flatten + format)

cmets.xlsx                        ← Output: consolidated Excel report
```

## Sub-Layer Architecture

| File               | Responsibility                                     |
|--------------------|----------------------------------------------------|
| `prompts.py`       | LLM system & user prompt templates                 |
| `models.py`        | Pydantic schemas (`MappedRow`, `PageResult`, etc.)  |
| `gate.py`          | Regex column-header gate — filters relevant pages   |
| `normalization.py` | Value cleaning, state extraction, date parsing, dedup |
| `extraction.py`    | PDF reading (pdfplumber) + LLM row extraction       |
| `runner.py`        | Orchestration: discover → cache → extract → Excel   |

## How Extraction Works

1. **Sub-layer A** (`extraction.py → extract_pages()`):
   Read all pages from the PDF using pdfplumber.

2. **Sub-layer B** (`gate.py → page_passes_gate()`):
   For each page, check if the text contains known column header
   patterns (e.g. "Application ID", "Project Location"). Pages
   without table headers are skipped.

3. **Sub-layer C** (`extraction.py → llm_extract_rows()`):
   Send the page text + detected column names to GPT-4o-mini.
   The LLM returns a JSON object with extracted rows.

4. **Normalisation** (`normalization.py`):
   Clean values, extract Indian state from location, parse dates,
   derive GNA operationalization status, and deduplicate.

5. **Caching** (`runner.py`):
   Each PDF's extraction result is saved as `source_1_output/<stem>.json`.
   On re-run, PDFs with existing JSON cache are skipped.

## How to Change Extraction Logic

- **Add/remove output columns**: Edit `models.py` → `MappedRow`
- **Change LLM instructions**: Edit `prompts.py`
- **Support new table formats**: Edit `gate.py` → `TARGET_COLUMN_VARIANTS`
- **Change value cleaning rules**: Edit `normalization.py`
- **Change page-reading strategy**: Edit `extraction.py` → `extract_pages()`
