# Effectiveness Handler — Module 2

## Purpose
Extracts structured data from **RE Effectiveness / Connectivity status**
PDF reports published by CTUIL for the Indian renewable energy sector.

## Data Flow

```
source/effectiveness_pdfs/           ← Input: effectiveness PDFs
    ├── Effective_Jan_2025.pdf         (recursive scan)
    └── subdir/
        └── Effective_Feb_2025.pdf

          ↓  (pdfplumber + GPT-4o-mini)

output/effectiveness_cache/          ← Cache: per-PDF JSON
    ├── Effective_Jan_2025.json
    └── Effective_Feb_2025.json

          ↓  (flatten + format)

excels/effectiveness_extracted.xlsx  ← Output: consolidated Excel
```

## Sub-Layer Architecture

| File                       | Role | Type |
|----------------------------|------|------|
| `prompts.py`               | LLM system & user prompt templates | Extraction |
| `models.py`                | Pydantic schema (`RERecord`), dedup, column order | Schema |
| `extraction.py`            | LLM extraction (primary) + pdfplumber fallback | Extraction |
| `runner.py`                | Orchestration: discover → cache → extract → Excel | I/O |
| `date_updater.py`          | **Logic**: GNA date + Additional Capacity Date comparison | Logic |
| `capacity_calculator.py`   | **Logic**: Installed/Break-up Capacity (MW) computation | Logic |

## How Extraction Works

1. **PDF Discovery** (`runner.py`):
   Recursively scans the source folder for `*.pdf` files.

2. **Cache Check** (`runner.py`):
   If `effectiveness_cache/<stem>.json` already exists, the PDF
   is skipped. Delete the JSON to force re-extraction.

3. **LLM Extraction** (`extraction.py → extract_with_llm()`):
   - Pages are read with pdfplumber
   - Text is batched into ~10,000-char chunks
   - Each chunk is sent to GPT-4o-mini with retry (3 attempts)
   - The LLM returns a JSON array of records

4. **Fallback** (`extraction.py → extract_with_tables()`):
   When no API key is configured, pdfplumber's built-in table
   detection is used instead of the LLM.

5. **Deduplication** (`models.py → dedup_records()`):
   Records are deduplicated by `application_id + name_of_applicant`.

## Logic: GNA Date Updater (`date_updater.py`)

This file contains **two standalone logic operations** (not extraction).
They are called by **Module 3** (mapping handler) after the merge step.

### 1. `update_gna_dates(df, lookup)` — GNA Operationalization Date

For each CMETS row matched to an effectiveness record:
1. Parses effectiveness `expected_date`
   (= "expected date of connectivity / GNA to be made effective")
2. Parses CMETS `GNA Operationalization Date`
3. If effectiveness date > CMETS date → updates CMETS column with the later date
4. If CMETS date ≥ effectiveness date → keeps CMETS date as-is
5. If CMETS date is empty → uses effectiveness date
6. Recomputes `GNA Operationalization (Yes/No)`:
   - "Yes" → date is in the future
   - "No" → date is today or in the past

### 2. `update_additional_capacity_dates(df, lookup)` — Additional Capacity Date

Same future-date logic as above, but targets:
- CMETS column: `Date from which additional capacity is to be added`
- Effectiveness column: `expected_date`

Matches rows using the GNA → LTA → 5.2 GNA ID cascade.

### Key functions

| Function | Purpose |
|----------|---------|
| `update_gna_dates(df, lookup)` | Compares GNA dates, updates columns |
| `update_additional_capacity_dates(df, lookup)` | Compares additional capacity dates |
| `parse_date(raw)` | Parses date strings in dd.mm.yyyy / yyyy-mm-dd / "d Month yyyy" |
| `_yes_no(d)` | Future → "Yes", past → "No" |

## Logic: Capacity Calculator (`capacity_calculator.py`)

Computes **Installed/Break-up Capacity (MW)** sub-columns for each CMETS row.
Called by Module 3 after the merge and date update steps.

### Output Columns

| Column | Description |
|--------|-------------|
| `Installed/Break-up Capacity (MW) Solar` | Sum of effectiveness solar_mw + CMETS Type solar value |
| `Installed/Break-up Capacity (MW) Wind` | Sum of effectiveness wind_mw + CMETS Type wind value |
| `Installed/Break-up Capacity (MW) Hybrid` | Sum of all effectiveness + CMETS type values (when hybrid) |
| `Installed/Break-up Capacity (MW) Hydro` | Sum of effectiveness hydro_mw + CMETS Type hydro value |

### How it works

1. Match CMETS row → effectiveness record via GNA → LTA → 5.2 GNA cascade
2. Parse CMETS `Type` column: e.g. `"Wind (12) + BESS (44)"` → `{wind: 12, ess: 44}`
3. Read effectiveness `solar_mw`, `wind_mw`, `ess_mw`, `hydro_mw`
4. Sum matching categories: `installed = effectiveness_value + cmets_type_value`

### Key functions

| Function | Purpose |
|----------|---------|
| `compute_installed_capacity(df, lookup)` | Main entry — compute + write capacity columns |
| `parse_cmets_type(type_str)` | Parse CMETS Type column into `{category: MW}` |

## How to Change

- **Add/remove columns**: Edit `models.py → RERecord`
- **Change LLM instructions**: Edit `prompts.py`
- **Change chunking / retry strategy**: Edit `extraction.py`
- **Change fallback table header mapping**: Edit `extraction.py → _HEADER_MAP`
- **Change date comparison logic**: Edit `date_updater.py`
- **Change capacity calculation logic**: Edit `capacity_calculator.py`
