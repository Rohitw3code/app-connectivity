# Effectiveness Handler — Module 2

## Purpose
Extracts structured data from **RE Effectiveness / Connectivity status**
PDF reports published by CTUIL for the Indian renewable energy sector.

## Data Flow

```
../CTUIL-Regenerators-Effective-Date-wise/   ← Input: effectiveness PDFs
    ├── Effective_Jan_2025.pdf                  (recursive scan)
    └── subdir/
        └── Effective_Feb_2025.pdf

          ↓  (pdfplumber + GPT-4o-mini)

effectiveness_output/                        ← Cache: per-PDF JSON
    ├── Effective_Jan_2025.json
    └── Effective_Feb_2025.json

          ↓  (flatten + format)

effectiveness_combined.xlsx                  ← Output: consolidated Excel
```

## Sub-Layer Architecture

| File               | Responsibility                                     |
|--------------------|----------------------------------------------------|
| `prompts.py`       | LLM system & user prompt templates                 |
| `models.py`        | Pydantic schema (`RERecord`), dedup, column order   |
| `extraction.py`    | LLM extraction (primary) + pdfplumber fallback      |
| `runner.py`        | Orchestration: discover → cache → extract → Excel   |

## How Extraction Works

1. **PDF Discovery** (`runner.py`):
   Recursively scans the source folder for `*.pdf` files.

2. **Cache Check** (`runner.py`):
   If `effectiveness_output/<stem>.json` already exists, the PDF
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

## How to Change Extraction Logic

- **Add/remove output columns**: Edit `models.py` → `RERecord`
- **Change LLM instructions**: Edit `prompts.py`
- **Change chunking / retry strategy**: Edit `extraction.py`
- **Change fallback table header mapping**: Edit `extraction.py` → `_HEADER_MAP`
