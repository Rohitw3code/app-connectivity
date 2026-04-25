# JCC Handler — Module 4

## Purpose
Extracts structured table data from **JCC (Joint Coordination Committee)
Meeting** PDF documents. These PDFs contain connectivity/pooling station
schedules published region-wise (ER, NER, NR, SR, WR).

## Data Flow

```
../app-connectivity-pdfs/source_2/   ← Input: JCC meeting PDFs (recursive)
    ├── ER/
    │   ├── Agenda/                     Agenda PDFs
    │   └── Minutes/                    Minutes-of-Meeting PDFs
    ├── NR/
    │   ├── Agenda/
    │   └── Minutes/
    └── …  (NER, SR, WR)

          ↓  (pdfplumber table detection)

jcc_output/                          ← Cache: per-PDF JSON
    ├── MoM_36th_JCC_NR.json
    └── Minutes_of_46th_JCC_ER.json

          ↓  (flatten + format)

excels/jcc_extracted.xlsx            ← Output: consolidated Excel
```

## Sub-Layer Architecture

| File             | Responsibility                                      |
|------------------|-----------------------------------------------------|
| `models.py`      | Keyword gate, column fragments, canonical column names |
| `extraction.py`  | Page gate, table detection, positional row parsing    |
| `runner.py`      | Orchestration: discover → cache → extract → Excel     |

## How Extraction Works

1. **PDF Discovery** (`runner.py`):
   Recursively scans `source_2/` for all `*.pdf` files across all
   region sub-folders (ER, NER, NR, SR, WR).

2. **Cache Check** (`runner.py`):
   If `jcc_output/<stem>.json` already exists, the PDF is skipped.
   Delete the JSON to force re-extraction.

3. **Keyword Gate** (`extraction.py → page_passes_gate()`):
   Each page must contain ALL of: "Pooling", "Quantum", "Connectivity"
   to be considered a target page.

4. **Table Detection** (`extraction.py → extract_page_data()`):
   - pdfplumber extracts all tables on the page
   - The table whose header row matches ≥3 of the target column
     fragments is selected
   - Header/sub-header rows are skipped
   - Data rows are positionally mapped to canonical column names

5. **Output**:
   - Per-PDF JSON cache in `jcc_output/`
   - Combined Excel in `excels/jcc_extracted.xlsx`

## How to Change Extraction Logic

- **Change target keywords**: Edit `models.py` → `REQUIRED_KEYWORDS`
- **Change column header matching**: Edit `models.py` → `TARGET_COLUMN_FRAGMENTS`
- **Change output column names**: Edit `models.py` → `COLUMN_NAMES`
- **Change table detection logic**: Edit `extraction.py` → `_is_target_table()`
- **Change row parsing**: Edit `extraction.py` → `extract_page_data()`
