# JCC Handler — Module 4

## Purpose
Extracts structured table data from **JCC (Joint Coordination Committee)
Meeting** PDF documents. Then runs two post-extraction logic stages:
the **Output Layer** (effectiveness → JCC matching) and **Layer 4**
(CMETS-first JCC mapping by application IDs).

## Data Flow

```
source/jcc_pdfs/                     ← Input: JCC meeting PDFs (recursive)
    ├── ER/
    │   ├── Agenda/                    Agenda PDFs
    │   └── Minutes/                   Minutes-of-Meeting PDFs
    ├── NR/
    │   ├── Agenda/
    │   └── Minutes/
    └── …  (NER, SR, WR)

          ↓  (pdfplumber table detection)

output/jcc_cache/                    ← Cache: per-PDF JSON
    ├── MoM_36th_JCC_NR.json
    └── Minutes_of_46th_JCC_ER.json

          ↓  (flatten + format)

excels/jcc_extracted.xlsx            ← Stage 1 Output: raw JCC data
excels/jcc_output_layer.xlsx         ← Stage 2 Output: 4-column GNA/TGNA
excels/jcc_extracted_mapped.xlsx     ← Stage 2 Output: full matched data
excels/cmets_jcc_mapped.xlsx         ← Stage 3 Output: CMETS + GNA/TGNA
```

## Sub-Layer Architecture

| File                 | Role | Type |
|----------------------|------|------|
| `models.py`          | Keyword gate, column fragments, canonical column names | Schema |
| `extraction.py`      | Page gate, table detection, positional row parsing | Extraction |
| `runner.py`          | Orchestration: discover → cache → extract → output layer → layer 4 | I/O |
| `jcc_output_layer.py`| **Logic**: Output Layer (eff→JCC matching) + Layer 4 (CMETS→JCC mapping) + GNA/TGNA computation | Logic |

---

## Stage 1 — JCC PDF Extraction (Extraction)

**Type**: Pure extraction (PDF → structured data)
**Output**: `jcc_extracted.xlsx`

### Pipeline

1. **PDF Discovery** (`runner.py`):
   Recursively scans `source/jcc_pdfs/` for all `*.pdf` files.

2. **Cache Check** (`runner.py`):
   If `output/jcc_cache/<stem>.json` exists, the PDF is skipped.

3. **Keyword Gate** (`extraction.py → page_passes_gate()`):
   Page must contain ALL of: "Pooling", "Quantum", "Connectivity".

4. **Table Detection** (`extraction.py → extract_page_data()`):
   - pdfplumber extracts all tables
   - Header row matching ≥3 target column fragments → selected
   - Data rows positionally mapped to canonical column names

### Columns Extracted

| Column | Description |
|--------|-------------|
| `pooling_station` | Pooling / interconnection station name |
| `connectivity_applicant` | Developer name + application IDs |
| `connectivity_quantum_mw` | Applied connectivity MW |
| `schedule_as_per_current_jcc` | Current JCC schedule (MW values + dates) |
| `connectivity_start_date_under_gna` | GNA status ("Effective" or pending text) |
| (and more: sr_no, gen_comm_schedule_prev_jcc, ists_scope, remarks) |

---

## Stage 2 — JCC Output Layer (Logic)

**Type**: Logic operation — cross-references effectiveness with JCC data
**Output**: `jcc_output_layer.xlsx` (4 columns), `jcc_extracted_mapped.xlsx`
**File**: `jcc_output_layer.py → run_jcc_output_layer()`

### Pipeline

```
Effectiveness data (Module 2)  +  JCC extracted data (Stage 1)
 │
 ├→ Step 1: Load effectiveness records
 ├→ Step 2: Flatten JCC data into rows
 ├→ Step 3: Match each effectiveness row → JCC row
 │   Priority: Application IDs (GNA/LTA/5.2) → Fuzzy match (substation + developer)
 ├→ Step 4: Compute GNA / TGNA from matched JCC row
 └→ Step 5: Write output Excels
```

### Key Logic Functions

| Function | What it does |
|----------|-------------|
| `find_best_jcc_match()` | Weighted fuzzy: substation↔pooling (0.5) + developer↔applicant (0.5) |
| `_row_id_match_count()` | Counts how many application IDs match in a JCC row |
| `compute_gna_tgna()` | Computes GNA and TGNA from matched JCC row |

### GNA / TGNA Logic

```
connectivity_start_date_under_gna column:
 │
 ├→ Contains "Effective" (not "not effective"):
 │   └→ GNA = sum of ALL MW values in schedule_as_per_current_jcc
 │
 └→ Does NOT contain "Effective":
     └→ TGNA = sum of MW values tagged "(Commissioned)" in schedule
```

---

## Stage 3 — Layer 4: CMETS-first JCC Mapping (Logic)

**Type**: Logic operation — maps CMETS rows to JCC by application IDs
**Output**: `cmets_jcc_mapped.xlsx`
**File**: `jcc_output_layer.py → run_layer4_excel()`

### Pipeline

```
cmets_extracted.xlsx (Module 1)  +  JCC extracted data (Stage 1)
 │
 ├→ Step 1: Load CMETS sheet (all rows, all columns)
 ├→ Step 2: Flatten JCC data
 ├→ Step 3: For each CMETS row, search JCC connectivity_applicant:
 │   Priority 1: GNA Application ID → search connectivity_applicant
 │   Priority 2: LTA Application ID → search connectivity_applicant
 │   Priority 3: 5.2 Enhancement ID → search connectivity_applicant
 ├→ Step 4: Compute GNA / TGNA from matched JCC row
 └→ Step 5: Write Excel (all CMETS columns + TGNA + GNA + Match Source)
```

### Key Logic Functions

| Function | What it does |
|----------|-------------|
| `_id_in_connectivity_applicant()` | Searches for normalised ID in JCC connectivity_applicant column |
| `_find_jcc_by_ids()` | Cascading search: GNA → LTA → 5.2, returns first match + source label |
| `compute_gna_tgna()` | Same GNA/TGNA computation as Stage 2 |

### Output Columns

All CMETS columns (preserved from cmets_extracted.xlsx) plus:
- **TGNA** — Commissioned MW (when status ≠ "Effective")
- **GNA** — All MW (when status = "Effective")
- **Match Source** — Which ID matched: "GNA", "LTA", "5.2", or empty

---

## How to Change

- **Change target keywords**: Edit `models.py → REQUIRED_KEYWORDS`
- **Change column header matching**: Edit `models.py → TARGET_COLUMN_FRAGMENTS`
- **Change table detection logic**: Edit `extraction.py → _is_target_table()`
- **Change row parsing**: Edit `extraction.py → extract_page_data()`
- **Change effectiveness→JCC matching**: Edit `jcc_output_layer.py → find_best_jcc_match()`
- **Change CMETS→JCC ID search**: Edit `jcc_output_layer.py → _find_jcc_by_ids()`
- **Change GNA/TGNA computation**: Edit `jcc_output_layer.py → compute_gna_tgna()`
