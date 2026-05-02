# JCC Handler — Module 4

## What It Does

Reads **JCC (Joint Coordination Committee) Meeting** PDF documents and extracts structured table data about connectivity schedules. Then runs two post-extraction logic stages to compute **GNA** and **TGNA** values by cross-referencing with Effectiveness and CMETS data.

---

## Input → Output

| | Path |
|---|---|
| **Input PDFs** | `source/jcc_pdfs/` — organized by region (ER, NR, NER, SR, WR) and type (Agenda, Minutes) |
| **Cache (JSON)** | `output/jcc_cache/<pdf_name>.json` |
| **Output Excel (Stage 1)** | `excels/jcc_extracted.xlsx` — raw JCC extracted data |
| **Output Excel (Stage 2)** | `excels/jcc_output_layer.xlsx` — Effectiveness→JCC matched GNA/TGNA |
| **Output Excel (Stage 2)** | `excels/jcc_extracted_mapped.xlsx` — matched subset |
| **Output Excel (Stage 3)** | `excels/cmets_jcc_mapped.xlsx` — CMETS rows + GNA/TGNA |

---

## Stage 1 — JCC PDF Extraction

### How Extraction Works

#### Step 1 — PDF Discovery

Recursively scans `source/jcc_pdfs/` for all `*.pdf` files across all regional sub-folders.

#### Step 2 — Cache Check

If a JSON cache already exists for a PDF, it is skipped.

#### Step 3 — Page Gate (Keyword Filter)

A page must contain **ALL three** of these keywords to pass:
- "Pooling"
- "Quantum"
- "Connectivity"

Pages that fail are skipped.

#### Step 4 — Table Detection

- pdfplumber extracts all tables on the page
- The system checks if any table header row matches at least **3 target column fragments** (e.g. "pooling station", "connectivity quantum", "schedule")
- Matching table → data rows are positionally mapped to column names

### Columns Extracted

| Column | Data Source | What It Contains |
|---|---|---|
| **pooling_station** | JCC PDF table | Pooling / interconnection station name |
| **connectivity_applicant** | JCC PDF table | Developer name + application IDs in the same cell |
| **connectivity_quantum_mw** | JCC PDF table | Applied connectivity quantum in MW |
| **schedule_as_per_current_jcc** | JCC PDF table | Current JCC schedule — contains MW values and dates |
| **connectivity_start_date_under_gna** | JCC PDF table | GNA status text (e.g. "Effective" or "Connectivity likely to be operationalized...") |
| **sr_no** | JCC PDF table | Serial number |
| **gen_comm_schedule_prev_jcc** | JCC PDF table | Previous JCC generation commissioning schedule |
| **ists_scope** | JCC PDF table | ISTS scope details |
| **remarks** | JCC PDF table | Remarks text |

---

## Stage 2 — JCC Output Layer (Effectiveness → JCC Matching)

### Purpose

Cross-reference **Effectiveness** data with **JCC** extracted data to compute GNA and TGNA values for each developer.

### Data Sources

| Data Source | What Is Used |
|---|---|
| **Effectiveness PDF** (Module 2 output) | Developer name, substation, application IDs (GNA/LTA/5.2) |
| **JCC PDF** (Stage 1 output) | Pooling station, connectivity applicant, GNA status, schedule MW values |

### How Matching Works

For each effectiveness row, the system tries to find the best matching JCC row:

```
┌──────────────────────────────────────────────────────────────────────┐
│  MATCHING STRATEGY (priority order)                                  │
│                                                                      │
│  PRIORITY 1: Application ID Matching                                 │
│  ──────────────────────────────────                                  │
│  Check if any GNA, LTA, or Enhancement 5.2 ID from the              │
│  effectiveness record appears anywhere in the JCC row values.        │
│  The JCC row with the most ID hits wins.                             │
│                                                                      │
│  PRIORITY 2: Fuzzy Name Matching (fallback)                          │
│  ──────────────────────────────────────────                           │
│  Weighted similarity score:                                          │
│    Substation ↔ Pooling Station       (weight: 50%)                  │
│    Developer Name ↔ Connectivity Applicant (weight: 50%)             │
│    Substring containment bonus: +15%                                 │
│    Minimum threshold: 45% combined score                             │
└──────────────────────────────────────────────────────────────────────┘
```

### GNA / TGNA Computation Rules

```
┌──────────────────────────────────────────────────────────────────────┐
│  Read "connectivity_start_date_under_gna" from matched JCC row       │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  CASE 1: Contains "Effective"                                  │  │
│  │  (but NOT "not effective" or "non-effective")                  │  │
│  │                                                                │  │
│  │  → GNA = SUM of ALL MW values in "schedule_as_per_current_jcc"│  │
│  │                                                                │  │
│  │  Data source: JCC PDF → schedule column                        │  │
│  │  Example: "300 MW: 15.06.2025" → GNA = 300                    │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  CASE 2: Does NOT contain "Effective"                          │  │
│  │  (e.g. "Connectivity likely to be operationalized upon         │  │
│  │   commissioning of required Transmission system")              │  │
│  │                                                                │  │
│  │  → TGNA = SUM of MW values tagged "(Commissioned)" only       │  │
│  │                                                                │  │
│  │  Data source: JCC PDF → schedule column                        │  │
│  │  Example: "111.8 MW: 19.05.2025 (Commissioned)"               │  │
│  │           "88.2 MW: 01.06.2025 (Commissioned)"                 │  │
│  │           → TGNA = 111.8 + 88.2 = 200                         │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  Note: GNA and TGNA are mutually exclusive — exactly one is          │
│  computed per matched row (or both are null if data is insufficient).│
└──────────────────────────────────────────────────────────────────────┘
```

### Output Columns

| Column | Data Source | What It Contains |
|---|---|---|
| **Developer Name** | Effectiveness PDF (or JCC if eff missing) | Developer/applicant name |
| **Substation** | Effectiveness PDF (or JCC if eff missing) | Substation name |
| **GNA/ST II Application ID** | Effectiveness PDF | GNA application ID |
| **LTA Application ID** | Effectiveness PDF | LTA application ID |
| **Application ID under Enhancement 5.2 or revision** | Effectiveness PDF | Enhancement 5.2 ID |
| **TGNA** | Computed from JCC schedule | Sum of Commissioned MW |
| **GNA** | Computed from JCC schedule | Sum of all MW (when Effective) |

---

## Stage 3 — Layer 4: CMETS-First JCC Mapping

### Purpose

Start from the **CMETS** extracted sheet and find matching JCC rows to add GNA/TGNA data.

### Data Sources

| Data Source | What Is Used |
|---|---|
| **CMETS PDF** (Module 1 output) | All columns, especially GNA/LTA/5.2 Application IDs |
| **JCC PDF** (Stage 1 output) | `connectivity_applicant` column (searched for matching IDs) |

### How Matching Works

```
┌──────────────────────────────────────────────────────────────────────┐
│  For each CMETS row:                                                 │
│                                                                      │
│  Step 1: Take GNA Application ID from CMETS                         │
│     → Search for this ID inside JCC "connectivity_applicant" text    │
│     → If found → MATCH (source = "GNA")                             │
│                                                                      │
│  Step 2: If GNA fails, take LTA Application ID from CMETS           │
│     → Search inside JCC "connectivity_applicant" text                │
│     → If found → MATCH (source = "LTA")                             │
│                                                                      │
│  Step 3: If LTA fails, take Enhancement 5.2 ID from CMETS           │
│     → Search inside JCC "connectivity_applicant" text                │
│     → If found → MATCH (source = "5.2")                             │
│                                                                      │
│  Search is case-insensitive, whitespace-normalized, and uses         │
│  substring matching (ID must be at least 3 characters).              │
│                                                                      │
│  When matched → compute GNA / TGNA using same rules as Stage 2.     │
└──────────────────────────────────────────────────────────────────────┘
```

### What Gets Added

| Column | Data Source | What It Contains |
|---|---|---|
| **GNA** | Computed from JCC schedule | Sum of all MW values (when JCC status = "Effective") |
| **TGNA** | Computed from JCC schedule | Sum of Commissioned MW (when JCC status ≠ "Effective") |
| **Match Source** | Computed | Which ID was used: "GNA", "LTA", "5.2", or empty |

The output preserves **all original CMETS columns** and appends these three.
