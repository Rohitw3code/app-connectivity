# App Connectivity — Pipeline Documentation

Complete reference for all extraction rules, operations, data sources, and conditions across the pipeline.

---

## Table of Contents

- [Pipeline Overview](#pipeline-overview)
- [Module 1 — CMETS Extraction](#module-1--cmets-extraction)
- [Module 2 — Effectiveness Extraction](#module-2--effectiveness-extraction)
- [Module 3 — Mapping & Merge Operations](#module-3--mapping--merge-operations)
- [Module 4 — JCC Extraction & Matching](#module-4--jcc-extraction--matching)
- [Module 5 — Bay Allocation Extraction](#module-5--bay-allocation-extraction)
- [Module 6 — Bay Mapping](#module-6--bay-mapping)
- [Cross-Module Features](#cross-module-features)

---

## Pipeline Overview

### Entry Scripts

- `main.py` — download + extract only the PDFs that are not yet marked as extracted.
- `downloader_main.py` — download only (no extraction).
- `extraction_main.py` — extract only pending PDFs (based on per-source tables).

```
┌───────────────┐   ┌─────────────────────┐   ┌────────────────┐   ┌─────────────────────┐
│  CMETS PDFs   │   │  Effectiveness PDFs │   │   JCC PDFs     │   │ Bay Allocation PDFs │
│  (Module 1)   │   │  (Module 2)         │   │   (Module 4)   │   │ (Module 5)          │
└──────┬────────┘   └─────────┬───────────┘   └───────┬────────┘   └──────────┬──────────┘
       │                      │                       │                       │
       ▼                      ▼                       │                       │
┌──────────────────────────────────┐                  │                       │
│  Module 3 — Mapping & Merge     │                  │                       │
│  • Merge CMETS + Effectiveness  │                  │                       │
│  • GNA Date Update              │                  │                       │
│  • Capacity Calculation         │◄─────────────────┘                       │
│  • Bay Mapping                  │◄─────────────────────────────────────────┘
└──────────────┬───────────────────┘
               ▼
        Final Enriched Output
```

### All Input/Output Paths

| Module | Input | Cache | Output Excel |
|---|---|---|---|
| CMETS | `source/cmets_pdfs/*.pdf` | `output/cmets_cache/` | `excels/cmets_extracted.xlsx` |
| Effectiveness | `source/effectiveness_pdfs/*.pdf` | `output/effectiveness_cache/` | `excels/effectiveness_extracted.xlsx` |
| JCC | `source/jcc_pdfs/<Region>/<Type>/*.pdf` | `output/jcc_cache/` | `excels/jcc_extracted.xlsx`, `jcc_output_layer.xlsx`, `cmets_jcc_mapped.xlsx` |
| Bay Allocation | `source/bayallocation/*.pdf` | `output/bayallocation_cache/` | `excels/bayallocation_extracted.xlsx` |

### ID Cascade Strategy (Used Everywhere)

Wherever a CMETS row needs to match an effectiveness record, this priority order is used:

1. **GNA Application ID** — try first
2. **LTA Application ID** — fallback
3. **5.2 GNA (Enhancement) ID** — last resort

First successful match wins.

### Date Rules (Global)

- **Parsing accepts**: `dd.mm.yyyy`, `yyyy-mm-dd`, `d Month yyyy`
- **Output format**: Always `dd.mm.yyyy` (Indian convention)

---

## Module 1 — CMETS Extraction

Reads **CMETS / GNI connectivity** PDF documents → extracts structured row-level data about renewable energy connectivity applications.

### Extraction Steps

1. **Page Gate** — page must match ≥3 known column headers (e.g. "Applicant", "Connectivity", "Application No.") or value fingerprints
2. **LLM Extraction** — qualifying pages sent to GPT-4o-mini → returns structured JSON rows
3. **Normalization** — every value cleaned and standardized
4. **Caching** — results saved as JSON; delete cache to re-extract

### Columns Extracted

| Column | What It Contains |
|---|---|
| **GNA/ST II Application ID** | 10-digit ID (starts with 12, 22, or 11) |
| **LTA Application ID** | ID prefixed with 04 or 41 |
| **Application ID under Enhancement 5.2** | Only when Enhancement 5.2 context is present |
| **Name of Developers** | Company/applicant name |
| **Substation** | Connectivity location (e.g. "Aligarh (PG)") |
| **Project Location** | Project location as stated |
| **State** | Derived from Project Location |
| **Type** | Strict keywords + MW values (see [Type Extraction Workflow](#type-extraction-workflow)) |
| **Application Quantum (MW)(ST II)** | Applied connectivity quantum |
| **Granted Quantum GNA/LTA(MW)** | Actually granted quantum |
| **Voltage level** | e.g. "400 kV", "220 kV" |
| **Battery MWh / Injection / Drawl** | Only when BESS/Battery context present |
| **PSP MWh / Injection / Drawl** | Only when pump storage context present |
| **Application/Submission Date** | Submission date |
| **Applied Start of Connectivity date** | Start date per application |
| **Date from which additional capacity is to be added** | If explicitly present |
| **GNA Operationalization Date** | Near SCoD/SCOD terms |
| **GNA Operationalization (Yes/No)** | Computed (see [GNA Rules](#gna-operationalization-date--yesno)) |
| **Nature of Applicant** | Generator, Bulk consumer, etc. |
| **Mode (Criteria for applying)** | e.g. "SECI LOA", "Land BG Route" |
| **Status of application** | Withdrawn / granted / Revoked |
| **CMETS GNA/LTA Approved** | Meeting number (per PDF) |
| **CMETS GNA/LTA Meeting Date** | Meeting date (per PDF) |

### Skip Rules

Rows are **never extracted** if:
- "Nature of Applicant" is "Bulk consumer", "Drawee entity", or "Drawee entity connected"
- Table contains GNARE columns (e.g. "GNARE within Region (MW)")
- Row has none of the three primary key IDs

### Primary Key Rule

A row **must** have at least one of: GNA/ST II Application ID, LTA Application ID, or Enhancement 5.2 ID. Rows with none are discarded.

---

## Module 2 — Effectiveness Extraction

Reads **RE Effectiveness / Connectivity Status** PDF reports → extracts current status of connectivity applications.

### Extraction Steps

1. **PDF Discovery** — recursive scan for `*.pdf`
2. **Cache Check** — skip if JSON exists
3. **LLM Extraction** — text batched into ~10,000-char chunks → GPT-4o-mini (3 retries)
4. **Fallback** — pdfplumber table detection when no API key
5. **Deduplication** — by `application_id + name_of_applicant`

### Columns Extracted

| Column | What It Contains |
|---|---|
| **application_id** | Application ID (numeric) |
| **name_of_applicant** | Developer name |
| **region** | NR, SR, ER, WR, NER |
| **type_of_project** | Solar, Wind, Hybrid, Hydro, ESS |
| **installed_capacity_mw** | Total installed capacity |
| **solar_mw** | Solar capacity breakdown |
| **wind_mw** | Wind capacity breakdown |
| **ess_mw** | ESS/BESS capacity breakdown |
| **hydro_mw** | Hydro capacity breakdown |
| **connectivity_mw** | Connectivity quantum |
| **present_connectivity_mw** | Present connectivity quantum |
| **substation** | Substation name |
| **state** | State/UT |
| **expected_date** | Expected date of connectivity / GNA effective |

---

## Module 3 — Mapping & Merge Operations

Orchestrates the merge of CMETS + Effectiveness data and runs all post-extraction operations.

### Data Sources

```
CMETS PDF (Module 1)  +  Effectiveness PDF (Module 2)  +  Bay Allocation PDF (Module 5)
         │                          │                               │
         └──────────┬───────────────┘                               │
                    ▼                                               │
           Merge via ID Cascade                                     │
           (GNA → LTA → 5.2)                                       │
                    │                                               │
                    ▼                                               │
         ┌──────────────────────┐                                   │
         │ 1. Merge             │                                   │
         │ 2. GNA Date Update   │                                   │
         │ 3. Add. Capacity Date│                                   │
         │ 4. Capacity Calc     │                                   │
         │ 5. Bay Mapping  ◄────┼───────────────────────────────────┘
         └──────────────────────┘
```

### What Gets Updated on Match

**Overlapping columns** (effectiveness overwrites CMETS if valid):

| Effectiveness Field | CMETS Column Updated |
|---|---|
| name_of_applicant | Name of Developers |
| substation | Substation |
| state | State |
| installed_capacity_mw | Application Quantum (MW)(ST II) |

**New enrichment columns**:

| Column | Source |
|---|---|
| Region | Effectiveness `region` |
| Type of Project | Effectiveness `type_of_project` |
| Installed capacity (MW) solar/wind/ess/hydro | Effectiveness per-technology MW |
| Installed capacity (MW) hybrid | Computed sum (when hybrid) |

---

## Module 4 — JCC Extraction & Matching

Reads **JCC Meeting** PDFs → extracts connectivity schedule data → computes GNA/TGNA.

### Stage 1 — Extraction

**Page gate**: Must contain ALL of "Pooling", "Quantum", "Connectivity".

**Table detection**: Header must match ≥3 target fragments.

| Column | What It Contains |
|---|---|
| **pooling_station** | Station name |
| **connectivity_applicant** | Developer name + application IDs |
| **connectivity_quantum_mw** | Applied MW |
| **schedule_as_per_current_jcc** | MW values and dates |
| **connectivity_start_date_under_gna** | GNA status text |

### Stage 2 — Effectiveness → JCC Matching

**Data sources**: Effectiveness PDF (IDs, names) + JCC PDF (schedule, status)

**Matching** (priority order):
1. **Application ID matching** — GNA/LTA/5.2 IDs checked against JCC row; most hits wins
2. **Fuzzy name matching** — substation↔pooling (50%) + developer↔applicant (50%), +15% substring bonus, 45% threshold

### Stage 3 — CMETS → JCC Mapping

For each CMETS row, search JCC `connectivity_applicant` text for GNA → LTA → 5.2 IDs.

### GNA / TGNA Computation

```
Read "connectivity_start_date_under_gna" from matched JCC row:

┌──────────────────────────────────────────────────────────────┐
│ CASE 1: Contains "Effective" (NOT "not effective"):          │
│   → GNA = SUM of ALL MW values in schedule column            │
│   Example: "300 MW: 15.06.2025" → GNA = 300                 │
│                                                              │
│ CASE 2: Does NOT contain "Effective":                        │
│   → TGNA = SUM of MW tagged "(Commissioned)" only           │
│   Example: "111.8 MW (Commissioned), 88.2 MW (Commissioned)"│
│            → TGNA = 200                                      │
│                                                              │
│ GNA and TGNA are mutually exclusive.                         │
└──────────────────────────────────────────────────────────────┘
```

---

## Module 5 — Bay Allocation Extraction

Reads Bay Allocation PDFs → extracts substation-level bay data at 220kV/400kV.

### Extraction Steps

1. **Page gate** — must contain all required keywords
2. **Table detection** — ≥3 matching column fragments
3. **Row parsing** with noise filtering (skip empty, sub-headers, section headers, totals)

### Data Per Substation

| Field | Source Column | What It Contains |
|---|---|---|
| **sl_no** | Column 0 | Serial number |
| **name_of_substation** | Column 1 | e.g. "Bhadla-V" |
| **substation_coordinates** | Column 2 | Geographic coordinates |
| **region** | Column 3 | NR, SR, ER, WR, NER |
| **220kV bay_no** | Col 7 (bay), Col 9 (entity) | Dict: bay number → entity name |
| **400kV bay_no** | Col 10 (bay), Col 12 (entity) | Dict: bay number → entity name |

### Bay Number Rules

- Numeric strings (e.g. "204", "34")
- Voltage-specific (220kV and 400kV are separate)
- Only bays with non-empty entity names are indexed for matching
- Empty entity → stored as empty string in extraction, excluded from lookup

---

## Module 6 — Bay Mapping

Enriches CMETS rows with bay allocation data.

### Matching Workflow

```
For each CMETS row:

Step 1: Normalize Voltage
  "220 kV" / "220kV"  →  "220kv"
  "400 kV" / "400kV"  →  "400kv"
  Other               →  SKIP

Step 2: Normalize Developer Name
  • Lowercase
  • Remove: "Pvt. Ltd.", "Private Limited", "LLP", "Ltd."
  • Strip punctuation, collapse whitespace

Step 3: Search Bay Entries (under matching voltage)
  For each bay entity:
    a. Exact match after normalization
    b. CMETS name inside bay entity (substring)
    c. Bay entity inside CMETS name (substring)
    d. Core names match (after removing parenthetical info)
    Any of a/b/c/d = MATCH

Step 4: Multiple Matches
  • Deduplicate by bay number
  • Concatenate with " | "
```

### Output Columns

| Column | Source | Rule |
|---|---|---|
| **Bay No (Bay Allocation)** | Bay Allocation PDF | Matched bay number(s), ` \| ` separated |
| **Substation Name (Bay Allocation)** | Bay Allocation PDF | Substation name, deduplicated |
| **Substation Coordinates (Bay Allocation)** | Bay Allocation PDF | Coordinates, only when match found |

### Coordinate Rules

- Come from Bay Allocation PDF table Column 2
- Only populated when a bay match is found
- Multiple substations → concatenated and deduplicated
- No match → stays empty

---

## Cross-Module Features

### Type Extraction Workflow

```
CMETS PDF Page (raw text)
  e.g. "Solar (300)", "Hybrid + BESS", "Generator (Wind)"
         │
         ▼
STAGE 1: LLM Extraction
  • Extract ONLY: Solar, BESS, Wind, Solar+Wind, Solar+BESS
  • Preserve MW values in parentheses
  • NO sentences, descriptions, or other words
  Output: "Solar (300)" or "Wind (12) + BESS (19)"
         │
         ▼
STAGE 2: Normalization (norm_type)
  Token-by-token regex parsing:
  1. Scan for: solar, wind, bess, hybrid
  2. Map "hybrid" → "Solar+Wind"
  3. Capture parenthetical MW: (300), (19)
  4. Reconstruct: "keyword (value) + keyword (value)"
  5. Drop unrecognized (Thermal → None)
         │
         ▼
FINAL OUTPUT
  Solar (300), Wind (12) + BESS (19), Solar+Wind (500),
  BESS (50), Solar (300) + BESS (19), or null
```

**Allowed keywords**: Solar, Wind, BESS, Solar+Wind, Solar+BESS

### GNA Operationalization Date & Yes/No

| Data Point | Source | Column |
|---|---|---|
| GNA Date (initial) | CMETS PDF | Near SCoD/SCOD terms |
| Expected Date | Effectiveness PDF | `expected_date` |
| GNA Date (final) | Computed | The later of the two |

**Update rule** (runs during Module 3 merge):

| Condition | Action |
|---|---|
| CMETS date is empty | Use effectiveness date |
| Effectiveness date > CMETS date | Update to effectiveness date |
| CMETS date ≥ Effectiveness date | Keep CMETS date |

**Yes/No computation** (from the final date):

| Condition | Value |
|---|---|
| Final date is in the **FUTURE** | **Yes** (not yet operationalized) |
| Final date is **TODAY or PAST** | **No** (already operationalized) |

### Installed/Break-up Capacity Calculator

**Data sources**:
- **Source 1**: CMETS PDF → "Type" column (e.g. `"Wind (12) + BESS (44)"` → `{wind: 12, ess: 44}`)
- **Source 2**: Effectiveness PDF → `solar_mw`, `wind_mw`, `ess_mw`, `hydro_mw`

**Computation** (per matched row):

```
Step 1: Parse CMETS Type → per-technology MW values
Step 2: Read Effectiveness per-technology MW values
Step 3: Sum matching categories:
  Solar  = eff solar_mw  + cmets solar value
  Wind   = eff wind_mw   + cmets wind value
  Hydro  = eff hydro_mw  + cmets hydro value
  Hybrid = SUM ALL eff MW + SUM ALL cmets MW (only when project is hybrid)
Step 4: Write non-zero values to output columns
```

**Category mapping**:

| Type Keyword | Category |
|---|---|
| Solar | solar |
| Wind | wind |
| BESS, ESS | ess |
| Hydro, PSP, Pump Storage | hydro |
| Hybrid | hybrid |

### Battery (BESS) Extraction

- Triggered only when BESS/Battery keywords are detected in Type or description
- Uses **LLM call** to extract MWh, Injection (MW), Drawl (MW)
- General rule: Drawl > Injection for BESS
- Values from Type column (e.g. `BESS (19)`) are also extracted

### Additional Capacity Date

Same future-date comparison as GNA Date Update but targets "Date from which additional capacity is to be added". Uses full GNA → LTA → 5.2 ID cascade. No Yes/No recomputation.
