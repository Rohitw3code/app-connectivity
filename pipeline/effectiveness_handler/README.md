# Effectiveness Handler — Module 2

## What It Does

Reads **RE Effectiveness / Connectivity Status** PDF reports and extracts structured data about the current status of renewable energy connectivity applications. This data is later cross-referenced with CMETS data to update dates and compute installed capacity.

---

## Input → Output

| | Path |
|---|---|
| **Input PDFs** | `source/effectiveness_pdfs/*.pdf` (recursive) |
| **Cache (JSON)** | `output/effectiveness_cache/<pdf_name>.json` |
| **Output Excel** | `excels/effectiveness_extracted.xlsx` |

---

## How Extraction Works

### Step 1 — PDF Discovery

Recursively scans the source folder for all `*.pdf` files.

### Step 2 — Cache Check

If a JSON cache file already exists for a PDF, it is skipped. Delete the JSON file to force re-extraction.

### Step 3 — LLM Extraction (Primary)

- Pages are read with pdfplumber
- Text is batched into chunks of approximately 10,000 characters
- Each chunk is sent to GPT-4o-mini with retry logic (up to 3 attempts)
- The LLM returns a JSON array of records

### Step 4 — Table Extraction (Fallback)

When no API key is configured, pdfplumber's built-in table detection is used instead of the LLM.

### Step 5 — Deduplication

Records are deduplicated by `application_id + name_of_applicant` to remove duplicates across pages.

---

## Columns Extracted

| Column | What It Contains |
|---|---|
| **application_id** | Application ID (numeric) |
| **name_of_applicant** | Developer/applicant company name |
| **region** | Region (NR, SR, ER, WR, NER) |
| **type_of_project** | Project type (Solar, Wind, Hybrid, Solar + Wind, Hydro, ESS) |
| **installed_capacity_mw** | Total installed capacity in MW |
| **solar_mw** | Solar capacity breakdown in MW |
| **wind_mw** | Wind capacity breakdown in MW |
| **ess_mw** | Energy storage (ESS/BESS) capacity in MW |
| **hydro_mw** | Hydro/pump storage capacity in MW |
| **connectivity_mw** | Connectivity quantum in MW |
| **present_connectivity_mw** | Present connectivity quantum in MW |
| **substation** | Connectivity substation name |
| **state** | State/UT |
| **expected_date** | Expected date of connectivity / GNA to be made effective |

---

## Post-Extraction Operations

These operations are **not part of extraction** — they are logic operations called by the Mapping Handler (Module 3) after CMETS and Effectiveness data are merged.

### Feature 1 — GNA Date Updater

> **Data Sources**: CMETS PDF → `GNA Operationalization Date` column | Effectiveness PDF → `expected_date` column

**Purpose**: Synchronize the GNA Operationalization Date between CMETS and Effectiveness data.

**Row matching**: GNA Application ID (primary) → LTA Application ID (fallback)

```
For each CMETS row matched to an effectiveness record:

┌─────────────────────────────────────────────────────────────────┐
│  Data Source A: CMETS PDF                                       │
│  Column: "GNA Operationalization Date"                          │
│  (extracted near SCoD/SCOD terms in the page text)              │
│                                                                 │
│  Data Source B: Effectiveness PDF                               │
│  Column: "expected_date"                                        │
│  (= expected date of connectivity / GNA to be made effective)   │
│                                                                 │
│  Comparison Rule:                                               │
│  ┌─────────────────────────────────┬──────────────────────────┐ │
│  │ CMETS date is empty             │ → Use effectiveness date │ │
│  │ Effectiveness date > CMETS date │ → Update with eff date   │ │
│  │ CMETS date ≥ Effectiveness date │ → Keep CMETS date        │ │
│  └─────────────────────────────────┴──────────────────────────┘ │
│                                                                 │
│  Then recompute "GNA Operationalization (Yes/No)":              │
│  ┌─────────────────────────────────┬──────────────────────────┐ │
│  │ Final date is in the FUTURE     │ → "Yes"                  │ │
│  │ Final date is TODAY or PAST     │ → "No"                   │ │
│  └─────────────────────────────────┴──────────────────────────┘ │
│                                                                 │
│  "Yes" = GNA not yet operationalized                            │
│  "No"  = GNA already operationalized                            │
└─────────────────────────────────────────────────────────────────┘
```

### Feature 2 — Additional Capacity Date Updater

> **Data Sources**: CMETS PDF → `Date from which additional capacity is to be added` | Effectiveness PDF → `expected_date`

**Purpose**: Synchronize the "Date from which additional capacity is to be added" column.

**Row matching**: GNA → LTA → 5.2 GNA ID cascade (all three tried in order)

Same future-date comparison logic as the GNA Date Updater but targets a different CMETS column. No Yes/No recomputation.

### Feature 3 — Installed/Break-up Capacity Calculator

**Purpose**: Compute the Installed/Break-up Capacity (MW) per technology type for each CMETS row by combining values from **two different source tables**.

#### Data Sources

```
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│  Source 1: CMETS PDF → "Type" column                                   │
│  ─────────────────────────────────────                                 │
│  Contains per-technology MW values embedded in text.                   │
│  Example: "Wind (12) + BESS (44)"                                      │
│  Parsed into: { wind: 12.0, ess: 44.0 }                               │
│                                                                        │
│  Source 2: Effectiveness PDF → per-technology columns                  │
│  ─────────────────────────────────────────────────────                  │
│  • solar_mw   → Solar capacity from effectiveness                     │
│  • wind_mw    → Wind capacity from effectiveness                      │
│  • ess_mw     → ESS/BESS capacity from effectiveness                  │
│  • hydro_mw   → Hydro capacity from effectiveness                     │
│  • type_of_project → Determines project classification                │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

#### Row Matching

Each CMETS row is matched to an effectiveness record using the **GNA → LTA → 5.2 GNA** ID cascade.

#### Computation Rules

```
┌──────────────────────────────────────────────────────────────────────┐
│  For each matched CMETS row:                                         │
│                                                                      │
│  Step 1: Parse CMETS "Type" column                                   │
│  ─────────────────────────────────                                   │
│  "Wind (12) + BESS (44)"  →  { wind: 12.0, ess: 44.0 }             │
│  "Solar (300)"            →  { solar: 300.0 }                       │
│  "Solar"                  →  { solar: 0.0 }  (present, no MW)       │
│                                                                      │
│  Step 2: Read Effectiveness MW values                                │
│  ────────────────────────────────────                                │
│  solar_mw = 150, wind_mw = 0, ess_mw = 20, hydro_mw = 0            │
│                                                                      │
│  Step 3: Sum matching categories                                     │
│  ───────────────────────────────                                     │
│  For SOLAR:  eff solar_mw (150) + cmets solar (300) = 450            │
│  For WIND:   eff wind_mw (0)    + cmets wind (12)   = 12             │
│  For HYDRO:  eff hydro_mw (0)   + cmets hydro (0)   = 0  (skip)     │
│                                                                      │
│  HYBRID rule (special):                                              │
│  ──────────────────────                                              │
│  IF effectiveness type_of_project contains "hybrid"                  │
│  OR effectiveness has multiple technology categories:                │
│    Hybrid = SUM of ALL eff MW + SUM of ALL cmets MW                  │
│  ELSE:                                                               │
│    Hybrid = 0 (not computed)                                         │
│                                                                      │
│  Step 4: Write to output sub-columns                                 │
│  ────────────────────────────────────                                │
│  Only non-zero values are written.                                   │
└──────────────────────────────────────────────────────────────────────┘
```

#### Output Columns

| Output Column | Formula |
|---|---|
| **Installed/Break-up Capacity (MW) Solar** | effectiveness `solar_mw` + CMETS Type solar value |
| **Installed/Break-up Capacity (MW) Wind** | effectiveness `wind_mw` + CMETS Type wind value |
| **Installed/Break-up Capacity (MW) Hydro** | effectiveness `hydro_mw` + CMETS Type hydro value |
| **Installed/Break-up Capacity (MW) Hybrid** | SUM of ALL effectiveness MW + SUM of ALL CMETS Type MW (only when project is hybrid) |

#### Category Mapping

The following keywords from the CMETS Type column map to these categories:

| Type Column Keyword | Mapped Category |
|---|---|
| Solar | solar |
| Wind | wind |
| BESS, ESS | ess |
| Hydro, PSP, Pump Storage | hydro |
| Hybrid | hybrid |

---

## ID Cascade Strategy

Wherever a CMETS row needs to be matched to an effectiveness record, the following priority order is always used:

1. **GNA Application ID** — try first
2. **LTA Application ID** — fallback if GNA doesn't match
3. **5.2 GNA (Enhancement) ID** — last resort

The first successful match wins.

---

## Date Parsing Rules

The system can parse dates in these formats:
- `dd.mm.yyyy` (e.g. "31.03.2030") — Indian convention
- `yyyy-mm-dd` (e.g. "2030-03-31") — ISO format
- `d Month yyyy` (e.g. "31 March 2030") — spelled out

All output dates are formatted as **dd.mm.yyyy**.
