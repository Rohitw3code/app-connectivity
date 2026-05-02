# CMETS Handler — Module 1

## What It Does

Reads **CMETS / GNI connectivity** PDF documents and extracts structured row-level data about renewable energy connectivity applications (solar, wind, BESS, etc.) into a consolidated Excel sheet.

---

## Input → Output

| | Path |
|---|---|
| **Input PDFs** | `source/cmets_pdfs/*.pdf` |
| **Cache (JSON)** | `output/cmets_cache/<pdf_name>.json` |
| **Output Excel** | `excels/cmets_extracted.xlsx` |

---

## How Extraction Works

### Step 1 — Page Filtering (Gate)

Not every page in a CMETS PDF contains data. The gate scans each page for **known column headers** (e.g. "Applicant", "Connectivity", "Application No.") and **value fingerprints** (e.g. 10-digit IDs, substation name patterns like "Bhadla-V").

- A page must match **at least 3** known column headers to pass the gate.
- Pages that fail the gate are skipped entirely.

### Step 2 — LLM Row Extraction

Each qualifying page is sent to GPT-4o-mini, which reads the full page text and returns structured JSON rows. The LLM maps each piece of data to the correct output column using:

- **Header name mapping** — e.g. "Connectivity Location (As per Application)" → Substation
- **Value fingerprint matching** — e.g. a 10-digit number starting with "12" → GNA Application ID

### Step 3 — Normalization & Validation

Every extracted value goes through cleaning and normalization rules before being written to Excel.

### Step 4 — Caching

Each PDF is processed once. Results are cached as JSON. Delete the cache file to force re-extraction.

---

## Columns Extracted

### Application Identifiers

| Column | What It Contains |
|---|---|
| **GNA/ST II Application ID** | 10-digit GNA or Stage-II application ID (starts with 12, 22, or 11) |
| **LTA Application ID** | Long-term access application ID (prefixed with 04 or 41) |
| **Application ID under Enhancement 5.2 or revision** | Only filled when context mentions Enhancement 5.2 or regulation 5.2 |

> **Primary Key Rule**: A row must have at least one of the three IDs above. Rows with none are discarded.

### Developer & Location

| Column | What It Contains |
|---|---|
| **Name of Developers** | Company/applicant name (e.g. "ACME Greentech Urja Private Limited") |
| **Substation** | Connectivity location (e.g. "Aligarh (PG)", "Bhadla-V") |
| **Project Location** | Project location as stated in the application |
| **State** | Indian state/UT — derived automatically from Project Location |

### Project Details

| Column | What It Contains |
|---|---|
| **Type** | Energy source type — see [Type Extraction Workflow](#type-extraction-workflow) below |
| **Application Quantum (MW)(ST II)** | Applied connectivity quantum in MW |
| **Granted Quantum GNA/LTA(MW)** | Quantum actually granted (may differ from applied) |
| **Voltage level** | Voltage of the substation/connectivity point (e.g. "400 kV", "220 kV") |

### Battery (BESS) Values

| Column | When Populated | What It Contains |
|---|---|---|
| **Battery MWh** | Only when BESS/Battery is mentioned | Battery energy storage capacity in MWh |
| **Battery Injection (MW)** | Only when BESS/Battery is mentioned | Injection capacity (generally smaller than drawl) |
| **Battery Drawl (MW)** | Only when BESS/Battery is mentioned | Drawl capacity (generally larger than injection) |

Battery extraction uses an **LLM call** to accurately identify BESS values from the Type column (e.g. `BESS (19)`) and from contextual text in the description.

### PSP (Pump Storage) Values

| Column | When Populated |
|---|---|
| **PSP MWh** | Only when pump storage / PSP is mentioned |
| **PSP Injection (MW)** | Only when PSP context is present |
| **PSP Drawl (MW)** | Only when PSP context is present |

### Dates

| Column | What It Contains |
|---|---|
| **Application/Submission Date** | Date the application was submitted |
| **Applied Start of Connectivity sought by developer date** | Start date of connectivity as per the application |
| **Date from which additional capacity is to be added** | Only filled if explicitly present in the table |
| **GNA Operationalization Date** | Found near SCoD/SCOD terms in the text |
| **GNA Operationalization (Yes/No)** | See [GNA Operationalization Rules](#gna-operationalization-date--yesno-rules) below |

### Other

| Column | What It Contains |
|---|---|
| **Nature of Applicant** | Generator, Bulk consumer, etc. |
| **Mode (Criteria for applying)** | e.g. "Land BG Route", "SECI LOA", "NTPC LOA" |
| **Status of application** | Withdrawn, granted, or Revoked |

### Meeting-Level Metadata

These are derived once per PDF from the first page and applied to all rows:

| Column | What It Contains |
|---|---|
| **CMETS GNA Approved** | Meeting number if the PDF is GNA-classified |
| **CMETS LTA Approved** | Meeting number if the PDF is LTA-classified |
| **CMETS GNA Meeting Date** | Meeting date (dd.mm.yyyy) for GNA PDFs |
| **CMETS LTA Meeting Date** | Meeting date (dd.mm.yyyy) for LTA PDFs |

---

## Skip Rules

The following rows are **never extracted**:

- Rows where "Nature of Applicant" is "Bulk consumer", "Drawee entity", or "Drawee entity connected"
- Rows from tables containing GNARE columns (e.g. "GNARE within Region (MW)", "Total GNARE Required (MW)")
- Rows that have none of the three primary key IDs

---

## Type Extraction Workflow

The **Type** field goes through a multi-stage pipeline with strict rules:

```
┌──────────────────────────────────────────────────────────┐
│  CMETS PDF Page (raw text)                               │
│  e.g. "Solar (300)", "Hybrid + BESS", "Generator (Wind)" │
└──────────────────┬───────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────┐
│  STAGE 1: LLM Extraction (prompts.py)                    │
│                                                          │
│  The LLM is instructed to:                               │
│  • Extract ONLY: Solar, BESS, Wind, Solar+Wind,          │
│    Solar+BESS keywords                                   │
│  • Preserve MW values in parentheses: "Solar (300)"      │
│  • NOT include sentences, descriptions, or other words   │
│                                                          │
│  Output: "Solar (300)" or "Wind (12) + BESS (19)"        │
└──────────────────┬───────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────┐
│  STAGE 2: Normalization (normalization.py → norm_type)    │
│                                                          │
│  Token-by-token regex parsing:                           │
│  1. Scan for keywords: solar, wind, bess, hybrid         │
│  2. Map "hybrid" → "Solar+Wind"                          │
│  3. Capture parenthetical MW values: (300), (19)         │
│  4. Reconstruct: "keyword (value) + keyword (value)"     │
│  5. Drop anything not matching (Thermal → None)          │
│                                                          │
│  Examples:                                               │
│    "solar (300)"          → "Solar (300)"                │
│    "Wind (12) + BESS (19)"→ "Wind (12) + BESS (19)"     │
│    "Hybrid (500)"         → "Solar+Wind (500)"           │
│    "Generator (Solar)"    → "Solar"                      │
│    "Thermal"              → None                         │
└──────────────────┬───────────────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────────────┐
│  FINAL OUTPUT (Excel)                                    │
│                                                          │
│  Strictly one of:                                        │
│   Solar, Solar (300), Wind, Wind (12),                   │
│   BESS, BESS (50), Solar+Wind, Solar+Wind (500),        │
│   Solar+BESS, Solar (300) + BESS (19)                    │
│   ... or empty/null                                      │
└──────────────────────────────────────────────────────────┘
```

### Allowed Keywords

| Keyword | Meaning | Example |
|---|---|---|
| **Solar** | Solar project | `Solar (300)` |
| **Wind** | Wind project | `Wind (12)` |
| **BESS** | Battery Energy Storage | `BESS (50)` |
| **Solar+Wind** | Hybrid project (also maps "Hybrid") | `Solar+Wind (500)` |
| **Solar+BESS** | Solar with battery | `Solar (300) + BESS (19)` |

---

## GNA Operationalization Date & Yes/No Rules

### Data Sources

| Data Point | Source Table | Column |
|---|---|---|
| **GNA Operationalization Date (initial)** | CMETS PDF | Near SCoD/SCOD terms in the page text |
| **Expected Date** | Effectiveness PDF | `expected_date` column |
| **GNA Operationalization Date (final)** | Computed | The later of the two dates above |

### Update Rule (runs during Module 3 merge)

```
┌──────────────────────────────────────────────────────────┐
│  For each CMETS row:                                     │
│                                                          │
│  1. Match to Effectiveness record using:                 │
│     GNA ID → LTA ID (if GNA fails)                       │
│                                                          │
│  2. Read Effectiveness "expected_date"                   │
│  3. Read CMETS "GNA Operationalization Date"             │
│                                                          │
│  4. Compare:                                             │
│     ┌───────────────────────────────┬──────────────────┐ │
│     │ Condition                     │ Action           │ │
│     ├───────────────────────────────┼──────────────────┤ │
│     │ CMETS date is empty           │ Use eff date     │ │
│     │ Eff date > CMETS date         │ Update to eff    │ │
│     │ CMETS date ≥ Eff date         │ Keep CMETS date  │ │
│     └───────────────────────────────┴──────────────────┘ │
│                                                          │
│  5. Recompute Yes/No from the final date:                │
│     ┌───────────────────────────────┬──────────────────┐ │
│     │ Final date is in the FUTURE   │ → "Yes"          │ │
│     │ Final date is TODAY or PAST   │ → "No"           │ │
│     └───────────────────────────────┴──────────────────┘ │
│                                                          │
│  "Yes" = GNA not yet operationalized (future date)       │
│  "No"  = GNA already operationalized (past/today date)   │
└──────────────────────────────────────────────────────────┘
```

### Date Output Format

All dates are written in Indian convention: **dd.mm.yyyy** (e.g. "31.03.2030")
