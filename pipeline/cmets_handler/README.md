# CMETS Handler — Module 1

## Purpose
Extracts structured data from **CMETS / GNI connectivity** PDF documents published by Indian power sector regulators (CTUIL/PGCIL).

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

| File | Role | Type |
|---|---|---|
| `column_registry.py` | Single source of truth for ALL columns and rules | Configuration |
| `prompts.py` | LLM system & user prompt templates | Extraction |
| `models.py` | Pydantic schemas (`MappedRow`, `PageResult`, etc.) | Schema |
| `gate.py` | Regex column-header gate — filters relevant pages | Extraction |
| `normalization.py` | Value cleaning, state extraction, date parsing, dedup | Logic |
| `battery_extractor.py` | BESS-specific logic for extracting Battery MWh/Inj/Drw | Logic |
| `extraction.py` | PDF reading (pdfplumber) + LLM row extraction | Extraction |
| `runner.py` | Orchestration: discover → cache → extract → Excel | I/O |

## Extraction Rules & Column Sources

Every column in the final CMETS output is defined in `column_registry.py`. Below is the complete classification detailing the data source and extraction logic for each column:

### Meeting-Level Metadata (Injected from Meeting Classifier)
These columns are derived once per PDF from the first page and applied to all rows in that document.
| Column | Description |
|---|---|
| **CMETS GNA Approved** | Meeting number if PDF is GNA-classified |
| **CMETS LTA Approved** | Meeting number if PDF is LTA-classified |
| **CMETS GNA Meeting Date** | Meeting date (dd.mm.yyyy) if PDF is GNA-classified |
| **CMETS LTA Meeting Date** | Meeting date (dd.mm.yyyy) if PDF is LTA-classified |

### Extracted Columns (Extracted via LLM)
These columns are extracted directly from the PDF page text using LLM prompts and mapped using aliases/headers.
| Column | LLM Key | Aliases / Headers | Normalization / Description |
|---|---|---|---|
| **Substation** | `substaion` | Connectivity Location (As per Application), Nearest Pooling Station (As per Application), Connectivity Granted at, Location requested for Grant of Stage-II Connectivity, Connectivity Injection Point, Sub-station, Substation | `clean` - Substation / connectivity location name (e.g., Aligarh (PG), Bhadla-V) |
| **Project Location** | `Project Location` | Project Location | `clean` - Project location as stated in the application |
| **Name of Developers** | `Name of the developers` | Applicant, Name of Applicant, Developer, Name of Developers | `norm_dev` - Name of the developer / applicant company |
| **GNA/ST II Application ID** | `GNA/ST II Application ID` | Application No. & Date, Application ID, GNA Application ID, ST-II Application ID, GNA/ST II Application ID | `norm_num_ids` - GNA or Stage-II application ID (10-digit, starts with 12/22/11) |
| **LTA Application ID** | `LTA Application ID` | App. No. & Conn. Quantum (MW) of already granted Connectivity, LTA Application ID, LTA App ID | `norm_num_ids_strip` - LTA application ID (prefixed with 04/41) |
| **Application Quantum (MW)(ST II)** | `Application Quantum (MW)(ST II)` | Installed Capacity (MW), Connectivity Quantum (MW), Application Quantum (MW)(ST II), Capacity (MW) | `clean` - Applied connectivity quantum in MW |
| **Granted Quantum GNA/LTA(MW)** | `Granted Quantum GNA/LTA(MW)` | Granted Quantum GNA/LTA(MW), Granted Quantum (MW), Connectivity Quantum (MW) granted, Granted Connectivity Quantum | `clean` - Granted connectivity quantum in MW for GNA/LTA |
| **Battery MWh** | `Battery MWh` | Battery MWh, BESS MWh, Battery (MWh), Battery Energy Storage (MWh) | `norm_battery_mwh` - Battery energy storage capacity in MWh (from BESS rows) |
| **Battery Injection (MW)** | `Battery Injection (MW)` | Battery Injection (MW), BESS Injection (MW), Injection (MW) | `norm_battery_injection` - Battery injection capacity in MW. With BESS, generally smaller than drawl. |
| **Battery Drawl (MW)** | `Battery Drawl (MW)` | Battery Drawl (MW), BESS Drawl (MW), Drawl (MW), Drawal (MW) | `norm_battery_drawl` - Battery drawl capacity in MW. With BESS, generally larger than injection. |
| **PSP MWh** | `PSP MWh` | PSP MWh, Pump Storage MWh | `norm_psp_mwh` - Pump storage project capacity in MWh |
| **PSP Injection (MW)** | `PSP Injection (MW)` | PSP Injection (MW), Pump Storage Injection (MW) | `norm_psp_injection` - Pump storage injection capacity in MW |
| **PSP Drawl (MW)** | `PSP Drawl (MW)` | PSP Drawl (MW), Pump Storage Drawl (MW), PSP Drawal (MW) | `norm_psp_drawl` - Pump storage drawl capacity in MW |
| **Application/Submission Date** | `Application/Submission Date` | Application No. & Date, Submission Date, Application Date, Application/Submission Date | `extract_date` - Application or submission date |
| **Mode(Criteria for applying)** | `Mode(Criteria for applying)` | Criterion for applying, Criteria for applying, Mode, Mode(Criteria for applying) | `clean` - Mode or criteria for applying (e.g., Land BG Route, SECI LOA) |
| **Applied Start of Connectivity sought by developer date<br>( start date of connectivity as per the application)** | `Applied Start of Connectivity sought by developer date` | Start Date of Connectivity (As per Application), Applied Start of Connectivity sought by developer date, Start Date of Connectivity | `extract_date` - Start date of connectivity as per the application |
| **Date from which additional capacity is to be added** | `Date from which additional capacity is to be added` | Date from which additional capacity is to be added, Additional Capacity Date | `extract_date` - Date from which additional capacity is to be added |
| **Nature of Applicant** | `Nature of Applicant` | Nature of Applicant | `clean` - Nature of applicant (Generator, Bulk consumer, etc.) |
| **Status of application(Withdrawn / granted. Revoked.)** | `Status of application(Withdrawn / granted. Revoked.)` | Status of Application, Status, Withdrawn / granted / Revoked | `norm_status` - Application status: Withdrawn, granted, Revoked |
| **Voltage level** | `Voltage` | Voltage, Voltage Level, kV Level | `norm_voltage` - Voltage level of the substation/connectivity point (e.g., 400 kV, 220 kV) |
| **GNA Operationalization Date** | `GNA Operationalization Date` | GNA Operationalization Date, SCoD, SCOD | `extract_date` - GNA operationalization date (near SCoD/SCOD in text) |

### Derived & Calculated Columns
These columns are derived from extracted values or calculated using logic.
| Column | Source | Derived From / Rule | Normalization / Description |
|---|---|---|---|
| **State** | `derived` | `Project Location` | `extract_state` - Indian state / UT derived from Project Location text |
| **Type** | `derived` | `type` (extracted) | `norm_type` - Energy source type (Solar, Wind, Hybrid, BESS, Solar + BESS, etc.) |
| **Application ID under Enhancement 5.2 or revision** | `calculated` | `GNA/ST II Application ID`, `LTA Application ID`, `Mode(Criteria for applying)` | `derive_enhancement_id` - Derived only when context mentions Enhancement 5.2 / regulation 5.2 / revision |
| **GNA Operationalization (Yes/No)** | `calculated` | `GNA Operationalization Date` | `gna_yes_no` - Yes if GNA Operationalization Date is in the future, No otherwise |

## Internal Bookkeeping Columns
- **PDF**: Source PDF file path
- **Page Number**: Page number within the PDF

## How to Change

- **Modify Extraction Rules**: All column definitions (aliases, normalization functions, sources) are centralized in `column_registry.py`. Simply update the respective `ColumnDef` inside `COLUMN_DEFS`.
- **Add/Remove Output Columns**: Update `COLUMN_DEFS` in `column_registry.py` and modify `models.py -> MappedRow`.
- **Change LLM Instructions**: Edit `prompts.py` (which references rules from `column_registry.py`).
- **Update Normalization Logic**: Edit `normalization.py` where functions mapped via `NORM_FUNCTIONS` reside.
- **Support New Table Formats / Value Fingerprints**: Edit `gate.py` (`TARGET_COLUMN_VARIANTS`, `VALUE_FINGERPRINTS`).
