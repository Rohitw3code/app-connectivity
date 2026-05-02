# Bay Allocation — Modules 5 & 6

## What It Does

The Bay Allocation pipeline has two parts:
1. **Module 5 — Bay Allocation Extraction**: Reads Bay Allocation PDF documents and extracts substation-level data including bay numbers and entity names at 220kV and 400kV voltage levels.
2. **Module 6 — Bay Mapping**: Cross-references the extracted bay allocation data with CMETS data to enrich each CMETS row with bay numbers, substation names, and coordinates.

---

## Input → Output

| | Path |
|---|---|
| **Input PDFs** | `source/bayallocation/*.pdf` |
| **Cache (JSON)** | `output/bayallocation_cache/<pdf_name>.json` |
| **Extraction Excel** | `excels/bayallocation_extracted.xlsx` |
| **Final Enriched Output** | Written back to the CMETS mapped output |

---

## Module 5 — Bay Allocation Extraction

### How Extraction Works

#### Step 1 — Page Gate (Keyword Filter)

Each page must contain **all required keywords** (e.g. "Bay", "Substation", "Entity") to pass the gate. Pages that fail are skipped.

#### Step 2 — Table Detection

- pdfplumber extracts all tables on the page
- The system looks for a table whose header rows match at least **3 target column fragments** (e.g. "name of substation", "bay no", "name of entity")
- Header rows are skipped (first few rows are treated as headers)

#### Step 3 — Row Parsing

For each data row in the matching table:

1. **Noise filtering** — the following rows are automatically skipped:
   - Empty rows
   - Sub-header rows (containing labels like "220kV", "400kV", "Bay No.", "Name of Entity")
   - Section headers (e.g. "Section-A")
   - Total/subtotal rows (rows containing only a single number)

2. **Substation identification** — a new substation starts when column 0 (Sl. No.) or column 1 (Name of Substation) has a value

3. **Bay data collection** — each row contributes bay numbers and entity names to the current substation

### Data Extracted Per Substation

| Field | Data Source | What It Contains |
|---|---|---|
| **sl_no** | Bay Allocation PDF → Column 0 | Serial number |
| **name_of_substation** | Bay Allocation PDF → Column 1 | Substation name (e.g. "Bhadla-V") |
| **substation_coordinates** | Bay Allocation PDF → Column 2 | Geographic coordinates of the substation |
| **region** | Bay Allocation PDF → Column 3 | Region (NR, SR, ER, WR, NER) |
| **220kV bay_no** | Bay Allocation PDF → Column 7 (bay no), Column 9 (entity) | Dictionary: bay number → entity name at 220kV |
| **400kV bay_no** | Bay Allocation PDF → Column 10 (bay no), Column 12 (entity) | Dictionary: bay number → entity name at 400kV |

### Bay Number Data Structure

Each bay number is stored as a dictionary mapping the bay number to the entity name. If a bay exists but has no entity assigned, the value is an empty string.

**Example**:
```
{
    "204": "ACME Greentech Urja Private Limited",
    "34":  "",
    "24":  "Adani Renewable Energy"
}
```

### Excel Output Format

In the Excel output, the bay data is formatted as readable strings:
- `"204: ACME Greentech | 34:  | 24: Adani Renewable Energy"`

---

## Module 6 — Bay Mapping (CMETS Enrichment)

### Purpose

For each row in the CMETS data, find matching bay allocation entries and add bay number, substation name, and coordinates.

### Data Sources

```
┌────────────────────────────────────────────────────────────────────────┐
│                                                                        │
│  Source 1: CMETS Extracted Data (Module 1 output)                      │
│  ──────────────────────────────────────────────────                     │
│  Columns used for matching:                                            │
│  • "Voltage level" → determines which voltage group to search          │
│    (220kV or 400kV)                                                    │
│  • "Name of the developers" → used to find the entity in bay data     │
│                                                                        │
│  Source 2: Bay Allocation Data (Module 5 output)                       │
│  ──────────────────────────────────────────────────                     │
│  • 220kV bay entries → entity name, bay number, substation info        │
│  • 400kV bay entries → entity name, bay number, substation info        │
│  • Substation coordinates                                              │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

### How the Lookup Index Works

Before matching begins, a lookup index is built from all bay allocation cache files:

```
┌──────────────────────────────────────────────────────────────────────┐
│  Lookup Index Structure                                              │
│                                                                      │
│  {                                                                   │
│    "220kv": [                                                        │
│      {                                                               │
│        "entity_name": "ACME Greentech Urja Private Limited",         │
│        "bay_no": "204",                                              │
│        "name_of_substation": "Bhadla-V",                             │
│        "substation_coordinates": "27.5°N, 71.9°E",                   │
│        "region": "NR",                                               │
│        "sl_no": "15"                                                 │
│      },                                                              │
│      ...                                                             │
│    ],                                                                │
│    "400kv": [ ... ]                                                  │
│  }                                                                   │
│                                                                      │
│  Rules:                                                              │
│  • Only entries with a non-empty entity_name are included            │
│  • Empty bays (no entity assigned) are excluded from the index       │
│  • All cache JSON files in the output directory are scanned          │
└──────────────────────────────────────────────────────────────────────┘
```

### Matching Workflow

```
┌──────────────────────────────────────────────────────────────────────┐
│  For each CMETS row:                                                 │
│                                                                      │
│  Step 1: Normalize Voltage                                           │
│  ─────────────────────────                                           │
│  CMETS "Voltage level" → canonical key                               │
│  "220 kV", "220kV", "220kv"  →  "220kv"                             │
│  "400 kV", "400kV", "400kv"  →  "400kv"                             │
│  Anything else               →  SKIP (no voltage = no match)        │
│                                                                      │
│  Step 2: Normalize Developer Name                                    │
│  ────────────────────────────────                                    │
│  CMETS "Name of the developers" is cleaned:                         │
│  • Convert to lowercase                                              │
│  • Remove suffixes: "Pvt. Ltd.", "Private Limited", "LLP", "Ltd."   │
│  • Remove punctuation: . - , ; : ( )                                │
│  • Collapse whitespace                                               │
│                                                                      │
│  Step 3: Search Bay Entries                                          │
│  ──────────────────────────                                          │
│  Scan all entries under the matching voltage key (220kv or 400kv).   │
│                                                                      │
│  Matching rule (for each bay entity):                                │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │  1. If bay cell has multiple entities (separated by ";"):      │  │
│  │     → Each entity is checked independently                    │  │
│  │                                                                │  │
│  │  2. Normalize both names (same rules as step 2)                │  │
│  │                                                                │  │
│  │  3. Check for match:                                           │  │
│  │     a. Exact match after normalization                         │  │
│  │     b. CMETS name is inside bay entity name (substring)        │  │
│  │     c. Bay entity name is inside CMETS name (substring)        │  │
│  │     d. Core names match (after removing parenthetical          │  │
│  │        capacity info like "(50.6 MW)")                         │  │
│  │                                                                │  │
│  │  Any of a/b/c/d = MATCH                                       │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  Step 4: Handle Multiple Matches                                     │
│  ────────────────────────────────                                    │
│  • Deduplicate by bay number (same bay won't appear twice)           │
│  • All unique matching bays are concatenated with " | "              │
│  • Substation names/coordinates are also concatenated and            │
│    deduplicated (multiple bays often share the same substation)      │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Output Columns Added to CMETS

| Column | Data Source | What It Contains |
|---|---|---|
| **Bay No (Bay Allocation)** | Bay Allocation PDF | Matched bay number(s), separated by ` \| ` if multiple |
| **Substation Name (Bay Allocation)** | Bay Allocation PDF | Name of the substation from bay allocation data |
| **Substation Coordinates (Bay Allocation)** | Bay Allocation PDF | Geographic coordinates of the substation |

### Coordinate Rules

- Coordinates come directly from the Bay Allocation PDF table (Column 2)
- They are only populated when a bay match is found
- If multiple bays match from different substations, coordinates are concatenated and deduplicated
- If no bay match is found, the coordinate column stays empty

### Bay Number Rules

- A bay number is a numeric string (e.g. "204", "34", "100")
- Bay numbers are voltage-specific — a 220kV bay and a 400kV bay are searched separately based on the CMETS Voltage column
- Only bays with a non-empty entity name are considered for matching
- If the same developer has multiple bays at the same substation, all bay numbers are returned (e.g. "204 | 208")

---

## Mapping Handler — Module 3 (Merge Operations)

### Purpose

The Mapping Handler is the orchestration layer that merges CMETS and Effectiveness data and runs all the post-extraction operations in sequence.

### Data Sources Used

```
┌──────────────────────────────────────────────────────────────────────┐
│  Merge brings together data from THREE source PDFs:                  │
│                                                                      │
│  ┌─────────────────┐    ┌──────────────────────┐                    │
│  │  CMETS PDF       │    │  Effectiveness PDF    │                   │
│  │  (Module 1)      │    │  (Module 2)           │                   │
│  │                  │    │                       │                    │
│  │  • Application   │    │  • application_id     │                   │
│  │    IDs           │    │  • developer name     │                   │
│  │  • Developer     │    │  • region             │                   │
│  │  • Substation    │    │  • type_of_project    │                   │
│  │  • Type          │    │  • solar/wind/ess/    │                   │
│  │  • Dates         │    │    hydro MW           │                   │
│  │  • Quantum       │    │  • expected_date      │                   │
│  └────────┬─────────┘    └──────────┬────────────┘                   │
│           │                         │                                │
│           └─────────┬───────────────┘                                │
│                     ▼                                                │
│          ┌─────────────────────┐                                     │
│          │  Bay Allocation PDF  │                                    │
│          │  (Module 5)          │                                    │
│          │                     │                                     │
│          │  • Bay numbers      │                                     │
│          │  • Substation name  │                                     │
│          │  • Coordinates      │                                     │
│          └─────────────────────┘                                     │
└──────────────────────────────────────────────────────────────────────┘
```

### Merge Logic

For each CMETS row, the system tries to find a matching effectiveness record using the **GNA → LTA → 5.2 GNA** ID cascade:

1. Split the CMETS Application ID cell into individual IDs
2. Look up each ID in the effectiveness data
3. First match wins

### What Gets Updated When a Match Is Found

**Overlapping columns** (effectiveness value overwrites CMETS if valid):

| Effectiveness Field | CMETS Column Updated | Data Source |
|---|---|---|
| name_of_applicant | Name of Developers | Effectiveness PDF |
| substation | Substation | Effectiveness PDF |
| state | State | Effectiveness PDF |
| installed_capacity_mw | Application Quantum (MW)(ST II) | Effectiveness PDF |

**New enrichment columns added**:

| Column | Data Source | Source Column |
|---|---|---|
| Region | Effectiveness PDF | `region` |
| Type of Project | Effectiveness PDF | `type_of_project` |
| Installed capacity (MW) solar | Effectiveness PDF | `solar_mw` |
| Installed capacity (MW) wind | Effectiveness PDF | `wind_mw` |
| Installed capacity (MW) ess | Effectiveness PDF | `ess_mw` |
| Installed capacity (MW) hydro | Effectiveness PDF | `hydro_mw` |
| Installed capacity (MW) hybrid | Computed | Sum of all MW (when project is hybrid type) |

### Execution Order

The mapping handler runs these operations in sequence:

```
Step 1: Merge
  CMETS data + Effectiveness data
  → Match rows via GNA → LTA → 5.2 ID cascade
  → Populate enrichment columns

Step 2: GNA Date Update
  Data sources: CMETS "GNA Operationalization Date" + Effectiveness "expected_date"
  → Compare dates, keep the later one
  → Recompute Yes/No

Step 3: Additional Capacity Date Update
  Data sources: CMETS "Date from which additional capacity..." + Effectiveness "expected_date"
  → Compare dates, keep the later one

Step 4: Installed Capacity Calculation
  Data sources: CMETS "Type" column + Effectiveness solar/wind/ess/hydro MW columns
  → Parse Type column for MW values
  → Sum matching technology categories

Step 5: Bay Mapping
  Data sources: CMETS "Voltage" + "Developer Name" + Bay Allocation extracted data
  → Match developer to bay entity by voltage level and name
  → Add bay number, substation name, coordinates
```
