# Mapping Handler — Module 3

## Purpose
Merges **CMETS data** (from Module 1) with **Effectiveness data** (from Module 2)
to produce an enriched output with region, capacity breakdowns, developer details,
and updated GNA Operationalization dates.

## Data Flow

```
cmets_extracted.xlsx                ← Input: from Module 1
output/effectiveness_cache/*.json   ← Input: from Module 2 (JSON cache)

          ↓  (lookup build + row-by-row merge + date update)

effectiveness_mapped.json           ← Output: merged JSON (for downstream)
effectiveness_mapped.xlsx           ← Output: formatted Excel report
```

## Sub-Layer Architecture

| File               | Role | Type |
|--------------------|------|------|
| `lookup.py`        | Builds `application_id → record` lookup dictionary | Logic |
| `merge.py`         | Row-by-row merge logic, enrichment column population | Logic |
| `formatting.py`    | Professional Excel styling (colours, borders, etc.) | Formatting |
| `runner.py`        | Orchestration: load → lookup → merge → date-update → JSON + Excel | I/O |

**External dependency**: `date_updater.py` from `effectiveness_handler` (called after merge)

## Processing Pipeline

```
Step 1: Build lookup dict (application_id → record)       [lookup.py]
Step 2: Load CMETS Excel                                   [runner.py]
Step 3: Row-by-row merge (GNA ID → LTA ID fallback)        [merge.py]
   └→ Update: developer name, substation, state, quantum
   └→ Add: Region, Type of Project, capacity breakdowns
Step 4: GNA Operationalization Date update                  [date_updater.py]
   └→ Compare eff expected_date vs CMETS GNA Op Date
   └→ Update to later date + recompute Yes/No
Step 5: Dump JSON                                           [runner.py]
Step 6: Write formatted Excel                               [runner.py + formatting.py]
```

## Logic Operations

### Step 1 — Lookup Build (`lookup.py → build_lookup()`)
- Seeds from the in-memory DataFrame (Module 2's current run)
- Supplements/overrides with all on-disk JSON cache files
- Result: `{application_id: record_dict}` dictionary

### Step 3 — Row Merge (`merge.py → merge_rows()`)
For each CMETS row:
- **Primary match**: Extract IDs from `GNA/ST II Application ID` → look up
- **Fallback match**: If no primary match → try `LTA Application ID`
- If matched: update developer name, substation, state, quantum
- Add enrichment columns: Region, Type of Project, installed capacity breakdowns

### Step 4 — GNA Date Update (`date_updater.py → update_gna_dates()`)
For each matched CMETS row:
- Parse effectiveness `expected_date` (connectivity/GNA to be made effective)
- Parse CMETS `GNA Operationalization Date`
- If eff date > CMETS date → update to effectiveness date
- Recompute `GNA Operationalization (Yes/No)` based on new date

## How to Change

- **Change matching strategy**: Edit `merge.py → merge_rows()`
- **Add/remove enrichment columns**: Edit `merge.py → NEW_COLUMNS`, `_EFF_FIELD_TO_COL`
- **Change lookup source priority**: Edit `lookup.py → build_lookup()`
- **Change date comparison logic**: Edit `effectiveness_handler/date_updater.py`
- **Change Excel styling**: Edit `formatting.py`
