# Mapping Handler — Module 3

## Purpose
Merges **CMETS data** (from Module 1) with **Effectiveness data** (from Module 2)
to produce an enriched output with region, capacity breakdowns, and developer details.

## Data Flow

```
cmets.xlsx                          ← Input: from Module 1
effectiveness_output/*.json         ← Input: from Module 2 (JSON cache)

          ↓  (lookup build + row-by-row merge)

effectiveness_mapped.json           ← Output: merged JSON (for downstream)
effectiveness_mapped.xlsx           ← Output: formatted Excel report
```

## Sub-Layer Architecture

| File               | Responsibility                                     |
|--------------------|----------------------------------------------------|
| `lookup.py`        | Builds `application_id → record` lookup dictionary  |
| `merge.py`         | Row-by-row merge logic, enrichment column population |
| `formatting.py`    | Professional Excel styling (colours, borders, etc.)  |
| `runner.py`        | Orchestration: load → lookup → merge → JSON + Excel  |

## How Mapping Works

1. **Lookup Build** (`lookup.py → build_lookup()`):
   - Seeds from the in-memory DataFrame (Module 2's current run)
   - Supplements/overrides with all on-disk JSON cache files
   - Result: `{application_id: record_dict}` dictionary

2. **ID Matching** (`merge.py → merge_rows()`):
   For each CMETS row:
   - **Primary match**: Extract IDs from `GNA/ST II Application ID`
     and look them up in the effectiveness dict
   - **Fallback match**: If no primary match, try `LTA Application ID`
   - If matched, update existing columns (developer name, substation,
     state, quantum) and populate new enrichment columns

3. **Enrichment Columns** (added by `merge.py`):
   - `Region`
   - `Type of Project`
   - `Installed capacity (MW) solar / wind / ess / hydro / hybrid`

4. **Output** (`runner.py`):
   - `effectiveness_mapped.json` — full merged data + match statistics
   - `effectiveness_mapped.xlsx` — formatted Excel with frozen header

## How to Change Mapping Logic

- **Change matching strategy**: Edit `merge.py` → `merge_rows()`
- **Add/remove enrichment columns**: Edit `merge.py` → `NEW_COLUMNS`, `_EFF_FIELD_TO_COL`
- **Change lookup source priority**: Edit `lookup.py` → `build_lookup()`
- **Change Excel styling**: Edit `formatting.py`
