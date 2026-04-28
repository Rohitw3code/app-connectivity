"""
jcc_handler/jcc_output_layer.py — JCC Output Layer (GNA / TGNA Extraction)
===========================================================================
Cross-references JCC extracted data with Effectiveness output to produce
a final "JCC Output" Excel with 4 columns:

    Developer Name | Substation | TGNA | GNA

Pipeline Steps
--------------
  Step 1 — Load effectiveness output sheet (substation + developer name)
  Step 2 — Load all extracted JCC JSON data (flattened rows)
    Step 3 — For each effectiveness row, match to a JCC row using
                     GNA/LTA/Enhancement 5.2 application IDs when available.
                     If no IDs are present, fallback to fuzzy match:
                         substation ↔ pooling_station
                         developer  ↔ connectivity_applicant
  Step 4 — From the matched JCC row, compute GNA and TGNA:
             GNA  = sum of Generation (MW) when status is "Effective"
             TGNA = sum of Commissioned MW when status is NOT "Effective"
  Step 5 — Write the 4-column output Excel

GNA Logic
---------
  1. Read "connectivity_start_date_under_gna" column from the matched JCC row
  2. If the cell contains the word "Effective" (and NOT "not effective"):
     → Go to the "schedule_as_per_current_jcc" column
       (= "Under Grantee scope Gen Commissioning / Connectivity line schedule")
     → Find all MW values under the Generation (MW) / COD section
     → GNA = sum of all those MW values

TGNA Logic
----------
  1. Read "connectivity_start_date_under_gna" column from the matched JCC row
  2. If the cell does NOT contain "Effective" but instead contains a phrase like
     "Connectivity likely to be operationalized upon commissioning of required
      Transmission system":
     → Go to the "schedule_as_per_current_jcc" column
     → Find all MW values that are tagged "(Commissioned)"
       e.g.  "111.8 MW: 19.05.2025 (Commissioned)"
             "88.2 MW: 01.06.2025 (Commissioned)"
     → TGNA = sum of those Commissioned MW values  (111.8 + 88.2 = 200)
"""

from __future__ import annotations

import json
import logging
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

import pandas as pd

from pipeline.excel_utils import export_to_excel

logger = logging.getLogger(__name__)

# ── Output column definitions ────────────────────────────────────────────────

JCC_OUTPUT_COLUMNS = [
    "Developer Name",
    "Substation",
    "TGNA",
    "GNA",
]


# ─────────────────────────────────────────────────────────────────────────────
# STEP 1 — Load Effectiveness Data
# ─────────────────────────────────────────────────────────────────────────────

def load_effectiveness_data(
    effectiveness_excel_path: Path | str | None = None,
    effectiveness_df: pd.DataFrame | None = None,
    effectiveness_output_dir: Path | str | None = None,
) -> list[dict]:
    """Load effectiveness records containing substation/developer or IDs.

    Priority:
      1. Pre-loaded DataFrame (from Module 2/3 in the same run)
      2. Excel file on disk  (effectiveness_combined.xlsx)
      3. JSON cache files    (effectiveness_output/*.json)

    Returns a list of dicts, each with at least:
        { "substation": "...", "name_of_applicant": "..." }
    or application ID columns such as GNA/LTA/Enhancement 5.2.
    """
    records: list[dict] = []

    # ── From DataFrame ────────────────────────────────────────────────────
    if effectiveness_df is not None and not effectiveness_df.empty:
        logger.info("[JCC-Output] Using in-memory effectiveness DataFrame (%d rows)", len(effectiveness_df))
        records = effectiveness_df.to_dict(orient="records")
        return _filter_valid(records)

    # ── From Excel ────────────────────────────────────────────────────────
    if effectiveness_excel_path:
        xlsx = Path(effectiveness_excel_path).resolve()
        if xlsx.exists():
            logger.info("[JCC-Output] Loading effectiveness Excel: %s", xlsx)
            df = pd.read_excel(xlsx, sheet_name=0)
            records = df.to_dict(orient="records")
            return _filter_valid(records)

    # ── From JSON cache ───────────────────────────────────────────────────
    if effectiveness_output_dir:
        eff_dir = Path(effectiveness_output_dir).resolve()
        if eff_dir.exists():
            logger.info("[JCC-Output] Loading effectiveness JSONs from: %s", eff_dir)
            for jf in sorted(eff_dir.glob("*.json")):
                try:
                    with open(jf, "r", encoding="utf-8") as fh:
                        data = json.load(fh)
                    if isinstance(data, list):
                        records.extend(data)
                    elif isinstance(data, dict):
                        records.append(data)
                except Exception as exc:
                    logger.warning("[JCC-Output] Could not read %s: %s", jf.name, exc)
            return _filter_valid(records)

    logger.warning("[JCC-Output] No effectiveness data source available.")
    return []


def _filter_valid(records: list[dict]) -> list[dict]:
    """Keep records that have substation/developer or any application ID."""
    valid = []
    for rec in records:
        substation = _safe_str(rec.get("substation"))
        developer  = _safe_str(rec.get("name_of_applicant"))
        id_values = _collect_ids_from_record(rec)
        if substation or developer or id_values:
            valid.append(rec)
    return valid


# ─────────────────────────────────────────────────────────────────────────────
# STEP 2 — Flatten JCC Extracted Data
# ─────────────────────────────────────────────────────────────────────────────

def flatten_jcc_data(all_results: list[dict]) -> list[dict]:
    """Flatten per-PDF JCC results into a single list of row dicts.

    Input shape (from runner / JSON cache):
        [ { "source": "...",
            "pages": [ { "rows": [ { "pooling_station": ..., ... }, ... ] } ] } ]

    Output:
        [ { "source_pdf": ..., "pooling_station": ..., "connectivity_applicant": ..., ... } ]
    """
    flat: list[dict] = []
    for pdf_result in all_results:
        source = pdf_result.get("source", "")
        for page in pdf_result.get("pages", []):
            for row in page.get("rows", []):
                rec = {"source_pdf": source}
                rec.update(row)
                flat.append(rec)
    return flat


# ─────────────────────────────────────────────────────────────────────────────
# STEP 3 — ID Matching + Fuzzy Fallback (Substation + Developer Name)
# ─────────────────────────────────────────────────────────────────────────────

def _safe_str(val) -> str:
    """Convert a value to a clean string, handling None/NaN."""
    if val is None:
        return ""
    if isinstance(val, float) and pd.isna(val):
        return ""
    return " ".join(str(val).split()).strip()


def _normalize(text: str) -> str:
    """Lower-case and collapse whitespace for comparison."""
    return " ".join(str(text).lower().strip().split())


def _fuzzy_score(a: str, b: str) -> float:
    """Similarity ratio (0.0 – 1.0) between two strings."""
    na, nb = _normalize(a), _normalize(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


def _substring_match(needle: str, haystack: str) -> bool:
    """Check if the shorter string is contained in the longer one."""
    nn, nh = _normalize(needle), _normalize(haystack)
    if not nn or not nh:
        return False
    return nn in nh or nh in nn


def _split_ids(raw) -> list[str]:
    """Split a cell containing one or more application IDs."""
    if raw is None:
        return []
    text = str(raw).strip()
    if not text:
        return []
    parts = [p.strip() for p in re.split(r"[;,|\n]+", text) if p.strip()]
    return parts if parts else [text]


def _normalize_id(text: str) -> str:
    """Normalize an application ID for substring matching."""
    if not text:
        return ""
    norm = _normalize(text)
    return re.sub(r"[^a-z0-9/\-]", "", norm)


def _collect_ids_from_record(record: dict) -> list[str]:
    """Collect candidate application IDs from a record (case-insensitive keys)."""
    if not record:
        return []

    key_map = {k.lower(): k for k in record.keys()}
    candidates = [
        "gna application no",
        "gna application id",
        "gna/st ii application id",
        "gna st ii application id",
        "lta application id",
        "lta application no",
        "lta no",
        "application id under enhancement 5.2 or revision",
        "application id under enhancement 5.2",
        "enhancement 5.2 application id",
        "application_id",
        "application id",
    ]

    ids: list[str] = []
    for cand in candidates:
        key = key_map.get(cand)
        if key:
            ids.extend(_split_ids(record.get(key)))

    seen: set[str] = set()
    out: list[str] = []
    for item in ids:
        norm = _normalize_id(item)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(item)
    return out


def _row_id_match_count(row: dict, ids: list[str]) -> int:
    """Count how many IDs are found in any value of the JCC row."""
    if not row or not ids:
        return 0
    normalized_values = [_normalize_id(v) for v in row.values() if v is not None]
    count = 0
    for candidate in ids:
        cid = _normalize_id(candidate)
        if len(cid) < 3:
            continue
        if any(cid in value for value in normalized_values if value):
            count += 1
    return count


def find_best_jcc_match(
    substation: str,
    developer_name: str,
    jcc_rows: list[dict],
    id_values: list[str] | None = None,
    threshold: float = 0.45,
) -> Optional[dict]:
    """Find the JCC row that best matches given IDs or (substation, developer_name).

    Matching strategy (priority order):
      1) Application IDs (GNA/LTA/Enhancement 5.2) across any JCC row value
      2) Weighted fuzzy match:
           substation    ↔ pooling_station        (weight 0.5)
           developer     ↔ connectivity_applicant (weight 0.5)

    A substring containment match gets a bonus of +0.15 on that dimension.

    Returns the best-matching JCC row dict, or None if below threshold.
    """
    best_id_match: Optional[dict] = None
    best_id_hits: int = 0
    best_id_fuzzy: float = 0.0

    best_fuzzy_match: Optional[dict] = None
    best_fuzzy_score: float = 0.0

    ids = id_values or []

    for row in jcc_rows:
        pooling   = _safe_str(row.get("pooling_station"))
        applicant = _safe_str(row.get("connectivity_applicant"))

        # Fuzzy similarity
        sub_score = _fuzzy_score(substation, pooling)
        dev_score = _fuzzy_score(developer_name, applicant)

        # Substring bonus
        if _substring_match(substation, pooling):
            sub_score = min(1.0, sub_score + 0.15)
        if _substring_match(developer_name, applicant):
            dev_score = min(1.0, dev_score + 0.15)

        combined = 0.5 * sub_score + 0.5 * dev_score
        id_hits = _row_id_match_count(row, ids)
        if id_hits > 0:
            if id_hits > best_id_hits or (id_hits == best_id_hits and combined > best_id_fuzzy):
                best_id_hits = id_hits
                best_id_fuzzy = combined
                best_id_match = row

        if combined > best_fuzzy_score and combined >= threshold:
            best_fuzzy_score = combined
            best_fuzzy_match = row

    return best_id_match or best_fuzzy_match


# ─────────────────────────────────────────────────────────────────────────────
# STEP 4 — GNA / TGNA Computation
# ─────────────────────────────────────────────────────────────────────────────

def _is_effective(text: str) -> bool:
    """Return True if the connectivity status text indicates 'Effective'.

    Must contain the word 'effective' but NOT be negated
    (e.g. "not effective", "non-effective" are excluded).
    """
    if not text:
        return False
    norm = _normalize(text)
    # Must contain "effective"
    if "effective" not in norm:
        return False
    # Must NOT be preceded by "not" / "non"
    if re.search(r"\bnot\s+effective\b", norm):
        return False
    if re.search(r"\bnon[\s-]?effective\b", norm):
        return False
    return True


def _is_tgna_candidate(text: str) -> bool:
    """Return True if connectivity status is NOT 'Effective' but indicates
    a pending operationalisation (TGNA-eligible).

    Typical text: "Connectivity likely to be operationalized upon
    commissioning of required Transmission system."
    """
    if not text:
        return False
    if _is_effective(text):
        return False
    # Any non-empty, non-effective text is a TGNA candidate
    norm = _normalize(text)
    return len(norm) > 0


def _extract_all_mw(text: str) -> list[float]:
    """Extract every MW value from free-text.

    Matches patterns like:
        111.8 MW     |  200MW  |  1,200.5 MW
    """
    if not text:
        return []
    matches = re.findall(r"([\d,]+\.?\d*)\s*MW", text, re.IGNORECASE)
    values: list[float] = []
    for m in matches:
        try:
            values.append(float(m.replace(",", "")))
        except ValueError:
            continue
    return values


def _extract_commissioned_mw(text: str) -> list[float]:
    """Extract MW values that are explicitly tagged '(Commissioned)'.

    Matches patterns like:
        111.8 MW: 19.05.2025 (Commissioned)
        88.2 MW: 01.06.2025 (Commissioned)

    Also handles variations with/without the colon and date.
    """
    if not text:
        return []
    # Greedy: capture MW values whose surrounding context includes "(Commissioned)"
    # Pattern: <number> MW <anything up to 80 chars> (Commissioned)
    pattern = r"([\d,]+\.?\d*)\s*MW[^()]{0,80}?\(?\s*Commissioned\s*\)?"
    matches = re.findall(pattern, text, re.IGNORECASE)
    values: list[float] = []
    for m in matches:
        try:
            values.append(float(m.replace(",", "")))
        except ValueError:
            continue
    return values


def compute_gna_tgna(jcc_row: dict) -> tuple[Optional[float], Optional[float]]:
    """Compute GNA and TGNA from a single matched JCC row.

    Returns (gna_value, tgna_value).  Exactly one will be non-None
    (or both None if data is insufficient).

    Logic:
      • GNA  — status is "Effective"
               → sum ALL MW values in the schedule column (Generation MW / COD)
      • TGNA — status is NOT "Effective" (pending transmission)
               → sum only the MW values marked "(Commissioned)"
    """
    connectivity_status = _safe_str(jcc_row.get("connectivity_start_date_under_gna"))
    schedule_text       = _safe_str(jcc_row.get("schedule_as_per_current_jcc"))

    gna_value:  Optional[float] = None
    tgna_value: Optional[float] = None

    if _is_effective(connectivity_status):
        # ── GNA path ─────────────────────────────────────────────────────
        mw_values = _extract_all_mw(schedule_text)
        if mw_values:
            gna_value = round(sum(mw_values), 2)
            logger.debug(
                "[GNA]  status='Effective'  schedule MW=%s  → GNA=%.2f",
                mw_values, gna_value,
            )

    elif _is_tgna_candidate(connectivity_status):
        # ── TGNA path ────────────────────────────────────────────────────
        commissioned = _extract_commissioned_mw(schedule_text)
        if commissioned:
            tgna_value = round(sum(commissioned), 2)
            logger.debug(
                "[TGNA] status='%s'  commissioned MW=%s  → TGNA=%.2f",
                connectivity_status[:60], commissioned, tgna_value,
            )

    return gna_value, tgna_value


# ─────────────────────────────────────────────────────────────────────────────
# STEP 5 — Orchestrate: match + compute + write Excel
# ─────────────────────────────────────────────────────────────────────────────

_START_DIR = Path(__file__).resolve().parent.parent.parent

# Default paths
EFFECTIVENESS_EXCEL : Path = _START_DIR / "excels" / "effectiveness_combined.xlsx"
EFFECTIVENESS_DIR   : Path = _START_DIR / "output" / "effectiveness_cache"
JCC_OUTPUT_EXCEL    : Path = _START_DIR / "excels" / "jcc_output_layer.xlsx"


def run_jcc_output_layer(
    jcc_results: list[dict],
    *,
    effectiveness_excel_path: Path | str | None = None,
    effectiveness_df: pd.DataFrame | None = None,
    effectiveness_output_dir: Path | str | None = None,
    output_excel_path: Path | str | None = None,
    match_threshold: float = 0.45,
) -> pd.DataFrame:
    """Produce the 4-column JCC output sheet (Developer Name, Substation, TGNA, GNA).

    Parameters
    ----------
    jcc_results : list[dict]
        Raw per-PDF JCC extraction results (same shape as runner produces).
    effectiveness_excel_path : Path, optional
        Path to effectiveness_combined.xlsx.
    effectiveness_df : DataFrame, optional
        Pre-loaded effectiveness DataFrame (takes priority over Excel).
    effectiveness_output_dir : Path, optional
        Fallback: folder with effectiveness JSON caches.
    output_excel_path : Path, optional
        Where to write the JCC output Excel. Default: excels/jcc_output_layer.xlsx.
    match_threshold : float
        Minimum fuzzy-match score (0–1) to accept a match. Default 0.45.

    Returns
    -------
    pd.DataFrame with columns: Developer Name, Substation, TGNA, GNA
    """
    xlsx_out = Path(output_excel_path).resolve() if output_excel_path else JCC_OUTPUT_EXCEL
    xlsx_out.parent.mkdir(parents=True, exist_ok=True)

    eff_excel = Path(effectiveness_excel_path).resolve() if effectiveness_excel_path else EFFECTIVENESS_EXCEL
    eff_dir   = Path(effectiveness_output_dir).resolve()  if effectiveness_output_dir else EFFECTIVENESS_DIR

    print("\n" + "─" * 64)
    print("  JCC OUTPUT LAYER — GNA / TGNA Extraction")
    print("─" * 64)

    # ── Step 1: Load effectiveness data ───────────────────────────────────
    print("  Step 1 → Loading effectiveness data …")
    eff_records = load_effectiveness_data(
        effectiveness_excel_path = eff_excel,
        effectiveness_df         = effectiveness_df,
        effectiveness_output_dir = eff_dir,
    )
    print(f"           {len(eff_records)} effectiveness records with substation/developer/IDs")

    if not eff_records:
        print("  ⚠ No effectiveness records found — cannot produce JCC output.")
        return pd.DataFrame(columns=JCC_OUTPUT_COLUMNS)

    # ── Step 2: Flatten JCC data ──────────────────────────────────────────
    print("  Step 2 → Flattening JCC extracted data …")
    jcc_rows = flatten_jcc_data(jcc_results)
    print(f"           {len(jcc_rows)} total JCC rows available for matching")

    if not jcc_rows:
        print("  ⚠ No JCC rows available — cannot produce JCC output.")
        return pd.DataFrame(columns=JCC_OUTPUT_COLUMNS)

    # ── Step 3 + 4: Match & compute ───────────────────────────────────────
    print("  Step 3 → Matching effectiveness rows to JCC rows …")
    print("  Step 4 → Computing GNA / TGNA from matched rows …")

    output_rows: list[dict] = []
    matched_count = 0
    gna_count = 0
    tgna_count = 0

    for eff_rec in eff_records:
        substation = _safe_str(eff_rec.get("substation"))
        developer  = _safe_str(eff_rec.get("name_of_applicant"))
        id_values  = _collect_ids_from_record(eff_rec)

        if not substation and not developer and not id_values:
            continue

        # Fuzzy match
        jcc_match = find_best_jcc_match(
            substation     = substation,
            developer_name = developer,
            jcc_rows       = jcc_rows,
            id_values      = id_values,
            threshold      = match_threshold,
        )

        if jcc_match is None:
            # No match found — still include the row with empty GNA/TGNA
            output_rows.append({
                "Developer Name": developer,
                "Substation":     substation,
                "TGNA":           None,
                "GNA":            None,
            })
            continue

        matched_count += 1

        # Compute GNA / TGNA
        gna_val, tgna_val = compute_gna_tgna(jcc_match)

        if gna_val is not None:
            gna_count += 1
        if tgna_val is not None:
            tgna_count += 1

        output_rows.append({
            "Developer Name": developer or _safe_str(jcc_match.get("connectivity_applicant")),
            "Substation":     substation or _safe_str(jcc_match.get("pooling_station")),
            "TGNA":           tgna_val,
            "GNA":            gna_val,
        })

    # ── Step 5: Write Excel ───────────────────────────────────────────────
    print(f"\n  Results:")
    print(f"    Effectiveness rows processed : {len(eff_records)}")
    print(f"    Matched to JCC rows          : {matched_count}")
    print(f"    GNA values found             : {gna_count}")
    print(f"    TGNA values found            : {tgna_count}")
    print(f"    Output rows                  : {len(output_rows)}")

    df_out = pd.DataFrame(output_rows, columns=JCC_OUTPUT_COLUMNS)

    if output_rows:
        export_to_excel(
            rows         = output_rows,
            output_path  = xlsx_out,
            sheet_name   = "JCC Output",
            column_order = JCC_OUTPUT_COLUMNS,
            summary_rows = [
                ("Effectiveness rows processed", len(eff_records)),
                ("Matched to JCC",               matched_count),
                ("GNA values found",             gna_count),
                ("TGNA values found",            tgna_count),
            ],
        )
        print(f"\n  ✓ JCC Output Excel → {xlsx_out}")
    else:
        print("\n  ⚠ No output rows — Excel not written.")

    print("─" * 64)
    return df_out


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 4 — Full Mapped Data + GNA / TGNA
# ─────────────────────────────────────────────────────────────────────────────

LAYER4_EXCEL: Path = _START_DIR / "excels" / "layer_4.xlsx"


def _find_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """Find the first column name in *df* that matches any of *candidates* (case-insensitive)."""
    for c in candidates:
        for col in df.columns:
            if c.lower() == col.lower():
                return col
    return None


def find_strict_jcc_match(
    substation: str,
    developer_name: str,
    jcc_rows: list[dict],
    id_values: list[str] | None = None,
    sub_threshold: float = 0.40,
    dev_threshold: float = 0.40,
) -> Optional[dict]:
    """Find the best JCC row where IDs match, else BOTH substation AND developer match.

    Unlike find_best_jcc_match (which uses a combined score),
    this function requires BOTH dimensions to independently exceed
    their respective thresholds.

    Parameters
    ----------
    substation : str
        Substation value from mapped data.
    developer_name : str
        Developer name from mapped data.
    jcc_rows : list[dict]
        Flattened JCC rows.
    sub_threshold : float
        Minimum fuzzy score for substation match (default 0.40).
    dev_threshold : float
        Minimum fuzzy score for developer match (default 0.40).

    Returns
    -------
    Best matching JCC row dict, or None if no row passes both thresholds.
    """
    if not substation and not developer_name and not (id_values or []):
        return None

    best_id_match: Optional[dict] = None
    best_id_hits: int = 0
    best_id_fuzzy: float = 0.0

    best_match: Optional[dict] = None
    best_combined: float = 0.0

    ids = id_values or []

    for row in jcc_rows:
        pooling   = _safe_str(row.get("pooling_station"))
        applicant = _safe_str(row.get("connectivity_applicant"))

        # --- Substation score ---
        sub_score = _fuzzy_score(substation, pooling)
        if _substring_match(substation, pooling):
            sub_score = min(1.0, sub_score + 0.15)

        # --- Developer score ---
        dev_score = _fuzzy_score(developer_name, applicant)
        if _substring_match(developer_name, applicant):
            dev_score = min(1.0, dev_score + 0.15)

        combined = sub_score + dev_score

        # ID-based match (priority)
        id_hits = _row_id_match_count(row, ids)
        if id_hits > 0:
            if id_hits > best_id_hits or (id_hits == best_id_hits and combined > best_id_fuzzy):
                best_id_hits = id_hits
                best_id_fuzzy = combined
                best_id_match = row

        # BOTH must independently pass their thresholds
        if sub_score < sub_threshold or dev_score < dev_threshold:
            continue

        if combined > best_combined:
            best_combined = combined
            best_match = row

    return best_id_match or best_match


def run_layer4_excel(
    jcc_results: list[dict],
    *,
    mapped_excel_path: Path | str | None = None,
    mapped_df: pd.DataFrame | None = None,
    output_excel_path: Path | str | None = None,
    sub_threshold: float = 0.40,
    dev_threshold: float = 0.40,
) -> pd.DataFrame:
    """Produce the Layer 4 Excel: all mapped data + GNA / TGNA columns.

    Takes the FULL Module 3 mapped output (CMETS × Effectiveness) and
    enriches it with GNA and TGNA values from JCC data.

    Matching prioritizes GNA/LTA/Enhancement 5.2 IDs when available,
    else requires BOTH developer name AND substation to match.

    Parameters
    ----------
    jcc_results : list[dict]
        Raw per-PDF JCC extraction results.
    mapped_excel_path : Path, optional
        Path to effectiveness_mapped.xlsx (Module 3 output).
    mapped_df : DataFrame, optional
        Pre-loaded mapped DataFrame (takes priority over Excel).
    output_excel_path : Path, optional
        Output path. Default: excels/layer_4.xlsx.
    sub_threshold : float
        Min fuzzy score for substation (default 0.40).
    dev_threshold : float
        Min fuzzy score for developer name (default 0.40).

    Returns
    -------
    pd.DataFrame — the full mapped data with GNA and TGNA columns appended.
    """
    xlsx_out = Path(output_excel_path).resolve() if output_excel_path else LAYER4_EXCEL
    xlsx_out.parent.mkdir(parents=True, exist_ok=True)

    mapped_xlsx = Path(mapped_excel_path).resolve() if mapped_excel_path else (
        _START_DIR / "excels" / "effectiveness_mapped.xlsx"
    )

    print("\n" + "=" * 64)
    print("  LAYER 4 — FULL MAPPED DATA + GNA / TGNA")
    print("=" * 64)

    # ── Load mapped data ──────────────────────────────────────────────────
    if mapped_df is not None and not mapped_df.empty:
        df = mapped_df.copy()
        print(f"  Mapped data  : in-memory DataFrame ({len(df)} rows)")
    elif mapped_xlsx.exists():
        df = pd.read_excel(mapped_xlsx, sheet_name=0)
        print(f"  Mapped Excel : {mapped_xlsx}")
        print(f"  Rows loaded  : {len(df)}")
    else:
        print(f"  ⚠ Mapped Excel not found: {mapped_xlsx}")
        print("    Run Module 3 (mapping) first.")
        print("=" * 64)
        return pd.DataFrame()

    # ── Flatten JCC data ──────────────────────────────────────────────────
    jcc_rows = flatten_jcc_data(jcc_results)
    print(f"  JCC rows     : {len(jcc_rows)}")

    if not jcc_rows:
        print("  ⚠ No JCC rows — GNA/TGNA columns will be empty.")
        df["TGNA"] = None
        df["GNA"]  = None
        df.to_excel(str(xlsx_out), index=False, sheet_name="Layer 4 Data")
        print(f"  Excel → {xlsx_out}")
        print("=" * 64)
        return df

    # ── Identify developer name + substation + ID columns ────────────────
    col_dev  = _find_col(df, "Name of the developers", "Name of developers",
                         "Developer Name", "name_of_applicant")
    col_sub  = _find_col(df, "substaion", "Substation", "substation")
    col_gna  = _find_col(df, "GNA/ST II Application ID", "GNA ST II Application ID",
                         "GNA Application ID", "GNA Application No")
    col_lta  = _find_col(df, "LTA Application ID", "LTA Application No", "LTA No")
    col_52   = _find_col(df, "Application ID under Enhancement 5.2 or revision",
                         "Application ID under Enhancement 5.2",
                         "Enhancement 5.2 Application ID")

    if not col_dev:
        print("  ⚠ Cannot find developer name column in mapped data.")
    if not col_sub:
        print("  ⚠ Cannot find substation column in mapped data.")

    print(f"  Developer col: {col_dev}")
    print(f"  Substation col: {col_sub}")
    print(f"  GNA ID col: {col_gna}")
    print(f"  LTA ID col: {col_lta}")
    print(f"  5.2 ID col: {col_52}")
    print("-" * 64)

    # ── Match each row and compute GNA / TGNA ─────────────────────────────
    gna_values:  list[Optional[float]] = []
    tgna_values: list[Optional[float]] = []

    matched_count = 0
    gna_count  = 0
    tgna_count = 0

    for idx, row in df.iterrows():
        developer  = _safe_str(row.get(col_dev)) if col_dev else ""
        substation = _safe_str(row.get(col_sub)) if col_sub else ""
        id_values: list[str] = []
        if col_gna:
            id_values.extend(_split_ids(row.get(col_gna)))
        if col_lta:
            id_values.extend(_split_ids(row.get(col_lta)))
        if col_52:
            id_values.extend(_split_ids(row.get(col_52)))

        if not developer and not substation and not id_values:
            gna_values.append(None)
            tgna_values.append(None)
            continue

        # Strict match: BOTH developer AND substation must pass
        jcc_match = find_strict_jcc_match(
            substation     = substation,
            developer_name = developer,
            jcc_rows       = jcc_rows,
            id_values      = id_values,
            sub_threshold  = sub_threshold,
            dev_threshold  = dev_threshold,
        )

        if jcc_match is None:
            gna_values.append(None)
            tgna_values.append(None)
            continue

        matched_count += 1
        gna_val, tgna_val = compute_gna_tgna(jcc_match)

        if gna_val is not None:
            gna_count += 1
        if tgna_val is not None:
            tgna_count += 1

        gna_values.append(gna_val)
        tgna_values.append(tgna_val)

    # ── Append columns ────────────────────────────────────────────────────
    df["TGNA"] = tgna_values
    df["GNA"]  = gna_values

    # ── Write Excel ───────────────────────────────────────────────────────
    print(f"\n  Layer 4 Results:")
    print(f"    Total rows               : {len(df)}")
    print(f"    Matched to JCC (both)    : {matched_count}")
    print(f"    GNA values populated     : {gna_count}")
    print(f"    TGNA values populated    : {tgna_count}")
    print(f"    Unmatched                : {len(df) - matched_count}")

    flat_records = df.to_dict(orient="records")
    col_order = list(df.columns)

    export_to_excel(
        rows         = flat_records,
        output_path  = xlsx_out,
        sheet_name   = "Layer 4 Data",
        column_order = col_order,
        summary_rows = [
            ("Total rows",          len(df)),
            ("Matched to JCC",      matched_count),
            ("GNA values found",    gna_count),
            ("TGNA values found",   tgna_count),
            ("Unmatched rows",      len(df) - matched_count),
        ],
    )
    print(f"\n  ✓ Layer 4 Excel → {xlsx_out}")
    print("=" * 64)
    return df
