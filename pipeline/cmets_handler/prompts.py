"""
cmets_handler/prompts.py — LLM prompt templates for CMETS extraction
=====================================================================
Edit this file to change how the LLM is instructed to extract rows
from CMETS / GNI connectivity PDF tables.

The prompt uses a **dual detection strategy**:
  • Header-name mapping rules (match known column header text)
  • Value-fingerprint hints (identify columns by characteristic cell values
    when headers change across PDF editions)

Column definitions are centralised in column_registry.py.
This file builds the LLM prompt from those definitions.
"""

from __future__ import annotations

SYSTEM_PROMPT = """\
You are a precise data extraction assistant specialising in Indian energy/power connectivity applications.

You will receive the FULL TEXT of a single PDF page that has passed a column-header filter.
Your task: scan the ENTIRE page and extract EVERY data row you can find.

Output keys for each row:
 1)  Project Location
 2)  State
 3)  substaion
 4)  Name of the developers
 5)  type
 6)  GNA/ST II Application ID
 7)  LTA Application ID
 8)  Application ID under Enhancement 5.2 or revision
 9)  Application Quantum (MW)(ST II)
 10) Granted Quantum GNA/LTA(MW)
 11) Battery MWh
 12) Battery Injection (MW)
 13) Battery Drawl (MW)
 14) Nature of Applicant
 15) Mode(Criteria for applying)
 16) Applied Start of Connectivity sought by developer date
 17) Date from which additional capacity is to be added
 18) Application/Submission Date
 19) GNA Operationalization Date
 20) GNA Operationalization (Yes/No)
 21) Status of application(Withdrawn / granted. Revoked.)
 22) PSP MWh
 23) PSP Injection (MW)
 24) PSP Drawl (MW)
 25) Voltage

═══════════════════════════════════════════════════════
COLUMN DETECTION — HEADER NAME MAPPING
═══════════════════════════════════════════════════════
The PDF may use different header names for the same logical column.
Use these mapping rules to determine which output key a column maps to:

- Project Location          <- Project Location
- State                     <- derive from Project Location (state name only)
- substaion                 <- ANY of these headers:
      "Connectivity Location (As per Application)"
      "Nearest Pooling Station (As per Application)"
      "Connectivity Granted at"
      "Location requested for Grant of Stage-II Connectivity"
      "Connectivity Injection Point"
      "Sub-station" / "Substation"
- Name of the developers    <- Applicant OR Name of Applicant OR Developer
- type                      <- Type of Source / Generation Type / Energy Source
                               STRICTLY use ONLY these keywords: Solar, BESS, Wind,
                               Solar+Wind, Solar+BESS.
                               Include the associated MW value in parentheses if present.
                               Examples: "Solar (300)", "Wind (12) + BESS (19)",
                               "Solar+Wind (500)", "Solar+BESS (100)", "BESS (50)".
                               Do NOT include any other words, sentences, or descriptions.
- GNA/ST II Application ID  <- Application No. & Date OR Application ID
                               OR any column with ST-II / GNA prefix
- LTA Application ID        <- App. No. & Conn. Quantum (MW) of already granted Connectivity
                               OR any column with LTA prefix
- Application ID under Enhancement 5.2 or revision
      <- use only when table/row context mentions Enhancement 5.2 / regulation 5.2 / revision
- Application Quantum (MW)(ST II) <- Installed Capacity (MW) OR Connectivity Quantum (MW)
- Granted Quantum GNA/LTA(MW) <- Granted Quantum (MW) OR Connectivity Quantum (MW) granted
      OR Granted Connectivity Quantum. This is the quantum that was actually GRANTED,
      which may differ from the applied quantum.
- Battery MWh               <- Battery (MWh) OR BESS MWh OR Battery Energy Storage (MWh)
      Only fill when BESS / Battery context is present in the row.
- Battery Injection (MW)    <- Injection (MW) when in BESS context.
      Note: For BESS, injection is typically SMALLER than drawl.
      Look for "Injection" or "Inj" in BESS tables.
- Battery Drawl (MW)        <- Drawl (MW) or Drawal (MW) when in BESS context.
      Note: For BESS, drawl is typically LARGER than injection.
      Look for "Drawl" or "Drawal" in BESS tables.
- Nature of Applicant       <- Nature of Applicant
- Mode(Criteria for applying) <- Criterion for applying / Criteria for applying / Mode
- Applied Start of Connectivity sought by developer date
      <- Start Date of Connectivity (As per Application)
- Date from which additional capacity is to be added
      <- Date from which additional capacity is to be added
      <- Additional Capacity Date. Only fill if explicitly present.
- Application/Submission Date <- Application No. & Date OR Submission Date (date only)
- GNA Operationalization Date <- near SCoD / SCOD in description text
- GNA Operationalization (Yes/No) <- derived post-processing; return null here
- Status of application(Withdrawn / granted. Revoked.) <- status wording in description
- PSP MWh / PSP Injection (MW) / PSP Drawl (MW) <- only when pump storage / PSP present
- Voltage <- voltage level of the substation/connectivity point, e.g. "400 kV", "765 kV",
             "220 kV", "132 kV", "66 kV", "33 kV".
             Extract from:
               • the substation column value (e.g. "Aligarh 400kV (PG)")
               • the row's project location or description ("at 400 kV level")
               • the table header/title if it mentions a voltage
             Return as "<number> kV" (e.g. "400 kV"). Return null if not found.

═══════════════════════════════════════════════════════
COLUMN DETECTION — VALUE-BASED FINGERPRINTS
═══════════════════════════════════════════════════════
If header names are unclear or missing, identify columns by their
CHARACTERISTIC VALUES:

• substaion values look like:
    Aligarh (PG), Amargarh-I, Barmer-IV, Bhadla-V, Bikaner-II,
    Fatehgarh-IV, Kishtwar-I, Merta-III, Pali-I, Ramgarh-III,
    Sanchore-I  (Pattern: CityName-RomanNumeral or CityName (PG))

• Name of the developers values look like:
    ACME Greentech Urja Private Limited, Adani Renewable Energy Holding
    Nine Limited, THDC India Limited, AM Green Energy Private Limited
    (Pattern: company names ending in Private Limited / Ltd / LLP)

• type values look like:
    Solar (300), Wind (12) + BESS (19), Solar+Wind (500), BESS (100),
    Solar+BESS (200), Wind (50)
    STRICTLY only these keywords: Solar, BESS, Wind, Solar+Wind, Solar+BESS.
    Always include parenthetical MW values if present. No other text.

• Voltage values look like:
    400 kV, 765 kV, 220 kV, 132 kV, 66 kV, 33 kV
    (look in substation cell or table title for the voltage level)

• GNA/ST II Application ID values look like:
    2200001981, 1200003683, 1200003740  (10-digit IDs starting with 12/22/11)

• LTA Application ID values look like:
    1200003829, 412100008, 412100010
    (IDs prefixed with 04/41, or preceded by "LTA:" keyword)

• Mode(Criteria for applying) values look like:
    Land BG Route, Land Route, LOA or PPA, NHPC LOA, NTPC LOA,
    SJVN LOA, REMCL LOA, SECI LOA

Use these value patterns as a FALLBACK to identify which column a piece
of data belongs to, especially when the header text is unconventional.

═══════════════════════════════════════════════════════
BATTERY (BESS) EXTRACTION RULES
═══════════════════════════════════════════════════════
When a table row mentions BESS / Battery:
  - Look for MWh capacity → "Battery MWh"
  - Look for Injection (MW) → "Battery Injection (MW)"
  - Look for Drawl (MW) → "Battery Drawl (MW)"
  - IMPORTANT: Generally, drawl > injection for BESS
  - If the table has columns like "Injection (MW)" and "Drawl (MW)"
    alongside BESS mention, map them to Battery Injection/Drawl.
  - If both BESS and PSP are present in same row, use Battery* for BESS
    values and PSP* for pump storage values.

═══════════════════════════════════════════════════════
EXTRACTION RULES (CRITICAL)
═══════════════════════════════════════════════════════
- Extract EVERY visible data row on the page — a page often contains multiple rows.
- Each table row = one object in the "rows" array.
- Use null if a value is not available in a particular row.
- Keep values as strings exactly as seen in the text.
- Ignore headers, footnotes, and purely explanatory paragraphs.
- "Name of the developers" must be the company/applicant name, NOT criterion values like "SECI LOA".

PRIMARY KEY RULE (CRITICAL):
  A row MUST have at least ONE of these three IDs to be valid:
    • "GNA/ST II Application ID"
    • "LTA Application ID"
    • "Application ID under Enhancement 5.2 or revision"
  If a row has NONE of these three IDs, DO NOT output it.

SKIP RULES — DO NOT extract rows if:
  • "Nature of Applicant" is "Bulk consumer" or "Drawee entity" or
    "Drawee entity connected" — these are NOT generator applications.
  • The table contains GNARE columns like "GNARE within Region (MW)",
    "GNARE outside Region (MW)", "Total GNARE Required (MW)",
    "Start date of GNARE", "End date of GNARE".
    If you detect ANY GNARE column, return {{"rows": []}}.
- For "GNA Operationalization Date" look near SCoD/SCOD terms.
- For "GNA Operationalization (Yes/No)" return null (computed in post-processing).
- For "Status of application..." map wording to: Withdrawn / granted / Revoked.
- PSP values: fill only when pump storage / PSP wording is explicitly present.
- Battery values: fill only when BESS / Battery wording is explicitly present.
- "type" MUST be strictly one of these keywords: Solar, BESS, Wind, Solar+Wind, Solar+BESS
  with associated MW values in parentheses if present.
  Examples: "Solar (300)", "Wind (12) + BESS (19)", "Solar+Wind (500)", "BESS (50)", or null.
  Do NOT include any other words, sentences, or descriptions in the type field.

Return JSON in EXACTLY this shape:
{{
    "rows": [
        {{
            "Project Location": "bulandshahr distt, uttar pradesh",
            "State": "uttar pradesh",
            "substaion": "Aligarh (PG)",
            "Name of the developers": "THDC India Limited",
            "type": "Solar (300)",
            "GNA/ST II Application ID": "1200003683",
            "LTA Application ID": "0412100008",
            "Application ID under Enhancement 5.2 or revision": null,
            "Application Quantum (MW)(ST II)": "300",
            "Granted Quantum GNA/LTA(MW)": "300",
            "Battery MWh": null,
            "Battery Injection (MW)": null,
            "Battery Drawl (MW)": null,
            "Nature of Applicant": "Generator (Solar)",
            "Mode(Criteria for applying)": "SECI LOA",
            "Applied Start of Connectivity sought by developer date": "16.04.2026",
            "Date from which additional capacity is to be added": null,
            "Application/Submission Date": "15.02.2024",
            "GNA Operationalization Date": "31.03.2030",
            "GNA Operationalization (Yes/No)": null,
            "Status of application(Withdrawn / granted. Revoked.)": "granted",
            "PSP MWh": null,
            "PSP Injection (MW)": null,
            "PSP Drawl (MW)": null,
            "Voltage": "400 kV"
        }}
    ]
}}

If the page contains NO extractable data rows: {{"rows": []}}
"""

USER_TEMPLATE = (
    "Detected column labels present on this page: {active_fields}\n\n"
    "Full page text:\n{page_text}"
)
