"""
cmets_handler/battery_extractor.py — BESS extraction logic
==========================================================
Separate file to handle Battery (BESS) related extraction operations.
Uses an LLM to accurately extract MWh, Injection, and Drawl from context text
and the 'type' column (e.g., "BESS (19)").
"""

import json
import re
from typing import Optional

from config import MODEL, load_runtime_config
from llm_client import call_llm, extract_text_from_response
from pipeline.shared_utils import parse_json

# ── Prompt Templates ─────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an expert data extractor. Extract the Battery Energy Storage System (BESS) parameters from the provided text.
Look for:
1. Battery MWh capacity. Also check if the Type string specifies BESS (X) where X is the MWh.
2. Battery Injection (MW).
3. Battery Drawl (MW).

If a value is not found, return null. Ensure your output is ONLY a valid JSON object with the keys "mwh", "inj", and "drw". DO NOT include markdown formatting or explanations.
"""

USER_TEMPLATE = """\
Extract the BESS values from the following text snippets:
Type String: {type_col}
Additional Context: {context}

Return exactly this JSON format:
{{
    "mwh": <float or null>,
    "inj": <float or null>,
    "drw": <float or null>
}}
"""

# ── Helpers ───────────────────────────────────────────────────────────────────


_RUNTIME_CONFIG = None

# ── Main Extraction Logic ─────────────────────────────────────────────────────

def extract_battery_values(
    raw_mwh: Optional[str],
    raw_inj: Optional[str],
    raw_drw: Optional[str],
    raw_mode: Optional[str],
    type_col: Optional[str]
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Use an LLM to extract Battery MWh, Injection, and Drawl.
    Fast-fails using a keyword check to avoid calling the LLM unnecessarily.
    """
    global _RUNTIME_CONFIG

    # Fast-fail: only run LLM if BESS is actually mentioned
    text_to_search = " ".join(str(v or "") for v in [raw_mwh, raw_inj, raw_drw, raw_mode, type_col])
    if not re.search(r"\b(bess|battery\s*energy|battery)\b", text_to_search, re.IGNORECASE):
        return raw_mwh, raw_inj, raw_drw

    if _RUNTIME_CONFIG is None:
        _RUNTIME_CONFIG = load_runtime_config()

    prompt_payload = {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": USER_TEMPLATE.format(
                type_col=type_col or "None",
                context=text_to_search
            )}
        ],
        "temperature": 0,
        "max_tokens": 200,
    }

    mwh = raw_mwh
    inj = raw_inj
    drw = raw_drw

    try:
        resp = call_llm(
            prompt_payload,
            vm=_RUNTIME_CONFIG.vm_mode,
            api_key=_RUNTIME_CONFIG.api_key,
            model=MODEL,
            script_path=_RUNTIME_CONFIG.llm_script_path
        )
        content = extract_text_from_response(resp)
        result = parse_json(content)

        if "mwh" in result and result["mwh"] is not None:
            mwh = str(result["mwh"])
        if "inj" in result and result["inj"] is not None:
            inj = str(result["inj"])
        if "drw" in result and result["drw"] is not None:
            drw = str(result["drw"])

    except Exception as exc:
        print(f"      [Battery Extractor LLM Error] {exc}")

    return mwh, inj, drw
