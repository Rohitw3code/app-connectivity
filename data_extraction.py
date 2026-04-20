from __future__ import annotations

import json
import re
from typing import Optional

from langchain_text_splitters import RecursiveCharacterTextSplitter

from config import MODEL
from llm_client import call_llm, extract_text_from_response
from prompts import build_prompt_payload


def extract_json_payload(text: str) -> dict:
    """Parse JSON from model text, tolerating code fences."""
    raw = (text or "").strip()
    if not raw:
        return {}

    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if not match:
            return {}
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return {}


def extract_rows_from_chunk(
    chunk: str,
    active_fields: list[str],
    vm_mode: bool,
    api_key: Optional[str],
    llm_script_path: Optional[str],
) -> list[dict]:
    try:
        prompt_payload = build_prompt_payload(chunk, active_fields)
        response_json = call_llm(
            prompt_payload=prompt_payload,
            vm=vm_mode,
            api_key=api_key,
            model=MODEL,
            script_path=llm_script_path,
        )
        content = extract_text_from_response(response_json)
        result = extract_json_payload(content)
        rows = result.get("rows", []) if isinstance(result, dict) else []
        return rows if isinstance(rows, list) else []
    except Exception as e:
        print(f"      [Chain error] {e}")
        return []


def extract_rows_with_fallback(
    chunk: str,
    active_fields: list[str],
    fallback_splitter: RecursiveCharacterTextSplitter,
    vm_mode: bool,
    api_key: Optional[str],
    llm_script_path: Optional[str],
) -> list[dict]:
    """Try extraction on full chunk first, then retry with smaller sub-chunks if needed."""
    primary_rows = extract_rows_from_chunk(
        chunk,
        active_fields,
        vm_mode=vm_mode,
        api_key=api_key,
        llm_script_path=llm_script_path,
    )
    if primary_rows or len(chunk) < 700:
        return primary_rows

    fallback_rows: list[dict] = []
    for sub_chunk in fallback_splitter.split_text(chunk):
        fallback_rows.extend(
            extract_rows_from_chunk(
                sub_chunk,
                active_fields,
                vm_mode=vm_mode,
                api_key=api_key,
                llm_script_path=llm_script_path,
            )
        )

    return fallback_rows
