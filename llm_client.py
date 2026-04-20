from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Optional

from openai import OpenAI


DEFAULT_MODEL = "gpt-4o-mini"


def parse_bool(value: Optional[str], default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _resolve_script_path(script_path: Optional[str]) -> Path:
    if script_path:
        return Path(script_path).expanduser().resolve()
    return (Path(__file__).with_name("llm_client.bat")).resolve()


def call_vm_batch(prompt_payload: dict[str, Any], script_path: Optional[str] = None) -> dict[str, Any]:
    """Call local batch script that performs the LLM request and returns JSON."""
    resolved_script = _resolve_script_path(script_path)
    if not resolved_script.exists():
        raise FileNotFoundError(
            f"VM mode is enabled but script not found: {resolved_script}"
        )

    if resolved_script.suffix.lower() == ".bat" and os.name != "nt":
        raise RuntimeError(
            "VM mode points to a .bat script, which requires Windows. "
            "Set VM=false to use OPENAI_API_KEY directly, or provide a non-.bat script."
        )

    temp_file_path = ""
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as tmp:
            json.dump(prompt_payload, tmp, ensure_ascii=False)
            temp_file_path = tmp.name

        command = [str(resolved_script), temp_file_path]
        if resolved_script.suffix.lower() == ".bat":
            command = ["cmd", "/c", str(resolved_script), temp_file_path]

        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
            check=False,
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"LLM VM script failed (exit {result.returncode}): {result.stderr.strip()}"
            )

        raw = result.stdout.strip()
        if not raw:
            raise RuntimeError("LLM VM script returned empty output.")

        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"LLM VM script did not return valid JSON: {exc}") from exc
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)


def call_openai_direct(
    prompt_payload: dict[str, Any],
    api_key: str,
    model: str = DEFAULT_MODEL,
) -> dict[str, Any]:
    """Call OpenAI Chat Completions directly using API key."""
    if not api_key:
        raise ValueError("OpenAI API key is required when VM mode is disabled.")

    client = OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        messages=prompt_payload.get("messages", []),
        temperature=prompt_payload.get("temperature", 0),
        max_tokens=prompt_payload.get("max_tokens", 2000),
    )
    return response.model_dump()


def call_llm(
    prompt_payload: dict[str, Any],
    vm: bool,
    api_key: Optional[str] = None,
    model: str = DEFAULT_MODEL,
    script_path: Optional[str] = None,
) -> dict[str, Any]:
    """
    Unified LLM caller.
    - vm=True  -> use batch script flow (`llm_client.bat`).
    - vm=False -> use direct OpenAI API key flow.
    """
    if vm:
        return call_vm_batch(prompt_payload, script_path=script_path)
    return call_openai_direct(prompt_payload, api_key=api_key or "", model=model)


def extract_text_from_response(response_json: dict[str, Any]) -> str:
    """Extract first assistant message content from chat-completions style response."""
    try:
        return response_json["choices"][0]["message"]["content"] or ""
    except (KeyError, IndexError, TypeError):
        return ""
