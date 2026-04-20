from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from llm_client import parse_bool


# Pipeline defaults
MAX_PAGES = 10
CHUNK_SIZE = 1800
CHUNK_OVERLAP = 300
MODEL = "gpt-4o-mini"
DEFAULT_CHUNKS_PER_PAGE = 4
DEFAULT_PAGE_CHUNK_OVERLAP = 180


@dataclass(frozen=True)
class RuntimeConfig:
    """Runtime settings for LLM access and execution mode."""

    execution_target: str  # "vm" or "laptop"
    vm_mode: bool
    api_key: str
    llm_script_path: Optional[str]


def load_runtime_config(
    *,
    mode_override: Optional[str] = None,
    vm_override: Optional[str] = None,
    api_key_override: Optional[str] = None,
    llm_script_override: Optional[str] = None,
) -> RuntimeConfig:
    """
    Resolve runtime config from .env + CLI overrides.

    Priority:
    1) explicit CLI overrides
    2) environment variables
    3) defaults

    Supported env variables:
    - EXECUTION_TARGET: "vm" or "laptop" (default: "laptop")
    - VM: true/false (optional legacy override)
    - OPENAI_API_KEY
    - LLM_SCRIPT_PATH
    """
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)

    env_target = (os.getenv("EXECUTION_TARGET") or "laptop").strip().lower()
    if env_target not in {"vm", "laptop"}:
        env_target = "laptop"

    requested_target = (mode_override or env_target).strip().lower()
    if requested_target not in {"vm", "laptop"}:
        requested_target = "laptop"

    default_vm_from_target = requested_target == "vm"
    vm_mode = parse_bool(
        vm_override,
        default=parse_bool(os.getenv("VM"), default=default_vm_from_target),
    )

    api_key = (api_key_override or os.getenv("OPENAI_API_KEY") or "").strip()
    llm_script_path = (llm_script_override or os.getenv("LLM_SCRIPT_PATH") or "").strip() or None

    if not vm_mode and not api_key:
        raise SystemExit(
            "ERROR: OPENAI_API_KEY is required in laptop mode. "
            "Set EXECUTION_TARGET=vm (or VM=true) to use VM script mode."
        )

    return RuntimeConfig(
        execution_target="vm" if vm_mode else "laptop",
        vm_mode=vm_mode,
        api_key=api_key,
        llm_script_path=llm_script_path,
    )
