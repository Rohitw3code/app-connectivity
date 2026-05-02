from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


# Pipeline defaults
# MAX_PAGES:
#   > 0  -> process first N pages per PDF
#   = -1 -> process all pages per PDF
MAX_PAGES = 10
MODEL = "gpt-4o-mini"

# Single switch for runtime mode:
#   "laptop" -> direct OpenAI API key mode
#   "vm"     -> script-based VM mode
EXECUTION_TARGET = "vm"   # default: use VM script mode (change to "laptop" for direct API)

# DOWNLOAD_LIMIT:
#   > 0  -> download up to N PDFs per handler
#   = -1 -> download all available PDFs
DOWNLOAD_LIMIT = 5  # default: 5 PDFs per handler


@dataclass(frozen=True)
class RuntimeConfig:
    """Runtime settings for LLM access and execution mode."""

    execution_target: str  # "vm" or "laptop"
    vm_mode: bool
    api_key: str
    llm_script_path: Optional[str]
    download_limit: int  # -1 = all, N = first N PDFs per handler


def load_runtime_config(
    *,
    mode_override: Optional[str] = None,
    api_key_override: Optional[str] = None,
    llm_script_override: Optional[str] = None,
    download_limit_override: Optional[int] = None,
) -> RuntimeConfig:
    """
    Resolve runtime config from .env + CLI overrides.

    Priority:
    1) explicit CLI overrides
    2) environment variables
    3) defaults

    Supported env variables:
    - OPENAI_API_KEY
    - LLM_SCRIPT_PATH
    - DOWNLOAD_LIMIT
    """
    load_dotenv(dotenv_path=Path(__file__).with_name(".env"), override=False)

    requested_target = (mode_override or EXECUTION_TARGET).strip().lower()
    if requested_target not in {"vm", "laptop"}:
        requested_target = "laptop"

    vm_mode = requested_target == "vm"

    api_key = (api_key_override or os.getenv("OPENAI_API_KEY") or "").strip()
    llm_script_path = (llm_script_override or os.getenv("LLM_SCRIPT_PATH") or "").strip() or None

    # Download limit
    if download_limit_override is not None:
        dl_limit = download_limit_override
    else:
        env_limit = os.getenv("DOWNLOAD_LIMIT", "").strip()
        dl_limit = int(env_limit) if env_limit else DOWNLOAD_LIMIT

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
        download_limit=dl_limit,
    )
