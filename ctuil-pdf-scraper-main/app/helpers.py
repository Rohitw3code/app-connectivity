"""
Helpers - shared utilities for API route handlers.
"""

import asyncio
import logging
import time
import traceback
from typing import Any

from fastapi import status
from fastapi.responses import JSONResponse

from app.schemas import APIResponse

logger = logging.getLogger(__name__)


# ==== Scraper execution helper ====
def execute_scraper(script_module: Any, label: str, output_dir: str) -> dict:
    """
    Run a scraper module's ``main()`` (sync or async) and return
    execution metadata.  Used by every service method.
    """
    is_async = asyncio.iscoroutinefunction(script_module.main)
    script_name = script_module.__name__.rsplit(".", 1)[-1]

    logger.info("[START] %s  (module=%s, async=%s)", label, script_name, is_async)
    start = time.time()

    if is_async:
        asyncio.run(script_module.main())
    else:
        script_module.main()

    elapsed = round(time.time() - start, 2)
    logger.info("[DONE]  %s  completed in %ss", label, elapsed)

    return {
        "script": script_name,
        "execution_time_seconds": elapsed,
        "output_dir": output_dir,
    }

# Standard error response schema for OpenAPI docs
ERROR_RESPONSES = {
    status.HTTP_500_INTERNAL_SERVER_ERROR: {
        "description": "Scraper execution failed",
        "model": APIResponse,
    },
}


def handle_scraper(
    service_fn,
    success_message: str,
    error_message: str,
    error_code: str,
):
    """
    Execute a service function and return a standardised response.

    • On success → 200 with APIResponse(status=True)
    • On failure → 500 with APIResponse(status=False) + full error detail
    """
    try:
        result = service_fn()
        return APIResponse.success(message=success_message, data=result)

    except Exception:
        logger.exception("%s  [%s]", error_message, error_code)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content=APIResponse.failure(
                message=error_message,
                error_code=error_code,
                detail=traceback.format_exc(),
            ).model_dump(),
        )
