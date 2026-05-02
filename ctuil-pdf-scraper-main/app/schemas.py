"""
Unified response schema for all API endpoints.

Every endpoint returns an APIResponse envelope with:
  • status   - boolean success flag
  • message  - human-readable summary
  • data     - payload on success (generic)
  • error    - structured error info on failure
  • timestamp - ISO-8601 UTC timestamp
"""

from datetime import datetime, timezone
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")

class APIError(BaseModel):
    """Structured error detail attached to failed responses."""

    code: str = Field(..., description="Machine-readable error code")
    detail: str = Field(..., description="Human-readable error description")


class APIResponse(BaseModel, Generic[T]):
    """Standard envelope returned by every endpoint."""

    status: bool = Field(..., description="True when the operation succeeds")
    message: str = Field(..., description="Short summary of the outcome")
    data: Optional[T] = Field(default=None, description="Payload (present on success)")
    error: Optional[APIError] = Field(default=None, description="Error info (present on failure)")
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO-8601 UTC timestamp of the response",
    )

    @classmethod
    def success(cls, *, message: str, data: Any = None) -> "APIResponse":
        """Build a successful response."""
        return cls(status=True, message=message, data=data)

    @classmethod
    def failure(cls, *, message: str, error_code: str, detail: str) -> "APIResponse":
        """Build an error response."""
        return cls(
            status=False,
            message=message,
            error=APIError(code=error_code, detail=detail),
        )