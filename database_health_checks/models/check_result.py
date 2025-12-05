"""Check result models."""

from typing import Any, Optional

from pydantic import BaseModel, Field

from database_health_checks.models.check_catagory import CheckCategory


class CheckResult(BaseModel):
    """Result of a single health check."""

    check_name: str = Field(..., description="Name of the health check")
    database: str = Field(..., description="Database identifier")
    passed: bool = Field(..., description="Whether the check passed")
    actual_value: Any = Field(..., description="Actual value from the check")
    expected_value: Optional[Any] = Field(None, description="Expected value")
    message: str = Field("", description="Additional message")
    category: CheckCategory = Field(..., description="Category of the health check")
    is_override: bool = Field(
        False,
        description="Whether the expected value came from a database-specific override",
    )

    class Config:
        """Pydantic config."""

        extra = "ignore"
