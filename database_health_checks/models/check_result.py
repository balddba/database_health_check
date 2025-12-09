"""Check result models."""

from typing import Optional, Union

from pydantic import BaseModel, Field

from database_health_checks.models.check_catagory import CheckCategory


class CheckResult(BaseModel):
    """Result of a single health check."""

    check_name: str = Field(..., description="The name of the health check")
    database: str = Field(..., description="The database identifier")
    passed: bool = Field(..., description="Whether the check passed")
    actual_value: Union[str, int, float, bool] = Field(..., description="The actual value from the check")
    expected_value: Optional[Union[str, int, float, bool]] = Field(None, description="The expected value")
    message: str = Field("", description="An additional message")
    category: CheckCategory = Field(..., description="The category of the health check")
    is_override: bool = Field(
        False,
        description="Whether the expected value came from a database-specific override",
    )

    class Config:
        """Pydantic config."""

        extra = "ignore"
