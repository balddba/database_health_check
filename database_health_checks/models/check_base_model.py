"""Base class for all database checks."""

from abc import ABC, abstractmethod
from typing import Any

from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class CheckBaseModel(ABC):
    """Base class for all database checks."""

    def __init__(
        self, name: str, check_name: str, category: CheckCategory, description: str
    ):
        """Initialize a check.

        Args:
            name: Internal check identifier (e.g., "sga_target_min_gb")
            check_name: Display name for results (e.g., "SGA_TARGET_MIN")
            category: Category of the check
            description: Human-readable description
        """
        self.name = name
        self.check_name = check_name
        self.category = category
        self.description = description

    @abstractmethod
    def execute(self, cursor, database_name: str, **kwargs) -> CheckResult:
        """Execute the check against a database.

        Args:
            cursor: Database cursor for executing queries.
            database_name: Name of the database being checked.
            **kwargs: Additional arguments specific to the check type

        Returns:
            CheckResult: Result of the check.
        """
        pass

    def _create_result(
        self,
        database_name: str,
        passed: bool,
        actual_value: Any,
        expected_value: Any,
        message: str = "",
    ) -> CheckResult:
        """Create a CheckResult for this check.

        Args:
            database_name: Name of the database.
            passed: Whether the check passed.
            actual_value: Actual value found.
            expected_value: Expected value.
            message: Optional message.

        Returns:
            CheckResult: Formatted result.
        """
        if not message and not passed:
            message = self.description

        return CheckResult(
            check_name=self.check_name,
            database=database_name,
            passed=passed,
            actual_value=str(actual_value),
            expected_value=str(expected_value),
            message=message,
            category=self.category,
        )
