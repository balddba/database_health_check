"""Base class for all database checks."""

from abc import ABC, abstractmethod
from typing import Union

from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class CheckBaseModel(ABC):
    """Base class for all database checks."""

    def __init__(
        self, name: str, check_name: str, category: CheckCategory, description: str
    ):
        """Initialize a check.

        Args:
            name: The internal check identifier (e.g., "sga_target_min_gb")
            check_name: The display name for results (e.g., "SGA_TARGET_MIN")
            category: The category of the check
            description: The human-readable description
        """
        self.name = name
        self.check_name = check_name
        self.category = category
        self.description = description

    @abstractmethod
    def execute(self, cursor, database_name: str, **kwargs) -> CheckResult:
        """Execute the check against a database.

        Args:
            cursor: The database cursor for executing queries.
            database_name: The name of the database being checked.
            **kwargs: Additional arguments specific to the check type

        Returns:
            CheckResult: The result of the check.
        """
        pass

    def _create_result(
        self,
        database_name: str,
        passed: bool,
        actual_value: Union[str, int, float, bool],
        expected_value: Union[str, int, float, bool],
        message: str = "",
    ) -> CheckResult:
        """Create a CheckResult for this check.

        Args:
            database_name: The name of the database.
            passed: Whether the check passed.
            actual_value: The actual value found.
            expected_value: The expected value.
            message: An optional message.

        Returns:
            CheckResult: The formatted result.
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
