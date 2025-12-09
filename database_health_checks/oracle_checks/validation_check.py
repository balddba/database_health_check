"""Validation checks based on rules (simple threshold comparisons)."""

from enum import Enum
from typing import Callable, Optional, Union

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class ValidationType(Enum):
    """Types of validation checks."""

    REQUIRED = "required"  # Parameter must be set/enabled
    MINIMUM = "minimum"  # Value must be >= threshold
    MAXIMUM = "maximum"  # Value must be <= threshold
    EQUALS = "equals"  # Value must equal expected value


class ValidationCheck(CheckBaseModel):
    """A check that validates a value against a threshold or requirement."""

    def __init__(
        self,
        name: str,
        check_name: str,
        category: CheckCategory,
        description: str,
        query: str,
        validation_type: ValidationType,
        threshold: Optional[Union[str, int, float, bool]] = None,
        value_normalizer: Optional[Callable[[Union[str, int, float, bool]], Union[str, int, float, bool]]] = None,
    ):
        """Initialize a validation check.

        Args:
             name: An internal check identifier
             check_name: The display name
             category: The check category
             description: A human-readable description
             query: The SQL query to fetch value for validation
             validation_type: The type of validation to perform
             threshold: The threshold value for comparison (for MINIMUM, MAXIMUM, EQUALS)
             value_normalizer: An optional function to normalize query results before validation
        """
        super().__init__(name, check_name, category, description)
        self.query = query
        self.validation_type = validation_type
        self.threshold = threshold
        self.value_normalizer = value_normalizer

    def execute(
        self,
        cursor,
        database_name: str,
        rule_value: Optional[Union[str, int, float, bool]] = None,
        transform: Optional[Callable[[Union[str, int, float, bool]], Union[str, int, float, bool]]] = None,
        **kwargs,
    ) -> CheckResult:
        """Execute the validation check.

        Args:
             cursor: A database cursor for executing queries.
             database_name: The name of the database being checked.
             rule_value: An override threshold from rules (takes precedence over self.threshold).
             transform: An optional function to transform query results before comparison.
             **kwargs: Additional arguments

        Returns:
            CheckResult: Result of the validation check.
        """
        threshold = rule_value if rule_value is not None else self.threshold

        # If REQUIRED type check is not required (threshold is False/None), skip it by marking as N/A
        if (
            threshold is False or threshold is None
        ) and self.validation_type == ValidationType.REQUIRED:
            return self._create_result(
                database_name=database_name,
                passed=True,
                actual_value="N/A",
                expected_value="Not Required",
                message="Check not required",
            )

        try:
            cursor.execute(self.query)
            row = cursor.fetchone()

            # Handle missing data
            if not row or not row[0]:
                actual_value = "NOT SET"
                expected_value = self._get_expected_value(threshold)
                # Validate: if check not required (threshold is False/None), pass; otherwise fail
                passed = (threshold is False or threshold is None) or self._validate(
                    actual_value, threshold
                )
            else:
                actual_value = row[0]

                # Apply value normalizer if provided
                if self.value_normalizer:
                    actual_value = self.value_normalizer(actual_value)

                # Apply transformation if provided
                if transform:
                    actual_value = transform(actual_value)

                # Perform validation
                passed = self._validate(actual_value, threshold)
                expected_value = self._get_expected_value(threshold)

            return self._create_result(
                database_name=database_name,
                passed=passed,
                actual_value=actual_value,
                expected_value=expected_value,
            )
        except Exception as e:
            return self._create_result(
                database_name=database_name,
                passed=False,
                actual_value="ERROR",
                expected_value=self._get_expected_value(threshold),
                message=f"Error executing check: {e}",
            )

    def _validate(self, actual: Union[str, int, float, bool], threshold: Union[str, int, float, bool]) -> bool:
        """Validate actual value against threshold.

        Args:
            actual: The actual value from the database.
            threshold: The expected/threshold value.

        Returns:
            bool: True if valid, False otherwise.
        """
        if self.validation_type == ValidationType.REQUIRED:
            return threshold and actual is not None
        elif self.validation_type == ValidationType.EQUALS:
            return str(actual).upper() == str(threshold).upper()
        elif self.validation_type == ValidationType.MINIMUM:
            try:
                # Convert to int for numeric comparison
                actual_num = int(actual) if isinstance(actual, str) else actual
                threshold_num = (
                    int(threshold) if isinstance(threshold, str) else threshold
                )
                return actual_num >= threshold_num
            except (TypeError, ValueError):
                return False
        elif self.validation_type == ValidationType.MAXIMUM:
            try:
                # Convert to int for numeric comparison
                actual_num = int(actual) if isinstance(actual, str) else actual
                threshold_num = (
                    int(threshold) if isinstance(threshold, str) else threshold
                )
                return actual_num <= threshold_num
            except (TypeError, ValueError):
                return False
        return False

    def _get_expected_value(self, threshold: Union[str, int, float, bool]) -> str:
        """Get a human-readable expected value string.

        Args:
            threshold: The rule threshold or requirement value.

        Returns:
            str: A human-readable expected value.
        """
        if self.validation_type == ValidationType.REQUIRED:
            return "Set/Enabled" if threshold else "Not Required"
        elif self.validation_type == ValidationType.EQUALS:
            return str(threshold)
        elif self.validation_type == ValidationType.MINIMUM:
            return f">= {threshold}"
        elif self.validation_type == ValidationType.MAXIMUM:
            return f"<= {threshold}"
        return str(threshold)
