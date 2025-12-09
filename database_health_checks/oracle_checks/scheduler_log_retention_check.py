"""Check the scheduler log retention period."""

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class SchedulerLogRetentionCheck(CheckBaseModel):
    """Check the scheduler log retention period."""

    def __init__(self) -> None:
        """Initialize the scheduler log retention check."""
        super().__init__(
            name="scheduler_log_retention_days",
            check_name="SCHEDULER_LOG_RETENTION",
            category=CheckCategory.LOGGING_MONITORING,
            description="Scheduler logs should be retained for at least 30 days for the audit trail.",
        )

    def execute(
        self, cursor, database_name: str, min_days: int = 30, **kwargs
    ) -> CheckResult:
        """Execute the scheduler log retention check.

        Args:
            cursor: Database cursor.
            database_name: Name of the database.
            min_days: Minimum retention days required (default 30).
            **kwargs: Additional arguments (unused).

        Returns:
            CheckResult: Result of the check.
        """
        try:
            # Query scheduler log retention setting
            cursor.execute(
                "SELECT value FROM dba_scheduler_global_attribute WHERE attribute_name = 'log_history'"
            )
            row = cursor.fetchone()

            if not row or not row[0]:
                log_history_days = 0
                passed = False
                actual_value = "Not set"
            else:
                try:
                    log_history_days = int(row[0])
                    passed = log_history_days >= min_days
                    actual_value = f"{log_history_days} days"
                except (ValueError, TypeError):
                    passed = False
                    actual_value = str(row[0])

            return self._create_result(
                database_name=database_name,
                passed=passed,
                actual_value=actual_value,
                expected_value=f">= {min_days} days",
                message="" if passed else self.description,
            )
        except Exception as e:
            return self._create_result(
                database_name=database_name,
                passed=False,
                actual_value="ERROR",
                expected_value=f">= {min_days} days",
                message=f"Error executing check: {e}",
            )
