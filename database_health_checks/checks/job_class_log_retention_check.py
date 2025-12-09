"""Check log retention for each job class."""

from typing import List

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class JobClassLogRetentionCheck(CheckBaseModel):
    """Check log retention for each job class."""

    def __init__(self) -> None:
        """Initialize the job class log retention check."""
        super().__init__(
            name="job_class_log_retention_days",
            check_name="JOB_CLASS_LOG_RETENTION",
            category=CheckCategory.LOGGING_MONITORING,
            description="All job classes should have log retention configured.",
        )

    def execute(
        self, cursor, database_name: str, min_days: int = 1, **kwargs
    ) -> CheckResult:
        """Execute the job class log retention check.

        Args:
            cursor: Database cursor.
            database_name: Name of the database.
            min_days: Minimum retention days required (default 1).
            **kwargs: Additional arguments (unused).

        Returns:
            CheckResult: Result of the check.
        """
        try:
            # Query job class log retention settings
            cursor.execute(
                "SELECT job_class_name, log_history FROM dba_scheduler_job_classes ORDER BY job_class_name"
            )
            rows = cursor.fetchall()

            if not rows:
                # No job classes found - consider it passed
                return self._create_result(
                    database_name=database_name,
                    passed=True,
                    actual_value="No job classes found",
                    expected_value="N/A",
                    message="",
                )

            # Check if all job classes have log retention configured
            classes_without_retention: List[str] = []
            classes_with_retention: List[str] = []

            for row in rows:
                class_name = row[0] if row[0] else "Unknown"
                log_history = row[1] if row[1] else 0

                try:
                    log_history_days = int(log_history) if log_history else 0
                except (ValueError, TypeError):
                    log_history_days = 0

                if log_history_days >= min_days:
                    classes_with_retention.append(f"{class_name}({log_history_days}d)")
                else:
                    classes_without_retention.append(class_name)

            passed = len(classes_without_retention) == 0

            if passed:
                actual_value = f"All {len(classes_with_retention)} classes configured"
            else:
                actual_value = f"{len(classes_without_retention)} class(es) without retention: {', '.join(classes_without_retention[:3])}"
                if len(classes_without_retention) > 3:
                    actual_value += f" (+{len(classes_without_retention) - 3} more)"

            return self._create_result(
                database_name=database_name,
                passed=passed,
                actual_value=actual_value,
                expected_value=f"All job classes with log retention >= {min_days} day(s)",
                message="" if passed else self.description,
            )
        except Exception as e:
            return self._create_result(
                database_name=database_name,
                passed=False,
                actual_value="ERROR",
                expected_value=f"All job classes with log retention >= {min_days} day(s)",
                message=f"Error executing check: {e}",
            )
