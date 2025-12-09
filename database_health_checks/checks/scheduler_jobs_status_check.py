"""Check scheduler job status and highlight purge/cleanup jobs if disabled."""

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class SchedulerJobsStatusCheck(CheckBaseModel):
    """Check scheduler job status and report on critical jobs."""

    def __init__(self) -> None:
        """Initialize the scheduler jobs status check."""
        super().__init__(
            name="scheduler_jobs_status",
            check_name="SCHEDULER_JOBS_STATUS",
            category=CheckCategory.DATABASE_OBJECTS,
            description="Reports on scheduler jobs status. Alerts if purge/cleanup jobs are disabled.",
        )

    def execute(
        self, cursor, database_name: str, rule_value=None, **kwargs
    ) -> CheckResult:
        """Execute the scheduler jobs status check.

        Args:
            cursor: Database cursor.
            database_name: Name of the database.
            rule_value: Unused for this check (always reports).
            **kwargs: Additional arguments (unused).

        Returns:
            CheckResult: Result of the check.
        """
        try:
            # Query all scheduler jobs
            cursor.execute(
                "SELECT job_name, enabled FROM dba_scheduler_jobs ORDER BY job_name"
            )
            rows = cursor.fetchall()

            if not rows:
                return self._create_result(
                    database_name=database_name,
                    passed=True,
                    actual_value="0 jobs",
                    expected_value="N/A",
                    message="No scheduler jobs found",
                )

            # Analyze jobs
            total_jobs = len(rows)
            enabled_jobs = []
            disabled_jobs = []
            critical_disabled_jobs = []

            for row in rows:
                job_name = row[0]
                enabled = str(row[1]).upper() == "TRUE"

                if enabled:
                    enabled_jobs.append(job_name)
                else:
                    disabled_jobs.append(job_name)
                    # Check if it's a purge or cleanup job
                    if "purge" in job_name.lower() or "cleanup" in job_name.lower():
                        critical_disabled_jobs.append(job_name)

            # Determine pass/fail based on critical jobs
            passed = len(critical_disabled_jobs) == 0

            # Format actual value
            actual_value = f"{len(enabled_jobs)} enabled, {len(disabled_jobs)} disabled"

            # Build message
            message = ""
            if critical_disabled_jobs:
                message = f"ALERT: Critical job(s) disabled: {', '.join(critical_disabled_jobs)}"
            else:
                message = f"All scheduler jobs: {total_jobs} total ({len(enabled_jobs)} enabled, {len(disabled_jobs)} disabled)"

            return self._create_result(
                database_name=database_name,
                passed=passed,
                actual_value=actual_value,
                expected_value="All purge/cleanup jobs enabled",
                message=message,
            )
        except Exception as e:
            return self._create_result(
                database_name=database_name,
                passed=True,
                actual_value="ERROR",
                expected_value="N/A",
                message=f"Error checking scheduler jobs: {e}",
            )
