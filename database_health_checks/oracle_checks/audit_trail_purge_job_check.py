"""Check if the AUDIT_TRAIL_PURGE job is scheduled and enabled."""

from typing import List

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class AuditTrailPurgeJobCheck(CheckBaseModel):
    """Check if the AUDIT_TRAIL_PURGE job is scheduled and enabled."""

    def __init__(self) -> None:
        """Initialize the audit trail purge job check."""
        super().__init__(
            name="audit_trail_purge_job",
            check_name="AUDIT_TRAIL_PURGE_JOB",
            category=CheckCategory.LOGGING_MONITORING,
            description="A scheduler log purge job should be enabled to prevent audit trail bloat.",
        )

    def execute(self, cursor, database_name: str, **kwargs) -> CheckResult:
        """Execute the audit trail purge job check.

        Args:
            cursor: Database cursor.
            database_name: Name of the database.
            **kwargs: Additional arguments (unused).

        Returns:
            CheckResult: Result of the check.
        """
        try:
            # Check for AUDIT_TRAIL_PURGE or similar purge jobs
            cursor.execute(
                """SELECT job_name, enabled
                   FROM dba_scheduler_jobs
                   WHERE (job_name LIKE '%AUDIT_TRAIL_PURGE%'
                          OR job_name LIKE '%PURGE%LOG%'
                          OR job_name LIKE '%SCHEDULER%PURGE%')
                   AND owner = 'SYS'
                   ORDER BY job_name"""
            )
            rows = cursor.fetchall()

            # Check if at least one purge job is enabled
            purge_jobs: List[str] = []
            enabled_jobs: List[str] = []

            for row in rows:
                job_name = row[0] if row[0] else ""
                enabled = row[1] if row[1] else "FALSE"
                purge_jobs.append(job_name)
                if str(enabled).upper() in ("TRUE", "Y", "1"):
                    enabled_jobs.append(job_name)

            passed = len(enabled_jobs) > 0

            if passed:
                actual_value = ", ".join(enabled_jobs[:2])  # Show first 2 jobs
                if len(enabled_jobs) > 2:
                    actual_value += f" (+{len(enabled_jobs) - 2} more)"
            elif len(purge_jobs) > 0:
                actual_value = (
                    f"{len(purge_jobs)} job(s) found but disabled: {purge_jobs[0]}"
                )
            else:
                actual_value = "No purge jobs found"

            return self._create_result(
                database_name=database_name,
                passed=passed,
                actual_value=actual_value,
                expected_value="At least one enabled purge job",
                message="" if passed else self.description,
            )
        except Exception as e:
            return self._create_result(
                database_name=database_name,
                passed=False,
                actual_value="ERROR",
                expected_value="At least one enabled purge job",
                message=f"Error executing check: {e}",
            )
