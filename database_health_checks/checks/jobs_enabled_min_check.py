"""Check if database jobs are enabled."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create the JOBS_ENABLED_MIN check.

    Returns:
        ValidationCheck: The configured JOBS_ENABLED validation check.
    """
    return ValidationCheck(
        name="jobs_enabled_min",
        check_name="JOBS_ENABLED",
        category=CheckCategory.DATABASE_OBJECTS,
        description="Should have at least the minimum number of enabled scheduler jobs.",
        query="SELECT COUNT(*) FROM dba_scheduler_jobs WHERE enabled = 'TRUE'",
        validation_type=ValidationType.MINIMUM,
    )
