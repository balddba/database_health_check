"""Check if JOB_QUEUE_PROCESSES is set above the minimum."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create JOB_QUEUE_PROCESSES_MIN check.

    Returns:
        ValidationCheck: The configured JOB_QUEUE_PROCESSES validation check.
    """
    return ValidationCheck(
        name="job_queue_processes_min",
        check_name="JOB_QUEUE_PROCESSES",
        category=CheckCategory.DATABASE_OBJECTS,
        description="JOB_QUEUE_PROCESSES should be set above the minimum.",
        query="SELECT value FROM v$parameter WHERE name = 'job_queue_processes'",
        validation_type=ValidationType.MINIMUM,
    )
