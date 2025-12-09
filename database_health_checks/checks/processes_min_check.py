"""Check if PROCESSES parameter is set to minimum of 1000."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create PROCESSES minimum check.

    Returns:
        ValidationCheck: The configured PROCESSES validation check.
    """
    return ValidationCheck(
        name="processes_min",
        check_name="PROCESSES_MIN",
        category=CheckCategory.MEMORY_CONFIGURATION,
        description="The PROCESSES parameter should be set to at least 1000 to support sufficient database sessions.",
        query="SELECT value FROM v$parameter WHERE name = 'processes'",
        validation_type=ValidationType.MINIMUM,
        threshold=1000,
    )
