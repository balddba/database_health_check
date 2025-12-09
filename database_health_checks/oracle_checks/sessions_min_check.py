"""Check if SESSIONS parameter is set to minimum of 1000."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create SESSIONS minimum check.

    Returns:
        ValidationCheck: The configured SESSIONS validation check.
    """
    return ValidationCheck(
        name="sessions_min",
        check_name="SESSIONS_MIN",
        category=CheckCategory.MEMORY_CONFIGURATION,
        description="The SESSIONS parameter should be set to at least 1000 to support sufficient concurrent user sessions.",
        query="SELECT value FROM v$parameter WHERE name = 'sessions'",
        validation_type=ValidationType.MINIMUM,
        threshold=1000,
    )
