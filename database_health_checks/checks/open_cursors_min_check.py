"""Check if OPEN_CURSORS parameter is set to minimum of 1000."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create OPEN_CURSORS minimum check.

    Returns:
        ValidationCheck: The configured OPEN_CURSORS validation check.
    """
    return ValidationCheck(
        name="open_cursors_min",
        check_name="OPEN_CURSORS_MIN",
        category=CheckCategory.MEMORY_CONFIGURATION,
        description="The OPEN_CURSORS parameter should be set to at least 1000 to support sufficient concurrent cursor operations.",
        query="SELECT value FROM v$parameter WHERE name = 'open_cursors'",
        validation_type=ValidationType.MINIMUM,
        threshold=1000,
    )
