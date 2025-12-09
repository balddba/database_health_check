"""Check the number of open database links."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create the OPEN_DBLINKS_MAX check.

    Returns:
        ValidationCheck: The configured OPEN_DBLINKS validation check.
    """
    return ValidationCheck(
        name="open_dblinks_max",
        check_name="OPEN_DBLINKS",
        category=CheckCategory.DATABASE_OBJECTS,
        description="Maximum number of open database links (open_links parameter).",
        query="SELECT value FROM v$parameter WHERE name = 'open_links'",
        validation_type=ValidationType.MINIMUM,
    )
