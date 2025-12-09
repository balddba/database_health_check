"""Check if STATISTICS_LEVEL is set to TYPICAL."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def _normalize_statistics_level(value) -> str:
    """Normalize statistics level value to uppercase.

    Returns:
        str: The normalized statistics level value.
    """
    if value is None:
        return "NOT SET"

    return str(value).strip().upper()


def create_check() -> ValidationCheck:
    """Create the STATISTICS_LEVEL check.

    Returns:
        ValidationCheck: The configured STATISTICS_LEVEL validation check.
    """
    return ValidationCheck(
        name="statistics_level",
        check_name="STATISTICS_LEVEL",
        category=CheckCategory.MEMORY_CONFIGURATION,
        description="Statistics level should be set to TYPICAL for optimal performance monitoring.",
        query="SELECT value FROM v$parameter WHERE name = 'statistics_level'",
        validation_type=ValidationType.EQUALS,
        threshold="TYPICAL",
        value_normalizer=_normalize_statistics_level,
    )
