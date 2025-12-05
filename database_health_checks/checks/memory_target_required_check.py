"""Check if MEMORY_TARGET parameter is set."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create MEMORY_TARGET_REQUIRED check.

    Returns:
        ValidationCheck: The configured MEMORY_TARGET validation check.
    """
    return ValidationCheck(
        name="memory_target_required",
        check_name="MEMORY_TARGET",
        category=CheckCategory.MEMORY_CONFIGURATION,
        description="MEMORY_TARGET can be set for automatic memory management (optional for modern Oracle).",
        query="SELECT value FROM v$parameter WHERE name = 'memory_target' AND value != '0'",
        validation_type=ValidationType.REQUIRED,
    )
