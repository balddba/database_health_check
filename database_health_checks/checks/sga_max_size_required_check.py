"""Check if SGA_MAX_SIZE parameter is set."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create the SGA_MAX_SIZE_REQUIRED check.

    Returns:
        ValidationCheck: The configured SGA_MAX_SIZE validation check.
    """
    return ValidationCheck(
        name="sga_max_size_required",
        check_name="SGA_MAX_SIZE",
        category=CheckCategory.MEMORY_CONFIGURATION,
        description="SGA_MAX_SIZE should be set for memory management.",
        query="SELECT value FROM v$parameter WHERE name = 'sga_max_size' AND value != '0'",
        validation_type=ValidationType.REQUIRED,
    )
