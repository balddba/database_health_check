"""Check if SGA_TARGET is at least the configured minimum."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create SGA_TARGET_MIN_GB check.

    Returns:
        ValidationCheck: The configured SGA_TARGET_MIN validation check.
    """
    return ValidationCheck(
        name="sga_target_min_gb",
        check_name="SGA_TARGET_MIN",
        category=CheckCategory.MEMORY_CONFIGURATION,
        description="SGA_TARGET should be at least the configured minimum for optimal performance.",
        query="SELECT value FROM v$parameter WHERE name = 'sga_target'",
        validation_type=ValidationType.MINIMUM,
        threshold=None,  # Set from rules
    )
