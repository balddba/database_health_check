"""Check if PGA_AGGREGATE_TARGET parameter is set."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create the PGA_AGGREGATE_TARGET_REQUIRED check.

    Returns:
        ValidationCheck: The configured PGA_AGGREGATE_TARGET validation check.
    """
    return ValidationCheck(
        name="pga_aggregate_target_required",
        check_name="PGA_AGGREGATE_TARGET",
        category=CheckCategory.MEMORY_CONFIGURATION,
        description="PGA_AGGREGATE_TARGET should be set for memory management.",
        query="SELECT value FROM v$parameter WHERE name = 'pga_aggregate_target' AND value != '0'",
        validation_type=ValidationType.REQUIRED,
    )
