"""Check if OPTIMIZER_MODE is set to all_rows."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def _normalize_optimizer_mode(value) -> str:
    """Normalize optimizer mode value to lowercase.

    Returns:
        str: The normalized optimizer mode value.
    """
    if value is None:
        return "NOT SET"

    return str(value).strip().lower()


def create_check() -> ValidationCheck:
    """Create the OPTIMIZER_MODE check.

    Returns:
        ValidationCheck: The configured OPTIMIZER_MODE validation check.
    """
    return ValidationCheck(
        name="optimizer_mode",
        check_name="OPTIMIZER_MODE",
        category=CheckCategory.PERFORMANCE_TUNING,
        description="Optimizer mode should be set to ALL_ROWS for optimal query execution.",
        query="SELECT value FROM v$parameter WHERE name = 'optimizer_mode'",
        validation_type=ValidationType.EQUALS,
        threshold="all_rows",
        value_normalizer=_normalize_optimizer_mode,
    )
