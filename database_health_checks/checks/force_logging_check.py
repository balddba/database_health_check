"""Check if force logging is enabled."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def _normalize_boolean(value) -> str:
    """Normalize various boolean representations to True/False.

    Handles: YES, NO, TRUE, FALSE, Y, N, 1, 0, NONE, FORCE LOGGING, etc.

    Returns:
        str: Normalized boolean value as 'True' or 'False'.
    """
    if value is None:
        return "False"

    val_str = str(value).strip().upper()

    if val_str in ("YES", "TRUE", "Y", "1", "ON", "FORCE LOGGING"):
        return "True"
    elif val_str in ("NO", "FALSE", "N", "0", "OFF", "NONE", "NO FORCE LOGGING"):
        return "False"

    return val_str


def create_check() -> ValidationCheck:
    """Create FORCE_LOGGING check.

    Returns:
        ValidationCheck: The configured FORCE_LOGGING validation check.
    """
    return ValidationCheck(
        name="force_logging_enabled",
        check_name="FORCE_LOGGING",
        category=CheckCategory.FEATURE_CONFIGURATION,
        description="Force logging should be enabled to ensure all changes are logged.",
        query="SELECT force_logging FROM v$database",
        validation_type=ValidationType.EQUALS,
        threshold="True",
        value_normalizer=_normalize_boolean,
    )
