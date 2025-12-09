"""Check if flashback is enabled."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def _normalize_boolean(value) -> str:
    """Normalize various boolean representations to True/False.

    Handles: YES, NO, TRUE, FALSE, Y, N, 1, 0, NONE, etc.

    Returns:
        str: The normalized boolean value as 'True' or 'False'.
    """
    if value is None:
        return "False"

    val_str = str(value).strip().upper()

    if val_str in ("YES", "TRUE", "Y", "1", "ON"):
        return "True"
    elif val_str in ("NO", "FALSE", "N", "0", "OFF", "NONE"):
        return "False"

    return val_str


def create_check() -> ValidationCheck:
    """Create the FLASHBACK_ENABLED check.

    Returns:
        ValidationCheck: The configured FLASHBACK_ENABLED validation check.
    """
    return ValidationCheck(
        name="flashback_enabled",
        check_name="FLASHBACK_ENABLED",
        category=CheckCategory.FEATURE_CONFIGURATION,
        description="Flashback database should be enabled for recovery capabilities.",
        query="SELECT flashback_on FROM v$database",
        validation_type=ValidationType.EQUALS,
        threshold="True",
        value_normalizer=_normalize_boolean,
    )
