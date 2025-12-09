"""Check if unified auditing is enabled."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def _normalize_audit_trail(value) -> str:
    """Normalize audit_trail values.

    Treats NONE and empty values as disabled (False).
    Treats any other value as enabled (True).

    Returns:
        str: The normalized value as 'True' or 'False'.
    """
    if value is None:
        return "False"

    val_str = str(value).strip().upper()

    if val_str in ("", "NONE", "FALSE", "NO", "0", "OFF"):
        return "False"
    else:
        return "True"


def create_check() -> ValidationCheck:
    """Create the UNIFIED_AUDITING_ENABLED check.

    Returns:
        ValidationCheck: The configured UNIFIED_AUDITING validation check.
    """
    return ValidationCheck(
        name="unified_auditing_enabled",
        check_name="UNIFIED_AUDITING",
        category=CheckCategory.SECURITY_AUDITING,
        description="Unified Auditing should be enabled for comprehensive audit logging.",
        query="SELECT value FROM v$parameter WHERE name = 'audit_trail'",
        validation_type=ValidationType.EQUALS,
        threshold="True",
        value_normalizer=_normalize_audit_trail,
    )
