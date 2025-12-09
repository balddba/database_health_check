"""Check if archivelog mode is enabled."""

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

    if val_str in ("YES", "TRUE", "Y", "1", "ON", "ARCHIVELOG"):
        return "True"
    elif val_str in ("NO", "FALSE", "N", "0", "OFF", "NONE", "NOARCHIVELOG"):
        return "False"

    return val_str


def create_check() -> ValidationCheck:
    """Create the ARCHIVELOG_MODE check.

    Returns:
        ValidationCheck: The configured ARCHIVELOG_MODE validation check.
    """
    return ValidationCheck(
        name="archivelog_mode_enabled",
        check_name="ARCHIVELOG_MODE",
        category=CheckCategory.BACKUP_RECOVERY,
        description="Archivelog mode should be enabled for data protection and recovery.",
        query="SELECT log_mode FROM v$database",
        validation_type=ValidationType.EQUALS,
        threshold="True",
        value_normalizer=_normalize_boolean,
    )
