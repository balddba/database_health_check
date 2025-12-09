"""Check if DB_FILES parameter is set to minimum of 2500."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create DB_FILES minimum check.

    Returns:
        ValidationCheck: The configured DB_FILES validation check.
    """
    return ValidationCheck(
        name="db_files_min",
        check_name="DB_FILES_MIN",
        category=CheckCategory.MEMORY_CONFIGURATION,
        description="The DB_FILES parameter should be set to at least 2500 to support a sufficient number of database files.",
        query="SELECT value FROM v$parameter WHERE name = 'db_files'",
        validation_type=ValidationType.MINIMUM,
        threshold=2500,
    )
