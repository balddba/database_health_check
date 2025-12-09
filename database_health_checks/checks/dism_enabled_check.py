"""Check if Dynamic Instance Shared Memory (DISM) is enabled."""

from database_health_checks.models.check_catagory import CheckCategory

from .validation_check import ValidationCheck, ValidationType


def create_check() -> ValidationCheck:
    """Create the DISM_ENABLED check.

    Returns:
        ValidationCheck: The configured DISM_ENABLED validation check.
    """
    return ValidationCheck(
        name="dism_enabled",
        check_name="DISM_ENABLED",
        category=CheckCategory.MEMORY_CONFIGURATION,
        description="SGA_TARGET should be equal to SGA_MAX_SIZE.",
        query="""
            SELECT
                CASE WHEN
                    (SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'sga_target') =
                    (SELECT TO_NUMBER(value) FROM v$parameter WHERE name = 'sga_max_size')
                THEN 'True' ELSE 'False' END
            FROM dual
        """,
        validation_type=ValidationType.EQUALS,
        threshold="True",
    )
