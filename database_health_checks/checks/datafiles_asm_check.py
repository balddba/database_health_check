"""Check if all datafiles are stored in +DATA ASM disk group."""

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class DatafilesASMCheck(CheckBaseModel):
    """Check if all datafiles are stored in +DATA ASM disk group."""

    def __init__(self) -> None:
        """Initialize the datafiles ASM check."""
        super().__init__(
            name="datafiles_asm",
            check_name="DATAFILES_ASM",
            category=CheckCategory.DATABASE_OBJECTS,
            description="All datafiles should be stored in the +DATA ASM disk group.",
        )

    def execute(self, cursor, database_name: str, rule_value=None, **kwargs) -> CheckResult:
        """Execute the datafiles ASM check.

        Args:
            cursor: Database cursor.
            database_name: Name of the database.
            rule_value: Whether this check is enabled (True/False).
            **kwargs: Additional arguments (unused).

        Returns:
            CheckResult: Result of the check.
        """
        # Skip if check is disabled
        if rule_value is False or rule_value is None:
            return self._create_result(
                database_name=database_name,
                passed=True,
                actual_value="N/A",
                expected_value="Not Required",
                message="Check not required",
            )
        
        try:
            # Query all datafile names
            cursor.execute("SELECT name FROM v$datafile ORDER BY name")
            rows = cursor.fetchall()

            if not rows:
                return self._create_result(
                    database_name=database_name,
                    passed=False,
                    actual_value="0",
                    expected_value="+DATA/...",
                    message="No datafiles found",
                )

            # Check if all datafiles are in +DATA disk group
            datafiles_in_data = []
            datafiles_not_in_data = []

            for row in rows:
                datafile_path = row[0]
                if datafile_path.startswith("+DATA"):
                    datafiles_in_data.append(datafile_path)
                else:
                    datafiles_not_in_data.append(datafile_path)

            num_in_data = len(datafiles_in_data)
            num_not_in_data = len(datafiles_not_in_data)
            total_datafiles = len(rows)
            passed = num_not_in_data == 0

            if passed:
                actual_value = f"{num_in_data}/{total_datafiles} datafiles in +DATA"
            else:
                actual_value = f"{num_in_data}/{total_datafiles} in +DATA, {num_not_in_data} elsewhere"

            expected_value = "All datafiles in +DATA"

            message = ""
            if not passed:
                invalid_files = ", ".join(datafiles_not_in_data[:3])
                if len(datafiles_not_in_data) > 3:
                    invalid_files += f"... (+{len(datafiles_not_in_data) - 3} more)"
                message = f"Found {num_not_in_data} datafile(s) not in +DATA: {invalid_files}"

            return self._create_result(
                database_name=database_name,
                passed=passed,
                actual_value=actual_value,
                expected_value=expected_value,
                message=message,
            )
        except Exception as e:
            return self._create_result(
                database_name=database_name,
                passed=False,
                actual_value="ERROR",
                expected_value="+DATA/...",
                message=f"Error checking datafiles ASM location: {e}",
            )
