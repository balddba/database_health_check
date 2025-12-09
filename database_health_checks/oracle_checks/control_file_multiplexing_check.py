"""Check if control files are multiplexed across at least two different disk groups."""

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class ControlFileMultiplexingCheck(CheckBaseModel):
    """Check if control files are multiplexed across at least two different disk groups."""

    def __init__(self) -> None:
        """Initialize the control file multiplexing check."""
        super().__init__(
            name="control_file_multiplexing",
            check_name="CONTROL_FILE_MULTIPLEXING",
            category=CheckCategory.HIGH_AVAILABILITY_CLUSTER,
            description="Control files should be multiplexed across at least two different disk groups for high availability.",
        )

    def execute(
        self, cursor, database_name: str, rule_value=None, **kwargs
    ) -> CheckResult:
        """Execute the control file multiplexing check.

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
            # Query control file locations
            cursor.execute("SELECT name FROM v$controlfile ORDER BY name")
            rows = cursor.fetchall()

            if not rows:
                return self._create_result(
                    database_name=database_name,
                    passed=False,
                    actual_value="0",
                    expected_value=">= 2",
                    message="No control files found",
                )

            # Extract disk groups from control file paths
            # For ASM: +DISKGROUP/... , for filesystem: /path/to/file
            disk_groups = set()

            for row in rows:
                control_file_path = row[0]
                if control_file_path.startswith("+"):
                    # ASM control file: extract disk group name
                    # Format: +DISKGROUPNAME/...
                    parts = control_file_path.split("/")
                    if parts[0].startswith("+"):
                        disk_group = parts[0][1:]  # Remove the '+' prefix
                        disk_groups.add(disk_group)
                else:
                    # Filesystem control file: extract parent directory
                    # Use the parent directory as the "disk group" identifier
                    import os

                    parent_dir = os.path.dirname(control_file_path)
                    disk_groups.add(parent_dir)

            num_disk_groups = len(disk_groups)
            actual_value = (
                f"{num_disk_groups} disk group(s): {', '.join(sorted(disk_groups))}"
            )
            expected_value = ">= 2 disk groups"
            passed = num_disk_groups >= 2

            message = (
                ""
                if passed
                else f"Control files are not adequately multiplexed. Found {num_disk_groups} disk group(s), need at least 2."
            )

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
                expected_value=">= 2",
                message=f"Error checking control file multiplexing: {e}",
            )
