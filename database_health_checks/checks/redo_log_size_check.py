"""Check if online redo logs are at least 1GB in size."""

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class RedoLogSizeCheck(CheckBaseModel):
    """Check if online redo logs are at least 1GB in size."""

    def __init__(self) -> None:
        """Initialize the redo log size check."""
        super().__init__(
            name="redo_log_size",
            check_name="REDO_LOG_SIZE",
            category=CheckCategory.DATABASE_OBJECTS,
            description="Online redo logs should be at least 1GB in size for optimal performance.",
        )

    def execute(
        self, cursor, database_name: str, rule_value=None, **kwargs
    ) -> CheckResult:
        """Execute the redo log size check.

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
            # Query all online redo log sizes
            cursor.execute("SELECT group#, bytes FROM v$log ORDER BY group#")
            rows = cursor.fetchall()

            if not rows:
                return self._create_result(
                    database_name=database_name,
                    passed=False,
                    actual_value="0",
                    expected_value=">= 1GB",
                    message="No online redo logs found",
                )

            # 1GB in bytes
            one_gb = 1073741824

            # Check if all redo logs are at least 1GB
            logs_meeting_requirement = []
            logs_below_requirement = []

            for row in rows:
                group_num = row[0]
                log_size_bytes = row[1]

                if log_size_bytes >= one_gb:
                    logs_meeting_requirement.append((group_num, log_size_bytes))
                else:
                    logs_below_requirement.append((group_num, log_size_bytes))

            num_meeting = len(logs_meeting_requirement)
            num_below = len(logs_below_requirement)
            total_logs = len(rows)
            passed = num_below == 0

            # Format sizes for display
            def format_size(bytes_val):
                gb = bytes_val / one_gb
                return f"{gb:.2f}GB"

            if passed:
                min_size = min(log[1] for log in logs_meeting_requirement)
                actual_value = (
                    f"All {total_logs} redo logs >= 1GB (min: {format_size(min_size)})"
                )
            else:
                min_size = min(log[1] for log in logs_below_requirement)
                actual_value = f"{num_meeting}/{total_logs} >= 1GB, {num_below} below (min: {format_size(min_size)})"

            expected_value = "All redo logs >= 1GB"

            message = ""
            if not passed:
                invalid_groups = [
                    f"Group {g}({format_size(s)})"
                    for g, s in logs_below_requirement[:3]
                ]
                if len(logs_below_requirement) > 3:
                    invalid_groups.append(f"+{len(logs_below_requirement) - 3} more")
                message = f"Found {num_below} redo log(s) below 1GB: {', '.join(invalid_groups)}"

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
                expected_value=">= 1GB",
                message=f"Error checking redo log sizes: {e}",
            )
