"""Check if recovery logs are stored in +RECO ASM disk group."""

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class RecoveryLogsASMCheck(CheckBaseModel):
    """Check if recovery logs (redo logs) are stored in +RECO ASM disk group."""

    def __init__(self) -> None:
        """Initialize the recovery logs ASM check."""
        super().__init__(
            name="recovery_logs_asm",
            check_name="RECOVERY_LOGS_ASM",
            category=CheckCategory.DATABASE_OBJECTS,
            description="Recovery logs (redo logs) should be stored in the +RECO ASM disk group.",
        )

    def execute(
        self, cursor, database_name: str, rule_value=None, **kwargs
    ) -> CheckResult:
        """Execute the recovery logs ASM check.

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
            # Query all redo log files (online redo logs)
            cursor.execute("SELECT member FROM v$logfile ORDER BY member")
            rows = cursor.fetchall()

            if not rows:
                return self._create_result(
                    database_name=database_name,
                    passed=False,
                    actual_value="0",
                    expected_value="+RECO/...",
                    message="No redo log files found",
                )

            # Check if all redo log files are in +RECO disk group
            logs_in_reco = []
            logs_not_in_reco = []

            for row in rows:
                log_path = row[0]
                if log_path.startswith("+RECO"):
                    logs_in_reco.append(log_path)
                else:
                    logs_not_in_reco.append(log_path)

            num_in_reco = len(logs_in_reco)
            num_not_in_reco = len(logs_not_in_reco)
            total_logs = len(rows)
            passed = num_not_in_reco == 0

            if passed:
                actual_value = f"{num_in_reco}/{total_logs} redo logs in +RECO"
            else:
                actual_value = (
                    f"{num_in_reco}/{total_logs} in +RECO, {num_not_in_reco} elsewhere"
                )

            expected_value = "All redo logs in +RECO"

            message = ""
            if not passed:
                invalid_files = ", ".join(logs_not_in_reco[:3])
                if len(logs_not_in_reco) > 3:
                    invalid_files += f"... (+{len(logs_not_in_reco) - 3} more)"
                message = f"Found {num_not_in_reco} redo log file(s) not in +RECO: {invalid_files}"

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
                expected_value="+RECO/...",
                message=f"Error checking recovery logs ASM location: {e}",
            )
