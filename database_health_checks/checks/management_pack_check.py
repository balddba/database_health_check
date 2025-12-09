"""Check if management and tuning packs are enabled."""

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class ManagementPackCheck(CheckBaseModel):
    """Check if management and tuning packs are enabled."""

    def __init__(self) -> None:
        """Initialize the management pack check."""
        super().__init__(
            name="management_pack",
            check_name="MANAGEMENT_PACK",
            category=CheckCategory.FEATURE_CONFIGURATION,
            description="Management pack and tuning pack should be enabled for optimal monitoring and performance tuning.",
        )

    def execute(
        self, cursor, database_name: str, rule_value=None, **kwargs
    ) -> CheckResult:
        """Execute the management pack check.

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
            # Query for available packs/options
            # Tuning Pack, Diagnostics Pack, and other options can be checked
            cursor.execute(
                "SELECT parameter FROM v$option WHERE parameter IN ('Tuning Pack', 'Diagnostics Pack') AND value = 'TRUE' ORDER BY parameter"
            )
            rows = cursor.fetchall()

            if not rows:
                return self._create_result(
                    database_name=database_name,
                    passed=False,
                    actual_value="Neither pack enabled",
                    expected_value="Tuning Pack, Diagnostics Pack",
                    message="Management and tuning packs are not enabled",
                )

            # Check for required packs
            enabled_packs = [row[0] for row in rows]
            required_packs = {"Tuning Pack", "Diagnostics Pack"}
            enabled_set = set(enabled_packs)

            missing_packs = required_packs - enabled_set
            passed = len(missing_packs) == 0

            if passed:
                actual_value = (
                    f"All required packs enabled: {', '.join(sorted(enabled_packs))}"
                )
            else:
                actual_value = f"Enabled: {', '.join(sorted(enabled_packs)) if enabled_packs else 'None'}"

            expected_value = "Tuning Pack, Diagnostics Pack"

            message = ""
            if not passed:
                message = f"Missing pack(s): {', '.join(sorted(missing_packs))}"

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
                expected_value="Tuning Pack, Diagnostics Pack",
                message=f"Error checking management packs: {e}",
            )
