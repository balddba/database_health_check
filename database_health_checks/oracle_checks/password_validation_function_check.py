"""Check if password validation function is configured on required profiles."""

from typing import List, Optional

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class PasswordValidationFunctionCheck(CheckBaseModel):
    """Check if profiles have the required password validation function."""

    def __init__(self) -> None:
        """Initialize the password validation function check."""
        super().__init__(
            name="password_validation_function",
            check_name="PASSWORD_VALIDATION_FUNCTION",
            category=CheckCategory.SECURITY_AUDITING,
            description="Required profiles should use the specified password validation function.",
        )

    def execute(
        self,
        cursor,
        database_name: str,
        validation_function: str = "",
        profiles: Optional[List[str]] = None,
        **kwargs,
    ) -> CheckResult:
        """Execute the password validation function check.

        Args:
            cursor: Database cursor.
            database_name: Name of the database.
            validation_function: Name of the required password validation function.
            profiles: List of profile names that should use the validation function.
            **kwargs: Additional arguments (unused).

        Returns:
            CheckResult: Result of the check.
        """
        if profiles is None:
            profiles = []

        # If no validation function or profiles specified, skip the check
        if not validation_function or not profiles:
            return self._create_result(
                database_name=database_name,
                passed=True,
                actual_value="N/A",
                expected_value="N/A",
                message="No password validation configuration specified",
            )

        try:
            # Query for profiles and their password validation function
            cursor.execute(
                """SELECT profile, resource_name, limit
                   FROM dba_profiles
                   WHERE resource_name = 'PASSWORD_VERIFY_FUNCTION'
                   AND profile IN ({})
                   ORDER BY profile""".format(
                    ",".join([f"'{p}'" for p in profiles])
                )
            )
            rows = cursor.fetchall()

            if not rows:
                return self._create_result(
                    database_name=database_name,
                    passed=False,
                    actual_value="No validation function found",
                    expected_value=f"{validation_function} on profiles: {', '.join(profiles)}",
                    message=f"Required profiles {profiles} do not have PASSWORD_VERIFY_FUNCTION set",
                )

            # Check if profiles have the correct validation function
            profiles_with_function: List[str] = []
            profiles_without_function: List[str] = []
            profiles_with_wrong_function: List[str] = []

            for row in rows:
                profile_name = row[0]
                function_name = row[2]  # The limit column contains the function name

                if function_name is None:
                    profiles_without_function.append(profile_name)
                elif validation_function.upper() in str(function_name).upper():
                    profiles_with_function.append(profile_name)
                else:
                    profiles_with_wrong_function.append(
                        f"{profile_name}({function_name})"
                    )

            # Check if all required profiles have the validation function
            all_profiles_checked = set(row[0] for row in rows)
            missing_profiles = [p for p in profiles if p not in all_profiles_checked]

            if missing_profiles:
                profiles_without_function.extend(missing_profiles)

            passed = (
                len(profiles_with_function) == len(profiles)
                and len(profiles_without_function) == 0
                and len(profiles_with_wrong_function) == 0
            )

            if passed:
                actual_value = (
                    f"All {len(profiles_with_function)} profile(s) configured"
                )
            else:
                if profiles_with_wrong_function:
                    actual_value = (
                        f"Wrong function: {', '.join(profiles_with_wrong_function)}"
                    )
                elif profiles_without_function:
                    actual_value = (
                        f"Not configured: {', '.join(profiles_without_function)}"
                    )
                else:
                    actual_value = "Partial configuration"

            return self._create_result(
                database_name=database_name,
                passed=passed,
                actual_value=actual_value,
                expected_value=f"{validation_function} on {len(profiles)} profile(s)",
                message="" if passed else self.description,
            )

        except Exception as e:
            return self._create_result(
                database_name=database_name,
                passed=False,
                actual_value="ERROR",
                expected_value=f"{validation_function} on profiles: {', '.join(profiles)}",
                message=f"Error executing check: {e}",
            )
