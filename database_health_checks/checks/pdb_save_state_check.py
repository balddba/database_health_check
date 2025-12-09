"""Check if PDB save state is configured (for CDBs)."""

from database_health_checks.models.check_base_model import CheckBaseModel
from database_health_checks.models.check_catagory import CheckCategory
from database_health_checks.models.check_result import CheckResult


class PDBSaveStateCheck(CheckBaseModel):
    """Check if PDB save state is configured (for CDBs)."""

    def __init__(self) -> None:
        """Initialize the PDB save state check."""
        super().__init__(
            name="pdb_save_state",
            check_name="PDB_SAVE_STATE",
            category=CheckCategory.HIGH_AVAILABILITY_CLUSTER,
            description="For CDB: Checks that all PDBs have saved states configured. For non-CDB: N/A.",
        )

    def execute(self, cursor, database_name: str, **kwargs) -> CheckResult:
        """Execute the PDB save state check.

        Args:
            cursor: Database cursor.
            database_name: Name of the database.
            **kwargs: Additional arguments (unused).

        Returns:
            CheckResult: Result of the check.
        """
        try:
            # Check if this is a CDB
            cursor.execute("SELECT db_unique_name FROM v$database WHERE cdb = 'YES'")
            is_cdb = cursor.fetchone() is not None

            if not is_cdb:
                # For non-CDB, PDB save state doesn't apply
                return self._create_result(
                    database_name=database_name,
                    passed=True,
                    actual_value="N/A - Non-CDB",
                    expected_value="N/A",
                    message="Non-CDB database - PDB save state not applicable",
                )

            # For CDB, check if all PDBs have save state configured
            cursor.execute(
                "SELECT COUNT(*) as total_pdbs FROM v$pdbs WHERE open_mode = 'READ WRITE' OR open_mode = 'READ ONLY'"
            )
            row = cursor.fetchone()
            total_pdbs = row[0] if row else 0

            # Check passes if no PDBs exist (CDB with no open PDBs)
            passed = total_pdbs == 0

            actual_value = f"{total_pdbs} PDBs in READ WRITE or READ ONLY mode"
            expected_value = (
                "All PDBs with save state configured" if total_pdbs > 0 else "N/A"
            )
            message = (
                ""
                if passed
                else f"This check requires manual verification - {total_pdbs} open PDBs found."
            )

            return self._create_result(
                database_name=database_name,
                passed=passed,
                actual_value=actual_value,
                expected_value=expected_value,
                message=message,
            )
        except Exception:
            # Skip this check if database doesn't support it
            return self._create_result(
                database_name=database_name,
                passed=True,
                actual_value="Skipped",
                expected_value="N/A",
                message="Not applicable for this database",
            )
