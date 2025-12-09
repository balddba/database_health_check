"""Registry of all available checks."""

import importlib
import inspect
import pkgutil
from pathlib import Path
from typing import Dict

from database_health_checks.checks import (
    archivelog_mode_check,
    audit_trail_purge_job_check,
    control_file_multiplexing_check,
    datafiles_asm_check,
    db_files_min_check,
    dism_enabled_check,
    flashback_enabled_check,
    force_logging_check,
    job_class_log_retention_check,
    job_queue_processes_min_check,
    management_pack_check,
    memory_target_required_check,
    open_cursors_min_check,
    open_dblinks_max_check,
    optimizer_mode_check,
    password_validation_function_check,
    pdb_save_state_check,
    pga_aggregate_limit_required_check,
    pga_aggregate_target_required_check,
    processes_min_check,
    recovery_logs_asm_check,
    redo_log_size_check,
    scheduler_jobs_status_check,
    scheduler_log_retention_check,
    sessions_min_check,
    sga_max_size_required_check,
    sga_target_min_gb_check,
    statistics_level_check,
    unified_auditing_enabled_check,
)

from .models.check_base_model import CheckBaseModel


class CheckRegistry:
    """Central registry of all available checks."""

    def __init__(self, use_dynamic_loading: bool = False):
        """Initialize the check registry with all available checks.

        Args:
            use_dynamic_loading: If True, dynamically load checks from the checks directory.
                                If False, use the hardcoded check list (default).
        """
        self._checks: Dict[str, CheckBaseModel] = {}

        if use_dynamic_loading:
            self._load_checks_dynamically()
        else:
            self._load_checks_hardcoded()

    def _load_checks_hardcoded(self) -> None:
        """Load the hardcoded list of checks (default behavior)."""
        self._checks = {
            # Memory Configuration Checks
            "sga_target_min_gb": sga_target_min_gb_check.create_check(),
            "sga_max_size_required": sga_max_size_required_check.create_check(),
            "dism_enabled": dism_enabled_check.create_check(),
            "pga_aggregate_target_required": pga_aggregate_target_required_check.create_check(),
            "pga_aggregate_limit_required": pga_aggregate_limit_required_check.create_check(),
            "memory_target_required": memory_target_required_check.create_check(),
            "optimizer_mode": optimizer_mode_check.create_check(),
            "processes_min": processes_min_check.create_check(),
            "db_files_min": db_files_min_check.create_check(),
            "statistics_level": statistics_level_check.create_check(),
            "sessions_min": sessions_min_check.create_check(),
            "open_cursors_min": open_cursors_min_check.create_check(),
            # Feature Configuration Checks
            "archivelog_mode_enabled": archivelog_mode_check.create_check(),
            "flashback_enabled": flashback_enabled_check.create_check(),
            "force_logging_enabled": force_logging_check.create_check(),
            "management_pack": management_pack_check.ManagementPackCheck(),
            "unified_auditing_enabled": unified_auditing_enabled_check.create_check(),
            # Database Objects Checks
            "open_dblinks_max": open_dblinks_max_check.create_check(),
            "job_queue_processes_min": job_queue_processes_min_check.create_check(),
            "scheduler_jobs_status": scheduler_jobs_status_check.SchedulerJobsStatusCheck(),
            "scheduler_log_retention_days": scheduler_log_retention_check.SchedulerLogRetentionCheck(),
            "job_class_log_retention_days": job_class_log_retention_check.JobClassLogRetentionCheck(),
            # Complex Checks (custom logic)
            "pdb_save_state": pdb_save_state_check.PDBSaveStateCheck(),
            "audit_trail_purge_job": audit_trail_purge_job_check.AuditTrailPurgeJobCheck(),
            "password_validation_function": password_validation_function_check.PasswordValidationFunctionCheck(),
            "control_file_multiplexing": control_file_multiplexing_check.ControlFileMultiplexingCheck(),
            "datafiles_asm": datafiles_asm_check.DatafilesASMCheck(),
            "recovery_logs_asm": recovery_logs_asm_check.RecoveryLogsASMCheck(),
            "redo_log_size": redo_log_size_check.RedoLogSizeCheck(),
        }

    def _load_checks_dynamically(self) -> None:
        """Dynamically discover and load all checks from the checks directory."""
        checks_path = Path(__file__).parent / "checks"

        # Iterate through all modules in the checks package
        for _, module_name, _ in pkgutil.iter_modules([str(checks_path)]):
            if module_name.startswith("_") or module_name == "validation_check":
                # Skip private modules and the base validation_check module
                continue

            try:
                # Dynamically import the module
                module = importlib.import_module(
                    f".checks.{module_name}", package="database_health_checks"
                )

                # Try to instantiate checks from the module
                for name, obj in inspect.getmembers(module):
                    # Look for classes that inherit from CheckBaseModel
                    if (
                        inspect.isclass(obj)
                        and issubclass(obj, CheckBaseModel)
                        and obj is not CheckBaseModel
                        and not name.startswith("_")
                    ):
                        try:
                            # Try to instantiate the class
                            check_instance = obj()
                            # Use the check's name attribute as the key
                            if hasattr(check_instance, "name"):
                                self._checks[check_instance.name] = check_instance
                        except Exception:
                            # Skip classes that can't be instantiated without arguments
                            pass

                    # Also look for create_check functions
                    elif (
                        callable(obj)
                        and name == "create_check"
                        and not inspect.isclass(obj)
                    ):
                        try:
                            check_instance = obj()
                            if isinstance(check_instance, CheckBaseModel) and hasattr(
                                check_instance, "name"
                            ):
                                self._checks[check_instance.name] = check_instance
                        except Exception:
                            # Skip functions that fail to execute
                            pass
            except Exception:
                # Skip modules that fail to import
                pass

    def get_check(self, check_name: str) -> CheckBaseModel:
        """Get a check by name.

        Args:
            check_name: The name of the check (e.g., "sga_target_min_gb").

        Returns:
            CheckBaseModel: The check instance.

        Raises:
            KeyError: If the check is not found.
        """
        if check_name not in self._checks:
            raise KeyError(f"Check '{check_name}' not found in registry")
        return self._checks[check_name]

    def get_all_checks(self) -> Dict[str, CheckBaseModel]:
        """Get all registered checks.

        Returns:
            A dictionary of all checks.
        """
        return self._checks.copy()


# Global registry instance for backward compatibility
check_reg = CheckRegistry(use_dynamic_loading=True)
