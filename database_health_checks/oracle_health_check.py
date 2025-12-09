#!/usr/bin/env python3
"""Oracle Health Check Manager.

Validates Oracle database configuration and runtime parameters against a baseline.
"""

import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import oracledb
from jinja2 import Environment, FileSystemLoader
from loguru import logger
from tabulate import tabulate

# Connection pool configuration
oracledb.defaults.config_dir = None

# Add tools directory to path for importing libs
_script_dir = os.path.dirname(os.path.abspath(__file__))
_tools_dir = os.path.dirname(_script_dir)
if _tools_dir not in sys.path:
    sys.path.insert(0, _tools_dir)

from database_health_checks.check_registry import check_reg  # noqa: E402
from database_health_checks.inventory import Inventory  # noqa: E402
from database_health_checks.models.check_result import CheckResult  # noqa: E402
from database_health_checks.validation_manager import ValidationManager  # noqa: E402


# -----------------------------------------------------------------------
# Colors
# -----------------------------------------------------------------------
class Colors:
    """ANSI color codes for terminal output."""

    GREEN = "\033[32m"
    RED = "\033[31m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    RESET = "\033[0m"


# -----------------------------------------------------------------------
# Health Check Class
# -----------------------------------------------------------------------
class OracleHealthCheck:
    """Manager for validating Oracle database health and configuration."""

    def __init__(
        self, validation_rules_path: Optional[str] = None, debug: bool = False
    ) -> None:
        """Initialize the Oracle health check manager.

        Args:
            validation_rules_path (str): Path to validation_rules.yaml configuration file.
            debug (bool): Enable debug logging.
        """
        # Default to validation_rules.yaml in the database_health_checks directory
        if validation_rules_path is None:
            default_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "validation_rules.yaml"
            )
            self.validation_rules_path = default_path
        else:
            self.validation_rules_path = validation_rules_path
        self.debug = debug
        self.inventory: Optional[Inventory] = None
        self.validation_manager: Optional[ValidationManager] = None
        self.results: List[CheckResult] = []
        self.connection_pools: Dict[str, oracledb.ConnectionPool] = {}

        # Setup logging
        logger.remove()
        if self.debug:
            logger.add(sys.stderr, level="DEBUG")
        else:
            logger.add(sys.stderr, level="INFO")

        if self.debug:
            logger.debug(f"[INIT] OracleHealthCheck initialized: debug={debug}")

        # Always load inventory from default location
        try:
            self.inventory = Inventory()
            if self.debug:
                logger.debug(
                    f"[INIT] Database inventory loaded from databases.example.yaml, databases count: {len(self.inventory.get_all_databases())}"
                )
        except Exception as e:
            logger.error(f"[INIT] Failed to load inventory: {e}")

        # Load validation rules
        self.validation_manager = self._load_validation_rules(
            self.validation_rules_path
        )
        if self.validation_manager and self.debug:
            logger.debug("[INIT] Validation rules loaded")

    def __del__(self) -> None:
        """Clean up connection pools when the object is destroyed."""
        self.close_all_pools()

    # Configuration Loading
    # =====================================================================
    def _create_default_validation_rules(self, path: str) -> bool:
        """Create a default validation_rules.yaml file if it doesn't exist.

        Args:
            path (str): File path to validation_rules.yaml configuration file.

        Returns:
            bool: True if file was created, False if already existed.
        """
        if os.path.exists(path):
            return False

        default_content = """# Validation Rules for Oracle Health Checks
# Defines default thresholds and allows per-database overrides

validation_rules:
  defaults:
    # Memory Configuration Checks
    sga_target_min_gb: 8                              # Minimum SGA_TARGET in GB
    sga_max_size_required: true                       # SGA_MAX_SIZE must be set
    dism_enabled: true                                # SGA_TARGET should be less than SGA_MAX_SIZE
    pga_aggregate_target_required: true               # PGA_AGGREGATE_TARGET must be set
    pga_aggregate_limit_required: true                # PGA_AGGREGATE_LIMIT must be set
    memory_target_required: false                     # MEMORY_TARGET - optional in modern Oracle

    # Feature Configuration Checks
    flashback_enabled: True                           # Flashback database should be enabled
    unified_auditing_enabled: True                    # Unified Auditing should be enabled

    # Database Objects Checks
    open_dblinks_max: 32                              # Minimum number of open database links
    jobs_enabled_min: 1                               # Minimum number of scheduler jobs
    job_queue_processes_min: 50                       # Minimum JOB_QUEUE_PROCESSES setting
    scheduler_log_retention_days: 30                  # Minimum scheduler log retention
    job_class_log_retention_days: 1                   # Minimum job class log retention

    # Database-specific overrides (key = database name from health_check.yaml)
    overrides:
    # Example: Override for FREE database (Oracle Free Tier)
    FREE:
      sga_target_min_gb: 1                            # FREE tier has limited memory
      memory_target_required: false                   # Not needed for FREE
      flashback_enabled: false                        # Not available in FREE tier
      unified_auditing_enabled: false                 # Not required for FREE
      job_queue_processes_min: 4                      # Lower threshold for FREE
      jobs_enabled_min: 0                             # Not critical for FREE
      scheduler_log_retention_days: 7                 # Relaxed for FREE
      job_class_log_retention_days: 0                 # Optional for FREE
"""

        try:
            with open(path, "w") as f:
                f.write(default_content)
            if self.debug:
                logger.debug(
                    f"[CONFIG] Created default validation rules file at {path}"
                )
            return True
        except Exception as e:
            logger.warning(
                f"[CONFIG] Failed to create validation rules file at {path}: {e}"
            )
            return False

    def _load_validation_rules(self, path: str) -> Optional[ValidationManager]:
        """Load validation rules from YAML file.

        Args:
            path (str): File path to validation_rules.yaml configuration file.

        Returns:
            ValidationManager: Validation rules manager, or None if file not found.
        """
        if self.debug:
            logger.debug(f"[CONFIG] Loading validation rules from {path}")

        # Try to create default file if it doesn't exist
        created = self._create_default_validation_rules(path)

        if not os.path.exists(path):
            if self.debug:
                logger.debug(
                    f"[CONFIG] Validation rules file not found at {path}, skipping validation rules"
                )
            return None

        try:
            manager = ValidationManager(path)
        except (FileNotFoundError, ValueError) as e:
            logger.warning(f"[CONFIG] Failed to load validation rules from {path}: {e}")
            return None

        if self.debug:
            logger.debug(
                "[CONFIG] Successfully loaded validation rules"
                + (" (newly created)" if created else "")
            )

        return manager

    # Connection Management
    # =====================================================================
    def _create_connection_pool(self, db) -> oracledb.ConnectionPool:
        """Create a connection pool for a database.

        Args:
            db: An OracleDatabase instance from the inventory.

        Returns:
            oracledb.ConnectionPool: A connection pool instance.
        """
        pool_key = db.name

        if pool_key in self.connection_pools:
            return self.connection_pools[pool_key]

        try:
            if self.debug:
                logger.debug(
                    f"[POOL] Creating connection pool for {db.name} ({db.hostname}:{db.port}/{db.service_name})"
                )

            # Determine auth mode
            auth_mode = db.get_auth_mode() or oracledb.AUTH_MODE_DEFAULT

            pool = oracledb.create_pool(
                user=db.username,
                password=db.password.get_secret_value(),
                dsn=db.dsn(),
                mode=auth_mode,
                min=2,
                max=10,
                increment=1,
            )

            self.connection_pools[pool_key] = pool

            if self.debug:
                logger.debug(f"[POOL] Connection pool created for {db.name}")

            return pool
        except Exception as e:
            logger.error(f"[POOL] Failed to create connection pool for {db.name}: {e}")
            raise

    def _get_connection(self, db) -> oracledb.Connection:
        """Get a connection from the pool.

        Args:
            db: An OracleDatabase instance from the inventory.

        Returns:
            oracledb.Connection: A database connection from the pool.
        """
        try:
            pool = self._create_connection_pool(db)
            conn = pool.acquire()

            # if self.debug:
            #     logger.debug(f"[CONNECT] Acquired connection from pool for {db.name}")
            #
            return conn
        except Exception as e:
            logger.error(f"[CONNECT] Failed to get connection for {db.name}: {e}")
            raise

    def _release_connection(self, conn: oracledb.Connection, db_name: str) -> None:
        """Release a connection back to the pool.

        Args:
            conn: The database connection to release.
            db_name: Name of the database for logging.
        """
        try:
            if conn:
                conn.close()
                # if self.debug:
                #     logger.debug(f"[CONNECT] Released connection back to pool for {db_name}")
        except Exception as e:
            logger.warning(f"[CONNECT] Error releasing connection for {db_name}: {e}")

    def close_all_pools(self) -> None:
        """Close all connection pools."""
        for pool_key, pool in self.connection_pools.items():
            try:
                pool.close()
                if self.debug:
                    logger.debug(f"[POOL] Closed connection pool for {pool_key}")
            except Exception as e:
                logger.warning(f"[POOL] Error closing pool for {pool_key}: {e}")

    # Health Check Methods
    # =====================================================================
    def run_all_checks(
        self, databases: Optional[List[str]] = None
    ) -> List[CheckResult]:
        """Run all health checks against configured databases.

        Args:
            databases (List[str], optional): Filter by specific database names. If None, check all.

        Returns:
            List[CheckResult]: Results of all checks performed.
        """
        if not self.inventory:
            logger.error("No database inventory loaded.")
            return []

        self.results = []
        db_names = self.inventory.get_database_names()

        # Filter databases if specified
        if databases:
            db_names = [name for name in db_names if name in databases]
            if self.debug:
                logger.debug(
                    f"[CHECK] Filtered to {len(db_names)} database(s) matching: {databases}"
                )
            if not db_names:
                logger.warning(f"No databases found matching: {databases}")
                return []

        for db_name in db_names:
            db = self.inventory.get_database(db_name)
            if db:
                conn = None
                try:
                    conn = self._get_connection(db)

                    if self.debug:
                        logger.debug(f"[CHECK] Running all checks for {db.name}")

                    # Execute all checks
                    self._execute_checks(conn, db)

                except Exception as e:
                    logger.error(f"[CHECK] Error running checks for {db.name}: {e}")
                finally:
                    # Release connection back to pool
                    if conn:
                        self._release_connection(conn, db.name)

        return self.results

    def _execute_checks(self, conn: oracledb.Connection, db) -> None:
        """Execute all configured checks for a database.

        Runs validation-based checks from validation_rules.yaml against the registry.

        Args:
            conn: Database connection.
            db: An OracleDatabase instance from the inventory.
        """
        if not self.validation_manager:
            if self.debug:
                logger.debug(
                    f"[CHECK] No validation manager for {db.name}, skipping checks"
                )
            return

        try:
            # Get rules for this database (defaults + overrides)
            rules = self.validation_manager.get_rules(db.name)
            overridden_keys = self.validation_manager.get_overridden_keys(db.name)
            cur = conn.cursor()

            # Execute each check from the registry
            for rule_name, rule_value in rules.items():
                # Skip if rule is disabled (None or False for required rules)
                if rule_value is None:
                    continue

                try:
                    # Get the check from registry
                    check = check_reg.get_check(rule_name)

                    # Get transformation function if needed for this check
                    transformer = self._get_value_transformer(rule_name)

                    # Execute the check with rule-based parameters
                    result = check.execute(
                        cursor=cur,
                        database_name=db.name,
                        rule_value=rule_value,
                        transform=transformer,
                    )

                    # Mark if this result used an override value
                    result.is_override = rule_name in overridden_keys

                    self.results.append(result)

                    if self.debug:
                        override_marker = " (OVERRIDE)" if result.is_override else ""
                        logger.debug(
                            f"[CHECK] {rule_name} for {db.name}: {result.passed}{override_marker}"
                        )

                except KeyError:
                    # Check not in registry, skip
                    if self.debug:
                        logger.debug(
                            f"[CHECK] Check '{rule_name}' not in registry, skipping"
                        )
                except Exception as e:
                    logger.warning(
                        f"[CHECK] Error executing check '{rule_name}' for {db.name}: {e}"
                    )

            # Execute password validation function check if configured
            self._execute_password_validation_check(cur, db.name)

        except Exception as e:
            logger.warning(f"[CHECK] Error running checks for {db.name}: {e}")

    def _fetch_hostnames_from_db(self, db) -> List[str]:
        """Fetch hostnames from gv$instance table.

        For RAC databases, returns all hostnames in the cluster.
        For single instance, returns the single hostname.

        Args:
            db: An OracleDatabase instance from the inventory.

        Returns:
            List[str]: List of hostnames, empty list if query fails.
        """
        hostnames = []
        conn = None
        try:
            conn = self._get_connection(db)
            cur = conn.cursor()
            
            # Try gv$instance first (works for RAC and single instance)
            try:
                cur.execute("SELECT DISTINCT host_name FROM gv$instance ORDER BY host_name")
                rows = cur.fetchall()
                if rows:
                    hostnames = [str(row[0]) for row in rows]
            except Exception:
                # Fall back to v$instance for single instance
                cur.execute("SELECT host_name FROM v$instance")
                row = cur.fetchone()
                if row:
                    hostnames = [str(row[0])]
            
            cur.close()
        except Exception as e:
            if self.debug:
                logger.debug(f"[HOSTNAME] Could not retrieve hostnames for {db.name}: {e}")
        finally:
            # Release connection back to pool
            if conn:
                self._release_connection(conn, db.name)
        
        return hostnames

    def _fetch_instance_names(self, db) -> List[str]:
        """Fetch instance names from gv$instance table.

        For RAC databases, returns all instance names in the cluster.
        For single instance, returns the single instance name.

        Args:
            db: An OracleDatabase instance from the inventory.

        Returns:
            List[str]: List of instance names, empty list if query fails.
        """
        instance_names = []
        conn = None
        try:
            conn = self._get_connection(db)
            cur = conn.cursor()
            
            # Try gv$instance first (works for RAC and single instance)
            try:
                cur.execute("SELECT DISTINCT instance_name FROM gv$instance ORDER BY instance_name")
                rows = cur.fetchall()
                if rows:
                    instance_names = [str(row[0]) for row in rows]
            except Exception:
                # Fall back to v$instance for single instance
                cur.execute("SELECT instance_name FROM v$instance")
                row = cur.fetchone()
                if row:
                    instance_names = [str(row[0])]
            
            cur.close()
        except Exception as e:
            if self.debug:
                logger.debug(f"[INSTANCE] Could not retrieve instance names for {db.name}: {e}")
        finally:
            # Release connection back to pool
            if conn:
                self._release_connection(conn, db.name)
        
        return instance_names

    def _get_value_transformer(self, rule_name: str) -> Optional[Any]:
        """Get optional value transformer function for a rule.

        Some rules need value transformation (e.g., bytes to GB).

        Args:
            rule_name: Name of the validation rule.

        Returns:
            Callable or None: Transformation function if needed.
        """
        transformers = {
            "sga_target_min_gb": lambda x: int(x) / (1024**3),  # bytes -> GB
        }
        return transformers.get(rule_name)

    def _execute_password_validation_check(
        self, cur: oracledb.Cursor, db_name: str
    ) -> None:
        """Execute password validation function check if configured.

        Args:
            cur: Database cursor.
            db_name: Name of the database.
        """
        if not self.validation_manager:
            return

        try:
            pwd_configs = self.validation_manager.get_password_validation_config(
                db_name
            )

            # Only execute if configured
            if not pwd_configs:
                if self.debug:
                    logger.debug(
                        "[CHECK] No password validation configuration found, skipping"
                    )
                return

            try:
                check = check_reg.get_check("password_validation_function")

                # Execute check for each validation function configuration
                for pwd_config in pwd_configs:
                    validation_function = pwd_config.get("validation_function", "")
                    profiles = pwd_config.get("profiles", [])

                    if not validation_function or not profiles:
                        continue

                    result = check.execute(
                        cursor=cur,
                        database_name=db_name,
                        validation_function=validation_function,
                        profiles=profiles,
                    )

                    self.results.append(result)

                    if self.debug:
                        logger.debug(
                            f"[CHECK] password_validation_function for {db_name} ({validation_function}): {result.passed}"
                        )

            except KeyError:
                if self.debug:
                    logger.debug(
                        "[CHECK] password_validation_function check not in registry"
                    )
            except Exception as e:
                logger.warning(
                    f"[CHECK] Error executing password_validation_function check for {db_name}: {e}"
                )

        except Exception as e:
            logger.warning(f"[CHECK] Error in password validation check setup: {e}")

    def list_checks(self) -> List[Dict[str, str]]:
        """List all available checks with their descriptions.

        Returns:
            List[Dict]: List of checks with name, display name, category, and description.
        """
        checks = check_reg.get_all_checks()
        result = []
        for check_name, check_obj in sorted(checks.items()):
            result.append(
                {
                    "name": check_name,
                    "check_name": check_obj.check_name,
                    "category": check_obj.category.value,
                    "description": check_obj.description,
                }
            )
        return result

    def print_checks(self) -> None:
        """Print all available checks in tabular format."""
        checks = self.list_checks()

        if not checks:
            print("No checks available.")
            return

        print("\n" + "=" * 120)
        print("Available Health Checks")
        print("=" * 120)

        # Group checks by category
        categories = {}
        for check in checks:
            category = check["category"]
            if category not in categories:
                categories[category] = []
            categories[category].append(check)

        # Print checks grouped by category
        category_order = [
            "Memory Configuration",
            "Feature Configuration",
            "Database Objects",
        ]

        headers = ["Check ID", "Display Name", "Description"]

        for category in category_order:
            if category not in categories:
                continue

            print(f"\n{category}")
            print("-" * 120)

            table_data = []
            for check in categories[category]:
                table_data.append(
                    [
                        check["name"],
                        check["check_name"],
                        check["description"],
                    ]
                )

            print(
                tabulate(
                    table_data,
                    headers=headers,
                    tablefmt="pretty",
                    colalign=("left", "left", "left"),
                )
            )

        print("\n" + "=" * 120)

    def get_results(self) -> List[CheckResult]:
        """Get all check results.

        Returns:
            List[CheckResult]: List of all check results.
        """
        return self.results

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of check results.

        Returns:
            Dict: Summary with passed/failed/total counts per database.
        """
        summary: Dict[str, Dict[str, int]] = {}

        for result in self.results:
            db_name = result.database
            if db_name not in summary:
                summary[db_name] = {"passed": 0, "failed": 0, "total": 0}

            summary[db_name]["total"] += 1
            if result.passed:
                summary[db_name]["passed"] += 1
            else:
                summary[db_name]["failed"] += 1

        return summary

    def print_results(self) -> None:
        """Print check results in tabular format with color coding, grouped by category."""
        # Print header with time, hostname, and database name
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Get unique database names from results
        databases = sorted(set(result.database for result in self.results))
        db_names = ", ".join(databases) if databases else "None"

        # Get database host(s) from validation manager if available
        db_hosts = []
        if hasattr(self, "validation_manager") and hasattr(
            self.validation_manager, "databases"
        ):
            for db_name in databases:
                if db_name in self.validation_manager.databases:
                    db_info = self.validation_manager.databases[db_name]
                    if hasattr(db_info, "host"):
                        db_hosts.append(db_info.hostname)

        hostname = ", ".join(db_hosts) if db_hosts else "N/A"

        print(f"\n{'=' * 100}")
        print("Oracle Health Check Report")
        print(f"{'=' * 100}")
        print(f"Generated: {timestamp}")
        print(f"Database Host(s): {hostname}")
        print(f"Database(s): {db_names}")
        print(f"{'=' * 100}")

        # Group results by category
        categories = {}
        for result in self.results:
            if result.category not in categories:
                categories[result.category] = []
            categories[result.category].append(result)

        # Print table for each category
        category_order = [
            "Memory Configuration",
            "Feature Configuration",
            "Database Objects",
        ]

        headers = ["Database", "Check", "Status", "Actual", "Expected"]

        for category in category_order:
            if category not in categories:
                continue

            print(f"\n{'=' * 100}")
            print(f"{category}")
            print(f"{'=' * 100}")

            table_data = []
            for result in categories[category]:
                # Color code the status based on pass/fail
                if result.passed:
                    status = f"{Colors.GREEN}PASS{Colors.RESET}"
                else:
                    status = f"{Colors.RED}FAIL{Colors.RESET}"

                # Format actual and expected values for uniformity
                actual = str(result.actual_value) if result.actual_value else "N/A"
                expected = (
                    str(result.expected_value) if result.expected_value else "N/A"
                )

                # Add override indicator if applicable
                if result.is_override:
                    expected = f"{expected} {Colors.BLUE}(override){Colors.RESET}"

                table_data.append(
                    [
                        result.database,
                        result.check_name,
                        status,
                        actual,
                        expected,
                    ]
                )

            print(
                tabulate(
                    table_data,
                    headers=headers,
                    tablefmt="pretty",
                    colalign=("left", "left", "center", "left", "left"),
                )
            )

        # Print summary with color coding
        summary = self.get_summary()
        print("\n" + "=" * 100)
        print("SUMMARY")
        print("=" * 100)
        for db_name, counts in summary.items():
            passed_pct = (
                int((counts["passed"] / counts["total"] * 100))
                if counts["total"] > 0
                else 0
            )

            # Color code the summary line
            if counts["failed"] == 0:
                color = Colors.GREEN
                status_text = "HEALTHY"
            elif counts["failed"] <= counts["total"] // 2:
                color = Colors.YELLOW
                status_text = "DEGRADED"
            else:
                color = Colors.RED
                status_text = "UNHEALTHY"

            print(
                f"  {color}{db_name}: {counts['passed']}/{counts['total']} passed ({passed_pct}%) - {status_text}{Colors.RESET}"
            )

        # Print scheduler jobs with a retention period
        self._print_scheduler_jobs()

    def _fetch_scheduler_jobs(self, db_name: str) -> Dict[str, Any]:
        """Fetch scheduler job information from the database.

        This is the single source of truth for retrieving scheduler job data.
        Used by both printing and HTML report generation.

        Args:
            db_name (str): Database name to retrieve jobs for.

        Returns:
            Dict with jobs list and global retention info.
        """
        result = {"jobs": [], "global_retention": 0, "error": None}
        
        if not self.inventory:
            return result
        
        db = self.inventory.get_database(db_name)
        if not db:
            return result
        
        conn = None
        try:
            conn = self._get_connection(db)
            cur = conn.cursor()
            
            # Get all scheduler jobs
            cur.execute(
                """
                SELECT job_name, job_type, job_class, enabled, state, repeat_interval, owner
                FROM dba_scheduler_jobs
                ORDER BY owner, job_name
            """
            )
            jobs = cur.fetchall()
            
            # Get global log_history
            try:
                cur.execute(
                    "SELECT value FROM dba_scheduler_global_attribute WHERE attribute_name = 'log_history'"
                )
                log_row = cur.fetchone()
                result["global_retention"] = int(log_row[0]) if (log_row and log_row[0]) else 0
            except Exception:
                pass
            
            # Process jobs into a standard format
            for job in jobs:
                job_name = str(job[0]) if job[0] else "Unknown"
                job_type = str(job[1]) if job[1] else "Unknown"
                job_class = str(job[2]) if job[2] else "DEFAULT"
                enabled = str(job[3]) if job[3] else "FALSE"
                state = str(job[4]) if job[4] else "Unknown"
                schedule = str(job[5])[:50] if (job[5] and len(job) > 5) else "None"
                
                enabled_str = "YES" if enabled.upper() in ("TRUE", "Y", "1") else "NO"
                is_purge = any(x in job_name.upper() for x in ["PURGE", "AUDIT_TRAIL"])
                
                result["jobs"].append({
                    "name": job_name,
                    "type": job_type,
                    "class": job_class,
                    "enabled": enabled_str,
                    "state": state,
                    "schedule": schedule,
                    "is_purge": is_purge
                })
            
        except Exception as e:
            result["error"] = str(e)
            if self.debug:
                logger.debug(f"Error getting scheduler jobs for {db_name}: {e}")
        finally:
            # Release connection back to pool
            if conn:
                self._release_connection(conn, db_name)
        
        return result
    
    def _format_scheduler_jobs_for_print(self, db_name: str, jobs_info: Dict[str, Any]) -> str:
        """Format scheduler jobs data for console printing.

        Args:
            db_name (str): Database name being reported.
            jobs_info (Dict): Jobs data from _fetch_scheduler_jobs.

        Returns:
            str: Formatted table data for tabulate.
        """
        table_data = []
        
        for job in jobs_info["jobs"]:
            # Mark purge jobs with checkmark
            is_purge = "✓" if job["is_purge"] else " "
            
            # Determine retention status
            retention = jobs_info["global_retention"]
            if retention >= 30:
                retention_status = f"{Colors.GREEN}✓{Colors.RESET}"
            elif retention > 0:
                retention_status = f"{Colors.YELLOW}⚠{Colors.RESET}"
            else:
                retention_status = f"{Colors.RED}✗{Colors.RESET}"
            
            table_data.append([
                is_purge,
                job["name"],
                job["type"],
                job["class"],
                job["enabled"],
                job["state"],
                job["schedule"],
                f"{retention}d" if retention > 0 else "Not set",
                retention_status
            ])
        
        return table_data

    def write_results_to_html(
        self, output_path: Optional[str] = None, database_name: Optional[str] = None
    ) -> str:
        """Write health check results to an HTML file.

        Args:
            output_path (str, optional): Path to write the HTML report. If None, generates 
                timestamped file in database_health_checks/reports/.
            database_name (str, optional): If provided, only includes results for this database.

        Returns:
            str: Path to the generated HTML file.
        """
        if not self.results:
            logger.warning(
                "No results to write. Run checks first with run_all_checks()."
            )
            return ""

        try:
            # Filter results by database if specified
            results_to_report = self.results
            if database_name:
                results_to_report = [r for r in self.results if r.database == database_name]
                if not results_to_report:
                    logger.warning(f"No results found for database: {database_name}")
                    return ""
            
            # Determine output path
            if output_path is None:
                # Create reports directory if it doesn't exist
                reports_dir = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "reports"
                )
                os.makedirs(reports_dir, exist_ok=True)
                
                # Generate timestamped filename
                timestamp_filename = datetime.now().strftime("%Y%m%d_%H%M%S")
                if database_name:
                    output_path = os.path.join(reports_dir, f"oracle_health_check_{database_name}_{timestamp_filename}.html")
                else:
                    output_path = os.path.join(reports_dir, f"oracle_health_check_{timestamp_filename}.html")
            
            # Get summary and database info
            summary = {}
            hostname = "N/A"  # Default hostname
            for result in results_to_report:
                db_name = result.database
                if db_name not in summary:
                    summary[db_name] = {"passed": 0, "failed": 0, "total": 0}
                summary[db_name]["total"] += 1
                if result.passed:
                    summary[db_name]["passed"] += 1
                else:
                    summary[db_name]["failed"] += 1
            
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Get database hosts and connection info
            db_info = {}
            if self.inventory:
                for db_name in self.inventory.get_database_names():
                    db = self.inventory.get_database(db_name)
                    if db:
                        # Store detailed info for each database
                        db_info[db_name] = {
                            "hostname": getattr(db, "hostname", "N/A"),
                            "hostnames": [],  # Will be populated from gv$instance
                            "port": getattr(db, "port", "N/A"),
                            "service_name": getattr(db, "service_name", "N/A"),
                            "oracle_version": "N/A",  # Will be populated from results
                            "instance_names": []  # Will be populated from gv$instance
                        }
            
            # Try to get oracle version and instance names from results or connection
            for result in results_to_report:
                db_name = result.database
                if db_name in db_info and result.actual_value and "oracle" in str(result.check_name).lower():
                    # Some checks might contain version info
                    pass
                elif db_name in db_info:
                    # Try to query oracle version and instance names from connection if available
                    conn = None
                    try:
                        if self.inventory:
                            db = self.inventory.get_database(db_name)
                            if db:
                                conn = self._get_connection(db)
                                cur = conn.cursor()
                                
                                # Get oracle version
                                try:
                                    cur.execute("SELECT banner FROM v$version WHERE ROWNUM = 1")
                                    version_row = cur.fetchone()
                                    if version_row:
                                        db_info[db_name]["oracle_version"] = version_row[0]
                                except Exception as e:
                                    if self.debug:
                                        logger.debug(f"Could not retrieve Oracle version for {db_name}: {e}")
                                
                                # Get hostnames from gv$instance
                                try:
                                    cur.execute("SELECT DISTINCT host_name FROM gv$instance ORDER BY host_name")
                                    rows = cur.fetchall()
                                    if rows:
                                        db_info[db_name]["hostnames"] = [str(row[0]) for row in rows]
                                    else:
                                        # Fall back to v$instance for single instance
                                        cur.execute("SELECT host_name FROM v$instance")
                                        row = cur.fetchone()
                                        if row:
                                            db_info[db_name]["hostnames"] = [str(row[0])]
                                except Exception as e:
                                    if self.debug:
                                        logger.debug(f"Could not retrieve hostnames for {db_name}: {e}")
                                
                                # Get instance names
                                try:
                                    cur.execute("SELECT DISTINCT instance_name FROM gv$instance ORDER BY instance_name")
                                    rows = cur.fetchall()
                                    if rows:
                                        db_info[db_name]["instance_names"] = [str(row[0]) for row in rows]
                                    else:
                                        # Fall back to v$instance for single instance
                                        cur.execute("SELECT instance_name FROM v$instance")
                                        row = cur.fetchone()
                                        if row:
                                            db_info[db_name]["instance_names"] = [str(row[0])]
                                except Exception as e:
                                    if self.debug:
                                        logger.debug(f"Could not retrieve instance names for {db_name}: {e}")
                                
                                cur.close()
                    except Exception as e:
                        if self.debug:
                            logger.debug(f"Could not establish connection for {db_name}: {e}")
                    finally:
                        # Release connection back to pool
                        if conn:
                            self._release_connection(conn, db_name)
            
            # Update hostname from db_info if available
            if database_name and database_name in db_info and db_info[database_name].get("hostnames"):
                hostname = db_info[database_name]["hostnames"][0]
            
            # Get password validation and scheduler job info for the databases
            pwd_validation_results = {}
            scheduler_jobs_data = None
            if database_name:
                # Get password validation results for specific database
                try:
                    pwd_validation_results = self.get_profile_validation_results([database_name])
                    if self.debug:
                        logger.debug(f"[REPORT] Password validation results for {database_name}: {pwd_validation_results.keys()}")
                except Exception as e:
                    logger.warning(f"[REPORT] Error getting password validation results: {e}")
                
                # Get scheduler job info
                try:
                    scheduler_jobs_result = self._fetch_scheduler_jobs(database_name)
                    # Only include if no error (error key will be None if successful)
                    if not scheduler_jobs_result.get("error"):
                        scheduler_jobs_data = scheduler_jobs_result
                        if self.debug:
                            logger.debug(f"[REPORT] Scheduler jobs for {database_name}: {len(scheduler_jobs_data.get('jobs', []))} jobs, retention={scheduler_jobs_data.get('global_retention')}")
                    else:
                        logger.warning(f"[REPORT] Error fetching scheduler jobs for {database_name}: {scheduler_jobs_result.get('error')}")
                except Exception as e:
                    logger.warning(f"[REPORT] Error getting scheduler jobs: {e}")

            # Group results by category
            categories = {}
            for result in results_to_report:
                if result.category not in categories:
                    categories[result.category] = []
                categories[result.category].append(result)

            # Prepare template context
            template_context = {
                "timestamp": timestamp,
                "hostname": hostname,
                "results_count": len(results_to_report),
                "summary": summary,
                "database_name": database_name,
                "single_database": bool(database_name),
                "db_info": db_info.get(database_name, {}) if database_name else {},
                "overall_score": 0,
                "score_color": "#10b981",
                "categories": categories,
                "category_order": [
                     "Memory Configuration",
                     "Feature Configuration",
                     "Database Objects",
                 ],
                 "password_validation_results": pwd_validation_results.get(database_name) if database_name else None,
                 "scheduler_jobs": scheduler_jobs_data,
                 }
            
            # Calculate overall score for single database
            if database_name and database_name in summary:
                counts = summary[database_name]
                template_context["overall_score"] = int((counts["passed"] / counts["total"] * 100)) if counts["total"] > 0 else 0
                score = template_context["overall_score"]
                template_context["score_color"] = "#10b981" if score >= 80 else "#f59e0b" if score >= 50 else "#ef4444"
            
            # Load and render template
            template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
            env = Environment(loader=FileSystemLoader(template_dir))
            template = env.get_template("report_mui.html")
            html_content = template.render(**template_context)

            # Write to file
            with open(output_path, "w") as f:
                f.write(html_content)

            logger.info(f"HTML report written to {output_path}")
            print(f"\n✓ HTML report generated: {output_path}")
            
            return output_path

        except Exception as e:
            logger.error(f"Failed to write HTML report: {e}")
            raise

    def create_reports_index(self) -> None:
        """Create an index page that links to all generated reports using Jinja2 template."""
        try:
            reports_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "reports"
            )
            
            if not os.path.exists(reports_dir):
                logger.warning(f"Reports directory does not exist: {reports_dir}")
                return
            
            # Find all HTML reports
            reports = []
            for filename in sorted(os.listdir(reports_dir), reverse=True):
                if filename.endswith(".html") and filename.startswith("oracle_health_check_"):
                    if filename == "index.html":
                        continue
                        
                    filepath = os.path.join(reports_dir, filename)
                    # Extract timestamp and database name from filename
                    # Format: oracle_health_check_DBNAME_YYYYMMDD_HHMMSS.html or oracle_health_check_YYYYMMDD_HHMMSS.html
                    try:
                        # Use regex to extract: oracle_health_check_([^_]*)_?(20\d{6})_(\d{6})\.html or oracle_health_check_(20\d{6})_(\d{6})\.html
                        match = re.match(r'oracle_health_check_(.*)_(20\d{6})_(\d{6})\.html$', filename)
                        if match:
                            db_name = match.group(1)
                            date_part = match.group(2)
                            time_part = match.group(3)
                        else:
                            # Try without database name
                            match = re.match(r'oracle_health_check_(20\d{6})_(\d{6})\.html$', filename)
                            if match:
                                db_name = "All Databases"
                                date_part = match.group(1)
                                time_part = match.group(2)
                            else:
                                # Doesn't match expected format
                                if self.debug:
                                    logger.debug(f"Skipping file {filename}: doesn't match expected naming pattern")
                                continue
                        
                        # Format: 20250105 -> 2025-01-05, 091141 -> 09:11:41
                        formatted_date = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                        formatted_time = f"{time_part[:2]}:{time_part[2:4]}:{time_part[4:6]}"
                        file_stat = os.stat(filepath)
                        file_size = file_stat.st_size
                        file_size_kb = file_size / 1024
                        size_display = f"{file_size_kb:.1f} KB"
                        
                        reports.append({
                            "filename": filename,
                            "database": db_name,
                            "timestamp": f"{formatted_date} {formatted_time}",
                            "timestamp_key": f"{date_part}{time_part}",
                            "size": file_size,
                            "size_display": size_display,
                            "url": filename
                        })
                    except (ValueError, IndexError) as e:
                        # Skip files that don't match expected naming
                        if self.debug:
                            logger.debug(f"Skipping file {filename}: {e}")
                        continue
            
            if not reports:
                logger.warning("No reports found in reports directory")
                return
            
            # Group by database
            by_database = {}
            for report in reports:
                db = report["database"]
                if db not in by_database:
                    by_database[db] = []
                by_database[db].append(report)
            
            # Sort each database's reports by timestamp (newest first)
            for db in by_database:
                by_database[db].sort(key=lambda x: x["timestamp_key"], reverse=True)
            
            # Group by run timestamp
            by_run = {}
            for report in reports:
                ts_key = report["timestamp_key"]
                ts_display = report["timestamp"]
                if ts_key not in by_run:
                    by_run[ts_key] = {"timestamp": ts_display, "reports": []}
                by_run[ts_key]["reports"].append(report)
            
            # Sort runs by timestamp (newest first)
            sorted_runs = sorted(by_run.items(), key=lambda x: x[0], reverse=True)
            runs = {ts_key: run_info for ts_key, run_info in sorted_runs}
            
            # Setup Jinja2 environment and render template
            template_dir = os.path.dirname(os.path.abspath(__file__))
            jinja_env = Environment(loader=FileSystemLoader(os.path.join(template_dir, "templates")))
            template = jinja_env.get_template("index.html")
            
            html_content = template.render(
                reports=reports,
                databases=sorted(by_database.keys()),
                by_database=by_database,
                runs=runs,
            )
            
            # Write index file
            index_path = os.path.join(reports_dir, "index.html")
            with open(index_path, "w") as f:
                f.write(html_content)
            
            logger.info(f"Reports index created at {index_path}")
            
        except Exception as e:
            logger.error(f"Failed to create reports index: {e}")

    def _print_scheduler_jobs(self) -> None:
        """Print all scheduler jobs with the log retention period for validation."""
        if not self.inventory:
            return

        print("\n" + "=" * 100)
        print("SCHEDULER JOBS AND LOG RETENTION")
        print("=" * 100)

        for db_name in self.inventory.get_database_names():
            db = self.inventory.get_database(db_name)
            if not db:
                continue

            print(f"\n{db.name}")

            # Fetch jobs using common method
            jobs_info = self._fetch_scheduler_jobs(db_name)
            
            if jobs_info.get("error"):
                logger.error(f"[SCHEDULER] Error retrieving jobs for {db_name}: {jobs_info['error']}")
                print(f"Error retrieving scheduler jobs - {str(jobs_info['error'])[:80]}")
                continue

            if not jobs_info["jobs"]:
                print("No scheduler jobs found")
                continue

            # Format data for printing
            table_data = self._format_scheduler_jobs_for_print(db_name, jobs_info)
            
            if table_data:
                headers = [
                    "P",
                    "Job Name",
                    "Type",
                    "Class",
                    "Enabled",
                    "State",
                    "Schedule",
                    "Log Retention",
                    "Status",
                ]
                print(
                    tabulate(
                        table_data,
                        headers=headers,
                        tablefmt="pretty",
                        colalign=(
                            "center",
                            "left",
                            "left",
                            "left",
                            "center",
                            "left",
                            "left",
                            "center",
                            "center",
                        ),
                    )
                )
                global_retention = jobs_info["global_retention"]
                if global_retention > 0:
                    print(
                        f"Legend: P=Purge Job, Total={len(table_data)} jobs, Global Retention={global_retention} days"
                    )
                else:
                    print(f"Legend: P=Purge Job, Total={len(table_data)} jobs")
            else:
                print("No scheduler jobs available")

    # Profile Validation Methods
    # =====================================================================
    def _get_profiles_from_database(self, cursor) -> List[Dict[str, str]]:
        """Query database for all profiles and their password validation functions.

        Args:
            cursor: Database cursor.

        Returns:
            List of dicts with 'profile' and 'validation_function' keys.
        """
        try:
            cursor.execute(
                """
                SELECT profile,
                       CASE
                           WHEN resource_name = 'PASSWORD_VERIFY_FUNCTION' THEN limit
                           ELSE NULL
                       END as validation_function
                FROM dba_profiles
                WHERE resource_name = 'PASSWORD_VERIFY_FUNCTION'
                ORDER BY profile
            """
            )
            rows = cursor.fetchall()

            profiles = []
            for row in rows:
                profiles.append(
                    {
                        "profile": row[0],
                        "validation_function": row[1] if row[1] else "UNLIMITED",
                    }
                )
            return profiles
        except Exception as e:
            logger.warning(f"[PROFILES] Error querying profiles: {e}")
            return []

    def get_profile_validation_results(
        self, databases: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Get profile validation results for configured databases.

        Args:
            databases (List[str], optional): Filter by specific database names. If None, check all.

        Returns:
            Dict mapping database names to validation results.
        """
        if not self.inventory or not self.validation_manager:
            if self.debug:
                logger.debug("[PROFILES] Inventory or validation manager not loaded")
            return {}

        db_names = self.inventory.get_database_names()

        # Filter databases if specified
        if databases:
            db_names = [name for name in db_names if name in databases]

        results = {}

        for db_name in db_names:
            # Get password validation config for this specific database
            pwd_configs = self.validation_manager.get_password_validation_config(
                db_name
            )

            if not pwd_configs:
                if self.debug:
                    logger.debug(
                        f"[PROFILES] No password validation configuration for {db_name}"
                    )
                continue

            db = self.inventory.get_database(db_name)
            if not db:
                continue

            conn = None
            try:
                conn = self._get_connection(db)
                cur = conn.cursor()

                # Get all profiles
                profiles = self._get_profiles_from_database(cur)
                profile_dict = {
                    p["profile"]: p["validation_function"] for p in profiles
                }

                # Check each validation function configuration
                profile_results = []
                passed_count = 0
                total_count = 0

                for pwd_config in pwd_configs:
                    validation_function = pwd_config.get("validation_function", "")
                    required_profiles = pwd_config.get("profiles", [])

                    if not validation_function or not required_profiles:
                        continue

                    for profile_name in required_profiles:
                        total_count += 1
                        actual_function = profile_dict.get(profile_name, "NOT FOUND")

                        # Determine pass/fail
                        if actual_function == "NOT FOUND":
                            passed = False
                        elif validation_function.upper() in actual_function.upper():
                            passed = True
                            passed_count += 1
                        else:
                            passed = False

                        profile_results.append(
                            {
                                "profile": profile_name,
                                "required_function": validation_function,
                                "actual_function": actual_function,
                                "passed": passed,
                            }
                        )

                results[db_name] = {
                    "profiles": profile_results,
                    "passed_count": passed_count,
                    "total_count": total_count,
                    "host": f"{db.hostname}:{db.port}/{db.service_name}",
                }

                if self.debug:
                    logger.debug(
                        f"[PROFILES] {db_name}: {passed_count}/{total_count} profiles passed"
                    )

            except Exception as e:
                logger.warning(f"[PROFILES] Error processing {db_name}: {e}")
                results[db_name] = {
                    "error": str(e),
                    "host": (
                        f"{db.hostname}:{db.port}/{db.service}" if db else "Unknown"
                    ),
                }
            finally:
                # Release connection back to pool
                if conn:
                    self._release_connection(conn, db_name)

        return results

    def print_profile_validation_report(
        self, databases: Optional[List[str]] = None
    ) -> None:
        """Print profile validation report in tabular format.

        Args:
            databases (List[str], optional): Filter by specific database names. If None, check all.
        """
        results = self.get_profile_validation_results(databases)

        if not results:
            print("\nNo profile validation results available.")
            return

        print("\n" + "=" * 100)
        print("PASSWORD VALIDATION FUNCTION REPORT")
        print("=" * 100)

        for db_name, db_result in results.items():
            if "error" in db_result:
                print(f"\n{db_name} ({db_result.get('host', 'Unknown')})")
                print("-" * 100)
                print(f"Error: {db_result['error']}\n")
                continue

            print(f"\n{db_name} ({db_result.get('host', 'Unknown')})")
            print("-" * 100)

            # Prepare table data
            table_data = []
            for profile_info in db_result["profiles"]:
                status = (
                    f"{Colors.GREEN}PASS{Colors.RESET}"
                    if profile_info["passed"]
                    else f"{Colors.RED}FAIL{Colors.RESET}"
                )

                table_data.append(
                    [
                        profile_info["profile"],
                        profile_info["required_function"],
                        profile_info["actual_function"],
                        status,
                    ]
                )

            headers = ["Profile", "Required Function", "Actual Function", "Status"]
            print(
                tabulate(
                    table_data,
                    headers=headers,
                    tablefmt="pretty",
                    colalign=("left", "left", "left", "center"),
                )
            )

            # Print summary
            passed_count = db_result["passed_count"]
            total_count = db_result["total_count"]

            if passed_count == total_count:
                summary_color = Colors.GREEN
                summary_status = "PASS"
            else:
                summary_color = Colors.RED
                summary_status = "FAIL"

            print(
                f"\n{summary_color}Result: {summary_status} ({passed_count}/{total_count}){Colors.RESET}\n"
            )

        print("=" * 100)


if __name__ == "__main__":
    """Run health checks when executed as a script."""
    try:
        # Initialize health check manager (uses default inventory.yaml)
        health_check = OracleHealthCheck(debug=True)

        # List out the known system checks
        # health_check.print_checks()

        # Run all checks
        health_check.run_all_checks()

        # Print results
        health_check.print_results()

        # Print profile validation report
        health_check.print_profile_validation_report()
        
        # Generate individual reports for each database
        if health_check.results:
            databases = sorted(set(result.database for result in health_check.results))
            for db_name in databases:
                health_check.write_results_to_html(database_name=db_name)
        
        # Create index page linking all reports
        health_check.create_reports_index()

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        sys.exit(1)
