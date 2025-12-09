"""Inventory manager for Oracle database connections.

Loads database connection information from a YAML file and provides
access to database configurations for health checks.
"""

import os
from typing import Any, Dict, List, Optional

import oracledb
import yaml
from pydantic import BaseModel, SecretStr


class OracleDatabase(BaseModel):
    """Represents an Oracle database connection configuration."""

    name: str
    hostname: str
    port: int
    service_name: str
    username: str
    password: SecretStr
    auth_mode: Optional[str] = "default"

    def dsn(self) -> str:
        """Generate Oracle DSN string.

        Returns:
            str: DSN in format (DESCRIPTION=...)
        """
        return f"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST={self.hostname})(PORT={self.port}))(CONNECT_DATA=(SERVICE_NAME={self.service_name})))"

    def get_auth_mode(self) -> Optional[int]:
        """Get oracledb authentication mode constant.

        Returns:
            int: oracledb authentication mode or None for default.
        """
        if not self.auth_mode or self.auth_mode.lower() == "default":
            return None

        mode_map = {
            "sysdba": oracledb.AUTH_MODE_SYSDBA,
            "sysoper": oracledb.AUTH_MODE_SYSOPER,
        }
        return mode_map.get(self.auth_mode.lower())


class Inventory:
    """Manages database inventory loaded from YAML configuration file."""

    def __init__(self, config_path: Optional[str] = None) -> None:
        """Initialize inventory from YAML file.

        Args:
            config_path (str, optional): Path to databases.example.yaml file.
                If None, uses default location in same directory.

        Raises:
            FileNotFoundError: If config file not found.
            ValueError: If config file is invalid.
        """
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "databases.example.yaml"
            )

        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Database inventory file not found: {config_path}")

        self.config_path = config_path
        self.databases: Dict[str, OracleDatabase] = {}

        self._load_from_yaml()

    def _load_from_yaml(self) -> None:
        """Load database configurations from YAML file.

        Raises:
            ValueError: If YAML is invalid or required fields are missing.
        """
        try:
            with open(self.config_path, "r") as f:
                config = yaml.safe_load(f)

            if not config or "databases" not in config:
                raise ValueError("Invalid inventory format: missing 'databases' section")

            databases_config = config.get("databases", {})
            if not databases_config:
                raise ValueError("No databases configured in inventory file")

            for db_name, db_config in databases_config.items():
                try:
                    # Resolve environment variables in password if needed
                    password = db_config.get("password", "")
                    if isinstance(password, str) and password.startswith("${") and password.endswith("}"):
                        env_var = password[2:-1]
                        password = os.environ.get(env_var, "")
                        if not password:
                            raise ValueError(
                                f"Environment variable {env_var} not set for database {db_name}"
                            )

                    db = OracleDatabase(
                        name=db_name,
                        hostname=db_config.get("hostname"),
                        port=db_config.get("port", 1521),
                        service_name=db_config.get("service_name"),
                        username=db_config.get("username"),
                        password=password,
                        auth_mode=db_config.get("auth_mode", "default"),
                    )
                    self.databases[db_name] = db
                except (KeyError, TypeError) as e:
                    raise ValueError(f"Invalid database configuration for {db_name}: {e}")

        except yaml.YAMLError as e:
            raise ValueError(f"Failed to parse inventory YAML: {e}")

    def get_database(self, name: str) -> Optional[OracleDatabase]:
        """Get a specific database by name.

        Args:
            name (str): Database name.

        Returns:
            OracleDatabase: Database configuration or None if not found.
        """
        return self.databases.get(name)

    def get_database_names(self) -> List[str]:
        """Get list of all configured database names.

        Returns:
            List[str]: Sorted list of database names.
        """
        return sorted(self.databases.keys())

    def get_all_databases(self) -> List[OracleDatabase]:
        """Get all configured databases.

        Returns:
            List[OracleDatabase]: List of all database configurations.
        """
        return list(self.databases.values())
