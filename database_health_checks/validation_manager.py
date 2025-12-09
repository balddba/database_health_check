"""Validation manager module."""

from pathlib import Path
from typing import Dict, Optional, Union

import yaml


class ValidationManager:
    """Manager for loading and accessing validation rules from YAML."""

    def __init__(self, yaml_path: Optional[str] = None) -> None:
        """Initialize the validation manager.

        Args:
             yaml_path: The path to the validation_rules.yaml file. If None, uses the default location.
        """
        if yaml_path is None:
            yaml_path = Path(__file__).parent / "validation_rules.yaml"
        else:
            yaml_path = Path(yaml_path)

        self.yaml_path = yaml_path
        self._data = self._load_yaml()

    def _load_yaml(self) -> Dict[str, Union[str, int, float, bool, dict, list]]:
        """Load and parse the validation rules YAML file.

        Returns:
            A dictionary with 'defaults' and 'overrides' keys.
        """
        if not self.yaml_path.exists():
            raise FileNotFoundError(
                f"Validation rules file not found: {self.yaml_path}"
            )

        try:
            with open(self.yaml_path, "r") as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {self.yaml_path}: {e}") from e

        if not data or "validation_rules" not in data:
            raise ValueError(
                f"Invalid validation rules file: missing 'validation_rules' key in {self.yaml_path}"
            )

        rules = data["validation_rules"]
        if "defaults" not in rules:
            raise ValueError(
                f"Invalid validation rules: missing 'defaults' section in {self.yaml_path}"
            )

        return {
            "defaults": rules.get("defaults", {}),
            "overrides": rules.get("overrides", {}),
        }

    def get_rules(self, database_name: str) -> Dict[str, Union[str, int, float, bool, dict, list]]:
        """Get validation rules for a specific database.

        Uses defaults with database-specific overrides applied.

        Args:
            database_name: The name of the database (from config).

        Returns:
            A dictionary of validation rules for the database.
        """
        # Start with defaults
        rules = dict(self._data["defaults"])

        # Apply database-specific overrides if they exist
        if database_name in self._data["overrides"]:
            rules.update(self._data["overrides"][database_name])

        return rules

    def get_overridden_keys(self, database_name: str) -> set:
        """Get the set of rule keys that are overridden for a specific database.

        Args:
            database_name: The name of the database (from config).

        Returns:
            A set of rule keys that have database-specific overrides.
        """
        if database_name in self._data["overrides"]:
            return set(self._data["overrides"][database_name].keys())
        return set()

    def get_default_rules(self) -> Dict[str, Union[str, int, float, bool, dict, list]]:
        """Get the default validation rules.

        Returns:
            A dictionary of default validation rules.
        """
        return dict(self._data["defaults"])

    def get_overrides(self) -> Dict[str, Dict[str, Union[str, int, float, bool, dict, list]]]:
        """Get all database-specific overrides.

        Returns:
            A dictionary of overrides (key = database name).
        """
        return dict(self._data["overrides"])

    def get_password_validation_config(self, database_name: str | None = None) -> list:
        """Get password validation function configurations.

        Args:
            database_name: The name of the database. If None, uses defaults.

        Returns:
            A list of dictionaries with 'validation_function' and 'profiles' keys.
        """
        rules = (
            self.get_rules(database_name) if database_name else self._data["defaults"]
        )
        pwd_config = rules.get("password_validation", [])

        if not pwd_config:
            return []

        # Ensure it's a list of dicts
        if isinstance(pwd_config, dict):
            # Old format for backwards compatibility
            return [
                {
                    "validation_function": pwd_config.get("validation_function", ""),
                    "profiles": pwd_config.get("profiles", []),
                }
            ]

        return pwd_config if isinstance(pwd_config, list) else []
