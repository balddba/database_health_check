"""Health check configuration models."""

from typing import List

from pydantic import BaseModel

from .database_config import DatabaseConfig


class HealthCheckConfig(BaseModel):
    """Top-level config model matching health_check.yaml."""

    databases: List[DatabaseConfig]

    class Config:
        """Pydantic config."""

        extra = "ignore"
