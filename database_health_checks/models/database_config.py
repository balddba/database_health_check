"""Database configuration models."""

from enum import Enum

from pydantic import BaseModel, Field


class ConnectionMode(str, Enum):
    """Database connection modes."""

    SYSDBA = "sysdba"
    NORMAL = "normal"


class DatabaseConfig(BaseModel):
    """Single database connection configuration."""

    name: str = Field(..., description="Database identifier/alias")
    host: str = Field(..., description="Hostname or IP address")
    port: int = Field(1521, description="Oracle listener port")
    service: str = Field(..., description="Oracle service name")
    username: str = Field("sys", description="Database username")
    password: str = Field(..., description="Database password")
    mode: ConnectionMode = Field(
        ConnectionMode.SYSDBA, description="Connection mode (sysdba, normal)"
    )

    class Config:
        """Pydantic config."""

        extra = "ignore"
