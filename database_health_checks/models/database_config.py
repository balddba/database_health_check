"""Database configuration models."""

from enum import Enum

from pydantic import BaseModel, Field


class ConnectionMode(str, Enum):
    """Database connection modes."""

    SYSDBA = "sysdba"
    NORMAL = "normal"


class DatabaseConfig(BaseModel):
    """Single database connection configuration."""

    name: str = Field(..., description="The database identifier/alias")
    host: str = Field(..., description="The hostname or IP address")
    port: int = Field(1521, description="The Oracle listener port")
    service: str = Field(..., description="The Oracle service name")
    username: str = Field("sys", description="The database username")
    password: str = Field(..., description="The database password")
    mode: ConnectionMode = Field(
        ConnectionMode.SYSDBA, description="The connection mode (sysdba, normal)"
    )

    class Config:
        """Pydantic config."""

        extra = "ignore"
