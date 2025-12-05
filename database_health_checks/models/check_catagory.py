"""Check category definitions."""

from enum import Enum


class CheckCategory(str, Enum):
    """Categories for health checks."""

    MEMORY_CONFIGURATION = "Memory Configuration"
    FEATURE_CONFIGURATION = "Feature Configuration"
    DATABASE_OBJECTS = "Database Objects"
