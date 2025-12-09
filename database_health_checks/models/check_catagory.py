"""Check category definitions."""

from enum import Enum


class CheckCategory(str, Enum):
    """Categories for health checks."""

    MEMORY_CONFIGURATION = "Memory Configuration"
    FEATURE_CONFIGURATION = "Feature Configuration"
    DATABASE_OBJECTS = "Database Objects"
    SECURITY_AUDITING = "Security & Auditing"
    BACKUP_RECOVERY = "Backup & Recovery"
    PERFORMANCE_TUNING = "Performance & Tuning"
    STORAGE_DISK_MANAGEMENT = "Storage & Disk Management"
    HIGH_AVAILABILITY_CLUSTER = "High Availability & Cluster"
    LICENSING_OPTIONS = "Licensing & Options"
    LOGGING_MONITORING = "Logging & Monitoring"
