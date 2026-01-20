"""
Database Abstraction Layer

Provides unified interface for different database backends (SQLite, PostgreSQL, MySQL).
"""

from .base import DatabaseBackend, UpsertResult
from .sqlite import SQLiteBackend
from .factory import DatabaseFactory

__all__ = [
    "DatabaseBackend",
    "UpsertResult",
    "SQLiteBackend",
    "DatabaseFactory",
]
