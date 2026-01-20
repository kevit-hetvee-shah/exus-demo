"""
DuckDB Operations Module

Provides DuckDB connection management and data processing operations.
All data processing (read, transform, aggregate, write) uses DuckDB.
"""

from .engine import DuckDBEngine

__all__ = ["DuckDBEngine"]
