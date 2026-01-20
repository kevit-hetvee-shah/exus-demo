"""
Database Backend Abstract Base Class

Defines the interface that all database backends must implement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd


@dataclass
class UpsertResult:
    """
    Result of an upsert operation.

    Attributes:
        inserted: Number of rows inserted
        updated: Number of rows updated
        deleted: Number of rows deleted
        skipped: Number of rows skipped
    """
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    skipped: int = 0


class DatabaseBackend(ABC):
    """
    Abstract base class for database backends.

    All database implementations (SQLite, PostgreSQL, MySQL) must
    inherit from this class and implement all abstract methods.
    """

    @abstractmethod
    def connect(self) -> None:
        """
        Establish database connection.

        Should create a connection that can be used for subsequent operations.
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass

    @abstractmethod
    def table_exists(self, table: str) -> bool:
        """
        Check if a table exists.

        Args:
            table: Table name

        Returns:
            True if table exists, False otherwise
        """
        pass

    @abstractmethod
    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        """
        Get table schema.

        Args:
            table: Table name

        Returns:
            List of column definitions
        """
        pass

    @abstractmethod
    def bulk_upsert(
        self,
        table: str,
        data: pd.DataFrame,
        key_columns: List[str],
        copy_mode: str = "insert_update"
    ) -> UpsertResult:
        """
        Perform bulk upsert operation.

        Args:
            table: Table name
            data: DataFrame with data to upsert
            key_columns: Columns that define uniqueness
            copy_mode: How to handle existing data
                - insert_only: Insert only, fail if key exists
                - update_only: Update only, skip new rows
                - insert_update: Insert new, update existing (upsert)
                - replace: Delete all, then insert all
                - append: Insert, ignore duplicates

        Returns:
            UpsertResult with counts
        """
        pass

    @abstractmethod
    def execute(self, sql: str, params: List[Any] = None) -> None:
        """
        Execute a SQL statement.

        Args:
            sql: SQL statement
            params: Optional parameters for parameterized query
        """
        pass

    @abstractmethod
    def query(self, sql: str, params: List[Any] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results.

        Args:
            sql: SQL query
            params: Optional parameters for parameterized query

        Returns:
            DataFrame with query results
        """
        pass

    @abstractmethod
    def commit(self) -> None:
        """Commit transaction."""
        pass

    @abstractmethod
    def rollback(self) -> None:
        """Rollback transaction."""
        pass

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.disconnect()
