"""
SQLite Database Backend

Implements DatabaseBackend for SQLite databases.
"""

import sqlite3
from typing import Any, Dict, List

import pandas as pd

from .base import DatabaseBackend, UpsertResult


class SQLiteBackend(DatabaseBackend):
    """
    SQLite database implementation.

    Provides database operations for SQLite databases.
    """

    def __init__(self, database: str):
        """
        Initialize SQLite backend.

        Args:
            database: Path to SQLite database file
        """
        self.database = database
        self.conn = None

    def connect(self) -> None:
        """Establish SQLite connection."""
        self.conn = sqlite3.connect(self.database)
        self.conn.row_factory = sqlite3.Row

    def disconnect(self) -> None:
        """Close SQLite connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def table_exists(self, table: str) -> bool:
        """Check if a table exists."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (table,)
        )
        return cursor.fetchone() is not None

    def get_table_schema(self, table: str) -> List[Dict[str, Any]]:
        """Get table schema."""
        cursor = self.conn.cursor()
        cursor.execute(f"PRAGMA table_info('{table}')")
        rows = cursor.fetchall()

        schema = []
        for row in rows:
            schema.append({
                "name": row[1],
                "type": row[2],
                "not_null": row[3] == 1,
                "default_value": row[4],
                "primary_key": row[5] == 1
            })

        return schema

    def bulk_upsert(
        self,
        table: str,
        data: pd.DataFrame,
        key_columns: List[str],
        copy_mode: str = "insert_update"
    ) -> UpsertResult:
        """Perform bulk upsert operation."""
        result = UpsertResult()

        cursor = self.conn.cursor()

        # Get table schema
        schema = self.get_table_schema(table)
        table_columns = [col["name"] for col in schema]

        # Ensure data has only columns that exist in table
        data_columns = [col for col in data.columns if col in table_columns]
        df = data[data_columns].copy()

        if copy_mode == "replace":
            # Delete all existing rows
            cursor.execute(f"DELETE FROM {table}")
            # Insert all new rows
            df.to_sql(table, self.conn, if_exists="append", index=False)
            result.inserted = len(df)

        elif copy_mode == "insert_only":
            # Try to insert each row
            for _, row in df.iterrows():
                try:
                    row.to_sql(table, self.conn, if_exists="append", index=False)
                    result.inserted += 1
                except sqlite3.IntegrityError:
                    # Key exists - error for insert_only
                    raise

        elif copy_mode == "append":
            # Insert, skip duplicates
            for _, row in df.iterrows():
                try:
                    row.to_sql(table, self.conn, if_exists="append", index=False)
                    result.inserted += 1
                except sqlite3.IntegrityError:
                    # Key exists - skip for append
                    result.skipped += 1

        elif copy_mode == "update_only":
            # Update existing rows only
            for _, row in df.iterrows():
                # Check if row exists
                where_clause = " AND ".join([f'"{col}" = ?' for col in key_columns])
                where_values = [row[col] for col in key_columns]

                cursor.execute(
                    f"SELECT 1 FROM {table} WHERE {where_clause}",
                    where_values
                )

                if cursor.fetchone():
                    # Row exists - update it
                    set_clause = ", ".join([f'"{col}" = ?' for col in data_columns])
                    values = list(row[data_columns].values) + where_values

                    cursor.execute(
                        f"UPDATE {table} SET {set_clause} WHERE {where_clause}",
                        values
                    )
                    result.updated += 1
                else:
                    # Row doesn't exist - skip
                    result.skipped += 1

        elif copy_mode == "insert_update":
            # Insert new rows, update existing rows
            for _, row in df.iterrows():
                # Check if row exists
                where_clause = " AND ".join([f'"{col}" = ?' for col in key_columns])
                where_values = [row[col] for col in key_columns]

                cursor.execute(
                    f"SELECT 1 FROM {table} WHERE {where_clause}",
                    where_values
                )

                if cursor.fetchone():
                    # Row exists - update it
                    set_clause = ", ".join([f'"{col}" = ?' for col in data_columns])
                    values = list(row[data_columns].values) + where_values

                    cursor.execute(
                        f"UPDATE {table} SET {set_clause} WHERE {where_clause}",
                        values
                    )
                    result.updated += 1
                else:
                    # Row doesn't exist - insert it
                    row.to_sql(table, self.conn, if_exists="append", index=False)
                    result.inserted += 1

        return result

    def execute(self, sql: str, params: List[Any] = None) -> None:
        """Execute a SQL statement."""
        cursor = self.conn.cursor()
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)

    def query(self, sql: str, params: List[Any] = None) -> pd.DataFrame:
        """Execute a SQL query and return results."""
        if params:
            result = pd.read_sql_query(sql, self.conn, params=params)
        else:
            result = pd.read_sql_query(sql, self.conn)
        return result

    def commit(self) -> None:
        """Commit transaction."""
        if self.conn:
            self.conn.commit()

    def rollback(self) -> None:
        """Rollback transaction."""
        if self.conn:
            self.conn.rollback()
