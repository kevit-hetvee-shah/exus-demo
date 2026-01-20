"""
Database Factory

Creates database backend instances based on configuration.
"""

from typing import Dict, Any

from .base import DatabaseBackend
from .sqlite import SQLiteBackend


class DatabaseFactory:
    """
    Factory for creating database backend instances.

    Supports creating database backends from configuration dictionaries.
    """

    @staticmethod
    def create(config: Dict[str, Any]) -> DatabaseBackend:
        """
        Create a database backend from configuration.

        Args:
            config: Configuration dictionary with 'type' key
                    Example: {'type': 'sqlite', 'database': 'data.db'}

        Returns:
            DatabaseBackend instance

        Raises:
            ValueError: If database type is unknown or config is invalid
        """
        db_type = config.get("type", "sqlite")
        database = config.get("database")

        if db_type == "sqlite":
            return SQLiteBackend(database=database)

        # TODO: Implement PostgreSQL and MySQL backends
        # elif db_type == "postgresql":
        #     from .postgresql import PostgreSQLBackend
        #     return PostgreSQLBackend(
        #         database=config.get("database"),
        #         user=config.get("user"),
        #         password=config.get("password"),
        #         host=config.get("host", "localhost"),
        #         port=config.get("port", 5432)
        #     )
        #
        # elif db_type == "mysql":
        #     from .mysql import MySQLBackend
        #     return MySQLBackend(
        #         database=config.get("database"),
        #         user=config.get("user"),
        #         password=config.get("password"),
        #         host=config.get("host", "localhost"),
        #         port=config.get("port", 3306)
        #     )

        raise ValueError(f"Unknown database type: {db_type}")

    @staticmethod
    def create_sqlite(database: str) -> SQLiteBackend:
        """
        Convenience method to create a SQLiteBackend instance.

        Args:
            database: Path to SQLite database file

        Returns:
            SQLiteBackend instance
        """
        return SQLiteBackend(database=database)
