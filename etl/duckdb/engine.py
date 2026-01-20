"""
DuckDB Engine

Manages DuckDB connections and provides query execution interface.
DuckDB is used for all data processing operations.
"""

from typing import Any, Dict, Optional

try:
    import duckdb
except ImportError:
    raise ImportError(
        "DuckDB is required. Install it with: pip install duckdb"
    )


class DuckDBEngine:
    """
    DuckDB connection manager.

    Provides a singleton-like interface to DuckDB connections
    with configurable memory and thread settings.
    """

    def __init__(
        self,
        database: str = ":memory:",
        memory_limit: str = "2GB",
        threads: int = 4,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize DuckDB engine.

        Args:
            database: Database path (":memory:" for in-memory)
            memory_limit: Memory limit for DuckDB (e.g., "2GB", "512MB")
            threads: Number of threads for parallel processing
            config: Additional DuckDB configuration options
        """
        self.database = database
        self.memory_limit = memory_limit
        self.threads = threads
        self.config = config or {}

        self._connection = None

    @property
    def con(self) -> duckdb.DuckDBPyConnection:
        """
        Get DuckDB connection (lazy initialization).

        Returns:
            DuckDB connection object
        """
        if self._connection is None:
            self._connect()
        return self._connection

    def _connect(self) -> None:
        """Establish DuckDB connection with configuration."""
        self._connection = duckdb.connect(self.database)

        # Set memory limit
        self._connection.execute(f"SET memory_limit='{self.memory_limit}'")

        # Set threads
        self._connection.execute(f"SET threads={self.threads}")

        # Apply additional configuration
        for key, value in self.config.items():
            self._connection.execute(f"SET {key}={value}")

    def query(self, sql: str) -> duckdb.DuckDBPyRelation:
        """
        Execute a SQL query.

        Args:
            sql: SQL query string

        Returns:
            DuckDB relation object
        """
        return self.con.execute(sql)

    def execute(self, sql: str) -> None:
        """
        Execute a SQL statement (no return value).

        Args:
            sql: SQL statement
        """
        self.con.execute(sql)

    def register_parquet(
        self,
        view_name: str,
        path: str,
        hive_partitioning: bool = False
    ) -> None:
        """
        Register a Parquet file as a view.

        Args:
            view_name: Name for the view
            path: Path to Parquet file
            hive_partitioning: Enable Hive partitioning
        """
        hive_sql = ", hive_partitioning=true" if hive_partitioning else ""
        sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{path}'{hive_sql})"
        self.execute(sql)

    def register_csv(
        self,
        view_name: str,
        path: str,
        delimiter: str = ",",
        header: bool = True,
        quote: str = '"',
        escape: str = "\\",
        nullstr: str = ""
    ) -> None:
        """
        Register a CSV file as a view.

        Args:
            view_name: Name for the view
            path: Path to CSV file
            delimiter: Field delimiter
            header: Whether file has header
            quote: Quote character
            escape: Escape character
            nullstr: String representing null values
        """
        sql = f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT * FROM read_csv(
                '{path}',
                delim='{delimiter}',
                header={str(header).lower()},
                quote='{quote}',
                escape='{escape}',
                nullstr='{nullstr}'
            )
        """
        self.execute(sql)

    def close(self) -> None:
        """Close the DuckDB connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
