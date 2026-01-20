"""
DuckDB Writer

Write operations for DuckDB relations to various formats.
"""

from typing import Optional

from .engine import DuckDBEngine


class DuckDBWriter:
    """
    Write DuckDB relations to files.
    """

    def __init__(self, engine: DuckDBEngine):
        """
        Initialize DuckDB writer.

        Args:
            engine: DuckDB engine instance
        """
        self.engine = engine

    def write_parquet(
        self,
        relation,
        path: str,
        compression: str = "snappy",
        mode: str = "overwrite"
    ) -> None:
        """
        Write relation to Parquet file.

        Args:
            relation: DuckDB relation
            path: Output Parquet file path
            compression: Compression type (snappy, gzip, brotli, lz4)
            mode: Write mode (overwrite, append)
        """
        compression = compression or "snappy"

        if mode == "overwrite":
            # Use COPY for efficient write
            sql = f"COPY relation TO '{path}' (FORMAT 'parquet', COMPRESSION '{compression}')"
            self.engine.execute(sql)
        else:
            # Append mode (requires reading existing and writing back)
            raise NotImplementedError(f"Write mode '{mode}' not yet implemented")

    def write_csv(
        self,
        relation,
        path: str,
        delimiter: str = ",",
        header: bool = True,
        quote: str = '"',
        escape: str = "\\",
        compression: Optional[str] = None
    ) -> None:
        """
        Write relation to CSV file.

        Args:
            relation: DuckDB relation
            path: Output CSV file path
            delimiter: Field delimiter
            header: Whether to write header
            quote: Quote character
            escape: Escape character
            compression: Compression type (none, gzip, zstd)
        """
        options = {
            "delimiter": delimiter,
            "quote": quote,
            "escape": escape,
            "header": str(header).lower()
        }

        if compression:
            options["compression"] = compression

        options_str = ", ".join(f"{k}='{v}'" for k, v in options.items())
        sql = f"COPY relation TO '{path}' (FORMAT CSV, {options_str})"
        self.engine.execute(sql)

    def get_row_count(self, relation) -> int:
        """
        Get row count of a relation.

        Args:
            relation: DuckDB relation

        Returns:
            Number of rows
        """
        result = self.engine.query("SELECT COUNT(*) FROM relation").fetchone()
        return result[0] if result else 0
