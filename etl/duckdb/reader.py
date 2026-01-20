"""
DuckDB Reader

File reading operations using DuckDB.
Streams data without loading entirely into memory.
"""

from typing import List, Optional

from .engine import DuckDBEngine


class DuckDBReader:
    """
    Read files using DuckDB for efficient streaming.
    """

    def __init__(self, engine: DuckDBEngine):
        """
        Initialize DuckDB reader.

        Args:
            engine: DuckDB engine instance
        """
        self.engine = engine

    def read_csv(
        self,
        path: str,
        delimiter: str = ",",
        header: bool = True,
        columns: Optional[List[str]] = None,
        quote: str = '"',
        escape: str = "\\",
        nullstr: str = "",
        skip_rows: int = 0,
        encoding: str = "utf-8"
    ) -> DuckDBEngine.con:
        """
        Read CSV file using DuckDB.

        Args:
            path: Path to CSV file
            delimiter: Field delimiter
            header: Whether file has header row
            columns: List of columns to read (None for all)
            quote: Quote character
            escape: Escape character
            nullstr: String representing null values
            skip_rows: Number of rows to skip
            encoding: File encoding

        Returns:
            DuckDB relation object
        """
        # Build read_csv options
        options = {
            "delim": delimiter,
            "header": str(header).lower(),
            "quote": quote,
            "escape": escape,
            "nullstr": nullstr,
            "skip": str(skip_rows),
            "encoding": encoding
        }

        options_str = ", ".join(f"{k}='{v}'" for k, v in options.items())

        sql = f"SELECT * FROM read_csv('{path}', {options_str})"

        # Apply column selection if specified
        if columns:
            col_list = ", ".join(f'"{col}"' for col in columns)
            sql = f"SELECT {col_list} FROM ({sql}) as subq"

        return self.engine.query(sql)

    def apply_row_filters(
        self,
        relation,
        filters: List[dict]
    ):
        """
        Apply row-level filters to a relation.

        Args:
            relation: DuckDB relation
            filters: List of filter dictionaries

        Returns:
            Filtered DuckDB relation
        """
        if not filters:
            return relation

        where_clauses = self._build_where_clauses(filters)
        sql = f"SELECT * FROM relation WHERE {where_clauses}"
        return self.engine.query(sql)

    def _build_where_clauses(self, filters: List[dict]) -> str:
        """Build SQL WHERE clause from filter list."""
        clauses = []

        for f in filters:
            column = f.get("column")
            operator = f.get("operator")
            value = f.get("value")

            if operator == "not_null":
                clauses.append(f'"{column}" IS NOT NULL')
            elif operator == "is_null":
                clauses.append(f'"{column}" IS NULL')
            elif operator == "equals":
                clauses.append(f'"{column}" = {self._quote_value(value)}')
            elif operator == "not_equals":
                clauses.append(f'"{column}" != {self._quote_value(value)}')
            elif operator == "greater_than":
                clauses.append(f'"{column}" > {self._quote_value(value)}')
            elif operator == "less_than":
                clauses.append(f'"{column}" < {self._quote_value(value)}')
            elif operator == "greater_equal":
                clauses.append(f'"{column}" >= {self._quote_value(value)}')
            elif operator == "less_equal":
                clauses.append(f'"{column}" <= {self._quote_value(value)}')
            elif operator == "in":
                values = ", ".join(self._quote_value(v) for v in value)
                clauses.append(f'"{column}" IN ({values})')
            elif operator == "not_in":
                values = ", ".join(self._quote_value(v) for v in value)
                clauses.append(f'"{column}" NOT IN ({values})')
            elif operator == "contains":
                clauses.append(f'"{column}" LIKE \'%{value}%\'')
            elif operator == "starts_with":
                clauses.append(f'"{column}" LIKE \'{value}%\'')
            elif operator == "ends_with":
                clauses.append(f'"{column}" LIKE \'%{value}\'')
            elif operator == "regex":
                clauses.append(f'"{column}" ~ \'{value}\'')

        return " AND ".join(clauses)

    def _quote_value(self, value):
        """Quote a value for SQL."""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            return f"'{value.replace(\"'\", \"''\")}'"
        elif isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        else:
            return str(value)

    def read_parquet(
        self,
        path: str,
        columns: Optional[List[str]] = None,
        hive_partitioning: bool = False
    ):
        """
        Read Parquet file using DuckDB.

        Args:
            path: Path to Parquet file
            columns: List of columns to read (None for all)
            hive_partitioning: Enable Hive partitioning

        Returns:
            DuckDB relation object
        """
        hive_sql = ", hive_partitioning=true" if hive_partitioning else ""
        sql = f"SELECT * FROM read_parquet('{path}'{hive_sql})"

        # Apply column selection if specified
        if columns:
            col_list = ", ".join(f'"{col}"' for col in columns)
            sql = f"SELECT {col_list} FROM ({sql}) as subq"

        return self.engine.query(sql)
