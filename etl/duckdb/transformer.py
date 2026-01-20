"""
DuckDB Transformer

Column transformation operations using DuckDB.
"""

from typing import List

from .engine import DuckDBEngine


class DuckDBTransformer:
    """
    Apply column transformations using DuckDB SQL.
    """

    def __init__(self, engine: DuckDBEngine):
        """
        Initialize DuckDB transformer.

        Args:
            engine: DuckDB engine instance
        """
        self.engine = engine

    def apply_transformations(
        self,
        relation,
        transformations: List[dict]
    ):
        """
        Apply column transformations to a relation.

        Args:
            relation: DuckDB relation
            transformations: List of transformation dictionaries

        Returns:
            Transformed DuckDB relation
        """
        if not transformations:
            return relation

        # Build SELECT clause with transformations
        select_clauses = self._build_select_clauses(relation, transformations)

        sql = f"SELECT {', '.join(select_clauses)} FROM relation"
        return self.engine.query(sql)

    def _build_select_clauses(
        self,
        relation,
        transformations: List[dict]
    ) -> List[str]:
        """Build SELECT clause items from transformations."""
        # Get existing columns
        try:
            existing_columns = relation.columns
        except AttributeError:
            # If relation doesn't have columns attribute, we need to handle it
            # For now, assume we read it
            existing_columns = []

        # Track which columns have been transformed
        transformed_outputs = set()

        # Process transformations
        select_items = []
        for transform in transformations:
            output_col = transform.get("output_column")
            input_col = transform.get("input_column")
            input_cols = transform.get("input_columns")
            transform_type = transform.get("type")

            # Track output column
            if output_col:
                transformed_outputs.add(output_col)

            # Build SQL for this transformation
            sql = self._build_transform_sql(transform)

            # Handle chained transformations (list of transformations on same input)
            if "transformations" in transform:
                # Apply chained transformations
                sql = self._build_chained_transform_sql(input_col, transform["transformations"])

            select_items.append(f"{sql} AS \"{output_col}\"" if output_col else sql)

        # Add columns that weren't transformed
        for col in existing_columns:
            if col not in transformed_outputs:
                select_items.append(f'"{col}"')

        return select_items

    def _build_transform_sql(self, transform: dict) -> str:
        """Build SQL for a single transformation."""
        transform_type = transform.get("type")
        input_col = transform.get("input_column")
        output_col = transform.get("output_column", input_col)
        value = transform.get("value")

        input_sql = f'"{input_col}"' if input_col else ""

        if transform_type == "cast":
            target_type = transform.get("target_type", "varchar")
            return f"CAST({input_sql} AS {target_type})"

        elif transform_type == "trim":
            return f"TRIM({input_sql})"

        elif transform_type == "upper_case":
            return f"UPPER({input_sql})"

        elif transform_type == "lower_case":
            return f"LOWER({input_sql})"

        elif transform_type == "substring":
            start = transform.get("start", 0)
            length = transform.get("length", 1)
            return f"SUBSTRING({input_sql}, {start + 1}, {length})"

        elif transform_type == "default_value":
            return f"COALESCE({input_sql}, {self._quote_value(value)})"

        elif transform_type == "concat":
            input_cols = transform.get("input_columns", [])
            separator = transform.get("separator", "")
            parts = [f'"{col}"' for col in input_cols]
            if separator:
                separator_escaped = separator.replace("'", "''")
                return f"CONCAT({separator_escaped}, {', '.join(parts)})"
            return f"CONCAT({', '.join(parts)})"

        elif transform_type == "coalesce":
            input_cols = transform.get("input_columns", [])
            parts = [self._quote_value(col) for col in input_cols]
            return f"COALESCE({', '.join(parts)})"

        elif transform_type == "date_format":
            input_format = transform.get("input_format", "%Y-%m-%d")
            output_format = transform.get("output_format", "%Y-%m-%d")
            # DuckDB date parsing
            return f"STRPTIME({input_sql}, '{self._format_to_strftime(input_format)})"

        elif transform_type == "regex":
            pattern = transform.get("pattern", "")
            replacement = transform.get("replacement", "")
            # DuckDB regex replace
            return f"REGEXP_REPLACE({input_sql}, '{pattern}', '{replacement}')"

        return input_sql

    def _build_chained_transform_sql(self, input_col: str, transformations: List[dict]) -> str:
        """Build SQL for chained transformations on same column."""
        sql = f'"{input_col}"'

        for transform in transformations:
            transform_type = transform.get("type")

            if transform_type == "trim":
                sql = f"TRIM({sql})"
            elif transform_type == "upper_case":
                sql = f"UPPER({sql})"
            elif transform_type == "lower_case":
                sql = f"LOWER({sql})"
            elif transform_type == "default_value":
                value = transform.get("value")
                sql = f"COALESCE({sql}, {self._quote_value(value)})"
            elif transform_type == "cast":
                target_type = transform.get("target_type", "varchar")
                sql = f"CAST({sql} AS {target_type})"

        return sql

    def _format_to_strftime(self, format_str: str) -> str:
        """Convert Python date format to DuckDB/strftime format."""
        # Python to strftime conversion
        conversions = {
            "%Y": "%Y",
            "%m": "%m",
            "%d": "%d",
            "%H": "%H",
            "%M": "%M",
            "%S": "%S",
            "%y": "%y",
            "%B": "%B",
            "%b": "%b",
            "%A": "%A",
            "%a": "%a",
        }
        for py, sf in conversions.items():
            format_str = format_str.replace(py, sf)
        return format_str

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
