"""
DuckDB Aggregator

Aggregation and window function operations using DuckDB.
"""

from typing import List

from .engine import DuckDBEngine


class DuckDBAggregator:
    """
    Apply aggregations and window functions using DuckDB SQL.
    """

    def __init__(self, engine: DuckDBEngine):
        """
        Initialize DuckDB aggregator.

        Args:
            engine: DuckDB engine instance
        """
        self.engine = engine

    def apply_aggregations(
        self,
        relation,
        aggregations: List[dict]
    ):
        """
        Apply GROUP BY aggregations.

        Args:
            relation: DuckDB relation
            aggregations: List of aggregation group configurations

        Returns:
            Aggregated DuckDB relation
        """
        if not aggregations:
            return relation

        results = []
        for agg_group in aggregations:
            sql = self._build_aggregation_sql(relation, agg_group)
            result = self.engine.query(sql)
            results.append(result)

        # Return first result (or combine multiple results)
        return results[0] if len(results) == 1 else results

    def _build_aggregation_sql(self, relation, agg_group: dict) -> str:
        """Build SQL for aggregation group."""
        group_by = agg_group.get("group_by", [])
        operations = agg_group.get("operations", [])
        having = agg_group.get("having")

        # Build SELECT clause
        select_parts = [f'"{col}"' for col in group_by]
        for op in operations:
            select_parts.append(self._build_aggregation_sql(op))

        sql = f"SELECT {', '.join(select_parts)} FROM relation"

        # Add GROUP BY
        if group_by:
            group_cols = ", ".join(f'"{col}"' for col in group_by)
            sql += f" GROUP BY {group_cols}"

        # Add HAVING
        if having:
            sql += f" HAVING {self._build_having_sql(having)}"

        return sql

    def _build_aggregation_sql(self, operation: dict) -> str:
        """Build SQL for aggregation operation."""
        column = operation.get("column")
        op_type = operation.get("type")
        output_column = operation.get("output_column", f"{op_type}_{column}")

        col_ref = f'"{column}"' if column else "*"

        if op_type == "sum":
            agg_sql = f"SUM({col_ref})"
        elif op_type == "count":
            agg_sql = f"COUNT({col_ref})"
        elif op_type == "avg":
            agg_sql = f"AVG({col_ref})"
        elif op_type == "min":
            agg_sql = f"MIN({col_ref})"
        elif op_type == "max":
            agg_sql = f"MAX({col_ref})"
        elif op_type == "first":
            agg_sql = f"FIRST({col_ref})"
        elif op_type == "last":
            agg_sql = f"LAST({col_ref})"
        elif op_type == "list":
            agg_sql = f"LIST({col_ref})"
        else:
            agg_sql = col_ref

        return f"{agg_sql} AS \"{output_column}\""

    def _build_having_sql(self, having: dict) -> str:
        """Build HAVING clause SQL."""
        column = having.get("column")
        operator = having.get("operator")
        value = having.get("value")

        if operator == "equals":
            return f'"{column}" = {self._quote_value(value)}'
        elif operator == "not_equals":
            return f'"{column}" != {self._quote_value(value)}'
        elif operator == "greater_than":
            return f'"{column}" > {self._quote_value(value)}'
        elif operator == "less_than":
            return f'"{column}" < {self._quote_value(value)}'
        elif operator == "greater_equal":
            return f'"{column}" >= {self._quote_value(value)}'
        elif operator == "less_equal":
            return f'"{column}" <= {self._quote_value(value)}'

        return ""

    def apply_window_functions(
        self,
        relation,
        window_functions: List[dict]
    ):
        """
        Apply window functions.

        Args:
            relation: DuckDB relation
            window_functions: List of window function configurations

        Returns:
            DuckDB relation with window functions applied
        """
        if not window_functions:
            return relation

        window_clauses = []
        for win in window_functions:
            window_clauses.append(self._build_window_sql(win))

        sql = f"SELECT *, {', '.join(window_clauses)} FROM relation"
        return self.engine.query(sql)

    def _build_window_sql(self, window: dict) -> str:
        """Build SQL for window function."""
        func = window.get("function", {})
        partition_by = window.get("partition_by", [])
        order_by = window.get("order_by", [])

        func_type = func.get("type")
        column = func.get("column")
        output_column = func.get("output_column")
        offset = func.get("offset", 1)
        window_size = func.get("window_size", 3)

        # Build PARTITION BY
        partition_sql = ""
        if partition_by:
            partition_sql = "PARTITION BY " + ", ".join(f'"{col}"' for col in partition_by)

        # Build ORDER BY
        order_sql = ""
        if order_by:
            order_parts = []
            for order_spec in order_by:
                if isinstance(order_spec, str):
                    order_parts.append(f'"{order_spec}"')
                elif isinstance(order_spec, list):
                    col, direction = order_spec
                    order_parts.append(f'"{col}" {direction}')
            order_sql = "ORDER BY " + ", ".join(order_parts)

        # Build function SQL
        func_sql = ""
        col_ref = f'"{column}"' if column else "*"

        if func_type == "row_number":
            func_sql = f"ROW_NUMBER()"
        elif func_type == "rank":
            func_sql = f"RANK()"
        elif func_type == "dense_rank":
            func_sql = f"DENSE_RANK()"
        elif func_type == "lag":
            func_sql = f"LAG({col_ref}, {offset})"
        elif func_type == "lead":
            func_sql = f"LEAD({col_ref}, {offset})"
        elif func_type == "running_total":
            func_sql = f"SUM({col_ref})"
        elif func_type == "moving_avg":
            func_sql = f"AVG({col_ref}) OVER (ROWS BETWEEN {window_size - 1} PRECEDING AND CURRENT ROW)"
        else:
            func_sql = col_ref

        # Build window clause
        window_def = f"({partition_sql} {order_sql})".strip()

        if func_type == "moving_avg":
            # Moving avg has window built-in
            return f"{func_sql} AS \"{output_column}\""
        else:
            return f"{func_sql} OVER {window_def} AS \"{output_column}\""

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
