"""
Transformation Engine for ETL Tool

This module applies transformations to data based on YAML configuration.
Supports type casting, string operations, and other data transformations.
"""

import re
from typing import Any, Dict, List, Optional
import pandas as pd
import numpy as np


class TransformationError(Exception):
    """Raised when transformation fails."""
    pass


class TransformationEngine:
    """
    Applies transformations to DataFrame columns based on configuration.

    Supported transformations:
    - trim: Remove whitespace from strings
    - upper_case/lower_case: Change case
    - cast: Convert data types
    - substring: Extract part of a string
    - regex: Apply regex patterns
    - remove_non_printable: Remove non-printable characters
    - default_value: Replace null/empty with default
    """

    def __init__(self):
        self.transformations = {
            'trim': self._trim,
            'upper_case': self._upper_case,
            'lower_case': self._lower_case,
            'cast': self._cast,
            'substring': self._substring,
            'regex': self._regex,
            'remove_non_printable': self._remove_non_printable,
            'default_value': self._default_value,
        }

    def apply_transformations(
        self,
        df: pd.DataFrame,
        column_config: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Apply all configured transformations to the DataFrame.

        Args:
            df: Input DataFrame
            column_config: List of column configurations with transformations

        Returns:
            Transformed DataFrame
        """
        result_df = df.copy()

        for col_def in column_config:
            col_name = col_def.get('name')
            transformations = col_def.get('transformations', [])

            if not transformations or col_name not in result_df.columns:
                continue

            for transform in transformations:
                transform_type = transform.get('type')
                params = transform.get('params', {})

                if transform_type in self.transformations:
                    try:
                        result_df = self.transformations[transform_type](
                            result_df, col_name, **params
                        )
                    except Exception as e:
                        raise TransformationError(
                            f"Error applying transformation '{transform_type}' "
                            f"to column '{col_name}': {e}"
                        )

        return result_df

    def _trim(self, df: pd.DataFrame, column: str, **kwargs) -> pd.DataFrame:
        """Remove leading and trailing whitespace."""
        df[column] = df[column].astype(str).str.strip()
        return df

    def _upper_case(self, df: pd.DataFrame, column: str, **kwargs) -> pd.DataFrame:
        """Convert to uppercase."""
        df[column] = df[column].astype(str).str.upper()
        return df

    def _lower_case(self, df: pd.DataFrame, column: str, **kwargs) -> pd.DataFrame:
        """Convert to lowercase."""
        df[column] = df[column].astype(str).str.lower()
        return df

    def _cast(
        self,
        df: pd.DataFrame,
        column: str,
        target_type: str = 'string',
        **kwargs
    ) -> pd.DataFrame:
        """Cast column to specified data type."""
        target_type = target_type.lower()

        if target_type == 'integer' or target_type == 'int':
            # First try to convert to numeric, then to integer
            df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
        elif target_type == 'decimal' or target_type == 'float':
            df[column] = pd.to_numeric(df[column], errors='coerce')
        elif target_type == 'boolean' or target_type == 'bool':
            df[column] = df[column].astype(str).str.lower().map({
                'true': True, '1': True, 'yes': True, 'y': True,
                'false': False, '0': False, 'no': False, 'n': False
            })
        elif target_type == 'string' or target_type == 'str':
            df[column] = df[column].astype(str)
        elif target_type == 'date':
            df[column] = pd.to_datetime(df[column], errors='coerce')

        return df

    def _substring(
        self,
        df: pd.DataFrame,
        column: str,
        start: int = 0,
        length: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Extract substring from column."""
        df[column] = df[column].astype(str).str.slice(start, start + length if length else None)
        return df

    def _regex(
        self,
        df: pd.DataFrame,
        column: str,
        pattern: str = '',
        replace: Optional[str] = None,
        **kwargs
    ) -> pd.DataFrame:
        """Apply regex pattern to column."""
        if replace is not None:
            # Replace matches
            df[column] = df[column].astype(str).str.replace(pattern, replace, regex=True)
        else:
            # Filter by pattern (keep only matching)
            df[column] = df[column].astype(str).str.extract(pattern, expand=False)

        return df

    def _remove_non_printable(self, df: pd.DataFrame, column: str, **kwargs) -> pd.DataFrame:
        """Remove non-printable characters."""
        # Remove characters with ASCII code < 32 (except newline, tab, etc.)
        df[column] = df[column].astype(str).apply(
            lambda x: ''.join(char for char in x if ord(char) >= 32 or char in '\n\r\t')
        )
        return df

    def _default_value(
        self,
        df: pd.DataFrame,
        column: str,
        value: Any = '',
        **kwargs
    ) -> pd.DataFrame:
        """Replace null/empty values with default."""
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column] = df[column].fillna(value)
        else:
            df[column] = df[column].astype(str).replace('', value).replace('None', value)
            df[column] = df[column].fillna(value)
        return df


def apply_row_filters(
    df: pd.DataFrame,
    filters: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    Apply row-level filters to DataFrame.

    Args:
        df: Input DataFrame
        filters: List of filter configurations

    Returns:
        Filtered DataFrame
    """
    if not filters:
        return df

    result_df = df.copy()

    for filter_config in filters:
        column = filter_config.get('column')
        operator = filter_config.get('operator')
        value = filter_config.get('value')

        if column not in result_df.columns:
            continue

        if operator == 'not_null':
            result_df = result_df[result_df[column].notna()]
            result_df = result_df[result_df[column].astype(str).ne('')]
        elif operator == 'equals':
            result_df = result_df[result_df[column] == value]
        elif operator == 'not_equals':
            result_df = result_df[result_df[column] != value]
        elif operator == 'greater_than':
            result_df = result_df[result_df[column] > value]
        elif operator == 'less_than':
            result_df = result_df[result_df[column] < value]
        elif operator == 'in':
            result_df = result_df[result_df[column].isin(value)]

    return result_df


def apply_aggregations(
    df: pd.DataFrame,
    group_by: List[str],
    aggregated_columns: List[Dict[str, Any]]
) -> pd.DataFrame:
    """
    Apply aggregation operations to DataFrame.

    Args:
        df: Input DataFrame
        group_by: Columns to group by
        aggregated_columns: Column aggregation definitions

    Returns:
        Aggregated DataFrame
    """
    if not group_by:
        return df

    # Build aggregation dictionary
    agg_dict = {}
    agg_func_mapping = {
        'first': 'first',
        'last': 'last',
        'min': 'min',
        'max': 'max',
        'count': 'count',
        'sum': 'sum',
        'avg': 'mean',
    }

    for col_def in aggregated_columns:
        col_name = col_def.get('name')
        agg_func = col_def.get('aggregation', 'first')

        if col_name in df.columns:
            mapped_func = agg_func_mapping.get(agg_func.lower(), 'first')
            if col_name not in agg_dict:
                agg_dict[col_name] = mapped_func
            else:
                # Multiple aggregations on same column
                if isinstance(agg_dict[col_name], str):
                    agg_dict[col_name] = [agg_dict[col_name]]
                agg_dict[col_name].append(mapped_func)

    if not agg_dict:
        # No aggregations, just drop duplicates
        return df.groupby(group_by, as_index=False).first()

    # Perform aggregation
    result = df.groupby(group_by, as_index=False).agg(agg_dict)

    # Flatten multi-level columns if created
    if isinstance(result.columns, pd.MultiIndex):
        result.columns = ['_'.join(col).strip('_') if col[1] else col[0]
                         for col in result.columns.values]

    return result
