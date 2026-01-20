"""
ETL Package for Configuration-Driven Data Integration

This package provides a configuration-driven ETL framework that reads
from YAML configuration files to orchestrate data pipelines.

Components:
- config_loader: Load and validate YAML configurations
- transformations: Apply data transformations based on configuration
- workflows: Execute ETL workflows based on configuration
"""

from .config_loader import ConfigLoader, get_config_loader, ConfigurationError
from .transformations import TransformationEngine, TransformationError, apply_row_filters, apply_aggregations
from .workflows import execute_copy_data_task

__all__ = [
    'ConfigLoader',
    'get_config_loader',
    'ConfigurationError',
    'TransformationEngine',
    'TransformationError',
    'apply_row_filters',
    'apply_aggregations',
    'execute_copy_data_task',
]
