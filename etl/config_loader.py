"""
Configuration Loader and Validator for ETL Tool

This module loads and validates all YAML configuration files for the ETL system.
It provides a unified interface for accessing configuration data.

New Scalable Structure:
- configs/schema/tables/         # Individual table definitions
- configs/sources/{system}/      # Source files per system
- configs/aggregations/{system}/ # Aggregations per system
- configs/copydata/{system}/     # CopyData tasks per system
- configs/jobs/{system}/         # Job definitions (1 per system)
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml


class ConfigurationError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


class ConfigLoader:
    """
    Loads and manages all ETL configuration files.

    Supports a scalable, modular structure where:
    - Each system has its own folder under sources, aggregations, copydata
    - Each aggregation/copydata task is in its own file
    - Job files contain all tasks for a system in one place
    """

    def __init__(self, config_root: Optional[Path] = None):
        """
        Initialize the configuration loader.

        Args:
            config_root: Root directory for configuration files.
                        Defaults to ../configs relative to this file.
        """
        if config_root is None:
            current_file = Path(__file__)
            config_root = current_file.parent.parent / "configs"

        self.config_root = Path(config_root)
        self._cache: Dict[str, Any] = {}

    def _normalize_system(self, system: str) -> str:
        """
        Normalize system name to match directory structure.

        Args:
            system: System name in various formats (a, A, system_a, SYSTEM_A, etc.)

        Returns:
            Normalized system name (system_a, system_b, etc.)
        """
        system = system.lower()
        if not system.startswith("system_"):
            return f"system_{system}"
        return system

    def _load_yaml(self, yaml_path: Path) -> Dict[str, Any]:
        """
        Load a YAML file and cache the result.

        Args:
            yaml_path: Path to the YAML file

        Returns:
            Parsed YAML content as a dictionary

        Raises:
            ConfigurationError: If file cannot be read or parsed
        """
        str_path = str(yaml_path)
        if str_path in self._cache:
            return self._cache[str_path]

        if not yaml_path.exists():
            raise ConfigurationError(f"Configuration file not found: {yaml_path}")

        try:
            with open(yaml_path, 'r') as f:
                data = yaml.safe_load(f)
                if data is None:
                    data = {}
                self._cache[str_path] = data
                return data
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML file {yaml_path}: {e}")
        except Exception as e:
            raise ConfigurationError(f"Error reading file {yaml_path}: {e}")

    # ========================================================================
    # Database Schema Configuration
    # ========================================================================

    def load_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Load schema for a specific table.

        Args:
            table_name: Name of the table (e.g., 'Customer', 'Product')
        """
        table_path = self.config_root / "schema" / "tables" / f"{table_name.lower()}.yaml"
        return self._load_yaml(table_path)

    def get_all_tables(self) -> List[str]:
        """Get list of all configured tables."""
        index_path = self.config_root / "schema" / "_index.yaml"
        if index_path.exists():
            index = self._load_yaml(index_path)
            return index.get('tables', [])

        # Fallback: scan tables directory
        tables_dir = self.config_root / "schema" / "tables"
        if tables_dir.exists():
            return [f.stem for f in tables_dir.glob("*.yaml")]
        return []

    # ========================================================================
    # Source File Configuration
    # ========================================================================

    def load_source_file(self, system: str, source_name: str) -> Dict[str, Any]:
        """
        Load configuration for a specific source file.

        Args:
            system: System name (e.g., 'a', 'b', 'system_a', 'system_b')
            source_name: Source name (e.g., 'source_file_a', 'source_file_b')
        """
        system = self._normalize_system(system)
        source_path = self.config_root / "sources" / system / f"{source_name}.yaml"
        return self._load_yaml(source_path)

    def get_all_sources(self, system: str) -> List[str]:
        """
        Get list of all source files for a system.

        Args:
            system: System name
        """
        system = self._normalize_system(system)
        index_path = self.config_root / "sources" / system / "_index.yaml"

        if index_path.exists():
            index = self._load_yaml(index_path)
            return index.get('sources', [])

        # Fallback: scan directory
        sources_dir = self.config_root / "sources" / system
        if sources_dir.exists():
            return [f.stem for f in sources_dir.glob("*.yaml") if f.stem != "_index"]
        return []

    # ========================================================================
    # Aggregation Configuration
    # ========================================================================

    def load_aggregation(self, system: str, agg_name: str) -> Dict[str, Any]:
        """
        Load configuration for a specific aggregation.

        Args:
            system: System name
            agg_name: Aggregation name (e.g., 'customer', 'address', 'phone')
        """
        system = self._normalize_system(system)
        agg_path = self.config_root / "aggregations" / system / f"{agg_name}.yaml"
        return self._load_yaml(agg_path)

    def get_all_aggregations(self, system: str) -> List[str]:
        """
        Get list of all aggregations for a system.

        Args:
            system: System name
        """
        system = self._normalize_system(system)
        index_path = self.config_root / "aggregations" / system / "_index.yaml"

        if index_path.exists():
            index = self._load_yaml(index_path)
            return index.get('aggregations', [])

        # Fallback: scan directory
        agg_dir = self.config_root / "aggregations" / system
        if agg_dir.exists():
            return [f.stem for f in agg_dir.glob("*.yaml") if f.stem != "_index"]
        return []

    # ========================================================================
    # CopyData Configuration
    # ========================================================================

    def load_copydata_task(self, system: str, task_name: str) -> Dict[str, Any]:
        """
        Load configuration for a specific CopyData task.

        Args:
            system: System name
            task_name: Task name (e.g., 'products', 'customers', 'addresses', 'phones')
        """
        system = self._normalize_system(system)
        task_path = self.config_root / "copydata" / system / f"{task_name}.yaml"
        return self._load_yaml(task_path)

    def get_all_copydata_tasks(self, system: str) -> List[str]:
        """
        Get list of all CopyData tasks for a system.

        Args:
            system: System name
        """
        system = self._normalize_system(system)
        index_path = self.config_root / "copydata" / system / "_index.yaml"

        if index_path.exists():
            index = self._load_yaml(index_path)
            return index.get('tasks', [])

        # Fallback: scan directory
        copydata_dir = self.config_root / "copydata" / system
        if copydata_dir.exists():
            return [f.stem for f in copydata_dir.glob("*.yaml") if f.stem != "_index"]
        return []

    # ========================================================================
    # Job Configuration
    # ========================================================================

    def load_job(self, system: str) -> Dict[str, Any]:
        """
        Load job configuration for a system.

        Args:
            system: System name
        """
        system = self._normalize_system(system)
        job_path = self.config_root / "jobs" / system / "job.yaml"
        return self._load_yaml(job_path)

    def get_all_systems(self) -> List[str]:
        """Get list of all configured systems."""
        systems = []

        jobs_dir = self.config_root / "jobs"
        if jobs_dir.exists():
            for d in jobs_dir.iterdir():
                if d.is_dir() and (d / "job.yaml").exists():
                    systems.append(d.name)

        return sorted(systems)

    # ========================================================================
    # Validation
    # ========================================================================

    def validate_system(self, system: str) -> List[str]:
        """
        Validate all configuration files for a system.

        Returns a list of validation errors (empty if valid).

        Args:
            system: System name to validate
        """
        system = self._normalize_system(system)
        errors = []

        try:
            # Validate job configuration
            job = self.load_job(system)
            if not job.get('tasks'):
                errors.append("Job configuration missing 'tasks' section")
        except ConfigurationError as e:
            errors.append(f"Job config error: {e}")

        try:
            # Validate sources
            for source in self.get_all_sources(system):
                self.load_source_file(system, source)
        except ConfigurationError as e:
            errors.append(f"Source config error: {e}")

        try:
            # Validate aggregations
            for agg in self.get_all_aggregations(system):
                self.load_aggregation(system, agg)
        except ConfigurationError as e:
            errors.append(f"Aggregation config error: {e}")

        try:
            # Validate copydata tasks
            for task in self.get_all_copydata_tasks(system):
                self.load_copydata_task(system, task)
        except ConfigurationError as e:
            errors.append(f"CopyData config error: {e}")

        return errors

    def validate_all(self) -> Dict[str, List[str]]:
        """
        Validate all configured systems.

        Returns a dictionary mapping system names to their validation errors.
        """
        results = {}
        for system in self.get_all_systems():
            results[system] = self.validate_system(system)
        return results


# Global configuration loader instance
_config_loader: Optional[ConfigLoader] = None


def get_config_loader(config_root: Optional[Path] = None) -> ConfigLoader:
    """
    Get the global configuration loader instance.

    Args:
        config_root: Optional custom config root path

    Returns:
        ConfigLoader instance
    """
    global _config_loader
    if _config_loader is None or config_root is not None:
        _config_loader = ConfigLoader(config_root)
    return _config_loader


def reset_config_loader():
    """Reset the global configuration loader (useful for testing)."""
    global _config_loader
    _config_loader = None
