"""
Storage Factory

Creates storage backend instances based on configuration.
"""

from typing import Dict, Any

from .base import StorageBackend
from .local import LocalStorage


class StorageFactory:
    """
    Factory for creating storage backend instances.

    Supports creating storage backends from configuration dictionaries.
    """

    @staticmethod
    def create(config: Dict[str, Any]) -> StorageBackend:
        """
        Create a storage backend from configuration.

        Args:
            config: Configuration dictionary with 'type' key
                    Example: {'type': 'local', 'path': '/data'}

        Returns:
            StorageBackend instance

        Raises:
            ValueError: If storage type is unknown or config is invalid
        """
        backend_type = config.get("type", "local")

        if backend_type == "local":
            return LocalStorage(
                base_path=config.get("path", "")
            )

        # TODO: Implement S3 and SFTP backends
        # elif backend_type == "s3":
        #     from .s3 import S3Storage
        #     return S3Storage(
        #         bucket=config["bucket"],
        #         prefix=config.get("prefix", ""),
        #         aws_config=config.get("aws_config", {})
        #     )
        #
        # elif backend_type == "sftp":
        #     from .sftp import SFTPStorage
        #     return SFTPStorage(
        #         host=config["host"],
        #         port=config.get("port", 22),
        #         username=config["username"],
        #         password=config.get("password"),
        #         base_path=config.get("base_path", "")
        #     )

        raise ValueError(f"Unknown storage type: {backend_type}")

    @staticmethod
    def create_local(base_path: str = "") -> LocalStorage:
        """
        Convenience method to create a LocalStorage instance.

        Args:
            base_path: Base directory for file operations

        Returns:
            LocalStorage instance
        """
        return LocalStorage(base_path=base_path)
