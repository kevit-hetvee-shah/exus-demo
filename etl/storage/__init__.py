"""
ETL Storage Abstraction Layer

Provides unified interface for different storage backends (Local, S3, SFTP).
All storage backends implement the StorageBackend interface.
"""

from .base import StorageBackend, FileMetadata
from .local import LocalStorage
from .factory import StorageFactory

__all__ = [
    "StorageBackend",
    "FileMetadata",
    "LocalStorage",
    "StorageFactory",
]
