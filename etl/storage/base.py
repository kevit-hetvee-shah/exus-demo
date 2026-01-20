"""
Storage Backend Abstract Base Class

Defines the interface that all storage backends must implement.
This abstraction allows the ETL system to work with different storage types
(Local filesystem, S3, SFTP) transparently.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import IO, List, Optional
from pathlib import Path


@dataclass
class FileMetadata:
    """
    Metadata about a file in storage.

    Attributes:
        path: Full path to the file
        size: File size in bytes
        modified_time: Last modification timestamp
        etag: Optional ETag or checksum (for S3)
    """
    path: str
    size: int
    modified_time: datetime
    etag: Optional[str] = None


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.

    All storage implementations (Local, S3, SFTP) must inherit from this class
    and implement all abstract methods.
    """

    @abstractmethod
    def file_exists(self, path: str) -> bool:
        """
        Check if a file exists at the given path.

        Args:
            path: Path to check

        Returns:
            True if file exists, False otherwise
        """
        pass

    @abstractmethod
    def wait_for_file(
        self,
        pattern: str,
        timeout: int,
        poke_interval: int = 60,
        base_path: Optional[str] = None
    ) -> FileMetadata:
        """
        Wait for a file matching the pattern to appear.

        Polls the storage backend until a file matching the pattern is found
        or timeout is reached.

        Args:
            pattern: File pattern to match (e.g., "*.csv", "source_file_A_*.csv")
            timeout: Maximum time to wait in seconds
            poke_interval: How often to check (in seconds)
            base_path: Base directory to search in (optional)

        Returns:
            FileMetadata for the first matching file

        Raises:
            TimeoutError: If no file is found within timeout
        """
        pass

    @abstractmethod
    def read_file(self, path: str) -> IO[bytes]:
        """
        Open a file for reading.

        Args:
            path: Path to the file

        Returns:
            File-like object in binary mode
        """
        pass

    @abstractmethod
    def write_file(self, path: str, data: IO[bytes]) -> None:
        """
        Write data to a file.

        Args:
            path: Path to write to
            data: Data to write (file-like object)
        """
        pass

    @abstractmethod
    def list_files(self, prefix: str = "", pattern: str = "*") -> List[str]:
        """
        List files matching a pattern.

        Args:
            prefix: Directory prefix to search in
            pattern: File pattern to match (e.g., "*.parquet")

        Returns:
            List of matching file paths
        """
        pass

    @abstractmethod
    def delete_file(self, path: str) -> None:
        """
        Delete a file.

        Args:
            path: Path to the file to delete
        """
        pass

    @abstractmethod
    def get_file_info(self, path: str) -> FileMetadata:
        """
        Get metadata about a file.

        Args:
            path: Path to the file

        Returns:
            FileMetadata object

        Raises:
            FileNotFoundError: If file doesn't exist
        """
        pass

    def join_path(self, *parts: str) -> str:
        """
        Join path parts using the appropriate separator for the backend.

        Args:
            *parts: Path parts to join

        Returns:
            Joined path string
        """
        return "/".join(str(part).strip("/") for part in parts if part)

    def get_base_path(self) -> str:
        """
        Get the base path for this storage backend.

        Returns:
            Base path string
        """
        return ""
