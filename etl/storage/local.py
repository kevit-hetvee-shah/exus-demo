"""
Local Filesystem Storage Backend

Implements StorageBackend for local filesystem operations.
"""

import os
import time
from datetime import datetime
from pathlib import Path
from typing import IO, List

from .base import FileMetadata, StorageBackend


class LocalStorage(StorageBackend):
    """
    Local filesystem storage implementation.

    Provides file operations on the local filesystem.
    All paths are relative to a base_path if specified.
    """

    def __init__(self, base_path: str = ""):
        """
        Initialize LocalStorage.

        Args:
            base_path: Base directory for all operations (optional)
        """
        self.base_path = Path(base_path) if base_path else Path.cwd()
        # Create base path if it doesn't exist
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _resolve_path(self, path: str) -> Path:
        """Resolve a path relative to base_path."""
        path_obj = Path(path)
        if path_obj.is_absolute():
            return path_obj
        return self.base_path / path_obj

    def file_exists(self, path: str) -> bool:
        """Check if a file exists at the given path."""
        resolved_path = self._resolve_path(path)
        return resolved_path.is_file()

    def wait_for_file(
        self,
        pattern: str,
        timeout: int,
        poke_interval: int = 60,
        base_path: Optional[str] = None
    ) -> FileMetadata:
        """
        Wait for a file matching the pattern to appear.

        Args:
            pattern: File pattern to match (e.g., "*.csv")
            timeout: Maximum time to wait in seconds
            poke_interval: How often to check (in seconds)
            base_path: Base directory to search in (optional, uses instance base_path if not provided)

        Returns:
            FileMetadata for the first matching file

        Raises:
            TimeoutError: If no file is found within timeout
        """
        search_path = Path(base_path) if base_path else self.base_path
        search_path.mkdir(parents=True, exist_ok=True)

        start_time = time.time()
        while time.time() - start_time < timeout:
            # List files matching pattern
            matching_files = list(search_path.glob(pattern))
            # Filter to only files (not directories)
            files = [f for f in matching_files if f.is_file()]

            if files:
                # Return the first matching file
                file_path = files[0]
                stat = file_path.stat()
                return FileMetadata(
                    path=str(file_path),
                    size=stat.st_size,
                    modified_time=datetime.fromtimestamp(stat.st_mtime)
                )

            # Wait before next check
            time.sleep(poke_interval)

        raise TimeoutError(
            f"File matching pattern '{pattern}' not found at '{search_path}' "
            f"within {timeout} seconds"
        )

    def read_file(self, path: str) -> IO[bytes]:
        """
        Open a file for reading.

        Args:
            path: Path to the file

        Returns:
            File-like object in binary mode
        """
        resolved_path = self._resolve_path(path)
        return open(resolved_path, "rb")

    def write_file(self, path: str, data: IO[bytes]) -> None:
        """
        Write data to a file.

        Args:
            path: Path to write to
            data: Data to write (file-like object)
        """
        resolved_path = self._resolve_path(path)
        # Create parent directories if they don't exist
        resolved_path.parent.mkdir(parents=True, exist_ok=True)

        mode = "wb" if hasattr(data, "read") else "w"
        with open(resolved_path, mode) as f:
            if hasattr(data, "read"):
                # If data is a file-like object, read from it
                chunk_size = 8192
                while True:
                    chunk = data.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
            else:
                # If data is already bytes/string
                f.write(data)

    def list_files(self, prefix: str = "", pattern: str = "*") -> List[str]:
        """
        List files matching a pattern.

        Args:
            prefix: Directory prefix to search in
            pattern: File pattern to match

        Returns:
            List of matching file paths (relative to base_path)
        """
        search_path = self.base_path / prefix if prefix else self.base_path
        matching_files = list(search_path.glob(pattern))

        # Filter to only files and return relative paths
        return [
            str(f.relative_to(self.base_path))
            for f in matching_files
            if f.is_file()
        ]

    def delete_file(self, path: str) -> None:
        """
        Delete a file.

        Args:
            path: Path to the file to delete
        """
        resolved_path = self._resolve_path(path)
        if resolved_path.exists():
            resolved_path.unlink()

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
        resolved_path = self._resolve_path(path)
        if not resolved_path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        stat = resolved_path.stat()
        return FileMetadata(
            path=str(resolved_path),
            size=stat.st_size,
            modified_time=datetime.fromtimestamp(stat.st_mtime)
        )

    def get_base_path(self) -> str:
        """Get the base path for this storage backend."""
        return str(self.base_path)
