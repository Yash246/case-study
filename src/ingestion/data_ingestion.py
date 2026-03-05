"""
Abstract base class for all data ingestion modules (Bronze layer).
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


class DataIngestion(ABC):
    """
    Base class for every ingestion module.

    Args:
        raw_folder: Folder containing raw source files.
        bronze_folder: Folder where Bronze layer parquet files are.
            written. Created automatically if it does not exist.
        **kwargs: Additional keyword arguments stored in
            _extra_config for sub class use.
    """

    def __init__(self, raw_folder: Path, bronze_folder: Path, **kwargs: Any):
        self._raw_folder = raw_folder
        self._bronze_folder = bronze_folder
        self._bronze_folder.mkdir(parents=True, exist_ok=True)
        self._extra_config = kwargs

    @property
    def raw_folder(self) -> Path:
        """
        Return the raw source directory path.
        """

        return self._raw_folder

    @property
    def bronze_folder(self) -> Path:
        """
        Return the Bronze output directory path.
        """

        return self._bronze_folder

    def ingest(self, filename: str, **kwargs: Any) -> Any:
        """
        Orchestrate the full ingestion workflow.

        Steps executed in order:
            1. Resolve the source file path.
            2. Validate the source (existence + format checks).
            3. Read the source into an internal representation.
            4. Write the Bronze layer artefact.
            5. Return the data for downstream use.

        Args:
            filename: Name of the source file relative to raw_folder.
            **kwargs: Passed through to _read_source().

        Returns:
            The ingested data — pl.DataFrame or
            list[dict] (for json source) depending on the source format.
        """

        source_path = self._resolve_path(filename)

        logger.info("Starting ingestion")

        self.validate_source(source_path)
        logger.debug(f"Source validation passed for {source_path}.")

        data = self._read_source(source_path, **kwargs)

        self._write_bronze(data)
        logger.info(f"Ingestion of {filename} complete, Bronze artefact written.")

        return data

    @abstractmethod
    def _read_source(self, path: Path, **kwargs: Any) -> Any:
        """
        Read the raw file.

        Args:
            path: Absolute path to the source file.
            **kwargs: Format specific options (sheet names for XLSX).

        Returns:
            Parsed data
        """

    @abstractmethod
    def _write_bronze(self, data: Any) -> None:
        """
        Persist the ingested data to the Bronze layer.

        Args:
            data: The object returned by _read_source().
        """

    def validate_source(self, file_path: Path) -> None:
        """
        Pre ingestion validation

        The default implementation checks that the file exists.

        Args:
            path: Resolved path to the source file.

        Raises:
            FileNotFoundError: If the source file does not exist.
        """

        if not file_path.exists():
            logger.error(f"Source file not found: {file_path}")
            raise FileNotFoundError(f"Source file not found: {file_path}")

    def _resolve_path(self, filename: str) -> Path:
        """
        Combine raw_folder with filename to produce an absolute path.

        Args:
            filename: File name within the raw folder.

        Returns:
            Fully resolved source file path.
        """

        resolved = self._raw_folder / filename
        logger.debug("Resolved source path: %s", resolved)

        return resolved
