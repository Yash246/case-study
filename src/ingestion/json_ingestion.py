"""
JSON ingestion class.
"""

import json
import logging
import polars as pl
from pathlib import Path
from typing import Any

from .data_ingestion import DataIngestion

logger = logging.getLogger(__name__)


class JSONIngestion(DataIngestion):
    """
    Class for ingestion of product information.

    Args:
        raw_folder: Folder containing raw JSON files.
        bronze_folder: Folder for Bronze-layer Parquet output.
    """

    def __init__(self, raw_folder: Path, bronze_folder: Path):

        super().__init__(raw_folder, bronze_folder)
        self._raw_data: list[dict] = []

    def _read_source(self, path: Path, **kwargs: Any) -> list[dict]:
        """
        Parse a JSON file into a list of dictionaries.

        Args:
            path: Absolute path to the JSON file.

        Returns:
            Parsed JSON array with each element as a dictionary.
        """

        with open(path, "r") as file:
            self._raw_data = json.load(file)

        return self._raw_data

    def _write_bronze(self, data: list[dict]) -> None:
        """
        Write a tabular snapshot of the JSON data to Bronze parquet.

        Args:
            data: Parsed product records from the JSON file.
        """

        rows = []
        for item in data:
            rows.append(
                {
                    "product_id": item["product_id"],
                    "name": item["name"],
                    "category": item["category"],
                    "raw_json": json.dumps(item),
                }
            )

        product_df = pl.DataFrame(rows)
        output_path = self._bronze_folder / "products_raw.parquet"
        product_df.write_parquet(output_path)

    def validate_source(self, path: Path) -> None:
        """
        Validate that the source file exists and contains a JSON array.

        Args:
            path: Resolved path to the JSON file.

        Raises:
            FileNotFoundError: If the file does not exist (from base class).
            ValueError: If the file is not valid JSON
        """

        super().validate_source(path)
        logger.debug(f"Validating JSON structure in {path}")

        try:
            with open(path, "r") as file:
                content = json.load(file)
            if not isinstance(content, list):
                raise ValueError(f"Expected a JSON array at top level, got {type(content)}")
            logger.debug("JSON validation passed — array with %d elements", len(content))
        
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {path}: {e}")
