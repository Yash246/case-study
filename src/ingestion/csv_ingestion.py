"""
CSV ingestion class with watermark based incremental load support.
"""

import json
import logging
import polars as pl
from pathlib import Path
from datetime import datetime
from typing import Any

from .data_ingestion import DataIngestion

logger = logging.getLogger(__name__)


class CSVIngestion(DataIngestion):
    """
    Class for ingestion of CSV customer files.

    Two modes:
    Baseline — full-load of the initial CSV snapshot.
    Incremental — reads a delta file, filters rows newer than the
    stored watermark and appends them into the existing dataset.

    Args:
        raw_folder: Folder containing raw CSV files.
        bronze_folder: Folder for Bronze-layer Parquet output.
        watermark_folder: Folder where watermark JSON files are stored.
            Created automatically if it does not exist.
    """

    def __init__(self, raw_folder: Path, bronze_folder: Path, watermark_folder: Path):

        super().__init__(raw_folder, bronze_folder, watermark_folder=watermark_folder)
        self._watermark_folder = watermark_folder
        self._watermark_folder.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Watermark folder: {self._watermark_folder}")

    def _read_source(self, path: Path, **kwargs: Any) -> pl.DataFrame:
        """
        Read a CSV file into a Polars DataFrame.

        Args:
            path: Absolute path to the CSV file.

        Returns:
            Parsed DataFrame with last_updated as datetime.
        """

        logger.debug(f"Reading CSV: {path}")
        customer_df = pl.read_csv(path, try_parse_dates=True)
        customer_df = self._check_datetime(customer_df)

        return customer_df

    def _write_bronze(self, data: pl.DataFrame) -> None:
        """
        Write the customer dataFrame to Bronze as parquet.

        Args:
            data: Customer dataframe.
        """

        output_path = self._bronze_folder / "customers.parquet"
        data.write_parquet(output_path)

        logger.info(f"Bronze customers written.")

    def ingest(self, filename: str, **kwargs: Any) -> pl.DataFrame:
        """
        Ingest CSV file into Bronze layer.

        Reads the file, writes Bronze parquet and initialises the
        watermark from the maximum last_updated value.

        Args:
            filename: Name of the CSV file within ``raw_folder``.

        Returns:
            The fully loaded customer DataFrame.
        """

        logger.info(f"CSV ingestion starting")

        source_path = self._resolve_path(filename)

        self.validate_source(source_path)

        customer_df = self._read_source(source_path)

        self._write_bronze(customer_df)
        
        self._save_watermark("customers", customer_df)

        return customer_df

    def ingest_incremental(self, filename: str, existing_data: pl.DataFrame) -> pl.DataFrame:
        """
        Perform an incremental load by appending new rows.

        Reads the delta CSVs, filters out rows where last_updated
        is before the current watermark and merges the remaining
        rows into existing data using an append on customer_id.

        Args:
            filename: Name of the incremental CSV file within raw_folder.
            existing_data: The current Bronze customer dataframe to merge into.

        Returns:
            The merged dataframe containing all current + updated rows.
        """

        source_path = self._resolve_path(filename)
        self.validate_source(source_path)
        incoming_data = self._read_source(source_path)

        watermark = self._load_watermark("customers")
        logger.debug(f"Current watermark: {watermark}")

        if watermark is not None:
            new_rows = incoming_data.filter(pl.col("last_updated") > watermark)
        else:
            new_rows = incoming_data
            logger.warning("No watermark found — treating all rows as new")

        if new_rows.shape[0] == 0:
            logger.info(f"No new rows in {filename}")

            return existing_data

        merged_data = self._append(existing_data, new_rows, key="customer_id")

        self._write_bronze(merged_data)

        self._save_watermark("customers", merged_data)

        logger.info(f"CSV incremental ingestion complete — {existing_data.shape[0]} to {merged_data.shape[0]} rows")

        return merged_data

    @staticmethod
    def _check_datetime(customer_df: pl.DataFrame) -> pl.DataFrame:
        """
        Cast last_updated from string to datetime if required.

        Args:
            customer_df: DataFrame that may contain last_updated as string.

        Returns:
            DataFrame with last_updated enforced as datetime.
        """

        if "last_updated" in customer_df.columns:
            if customer_df.schema["last_updated"] == pl.Utf8:
                logger.debug("Casting 'last_updated' from string to Datetime")
                customer_df = customer_df.with_columns(
                    pl.col("last_updated")
                    .str.to_datetime("%Y-%m-%dT%H:%M:%SZ", strict=False)
                    .alias("last_updated")
                )

        return customer_df

    @staticmethod
    def _append(base: pl.DataFrame, delta: pl.DataFrame, key: str) -> pl.DataFrame:
        """
        Append delta rows into base dataframe on the given key.

        For each key present in delta, the corresponding row in
        base is replaced. Keys only in base are preserved.
        Keys only in delta are appended.

        Args:
            base: Existing dataset.
            delta: New/updated rows to merge in.
            key: Column name used as the merge key.

        Returns:
            Merged result
        """

        for col in base.columns:
            if col not in delta.columns:
                logger.debug(f"Adding missing column {col} to delta frame")
                delta = delta.with_columns(pl.lit(None).alias(col))
        delta = delta.select(base.columns)

        updated_keys = delta.select(key).unique()

        # Append deltas
        base_filtered = base.join(updated_keys, on=key, how="anti")

        return pl.concat([base_filtered, delta], how="vertical")

    def _save_watermark(self, table_name: str, customer_df: pl.DataFrame) -> None:
        """
        Persist the high watermark for a table.

        Args:
            table_name: table name
            customer_df: DataFrame whose last_updated max becomes the watermark.
        """

        if "last_updated" not in customer_df.columns:
            logger.warning("Cannot save watermark — 'last_updated' column missing")

        max_ts = customer_df.select(pl.col("last_updated").max()).item()
        wm_file = self._watermark_folder / f"{table_name}_watermark.json"

        with open(wm_file, "w") as file:
            json.dump({"table": table_name, "last_updated": str(max_ts)}, file)

    def _load_watermark(self, table_name: str) -> datetime | None:
        """
        Load the previously saved watermark for a table.

        Args:
            table_name: table name matching the watermark filename.

        Returns:
            The watermark timestamp or None if no watermark exists.
        """

        wm_file = self._watermark_folder / f"{table_name}_watermark.json"

        if not wm_file.exists():
            logger.debug(f"Watermark file not found: {wm_file}")
            return None

        with open(wm_file, "r") as file:
            data = json.load(file)

        ts = datetime.fromisoformat(data["last_updated"])

        return ts
