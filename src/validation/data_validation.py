"""
Data quality framework.

Supported checks:
    Schema — verifies required columns are present.
    Nulls — counts null values per column.
    Duplicates — detects duplicate rows by primary key.
    Empty rows — counts rows that are entirely null.
"""

import logging
import polars as pl

logger = logging.getLogger(__name__)


class DataProfiler:
    """
    Configurable data quality checker.

    Args:
        rules: Optional rule configuration. Supported keys:
            required_columns — list of column names that must exist.
    """

    def __init__(self, rules: dict | None = None):
        self._rules = rules or {}
        logger.debug(f"DataProfiler initialised with rules: {list(self._rules.keys())}")

    def run_checks(self, df: pl.DataFrame, table_name: str, primary_key: str | list[str] | None = None) -> None:
        """
        Execute all applicable quality checks on a DataFrame.

        Args:
            df: The data to validate.
            table_name: table name.
            primary_key: Column(s) for duplicate detection. If None,
                the duplicate check is skipped.
        """

        logger.info(f"Running data quality checks on {table_name}")

        self._check_schema(df)

        self._check_nulls(df)

        self._check_empty_rows(df)

        if primary_key:
            self._check_duplicates(df, primary_key)

    def _check_schema(self, df: pl.DataFrame) -> None:
        """
        Verify that all required columns are present.
        """

        required_cols = self._rules.get("required_columns", [])
        if not required_cols:
            return None
        for col in required_cols:
            if col not in df.columns:
                logger.error(f"Missing required column {col} in table")

    def _check_nulls(self, df: pl.DataFrame) -> None:
        """
        Count null values in every column.
        """

        for col in df.columns:
            null_count = df.select(pl.col(col).is_null().sum()).item()
            logger.debug(f"Column {col} has {null_count} null rows.")

    def _check_empty_rows(df: pl.DataFrame) -> None:
        """
        Count rows where all values are null.
        """

        all_null = pl.all_horizontal([pl.col(c).is_null() for c in df.columns])
        logger.debug(f"{all_null} null rows present in table.")

    def _check_duplicates(df: pl.DataFrame, key: str | list[str]) -> None:
        """
        Detect duplicate rows based on a primary key.
        """

        if isinstance(key, str):
            key = [key]
        dup_count = df.height - df.unique(subset=key).height
        logger.debug(f"{dup_count} duplicate rows present in table.")
