"""
Silver ayer transformation for accounts data.
"""

import logging
import polars as pl
from pathlib import Path
from typing import Any

from .data_transformation import DataTransformation

logger = logging.getLogger(__name__)


class AccountsTransformation(DataTransformation):
    """
    Transform Bronze accounts data into Silver table.

    Args:
        silver_folder: Output folder for the Silver accounts Parquet file.
    """

    def __init__(self, silver_folder: Path):

        super().__init__(silver_folder)

    def _get_output_filename(self) -> str:
        """
        Return the Silver accounts output filename.
        """

        return "accounts.parquet"

    def _apply_transforms(self, data: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
        """
        Apply core account cleaning transformations.

        Args:
            data: Bronze accounts DataFrame.

        Returns:
            Cleaned accounts DataFrame.
        """

        accounts_df = data

        if "customer_id" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(pl.col("customer_id").cast(pl.Int64, strict=False).alias("customer_id"))

        if "account_type" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(pl.col("account_type").str.strip_chars().str.to_titlecase().alias("account_type"))

        if "country" in accounts_df.columns:
            accounts_df = accounts_df.with_columns(pl.col("country").str.strip_chars().str.to_uppercase().alias("country"))

        # dropping null and duplicated customer accounts
        accounts_df = accounts_df.filter(pl.col("customer_id").is_not_null())
        accounts_df = accounts_df.unique(subset=["customer_id"], keep="first")

        return accounts_df.sort("customer_id")
