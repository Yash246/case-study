"""
Silver layer transformation for orders data.
"""

import logging
import polars as pl
from pathlib import Path
from typing import Any

from .data_transformation import DataTransformation

logger = logging.getLogger(__name__)


class OrdersTransformation(DataTransformation):
    """
    Transform Bronze orders data into Silver table.

    Args:
        silver_folder: Output folder for the Silver orders parquet file.
    """

    def __init__(self, silver_folder: Path):

        super().__init__(silver_folder)

    def _get_output_filename(self) -> str:
        """
        Return the Silver orders output filename.
        """

        return "orders.parquet"

    def _apply_transforms(self, data: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
        """
        Apply core order cleaning transformations.

        Args:
            data: Bronze orders DataFrame.

        Returns:
            Cleaned orders dataframe.
        """

        orders_df = data
        logger.info("Applying orders transformations")

        orders_df = self._cast_types(orders_df)
        orders_df = self._parse_order_dates(orders_df)

        if "order_status" in orders_df.columns:
            orders_df = orders_df.with_columns(pl.col("order_status").str.to_uppercase().str.strip_chars().alias("order_status"))

        # remove dupe orders
        orders_df = orders_df.unique(subset=["order_id"], keep="first")

        return orders_df.sort("order_id")

    def _post_transform(self, orders_df: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
        """
        Data Enhancement
        """

        exprs = []
        
        # Add year, month, quarter columns
        if "order_date_parsed" in orders_df.columns:
            exprs.extend([
                pl.col("order_date_parsed").dt.year().alias("order_year"),
                pl.col("order_date_parsed").dt.month().alias("order_month"),
                (pl.lit("Q") + pl.col("order_date_parsed").dt.quarter().cast(pl.Utf8)).alias("order_quarter"),
            ])
        
        # Add cancellation flag
        if "order_status" in orders_df.columns:
            exprs.append(pl.col("order_status").is_in(["CANCELLED", "REFUNDED"]).alias("is_cancelled"))
        if exprs:
            orders_df = orders_df.with_columns(exprs)

        return orders_df

    def _cast_types(orders_df: pl.DataFrame) -> pl.DataFrame:
        """
        Cast string columns to appropriate types.
        """

        cast_exprs = []

        if "customer_id" in orders_df.columns:
            cast_exprs.append(pl.col("customer_id").cast(pl.Int64, strict=False).alias("customer_id"))

        if "order_amount" in orders_df.columns:
            cast_exprs.append(pl.col("order_amount").cast(pl.Float64, strict=False).alias("order_amount"))
        
        if cast_exprs:
            orders_df = orders_df.with_columns(cast_exprs)

        return orders_df

    def _parse_order_dates(orders_df: pl.DataFrame) -> pl.DataFrame:
        """
        Parse mixed format date strings into a standardized datetime column.
        """

        if "order_date" not in orders_df.columns:
            return orders_df

        orders_df = orders_df.with_columns(
            pl.col("order_date").str.strip_chars().str.to_datetime("%Y-%m-%d", strict=False).alias("order_date_parsed")
        )

        orders_df = orders_df.with_columns(
            pl.when(pl.col("order_date_parsed").is_null())
            .then(pl.col("order_date").str.strip_chars().str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False))
            .otherwise(pl.col("order_date_parsed"))
            .alias("order_date_parsed")
        )

        return orders_df
