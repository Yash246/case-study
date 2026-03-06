"""
Silver layer transformation for sales history data.
"""

import logging
import polars as pl
from pathlib import Path
from datetime import date
from typing import Any

from .data_transformation import DataTransformation

logger = logging.getLogger(__name__)

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


class SalesTransformation(DataTransformation):
    """
    Transform Bronze sales history data into Silver table.

    Args:
        silver_folder: Output folder for the Silver sales Parquet file.
        max_valid_date: Latest date considered valid. Defaults to 2026-12-31)``.
    """

    def __init__(self, silver_folder: Path, max_valid_date: date = date(2026, 12, 31)):

        super().__init__(silver_folder)
        self._max_valid_date = max_valid_date

    def _get_output_filename(self) -> str:
        """
        Return the Silver sales output filename.
        """

        return "sales_history.parquet"

    def _apply_transforms(self, data: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
        """
        Apply core sales cleaning transformations.

        Args:
            data: Bronze sales DataFrame.

        Returns:
            Cleaned sales DataFrame.
        """

        sales_df = data

        sales_df = self._cast_types(sales_df)

        if "channel" in sales_df.columns:
            sales_df = sales_df.with_columns(pl.col("channel").str.strip_chars().str.to_titlecase().alias("channel"))

        # Standardize datetime format and flag future dates (post 2026)
        sales_df = self._parse_spend_dates(sales_df)
        sales_df = self._flag_future_dates(sales_df)

        sales_df = sales_df.filter(pl.col("customer_id").is_not_null())

        # drop dupe sale records
        dedup_cols = ["customer_id", "sales_amount"]
        if "spend_date_parsed" in sales_df.columns:
            dedup_cols.append("spend_date_parsed")
        sales_df = sales_df.unique(subset=dedup_cols, keep="first")

        return sales_df

    def _post_transform(self, sales_df: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
        """
        Data enhancement
        """

        exprs = []

        # add additional date info and valid date flag 
        if "spend_date_parsed" in sales_df.columns:
            exprs.extend([
                pl.col("spend_date_parsed").dt.year().alias("spend_year"),
                pl.col("spend_date_parsed").dt.month().alias("spend_month"),
                (pl.lit("Q") + pl.col("spend_date_parsed").dt.quarter().cast(pl.Utf8)).alias("spend_quarter"),
                pl.col("spend_date_parsed").is_not_null().alias("has_valid_date"),
            ])

        # valid amount flag
        if "sales_amount" in sales_df.columns:
            exprs.append(pl.col("sales_amount").is_not_null().alias("has_valid_amount"))

        if exprs:
            sales_df = sales_df.with_columns(exprs)

        return sales_df

    def _cast_types(self, sales_df: pl.DataFrame) -> pl.DataFrame:
        """
        Cast string columns to appropriate numeric types.
        """

        cast_exprs = []

        if "customer_id" in sales_df.columns:
            cast_exprs.append(pl.col("customer_id").cast(pl.Int64, strict=False).alias("customer_id"))

        if "sales_amount" in sales_df.columns:
            cast_exprs.append(pl.col("sales_amount").cast(pl.Float64, strict=False).alias("sales_amount"))

        if cast_exprs:
            sales_df = sales_df.with_columns(cast_exprs)

        return sales_df

    def _parse_spend_dates(self, sales_df: pl.DataFrame) -> pl.DataFrame:
        """
        Parse the multi format spend column into a standar Datetime.
        """

        if "spend" not in sales_df.columns:
            return sales_df

        sales_df = sales_df.with_columns(
            pl.col("spend").str.strip_chars().str.replace_all('"', "").alias("spend_clean")
        )

        sales_df = sales_df.with_columns(
            pl.col("spend_clean").str.to_datetime("%Y-%m-%d", strict=False).alias("spend_date_parsed")
        )

        sales_df = sales_df.with_columns(
            pl.when(pl.col("spend_date_parsed").is_null())
            .then(pl.col("spend_clean").str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False))
            .otherwise(pl.col("spend_date_parsed"))
            .alias("spend_date_parsed")
        )

        sales_df = sales_df.with_columns(
            pl.when(pl.col("spend_date_parsed").is_null())
            .then(pl.col("spend_clean").str.to_datetime("%m/%d/%Y", strict=False))
            .otherwise(pl.col("spend_date_parsed"))
            .alias("spend_date_parsed")
        )

        # convert date which is in long date format, ex - November 10, 2026
        spend_series = sales_df["spend_clean"].to_list()
        parsed_series = sales_df["spend_date_parsed"].to_list()
        for idx, (raw, parsed) in enumerate(zip(spend_series, parsed_series)):
            if parsed is not None or raw is None:
                continue
            try:
                raw_str = str(raw).strip().strip('"')
                parts = raw_str.replace(",", "").split()
                if len(parts) == 3:
                    month_name = parts[0].lower()
                    if month_name in MONTH_MAP:
                        day = int(parts[1])
                        year = int(parts[2])
                        parsed_series[idx] = pl.Series(
                            [f"{year}-{MONTH_MAP[month_name]:02d}-{day:02d}"]
                        ).str.to_datetime("%Y-%m-%d", strict=False)[0]
            except (ValueError, IndexError, KeyError):
                pass

        sales_df = sales_df.with_columns(pl.Series("spend_date_parsed", parsed_series, dtype=pl.Datetime))
        sales_df = sales_df.drop("spend_clean")

        return sales_df

    def _flag_future_dates(self, sales_df: pl.DataFrame) -> pl.DataFrame:
        """
        Flag records with dates beyond the maximum
        """

        if "spend_date_parsed" not in sales_df.columns:
            return sales_df

        max_dt = pl.lit(self._max_valid_date).cast(pl.Datetime)

        sales_df = sales_df.with_columns((pl.col("spend_date_parsed") > max_dt).alias("is_future_date"))

        future_count = sales_df.select(pl.col("is_future_date").sum()).item()
        if future_count > 0:
            logger.warning(f"{future_count} sales records have future dates")

        return sales_df
