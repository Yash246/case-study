"""
Gold layer transformation - creates the unified analytical table.
"""

import logging
import polars as pl
from pathlib import Path
from typing import Any

from .data_transformation import DataTransformation

logger = logging.getLogger(__name__)


class GoldBuilder(DataTransformation):
    """
    Build the unified Gold analytical table.

    Args:
        gold_folder: Output folder for the Gold Parquet file.
    """

    def __init__(self, gold_folder: Path):

        super().__init__(gold_folder)

    def _get_output_filename(self) -> str:
        """
        Return the Gold unified table output filename.
        """

        return "report_data.parquet"

    def _apply_transforms(self, data: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
        """
        Join orders with customers, products, accounts and sales.

        Args:
            data: Silver orders DataFrame (pivot table).
            **kwargs: Required: customers, products, sales_history
                Optional: accounts

        Returns:
            Joined Gold (Report) dataframe.
        """

        orders_df = data
        customers_df: pl.DataFrame = kwargs["customers"]
        products_df: pl.DataFrame = kwargs["products"]
        sales_history_df: pl.DataFrame = kwargs["sales_history"]
        accounts_df: pl.DataFrame | None = kwargs.get("accounts")

        logger.info("Building Gold table")

        orders_df = self._cast_key(orders_df, "customer_id", pl.Int64)
        customers_df = self._cast_key(customers_df, "customer_id", pl.Int64)

        gold_df = orders_df.join(customers_df, on="customer_id", how="left", suffix="_cust")

        if "product_id" in gold_df.columns and "product_id" in products_df.columns:
            gold_df = gold_df.join(products_df, on="product_id", how="left", suffix="_prod")

        if sales_history is not None:
            sales_history = self._cast_key(sales_history, "customer_id", pl.Int64)

            agg_exprs = []

            # calculate sales stats by customer for insights/analysis
            if "sales_amount" in sales_history.columns:
                agg_exprs.extend([
                    pl.col("sales_amount").sum().alias("total_sales_amount"),
                    pl.col("sales_amount").mean().alias("avg_sales_amount"),
                    pl.col("sales_amount").count().alias("sales_transaction_count"),
                ])

            if agg_exprs:
                sales_agg = sales_history.group_by("customer_id").agg(agg_exprs)
                gold_df = gold_df.join(sales_agg, on="customer_id", how="left", suffix="_sale")

        return gold_df

    def _post_transform(self, gold_df: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
        """
        Data Enhancement
        """

        exprs = []

        if "order_amount" in gold_df.columns:
            exprs.append((pl.col("order_amount") > 1500).alias("is_high_value_order"))
        if "total_sales_amount" in gold_df.columns:
            exprs.append((pl.col("total_sales_amount") > 10000).alias("is_high_value_customer"))
        if exprs:
            result_df = gold_df.with_columns(exprs)

        return result_df

    def build(self, customers: pl.DataFrame, orders: pl.DataFrame, products: pl.DataFrame,
              sales_history: pl.DataFrame, accounts: pl.DataFrame | None = None) -> pl.DataFrame:
        """
        alias for base transform function -> to build the Gold table.
        """

        return self.transform(orders, customers=customers,products=products, sales_history=sales_history, accounts=accounts)

    @staticmethod
    def _cast_key(input_df: pl.DataFrame, col: str, dtype: pl.DataType) -> pl.DataFrame:
        """
        Cast a column to the specified data type if it differs.
        """

        if col in input_df.columns and input_df.schema[col] != dtype:
            input_df = input_df.with_columns(pl.col(col).cast(dtype, strict=False))

        return input_df
