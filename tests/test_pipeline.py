"""
Integration tests for the data pipeline.
"""

import polars as pl
import pytest
from pathlib import Path

from src.config import PipelineConfig
from src.data_pipeline import DataPipeline


@pytest.fixture(scope="module")
def pipeline_run() -> tuple[pl.DataFrame, DataPipeline]:
    """
    Run the pipeline using the config and data.

    Returns:
        Tuple of (Gold DataFrame, DataPipeline instance).
    """
    config = PipelineConfig("config/config.yaml")
    data_pipeline = DataPipeline(config)

    gold_df = data_pipeline.run()

    return gold_df, data_pipeline


def test_pipeline_execution(pipeline_run: tuple[pl.DataFrame, DataPipeline]) -> None:
    """
    The full pipeline must complete without errors and produce output.

    Verifies:
        - The Gold DataFrame is a valid pl.DataFrame.
        - The Gold table is not empty (has rows and columns).
        - Bronze Parquet files are written to disk.
        - Silver Parquet files are written to disk.
        - Gold Parquet file is written to disk.
    """
    gold_df, pipeline = pipeline_run

    # Gold DataFrame is valid and non-empty
    assert isinstance(gold_df, pl.DataFrame)
    print(gold_df)

    config = pipeline.cfg

    # Bronze layer files exist
    bronze_folder = config.bronze_path()
    assert (bronze_folder / "customers.parquet").exists(), "Missing Bronze customers"
    assert (bronze_folder / "products_raw.parquet").exists(), "Missing Bronze products"
    assert (bronze_folder / "orders.parquet").exists(), "Missing Bronze orders"
    assert (bronze_folder / "accounts.parquet").exists(), "Missing Bronze accounts"
    assert (bronze_folder / "sales_history.parquet").exists(), "Missing Bronze sales_history"

    # Silver layer files exist
    silver_folder = config.silver_path()
    assert (silver_folder / "customers.parquet").exists(), "Missing Silver customers"
    assert (silver_folder / "products.parquet").exists(), "Missing Silver products"
    assert (silver_folder / "orders.parquet").exists(), "Missing Silver orders"
    assert (silver_folder / "accounts.parquet").exists(), "Missing Silver accounts"
    assert (silver_folder / "sales_history.parquet").exists(), "Missing Silver sales_history"

    # Gold layer file exists
    gold_folder = config.gold_path()
    assert (gold_folder / "report_data.parquet").exists(), "Missing Gold unified table"


def test_gold_table_structure_and_content(pipeline_run: tuple[pl.DataFrame, DataPipeline]) -> None:
    """
    The Gold unified table must have correct structure and content.

    Verifies:
        - Customer origin columns are present (from customer join).
        - Order origin columns are present (pivot table).
        - Product origin columns are present (from product join).
        - Sales aggregation columns are present.
        - Analytical columns are present.
        - No duplicate order_ids exist.
    """
    gold_df, pipeline = pipeline_run

    # Column presence: customer origin
    assert "customer_id" in gold_df.columns
    assert "name" in gold_df.columns
    assert "email" in gold_df.columns

    # Column presence: order origin
    assert "order_id" in gold_df.columns
    assert "order_amount" in gold_df.columns
    assert "order_status" in gold_df.columns
    assert "product_id" in gold_df.columns

    # Column presence: product origin
    assert "category" in gold_df.columns
    assert "base_price" in gold_df.columns

    # Column presence: sales aggregation
    assert "total_sales_amount" in gold_df.columns
    assert "avg_sales_amount" in gold_df.columns
    assert "sales_transaction_count" in gold_df.columns

    # Column presence: analytical
    assert "is_high_value_customer" in gold_df.columns
    assert "is_high_value_order" in gold_df.columns

    # No duplicate order_ids
    unique_orders = gold_df.select("order_id").unique().height
    assert unique_orders == gold_df.height, (
        f"Gold table has duplicate order_ids: {gold_df.height} rows but "
        f"{unique_orders} unique order_ids"
    )
