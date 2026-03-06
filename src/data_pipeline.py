"""
End to end pipeline orchestrator
Coordinates the full Bronze -> Silver -> Gold data flow 
"""

import logging
import polars as pl
from pathlib import Path
from src.config import PipelineConfig
from src.ingestion import CSVIngestion, JSONIngestion, XLSXIngestion
from src.validation import DataProfiler
from src.transformation import (
    CustomerTransformation,
    ProductTransformation,
    OrdersTransformation,
    AccountsTransformation,
    SalesTransformation,
    GoldBuilder,
)

logger = logging.getLogger(__name__)


class DataPipeline:
    """
    Orchestrate the full data processing pipeline.

    Executes seven stages in order:
        1. CSV baseline + incremental customer data ingestion.
        2. JSON product data ingestion.
        3. XLSX data (orders + accounts + sales) ingestion.
        4. Data quality checks (Bronze layer).
        5. Silver transformations.
        6. Data quality checks (Silver layer).
        7. Gold unified table build.

    Args:
        config: Loaded pipeline configuration.
    """

    def __init__(self, config: PipelineConfig):

        self.cfg = config
        logger.info(f"Pipeline initialised with config paths: {config.paths}")

    def run(self) -> pl.DataFrame:
        """
        Execute the full Bronze -> Silver -> Gold pipeline.

        Returns:
            The final Gold table.
        """

        logger.info("DATA PIPELINE — START")

        # 1. BRONZE: CSV customer ingestion
        logger.info("[1/7] Ingesting CSV baseline...")
        csv_ingestion = CSVIngestion(
            raw_folder=self.cfg.raw_folder(),
            bronze_folder=self.cfg.bronze_folder(),
            watermark_folder=self.cfg.watermark_folder(),
        )

        customers_df = csv_ingestion.ingest_baseline(self.cfg.csv_files["baseline"])
        logger.info(f"[1/7] Baseline loaded: {customers_df.shape[0]} rows")

        logger.info("[1/7] Applying incremental loads...")
        for inc_file in self.cfg.csv_files["incremental"]:
            before = customers_df.height
            customers_df = csv_ingestion.ingest_incremental(inc_file, customers_df)

        # 2. BRONZE: JSON product ingestion
        logger.info("[2/7] Ingesting JSON products...")
        json_ingestion = JSONIngestion(
            raw_folder=self.cfg.raw_folder(),
            bronze_folder=self.cfg.bronze_folder(),
        )

        raw_products = json_ingestion.ingest(self.cfg.json_files[0])

        # 3. BRONZE: XLSX ingestion
        logger.info("[3/7] Ingesting XLSX...")
        xlsx_tables = self._ingest_sheets()
        for name, tbl in xlsx_tables.items():
            logger.info(f"[3/7] Sheet {name}")

        # 4. QUALITY CHECKS (Bronze)
        logger.info("[4/7] Running Bronze data quality checks...")
        self._run_bronze_quality_checks(customers_df, xlsx_tables)

        # 5. SILVER: transformations
        logger.info("[5/7] Transforming to Silver layer...")
        silver_folder = self.cfg.silver_folder()

        cust_transformation = CustomerTransformation(silver_folder)
        silver_customers = cust_transformation.transform(customers_df)
        logger.info(f"[5/7] Silver customers: {silver_customers.shape[0]} rows")

        json_flattening = ProductTransformation(silver_folder)
        silver_products = json_flattening.flatten(raw_products)
        logger.info(f"[5/7] Silver products: {silver_products.shape[0]} rows")

        silver_orders = pl.DataFrame()
        if "orders" in xlsx_tables:
            orders_transformation = OrdersTransformation(silver_folder)
            silver_orders = orders_transformation.transform(xlsx_tables["orders"])
            logger.info(f"[5/7] Silver orders: {silver_orders.shape[0]} rows")

        silver_accounts = None
        if "accounts" in xlsx_tables:
            accounts_transformation = AccountsTransformation(silver_folder)
            silver_accounts = accounts_transformation.transform(xlsx_tables["accounts"])
            logger.info(f"[5/7] Silver accounts: {silver_accounts.shape[0]} rows")

        silver_sales = None
        if "sales_history" in xlsx_tables:
            sales_transformation = SalesTransformation(silver_folder)
            silver_sales = sales_transformation.transform(xlsx_tables["sales_history"])
            logger.info(f"[5/7] Silver sales: {silver_sales.shape[0]} rows")

        # 6. QUALITY CHECKS (Silver)
        logger.info("[6/7] Running Silver data quality checks...")
        self._run_silver_quality_checks(silver_customers, silver_orders, silver_accounts, silver_sales)

        # 7. GOLD: unified table
        logger.info("[7/7] Building Gold unified tabl...")
        gold_builder = GoldBuilder(self.cfg.gold_folder())
        gold = gold_builder.build(
            customers=silver_customers,
            orders=silver_orders,
            products=silver_products,
            accounts=silver_accounts,
            sales_history=silver_sales,
        )
        logger.info(f"[7/7] Gold table: {gold.shape[0]} rows, {gold.shape[1]} columns")

        gold_checker = DataProfiler()
        gold_checker.run_checks(gold)

        logger.info("PIPELINE COMPLETE")
        logger.info("Output files:")
        logger.info(f"Bronze : {self.cfg.bronze_folder()}")
        logger.info(f"Silver : {self.cfg.silver_folder()}")
        logger.info(f"Gold   : {self.cfg.gold_folder()}")

        return gold

    def _ingest_sheets(self) -> dict[str, pl.DataFrame]:
        """
        Ingest XLSX sheets

        Returns:
            Mapping of sheet name to Bronze DataFrame.
        """

        xlsx_cfg = self.cfg.xlsx_files[0]
        xlsx_path = self.cfg.raw_folder() / xlsx_cfg["name"]

        if xlsx_path.exists():
            logger.info("Found XLSX file: %s", xlsx_path)
            xlsx_ingestion = XLSXIngestion(
                raw_folder=self.cfg.raw_folder(),
                bronze_folder=self.cfg.bronze_folder(),
            )

        return xlsx_ingestion.ingest(xlsx_cfg["name"], sheets=xlsx_cfg["sheets"])

    def _run_bronze_quality_checks(self, customers_df: pl.DataFrame, xlsx_tables: dict[str, pl.DataFrame]) -> None:
        """
        Run data quality checks on Bronze layer data.
        """

        cust_checker = DataProfiler(self.cfg.quality_rules.get("customer", {}))
        cust_checker.run_checks(customers_df, "customers_bronze", primary_key="customer_id")

        if "orders" in xlsx_tables:
            order_checker = DataProfiler(self.cfg.quality_rules.get("orders", {}))
            order_checker.run_checks(xlsx_tables["orders"], "orders_bronze", primary_key="order_id")

        if "accounts" in xlsx_tables:
            acct_checker = DataProfiler(self.cfg.quality_rules.get("accounts", {}))
            acct_checker.run_checks(xlsx_tables["accounts"], "accounts_bronze", primary_key="customer_id")

        if "sales_history" in xlsx_tables:
            sales_checker = DataProfiler(self.cfg.quality_rules.get("sales_history", {}))
            sales_checker.run_checks(xlsx_tables["sales_history"], "sales_bronze")

    def _run_silver_quality_checks(self, customers: pl.DataFrame, orders: pl.DataFrame,
        accounts: pl.DataFrame | None, sales: pl.DataFrame | None) -> None:
        """
        Run quality checks on Silver layer data.
        """

        silver_checker = DataProfiler()

        silver_checker.run_checks(customers, "customers_silver", primary_key="customer_id")

        silver_checker.run_checks(orders, "orders_silver", primary_key="order_id")

        if accounts is not None:
            silver_checker.run_checks(accounts, "accounts_silver", primary_key="customer_id")

        if sales is not None:
            silver_checker.run_checks(sales, "sales_silver", primary_key="customer_id")
