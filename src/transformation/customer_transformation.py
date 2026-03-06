"""
Silver layer transformation for customer data.
"""

import logging
import polars as pl
from pathlib import Path
from typing import Any

from .data_transformation import DataTransformation

logger = logging.getLogger(__name__)


class CustomerTransformation(DataTransformation):
    """
    Transform Bronze customer data into Silver table.

    Applies name/email standardisation, deduplication and
    derived column enrichment.

    Args:
        silver_folder: Output folder for the Silver customer parquet file.
    """

    def __init__(self, silver_folder: Path):

        super().__init__(silver_folder)

    def _get_output_filename(self) -> str:
        """
        Return the Silver customer output filename.
        """

        return "customers.parquet"

    def _apply_transforms(self, data: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
        """
        Apply core customer cleaning transformations.

        Steps:
            1. Standardise name casing -> Title Case.
            2. Standardise email casing -> lowercase.
            3. Deduplicate on customer_id keeping the newest.

        Args:
            data: Bronze customer dataframe.

        Returns:
            Cleaned customer DataFrame.
        """

        logger.info("Applying customer transformations")

        customer_df = self._standardise_names(data)
        logger.debug("Name standardisation complete")

        customer_df = self._standardise_emails(customer_df)
        logger.debug("Email standardisation complete")

        before = customer_df.shape[0]
        customer_df = self._deduplicate(customer_df)
        removed = before - customer_df.shape[0]

        if removed > 0:
            logger.info(f"Deduplication removed {removed} rows")
        else:
            logger.debug("No duplicates found during deduplication")

        return customer_df

    def _post_transform(self, customer_df: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
        """
        Add derived analytical columns after core cleaning.

        Columns added: age_group, email_domain and region.

        Args:
            customer_df: Cleaned customer DataFrame.

        Returns:
            Enriched customer Dadataframe with derived columns.
        """

        logger.debug("Enhancing customer data....")

        result = self._add_derived_columns(customer_df)

        return result

    @staticmethod
    def _standardise_names(customer_df: pl.DataFrame) -> pl.DataFrame:
        """
        Convert the name column to title case.

        Args:
            customer_df: DataFrame with a name column.

        Returns:
            DataFrame with name in title case.
        """

        return customer_df.with_columns(pl.col("name").str.to_titlecase().alias("name"))

    @staticmethod
    def _standardise_emails(customer_df: pl.DataFrame) -> pl.DataFrame:
        """
        Convert the email column to lowercase.

        Args:
            customer_df: DataFrame with an email column.

        Returns:
            DataFrame with email in lowercase.
        """

        return customer_df.with_columns(pl.col("email").str.to_lowercase().alias("email"))

    @staticmethod
    def _deduplicate(customer_df: pl.DataFrame) -> pl.DataFrame:
        """
        Remove duplicate customer_id rows by keeping the latest.

        Args:
            customer_df: dataframe containing duplicate customer ids.

        Returns:
            Deduplicated DataFrame sorted by customer_id.
        """

        return (
            customer_df.sort("last_updated", descending=True)
            .unique(subset=["customer_id"], keep="first")
            .sort("customer_id")
        )

    @staticmethod
    def _add_derived_columns(customer_df: pl.DataFrame) -> pl.DataFrame:
        """
        Data enhancement

        Columns created:
            - age_group: Bucketed age range string.
            - email_domain: Domain part of the email address.
            - region: Geographic region mapped from country.

        Args:
            customer_df: Cleaned customer DataFrame.

        Returns:
            DataFrame with three new derived columns appended.
        """

        customer_df = customer_df.with_columns(
            pl.when(pl.col("age") < 25).then(pl.lit("18-24"))
            .when(pl.col("age") < 35).then(pl.lit("25-34"))
            .when(pl.col("age") < 45).then(pl.lit("35-44"))
            .when(pl.col("age") < 55).then(pl.lit("45-54"))
            .when(pl.col("age") < 65).then(pl.lit("55-64"))
            .otherwise(pl.lit("65+"))
            .alias("age_group"),
            pl.col("email").str.split("@").list.get(1).alias("email_domain"),
            pl.when(pl.col("country").is_in(["United States", "Canada", "Brazil"]))
            .then(pl.lit("Americas"))
            .when(pl.col("country").is_in([
                "United Kingdom", "France", "Germany", "Italy", "Spain",
                "Sweden", "Netherlands", "Ireland", "Switzerland",
            ])).then(pl.lit("Europe"))
            .when(pl.col("country").is_in([
                "India", "Japan", "Singapore", "UAE", "South Korea",
            ])).then(pl.lit("Asia"))
            .when(pl.col("country").is_in(["Australia"])).then(pl.lit("Oceania"))
            .when(pl.col("country").is_in(["South Africa"])).then(pl.lit("Africa"))
            .otherwise(pl.lit("Other"))
            .alias("region"),
        )

        return customer_df
