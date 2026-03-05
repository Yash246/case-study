"""
Silver layer transformation for product data (JSON flattening).
"""

import logging
import polars as pl
from pathlib import Path
from typing import Any

from .data_transformation import DataTransformation

logger = logging.getLogger(__name__)


class ProductTransformation(DataTransformation):
    """
    Flatten nested product data JSON into a tabular Silver structure.

    Args:
        silver_folder: Output folder for the Silver products Parquet file.
    """

    def __init__(self, silver_folder: Path):

        super().__init__(silver_folder)

    def _get_output_filename(self) -> str:
        """
        Return the Silver products output filename.
        """

        return "products.parquet"

    def _apply_transforms(self, data: list[dict], **kwargs: Any) -> pl.DataFrame:
        """
        Flatten each product dictionary into a single flat row.

        Args:
            data: Raw product records from the JSON ingestion.
            **kwargs: Unused.

        Returns:
            DataFrame with one row per product.
        """

        rows = []
        for idx, product in enumerate(data):
            logger.debug(
                "Flattening product %d/%d: id=%s, name=%s",
                idx + 1, len(data), product.get("product_id"), product.get("name"),
            )
            rows.append(self._flatten_one(product))

        product_df = pl.DataFrame(rows, infer_schema_length=50)

        return product_df

    def flatten(self, raw_products: list[dict]) -> pl.DataFrame:
        """
        alias for base transform().

        Args:
            raw_products: Raw product records.

        Returns:
            Flat Silver products dataframe.
        """

        return self.transform(raw_products)

    @staticmethod
    def _flatten_one(product: dict) -> dict:
        """
        Flatten a single nested product dictionary into a flat dict.
        """

        flat_record: dict = {
            "product_id": product["product_id"],
            "product_name": product["name"],
            "category": product["category"],
        }

        # product specifications extraction
        specs = product.get("specs", {})

        # product dimensions extraction
        dims = specs.get("dimensions", {})
        flat_record["height"] = dims.get("height")
        flat_record["width"] = dims.get("width")
        flat_record["depth"] = dims.get("depth")
        flat_record["diameter"] = dims.get("diameter")

        # product storage info extraction
        storage = specs.get("storage", {})
        caps = storage.get("capacity_gb")
        if isinstance(caps, list):
            flat_record["storage_options_gb"] = ",".join(str(c) for c in caps)
            flat_record["max_storage_gb"] = max(caps)
        elif isinstance(caps, (int, float)):
            flat_record["storage_options_gb"] = str(int(caps))
            flat_record["max_storage_gb"] = int(caps)
        else:
            flat_record["storage_options_gb"] = None
            flat_record["max_storage_gb"] = None
        flat_record["storage_type"] = storage.get("type")

        # product battery info extraction
        battery = specs.get("battery", {})
        flat_record["battery_capacity_mah"] = battery.get("capacity_mAh")
        flat_record["fast_charging"] = battery.get("fast_charging")
        flat_record["playback_hours"] = battery.get("playback_hours")

        # product screen info extraction
        screen = specs.get("screen", {})
        flat_record["screen_type"] = screen.get("type")
        flat_record["screen_size_inches"] = screen.get("size_inches")

        # product processor extraction
        proc = specs.get("processor", {})
        flat_record["processor_brand"] = proc.get("brand")
        flat_record["processor_model"] = proc.get("model")

        # product RAM memory extraction
        flat_record["ram_gb"] = specs.get("ram_gb")

        # product sensors, connectivity extraction
        flat_record["sensors"] = ",".join(specs.get("sensors", []))
        flat_record["ports"] = ",".join(specs.get("ports", []))
        flat_record["features"] = ",".join(specs.get("features", []))
        flat_record["connectivity"] = ",".join(specs.get("connectivity", []))
        flat_record["compatibility"] = ",".join(specs.get("compatibility", []))

        # product pricing extraction
        pricing = product.get("pricing", {})
        flat_record["base_price"] = pricing.get("base_price")
        flat_record["currency"] = pricing.get("currency")

        # product discounts extraction
        discounts = pricing.get("discounts", [])
        flat_record["discount_count"] = len(discounts)
        flat_record["total_discount"] = sum(d.get("amount", 0) for d in discounts)
        flat_record["discount_types"] = ",".join(d.get("type", "") for d in discounts)
        flat_record["effective_price"] = (flat_record["base_price"] or 0) - flat_record["total_discount"]

        return flat_record
