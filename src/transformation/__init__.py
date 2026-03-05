"""
Data transformation modules (Silver and Gold layers).

Provides transformation classes that clean, standardise, flatten and
join data produced by the ingestion layer.

Classes:
    DataTransformation: Base class defining the transformation module.
    CustomerTransformation: Bronze -> Silver:  customer data cleaning and enrichment.
    ProductTransformation: Nested JSON -> flat tabular Silver structure: product data.
    OrdersTransformation: Bronze -> Silver: orders data cleaning and enrichment.
    AccountsTransformation: Bronze -> Silver: accounts data cleaning.
    SalesTransformation: Bronze -> Silver: sales data history cleaning.
    GoldBuilder: Joins Silver tables into a unified Gold table for reporting.
"""

from .data_transformation import DataTransformation
from .customer_transformation import CustomerTransformation
from .product_transformation import ProductTransformation
from .orders_transformation import OrdersTransformation
from .accounts_transformation import AccountsTransformation
from .sales_transformation import SalesTransformation

__all__ = [
    "BaseTransformation",
    "CustomerTransformation",
    "JSONFlattening",
    "OrdersTransformation",
    "AccountsTransformation",
    "SalesTransformation"
]