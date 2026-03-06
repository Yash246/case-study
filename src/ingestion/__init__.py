"""
Data ingestion modules (Bronze layer).

Each ingestion class reads a specific source format, validates the input,
and saves a raw snapshot to the Bronze layer as parquet.

Classes:
    DataIngestion: Abstract base defining the ingestion module.
    CSVIngestion: CSV files with watermark based incremental loads.
    JSONIngestion: JSON array files with structure validation.
    XLSXIngestion: Excel workbooks with per sheet extraction.
"""

from .data_ingestion import DataIngestion
from .csv_ingestion import CSVIngestion
from .json_ingestion import JSONIngestion
from .xlsx_ingestion import XLSXIngestion

__all__ = [
    "DataIngestion",
    "CSVIngestion",
    "JSONIngestion",
    "XLSXIngestion"
]
