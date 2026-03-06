# Data Pipeline for enterprise knowledge and analytics platform

This project contains a data pipeline which ingests data from multiple sources and prepares it for downstream workflows.

## Architecture

- **Bronze**: Raw ingested datasets written to parquet
- **Silver**: Cleaned/standardized datasets with typed columns and derived fields
- **Gold**: Unified analytical table built from joins/aggregations across Silver tables

## Repository Structure

- `src/` — pipeline implementation
  - `config.py` — YAML configuration loader (`config.yaml`) and typed path helpers
  - `ingestion/` — data ingestion into Bronze
    - `CSVIngestion` — baseline + incremental customer data ingestion (watermark driven)
    - `JSONIngestion` — JSON product data ingestion
    - `XLSXIngestion` — XLSX ingestion for orders/accounts/sales data
  - `transformation/` — transformations into Silver and Gold
    - `CustomerTransformation` — standardize customers, deduplicates, derived columns
    - `ProductTransformation` — flatten product JSON into tabular format
    - `OrdersTransformation` — cast types, parse dates, normalize status, deduplicates
    - `AccountsTransformation` — standardize and clean account records
    - `SalesTransformation` — parse spend dates, cast types, deduplicate/clean
    - `GoldBuilder` — build unified Gold table (joins + aggregations)
  - `validation/` — data quality checks
    - `DataProfiler` — schema, nulls, duplicates, empty rows
  - `data_pipeline.py` — end-to-end orchestrator (Bronze -> Silver -> Gold)

- `config/`
  - `config.yaml` — input paths, file lists, and quality rules

- `data/`
  - `raw/` — source datasets (CSV/JSON/XLSX)
  - `bronze/`, `silver/`, `gold/` — parquet outputs
  - `watermarks/` — incremental ingestion state for customer CSV loads

## Configuration

The pipeline is driven by `config/config.yaml`, which defines:
- Input/output paths (`raw`, `bronze`, `silver`, `gold`, `watermarks`)
- Source files (baseline + incremental CSVs, JSON files, XLSX sheets)
- Quality rules (required columns per dataset)

## How to Run

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Ensure the raw input data exists under the configured `paths.raw_data`.

3. Run the pipeline entrypoint you use in your project:
   ```bash
   python -c "from src.config import PipelineConfig; from src.data_pipeline import DataPipeline; cfg=PipelineConfig('config/config.yaml'); DataPipeline(cfg).run()"
   ```
   Can also be run via the integrates e2e tests.

Outputs are written to the configured `bronze/`, `silver/`, and `gold/` folders.

## Output Artifacts

**Bronze** (raw snapshots):
- `customers.parquet`
- `products_raw.parquet`
- `orders.parquet`
- `accounts.parquet`
- `sales_history.parquet`

**Silver** (cleaned/standardized):
- `customers.parquet`
- `products.parquet`
- `orders.parquet`
- `accounts.parquet`
- `sales_history.parquet`

**Gold** (unified analytics table):
- `report_table.parquet`

## Data Quality Checks

Validation is performed using `DataProfiler`:
- Required column checks (schema)
- Null counts
- Duplicate counts (by primary key if provided)
- Empty row counts (fully null rows)

Quality rules for required columns are configured in `config/config.yaml`.