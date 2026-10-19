# End-to-End FMCG Data Engineering Pipeline (Databricks)

## Overview

This project implements a production-style Databricks Lakehouse pipeline to unify analytics across a parent FMCG enterprise and an acquired startup.

The solution supports historical backfill, daily incremental ingestion, data quality enforcement, and BI-ready analytics.

## Repository Structure

```
consolidated_pipeline/
├── 1_setup/                          # Catalogs, schemas, date dimension
│   ├── setup_catalogs.py             # Initialize Unity Catalog structure
│   ├── dim_date_table_creation.py    # Monthly grain date dimension
│   └── utilities.py                  # Shared configuration & schema names
│
├── 2_dimension_data_processing/      # Customer, product, pricing pipelines
│   ├── 1_customer_data_processing.py # Customer dimension (Bronze → Silver → Gold)
│   ├── 2_products_data_processing.py # Product dimension (Bronze → Silver → Gold)
│   └── 3_pricing_data_processing.py  # Pricing dimension with temporal logic
│
└── 3_fact_data_processing/           # Full + incremental order processing
    ├── 1_full_load_fact.py           # Historical backfill of orders
    └── 2_incremental_load_fact.py    # Daily incremental order ingestion
```

## Architecture

**Medallion Architecture (Bronze → Silver → Gold)**

```
S3 or UC Volume → Bronze (Raw) → Silver (Cleansed) → Gold (Analytics Ready)
                                      ↓
                              Staging Tables
                          (Incremental Processing)
                                      ↓
                    Consolidated Parent Gold Tables
                                      ↓
                    BI Dashboards / Databricks Genie
```

- **Bronze**: Raw data ingestion from S3 or a Unity Catalog Volume, metadata capture
- **Silver**: Data quality checks, deduplication, schema standardization
- **Gold**: Business-ready facts and dimensions, monthly grain aggregation
- **Staging**: Supports idempotent incremental loads with MERGE operations

## Key Capabilities

✅ **Automated Data Contracts & CI**
- Validates required order columns before Databricks ingestion
- Normalizes all supported source date formats
- Flags invalid customer/product IDs, missing or non-positive quantities, and duplicate business keys
- Emits a machine-readable JSON quality report and supports a minimum-valid-rate deployment gate
- Runs contract tests on every push and pull request through GitHub Actions

✅ **Quarantine & Quality Observability**
- Applies one shared PySpark contract to both full and incremental order loads
- Preserves rejected records in `silver.quarantine_orders` with stable reason codes
- Records run ID, load type, row counts, valid rate, and timestamp in `silver.order_quality_metrics`
- Quarantines unknown product keys before fact-table joins instead of silently losing them
- Keeps malformed customer IDs intact for investigation instead of replacing them with a sentinel value

✅ **Slowly Changing Dimensions (SCD Type 2)**
- Preserves customer and product attribute history in dedicated Delta tables
- Uses stable business keys plus SHA-256 attribute hashes for change detection
- Expires the previous version and inserts a new current version in one staged `MERGE`
- Adds `dimension_sk`, `effective_from`, `effective_to`, and `is_current` fields
- Treats unchanged source replays as no-ops, with deterministic CI tests for idempotency

✅ **Deployment as Code**
- Defines manual historical and scheduled incremental Lakeflow Jobs in a Databricks bundle
- Encodes task dependencies so facts cannot run before required dimensions
- Isolates development in `fmcg_dev` with its schedule paused
- Enables the production daily workflow at 02:00 UTC
- Parameterizes the Unity Catalog, source URI, and date range across every notebook
- Includes an OIDC-based manual GitHub deployment workflow without long-lived tokens

✅ **Batch Backfill + Daily Incremental Processing**
- Full historical load for dimensions and facts
- Delta incremental ingestion with idempotent staging tables

✅ **Data Quality & Cleansing**
- Duplicate detection and handling
- Schema mismatch resolution
- Invalid ID filtering
- Metadata tracking (file_name, file_size, read_timestamp)

✅ **Delta Lake & ACID Guarantees**
- MERGE operations for idempotent upserts
- Change Data Feed enabled for data lineage
- Time-travel friendly design

✅ **Enterprise Ready**
- Databricks Jobs orchestration compatible
- Parameterized notebooks (catalog, data_source widgets)
- Monthly grain aggregation for reporting
- Parent-child consolidated table strategy

✅ **Analytics & Insights**
- Delta Lake optimization for BI queries
- Databricks Genie AI-driven dashboards
- Fact table joins across customer, product, pricing dimensions

## Tech Stack

- **Databricks** – Unified data & AI platform
- **PySpark** – Distributed data processing
- **SQL** – Data transformation & analytics
- **Delta Lake** – ACID transactional data lake
- **AWS S3** – Data storage & orchestration
- **Databricks Workflows** – Job scheduling & orchestration

## Getting Started

### Prerequisites

- Databricks Workspace (Free Edition supported)
- Databricks managed Volume for Free Edition, or an S3 landing zone for production
- Sample FMCG data (customers, products, gross_price, orders CSVs)

### Setup

0. **Run the local landing-zone quality gate**
   ```bash
   python scripts/audit_orders.py \
     '0_data/2_child_company/full_load/orders/landing/*.csv' \
     --output reports/full-load-quality.json \
     --fail-under 0.80
   ```

   The raw `0_data/` folder is intentionally kept local. CI tests the same
   contract against controlled fixtures without requiring S3 or Databricks.

   Baseline audit of the local full-load landing zone:

   | Metric | Result |
   |---|---:|
   | Files scanned | 151 |
   | Rows scanned | 51,810 |
   | Valid rows | 40,997 (79.13%) |
   | Duplicate records | 3,823 |
   | Invalid customer IDs | 5,223 |
   | Missing/invalid quantities | 2,607 |

   The current source data intentionally fails the 80% quality gate. This
   establishes a reproducible baseline for quarantine and remediation rather
   than silently converting malformed keys to fallback IDs.

1. **Initialize Catalog & Schemas**
   ```
   Run: 1_setup/setup_catalogs.py
   ```

2. **Create Date Dimension**
   ```
   Run: 1_setup/dim_date_table_creation.py
   ```

3. **Load Dimensions** (in order)
   ```
   Run: 2_dimension_data_processing/1_customer_data_processing.py
   Run: 2_dimension_data_processing/2_products_data_processing.py
   Run: 2_dimension_data_processing/3_pricing_data_processing.py
   ```

4. **Load Facts**
   ```
   Run: 3_fact_data_processing/1_full_load_fact.py (first time)
   Run: 3_fact_data_processing/2_incremental_load_fact.py (daily)
   ```

### Databricks Workflows

Create a workflow with the following order:

```
┌─ Setup (setup_catalogs.py)
├─ Date Dimension (dim_date_table_creation.py)
├─ Dimensions (customer → products → pricing, in parallel)
├─ Full Fact Load (1_full_load_fact.py)
└─ Incremental Fact Load (2_incremental_load_fact.py) [runs daily]
```

The workflow is already encoded in `databricks.yml` and `resources/jobs.yml`.
Deploy and run the development target with:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run --target dev fmcg_prepare_storage
./scripts/upload_demo_data.sh
databricks bundle run --target dev fmcg_initial_load
```

The development target uses the managed Volume
`/Volumes/fmcg_dev/bronze/landing`, so the portfolio demo does not require an
AWS account. Production retains the `s3://sportsbar-analytics` default.

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for production deployment,
variable overrides, schedule behavior, and GitHub OIDC configuration.

## Data Model

### Dimension Tables

| Table | Grain | Key Columns |
|-------|-------|-------------|
| `sb_dim_customers` | Customer | customer_id, name, email |
| `sb_dim_products` | Product | product_id, product_name, category |
| `sb_dim_customers_history` | Customer version | dimension_sk, customer_id, effective_from, effective_to, is_current |
| `sb_dim_products_history` | Product version | dimension_sk, product_id, effective_from, effective_to, is_current |
| `sb_dim_pricing` | Product × Effective Date | product_id, effective_date, price |
| `sb_dim_date` | Month | date_key, year, quarter |

### Fact Table

| Table | Grain | Key Columns |
|-------|-------|-------------|
| `sb_fact_orders` | Order | order_id, customer_id, product_id, order_date |

## Key Features

### Change Data Feed (CDF)

Enabled on all Bronze and Silver tables for:
- Data lineage tracking
- Downstream analytics
- Audit trails

```python
.option("delta.enableChangeDataFeed", "true")
```

### Idempotent Processing

Staging tables support duplicate runs:

```python
DeltaTable.forName(spark, gold_table)
    .merge(staging_df, "ON pk = pk_staging")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
```

### Query Dimension History

```sql
SELECT customer_id, customer, city, effective_from, effective_to, is_current
FROM fmcg.gold.sb_dim_customers_history
WHERE customer_id = '789403'
ORDER BY effective_from;
```

The existing `sb_dim_customers` and `sb_dim_products` tables remain current-state
snapshots for compatibility. Their corresponding `_history` tables are the
authoritative SCD Type 2 records.

### Parametrized Notebooks

Each notebook accepts Databricks widgets:

```
catalog: fmcg (default)
data_source: customers | products | gross_price | orders
```

## Monitoring & Troubleshooting

### View Pipeline Quality Metrics

```sql
SELECT *
FROM fmcg.silver.order_quality_metrics
ORDER BY measured_at DESC;
```

### Investigate Quarantined Records

```sql
SELECT reason, COUNT(*) AS rejected_rows
FROM (
  SELECT EXPLODE(quarantine_reasons) AS reason
  FROM fmcg.silver.quarantine_orders
)
GROUP BY reason
ORDER BY rejected_rows DESC;
```

Each quarantined record retains its raw date, source file metadata,
`pipeline_run_id`, `load_type`, and `quarantined_at` timestamp.

### Check Data Quality

```sql
SELECT COUNT(*), COUNT(DISTINCT customer_id)
FROM fmcg.silver.customers;
```

### View Change Data Feed

```sql
SELECT * FROM table_changes(fmcg.bronze.customers, 0);
```

### Inspect Staging Tables

```sql
SELECT * FROM fmcg.silver.customers_staging LIMIT 5;
```

## Notes

- **Built on Databricks Free Edition** – No additional licenses required
- **Data Anonymized** – Sample schemas for demonstration only
- **Portable Landing Zone** – Development uses a managed Volume; production defaults to `s3://sportsbar-analytics/{data_source}/`
- **Monthly Grain** – Facts aggregated to monthly level for enterprise reporting

## Future Enhancements

- Real-time streaming with Kafka → Bronze
- dbt integration for transformation orchestration
- Fine-grained Unity Catalog grants for multi-team deployments

## License

This project is provided as-is for educational and portfolio purposes.

---

**Author:** Mounusha Ram Metti
