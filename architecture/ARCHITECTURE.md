# Lakehouse Architecture

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                          S3 Landing Zone                              │
│          (customers.csv, products.csv, orders.csv, etc.)             │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw Data)                           │
│            customers | products | orders | gross_price              │
│        [Delta Lake, Change Data Feed enabled, Full History]         │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ↓               ↓               ↓
        ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
        │   SILVER     │  │   SILVER     │  │   SILVER     │
        │  customers   │  │  products    │  │ gross_price  │
        │  [Cleansed]  │  │  [Cleansed]  │  │  [Temporal]  │
        └──────────────┘  └──────────────┘  └──────────────┘
                    │               │               │
                    └───────────────┼───────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        │                           │                           │
        ↓                           ↓                           ↓
    ┌──────────────┐          ┌──────────────┐          ┌──────────────┐
    │ SILVER       │          │ SILVER       │          │ SILVER       │
    │ customers    │          │ products     │          │ gross_price  │
    │ (Staging)    │          │ (Staging)    │          │ (Staging)    │
    └──────────────┘          └──────────────┘          └──────────────┘
        │                           │                           │
        └───────────────┬───────────┴───────────┬───────────────┘
                        ↓                       ↓
                    ┌──────────────────────────────────┐
                    │      GOLD LAYER (Analytics)      │
                    │  sb_dim_customers                │
                    │  sb_dim_products                 │
                    │  sb_dim_pricing                  │
                    │  sb_dim_date                     │
                    │  [Upserted via MERGE]            │
                    └──────────────────────────────────┘
                                    │
                                    ↓
        ┌───────────────────────────────────────────────┐
        │    SILVER LAYER - FACT STAGING                │
        │         orders_staging                        │
        │  [Incremental input for daily processing]    │
        └───────────────────────────────────────────────┘
                                    │
                                    ↓
        ┌───────────────────────────────────────────────┐
        │         GOLD LAYER - FACT TABLE               │
        │          sb_fact_orders                       │
        │  [Monthly grain, joined with dimensions]     │
        │  [Upserted via MERGE for idempotence]        │
        └───────────────────────────────────────────────┘
                                    │
                ┌───────────────────┼───────────────────┐
                ↓                   ↓                   ↓
        ┌──────────────┐    ┌──────────────┐   ┌──────────────┐
        │ BI Dashboards│    │ Databricks   │   │ Data Lake    │
        │ (Tableau)    │    │ Genie (AI)   │   │ Export       │
        └──────────────┘    └──────────────┘   └──────────────┘
```

## Processing Patterns

### Dimension Processing (Full → Silver → Gold)

```
Bronze → Cleanse/Deduplicate → Silver Staging → MERGE into Silver → Gold
  ↓         (Schema Fix)          (Incremental)     (Aggregate)     (UPS)
  └─────────────────────────────────────────────────────────────────→
```

### Fact Processing (Bronze → Silver → Gold)

**Full Load (Historical Backfill)**
```
Bronze Orders → Cleanse → Silver Staging → TRUNCATE Gold → Full Reload
```

**Incremental Load (Daily)**
```
Bronze Orders (New) → Cleanse → Silver Staging → MERGE Gold
```

## Key Technologies

| Layer | Tech | Purpose |
|-------|------|---------|
| **Data Ingestion** | AWS S3, Databricks Auto Loader | Raw data landing |
| **Storage** | Delta Lake (ACID, Time-travel) | Transactional data lake |
| **Processing** | PySpark, SQL | Distributed transformation |
| **Orchestration** | Databricks Workflows | Job scheduling & coordination |
| **Analytics** | Databricks Genie, Tableau | BI & self-service analytics |

## ACID & Consistency Guarantees

✅ **Delta Lake MERGE** ensures:
- Idempotent upserts (safe for replay)
- Atomicity (all-or-nothing transactions)
- Consistency (no intermediate states)
- Isolation (no concurrent conflicts)

✅ **Change Data Feed** enables:
- Data lineage tracking
- Audit trails
- Downstream analytics

## Scalability

- **Horizontal**: PySpark auto-scales across Databricks clusters
- **Vertical**: Configurable cluster size for peak loads
- **Incremental**: Daily loads process only new data
- **Partitioning**: Implicit by data_source, extensible to by date

---

**Add `lakehouse_architecture.png` screenshot here** (PowerPoint/draw.io diagram showing the flow above)
