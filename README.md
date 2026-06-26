# E-Commerce Lakehouse Pipeline

An end-to-end data engineering pipeline built on **Databricks + Delta Lake**, processing the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) through a medallion architecture (Bronze → Silver → Gold).

---

## Architecture

```
Raw CSVs (Kaggle / Databricks Volume)
        │
        ▼
┌──────────────┐
│    BRONZE    │  Schema-on-read, as-is ingestion
│  Delta Lake  │  + _load_timestamp, _source_file lineage columns
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    SILVER    │  Deduplicated on PKs (window function)
│  Delta Lake  │  Type-cast, null-key filtered, standardised dates
└──────┬───────┘
       │
       ▼
┌──────────────┐
│     GOLD     │  Star schema — FactSales, DimCustomer (SCD2),
│  Delta Lake  │  DimProduct, DimDate            [in progress]
└──────────────┘
```

**Stack:** PySpark · Delta Lake · Databricks · Airflow (Docker) · Python

---

## Repository Structure

```
ecommerce-lakehouse-pipeline/
├── injestion/
│   ├── bronze_ingestion.ipynb          # Bulk CSV → Bronze Delta tables
│   └── bronze_ingestion_reviews.ipynb  # Reviews special-case (corrupt-record handling)
├── transformations/
│   ├── silver_orders.ipynb
│   ├── silver.order_items.ipynb
│   ├── silver.customers.ipynb
│   ├── silver_products.ipynb
│   ├── silver_payments.ipynb
│   └── silver_reviews.ipynb
├── sql/                                # Analytics queries          [coming — P3]
├── dags/                               # Airflow DAGs               [coming — P4]
└── tests/                              # Data-quality checks        [coming — P3]
```

---

## Dataset

**Olist Brazilian E-Commerce** — ~100k orders placed on the Olist marketplace between 2016–2018.

| Source file | Bronze table | Rows |
|---|---|---|
| olist_orders_dataset.csv | `bronze.orders` | ~99k |
| olist_order_items_dataset.csv | `bronze.order_items` | ~113k |
| olist_customers_dataset.csv | `bronze.customers` | ~99k |
| olist_products_dataset.csv | `bronze.products` | ~33k |
| olist_order_payments_dataset.csv | `bronze.payments` | ~104k |
| olist_order_reviews_dataset.csv | `bronze.reviews` | ~99k |

Raw CSVs are stored in a Databricks Unity Catalog Volume at `/Volumes/workspace/default/e-commerce/`.

---

## Layers

### Bronze — Raw Ingestion

- Reads each CSV with `inferSchema` (schema-on-read; no transformation).
- Appends `_load_timestamp` (ingestion time) and `_source_file` (Unity Catalog `_metadata.file_path`) for lineage.
- Reviews CSV required explicit schema + `PERMISSIVE` mode to handle embedded newlines and unescaped quotes.
- Written to Delta with `overwrite` — deterministic reruns on a static dataset.

### Silver — Cleaned & Typed

Each table goes through the same four-step pattern:

1. **Deduplicate** on the primary key using `row_number()` over a deterministic window (`purchase_timestamp DESC`, `_load_timestamp DESC`). Latest record wins.
2. **Cast types** — zip codes to `StringType`, timestamps standardised.
3. **Drop null-key rows** — rows missing the PK or a mandatory FK (e.g. `customer_id` on orders) cannot be joined downstream and are removed.
4. **Write to Delta** — `overwrite` + `overwriteSchema`.

### Gold — Star Schema *(in progress)*

Planned tables:

| Table | Type | Notes |
|---|---|---|
| `gold.fact_sales` | Fact | Grain: one row per order item |
| `gold.dim_customer` | SCD Type 2 | `effective_date`, `end_date`, `is_current` |
| `gold.dim_product` | Dimension | |
| `gold.dim_date` | Dimension | Generated programmatically |

---

## How to Run

### Prerequisites

- Databricks workspace with Unity Catalog enabled.
- Olist CSVs uploaded to `/Volumes/workspace/default/e-commerce/`.
- A cluster with Delta Lake (any Databricks Runtime ≥ 12.x).

### Execution Order

```
1. injestion/bronze_ingestion.ipynb           # ingest orders, items, customers, products, payments
2. injestion/bronze_ingestion_reviews.ipynb   # ingest reviews (separate due to CSV quirks)
3. transformations/silver_orders.ipynb
4. transformations/silver.order_items.ipynb
5. transformations/silver.customers.ipynb
6. transformations/silver_products.ipynb
7. transformations/silver_payments.ipynb
8. transformations/silver_reviews.ipynb
```

Run each notebook against an attached cluster. No configuration changes are needed beyond the volume path in step 1.

---

## Engineering Decisions

**Why deduplicate with `row_number()` instead of `dropDuplicates()`?**
`dropDuplicates()` picks an arbitrary survivor when duplicates exist. `row_number()` with an explicit ordering (latest timestamp) makes the choice deterministic and reproducible across reruns.

**Why drop null-PK rows rather than impute?**
A row with no `order_id` cannot be deduped, joined, or tracked — it has no identity. Dropping it is the only safe option; imputing a synthetic key would create phantom records.

**Why explicit schema for reviews?**
The reviews CSV contains free-text comments with embedded newlines and unescaped double-quotes. `inferSchema` crashes on these; an explicit schema with `PERMISSIVE` mode and `_corrupt_record` capture lets us recover the valid ~99k rows while isolating the bad ones.

**Why `overwrite` instead of `MERGE` at this stage?**
The Olist dataset is static (historical snapshot). `overwrite` keeps reruns simple and idempotent. Incremental `MERGE`-based loading will be introduced in the Airflow orchestration phase (P4) to demonstrate production-grade upsert patterns.

---

## Roadmap

| Phase | Status |
|---|---|
| P0 — Setup & Foundations | Done |
| P1 — Bronze & Silver | Done |
| P2 — Gold / Star Schema | In progress |
| P3 — SQL Analytics & Data Quality | Upcoming |
| P4 — Airflow Orchestration (Docker) | Upcoming |
| P5 — Polish & Documentation | Upcoming |
