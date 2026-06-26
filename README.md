# E-Commerce Data Pipeline 📊

> An end-to-end ELT pipeline built on the **medallion architecture** (Bronze → Silver → Gold), using the public Olist Brazilian e-commerce dataset.

![Status](https://img.shields.io/badge/status-in_progress-yellow?style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)

---

## Overview

This project ingests raw e-commerce data, cleans and models it through layered transformations, and serves analytics-ready tables — orchestrated as a scheduled workflow. It's built to demonstrate the core data-engineering loop: **ingest → store → transform → model → orchestrate.**

> ⚠️ **Note:** This is a portfolio project built on the public [Olist dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce). It is for learning and demonstration, not production use.

---

## Architecture

```
Raw CSVs ──► Bronze ──► Silver ──► Gold ──► Analytics
            (raw       (cleaned,   (star
             ingest)    deduped)    schema)
                                       │
                          orchestrated by Airflow
```

**Bronze** — raw ingestion into Delta tables, with load metadata (`_load_timestamp`, `_source_file`).
**Silver** — cleaning, type casting, deduplication on business keys, and conformed structures.
**Gold** — dimensional star schema (fact + dimension tables) with **SCD Type 2** for historical tracking.

---

## Tech Stack

| Layer | Tools |
|---|---|
| Processing | PySpark, Databricks |
| Storage | Delta Lake |
| Orchestration | Apache Airflow (containerized with Docker) |
| Modeling | Star schema, SCD Type 2 |
| Language | Python, SQL |

---

## Key Features

- **Medallion architecture** separating raw, cleaned, and serving layers
- **Idempotent ingestion** with load metadata for traceability
- **Business-key deduplication** (e.g. `order_id + payment_sequential`) rather than naive row dedup
- **SCD Type 2** dimensions to preserve historical changes
- **Star schema** modeling for analytics-friendly queries
- **Airflow DAG** orchestrating the full Bronze → Silver → Gold flow

---

## Project Structure

```
ecommerce-lakehouse-pipeline/
├── notebooks/
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_clean.py
│   └── 03_gold_model.py
├── dags/
│   └── pipeline_dag.py
├── data/                # sample / source data references
└── README.md
```

---

## Status & Roadmap

- [x] Bronze ingestion layer
- [x] Silver cleaning & deduplication
- [ ] Gold star schema with SCD Type 2
- [ ] Airflow DAG orchestration
- [ ] Data quality checks
- [ ] Documentation & sample queries

---

## What I Learned

Building this end to end taught me how the pieces of a real pipeline fit together — why the medallion layers exist, how to deduplicate on business keys without losing legitimate records, and how SCD Type 2 preserves history for accurate point-in-time analytics.

---

*Built by [Minhazul Islam](http://www.linkedin.com/in/minhaz74692) while transitioning into data engineering.*
