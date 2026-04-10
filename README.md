# Azure Databricks Customer 360 Medallion Lakehouse

Banking analytics platform built on Azure Databricks and Delta Lake — unifying five source systems into a Bronze/Silver/Gold medallion architecture with SCD Type 2 customer history and a Snowflake reporting layer.

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=flat)
![Azure](https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat&logo=apachespark&logoColor=white)

---

## Business Context

Retail banks typically store customer data across disconnected systems — CRM holds profiles, core banking holds accounts, a separate ledger holds transactions, branch management holds location data, and a CRM ticketing system holds support history. No single team has a complete view of a customer.

Relationship managers cannot identify which customers are at risk of leaving. Risk teams cannot flag high-value customers with deteriorating credit health. Marketing cannot find cross-sell opportunities without manually joining five systems.

This platform consolidates all five source systems into a single governed Customer 360 table — one row per customer, updated daily, with 30+ KPIs covering balance, spend, fraud exposure, support sentiment, and churn risk.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SOURCE SYSTEMS                              │
│                                                                     │
│   CRM          Core Banking     Transaction    Branch     ServiceNow│
│  (customers)    (accounts)      Processing     Mgmt       (tickets) │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼  Azure Data Factory — daily 02:00 UTC
                            │  Copy activities + notebook triggers
                            │
┌───────────────────────────▼─────────────────────────────────────────┐
│                  Azure Data Lake Storage Gen2                       │
│                                                                     │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────────────┐  │
│  │     BRONZE       │  │      SILVER       │  │       GOLD       │  │
│  │                  │  │                  │  │                   │  │
│  │  Append-only     │  │  SCD Type 2      │  │  Customer 360     │  │
│  │  Delta tables    │→ │  35+ DQ checks   │→ │  1 row/customer   │  │
│  │  Metadata stamp  │  │  Typed · Deduped │  │  30+ KPIs         │  │
│  │  Time travel     │  │  DQ gate blocks  │  │  Churn · Fraud    │  │
│  └─────────────────┘  └──────────────────┘  └────────────────────┘  │
└──────────────────────────────────────────────┬──────────────────────┘
                                               │  ADF Copy + COPY INTO
                                               ▼
                                    ┌──────────────────────┐
                                    │       Snowflake      │
                                    │                      │
                                    │  Clustered tables    │
                                    │  RBAC roles          │
                                    │  7 analytics views   │
                                    └──────────┬───────────┘
                                               │
                                               ▼
                                         Power BI
                                      Executive · Churn
                                      Fraud · Operations
```

---

## Tech Stack

| Category | Technology |
|---|---|
| Compute | Azure Databricks — Spark 3.5, DBR 14.x |
| Storage | Azure Data Lake Storage Gen2 |
| Table format | Delta Lake 3.0 — ACID, time travel, schema evolution |
| Orchestration | Azure Data Factory — master pipeline with DQ gate |
| Serving layer | Snowflake — clustered tables, RBAC, analytics views |
| Language | Python 3.11, PySpark, SQL |
| Testing | pytest — 14 unit tests for PySpark business logic |
| Secrets | Azure Key Vault |

---

## Data Model

```
BRONZE                 SILVER                      GOLD
──────────────────     ──────────────────────     ─────────────────────────────
customers.csv      →   silver_customers (SCD2) ┐
accounts.csv       →   silver_accounts         ├→ gold_customer_360
transactions.csv   →   silver_transactions     ┘  (1 row per customer · 30+ KPIs)
                                               └→ gold_txn_monthly_summary
branches.csv       →   silver_branches
support_tickets    →   silver_support_tickets  → gold_support_kpis
                                              → gold_segment_metrics
```

### SCD Type 2 — Customer history

Customer segment, credit score, income, address, and KYC status are tracked across versions. When an attribute changes, the old row is expired and a new version is inserted.

```
customer_id │ segment │ credit_score │ scd_start  │ scd_end    │ scd_is_current
────────────┼─────────┼──────────────┼────────────┼────────────┼───────────────
C001        │ Retail  │ 680          │ 2022-01-01 │ 2023-06-14 │ false
C001        │ Premier │ 720          │ 2023-06-15 │ NULL       │ true
```

---

## Repository Structure

```
azure-databricks-customer360/
│
├── data/
│   └── generate_synthetic_data.py     # Generates all 5 datasets locally
│
├── notebooks/
│   ├── 01_bronze_ingestion.py         # Raw ingest → Bronze Delta (partitioned by date)
│   ├── 02_silver_transforms.py        # SCD Type 2 merge, typing, dedup → Silver
│   ├── 03_gold_customer_360.py        # Customer 360 aggregation → 4 Gold tables
│   ├── 04_data_quality.py            # 35+ rule-based DQ checks → audit table
│   └── 05_performance_optimization.py # Partitioning, Z-order, broadcast joins
│
├── sql/
│   ├── snowflake/
│   │   ├── 01_snowflake_setup.sql    # DDL, RBAC, stage, clustered tables, COPY INTO
│   │   └── 02_snowflake_views.sql   # 7 Power BI views — churn, cross-sell, geo, exec
│   └── analytics/
│       └── analytics_queries.sql    # 10 business analytics queries
│
├── adf/
│   └── PL_CUSTOMER360_MASTER.json   # ADF master pipeline — DQ gate, alerting
│
└── tests/
    └── test_transformations.py      # 14 pytest unit tests for PySpark logic
```

---

## Quick Start

```bash
git clone https://github.com/Mbhogyas3006/azure-databricks-customer360
cd azure-databricks-customer360
pip install -r requirements.txt

# Generate synthetic datasets
python data/generate_synthetic_data.py

# Run unit tests
pytest tests/ -v
```

**Running on Databricks:**
Upload notebooks to your Databricks workspace via Repos. Run in order: `01 → 04 → 02 → 03 → 05`.

---

## Synthetic Dataset

| Table | Rows | Description |
|---|---|---|
| customers | 500 | Demographics, segment, credit score, KYC status |
| accounts | 800 | All product types — checking, savings, mortgage, credit card |
| transactions | 3,000 | 2023–2024 transaction history with fraud scores |
| branches | 15 | Branch locations across 5 US states |
| support_tickets | 700 | CRM tickets with CSAT scores and resolution times |

All data is synthetically generated — no real customer or financial data.

---

## Key Engineering Decisions

| Decision | Rationale |
|---|---|
| Delta Lake over Parquet | ACID transactions ensure no partial writes reach downstream consumers. Time travel enables point-in-time audit queries required for regulatory lookbacks |
| SCD Type 2 over Type 1 | Customer segment and credit score changes carry historical significance. SCD2 preserves the full version history for trend analysis and regulatory audit |
| Partition by query pattern | Transactions partitioned by `(txn_year, txn_month)` — matches the date-range filter pattern in 90% of analytics queries, enabling partition pruning |
| DQ gate in ADF pipeline | IfCondition activity blocks Gold promotion on DQ failure. Bad data never reaches Snowflake. Failure fires a webhook alert and writes to an audit Delta table |
| Snowflake clustering keys | `(segment, state)` on the Customer 360 table matches Power BI slicer patterns — segment and state are always in the WHERE clause |
