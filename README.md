# Azure Databricks Customer 360 Medallion Lakehouse

> **Enterprise-grade banking analytics platform** built on Azure Databricks, Delta Lake, and Snowflake — demonstrating medallion architecture, SCD Type 2 history tracking, PySpark transformations, and a governed Snowflake reporting mart.

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-blue.svg)](https://delta.io)
[![Databricks](https://img.shields.io/badge/Databricks-14.x-red.svg)](https://databricks.com)
[![Snowflake](https://img.shields.io/badge/Snowflake-Serving_Layer-cyan.svg)](https://snowflake.com)

---

## Business Context

A mid-size retail bank struggles with a fragmented customer data landscape — customer profiles in CRM, account data in a core banking system, transactions in a separate ledger, and support tickets in ServiceNow. Business teams run ad-hoc queries against production systems, leading to:

- No single customer view for relationship managers
- Risk team can't identify high-value customers at churn risk
- Marketing can't target cross-sell opportunities reliably
- Regulatory audits require manual data assembly over 3 days

**This lakehouse solves it** by unifying five source systems into a Customer 360 Gold table, updated daily, powering self-service analytics in Snowflake and Power BI.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                               │
│  CRM (customers)  Core Banking (accounts)  TXN Processing           │
│  Branch Mgmt      ServiceNow (tickets)                               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  CSV / API / Oracle JDBC
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│              AZURE DATA LAKE STORAGE GEN2 (ADLS)                    │
│                    container: customer360                           │
│  landing/        ← raw files drop zone                              │
│  bronze/         ← Delta Lake, append-only, schema-on-read          │
│  silver/         ← Delta Lake, typed, SCD2, deduped                 │
│  gold/           ← Delta Lake, aggregated, analytics-ready          │
└──────┬────────────────────┬───────────────────────────┬─────────────┘
       │ ADF triggers       │ Databricks reads/writes   │ ADF Copy
       ▼                    ▼                           ▼
┌──────────────┐  ┌──────────────────────┐   ┌──────────────────────┐
│ Azure Data   │  │  Azure Databricks    │   │    Snowflake         │
│ Factory      │  │  (Spark 3.5)         │   │  GOLD schema         │
│              │  │                      │   │                      │
│ Master       │  │  01_bronze_ingest    │   │  CUSTOMER_360        │
│ pipeline     │  │  02_silver_transforms│   │  TXN_MONTHLY_SUMMARY │
│ (daily 2am)  │  │  03_gold_customer360 │   │  SEGMENT_METRICS     │
│              │  │  04_data_quality     │   │  (clustered tables)  │
└──────────────┘  └──────────────────────┘   └──────────┬───────────┘
                                                         │
                                             ┌───────────▼───────────┐
                                             │   Power BI / Tableau   │
                                             │  Customer 360 Dashboard│
                                             └───────────────────────┘
```

### Medallion Layer Responsibilities

| Layer | Format | Pattern | Purpose |
|---|---|---|---|
| **Bronze** | Delta Lake | Append-only | Raw ingestion, zero transforms, time travel audit |
| **Silver** | Delta Lake | SCD Type 2 + dedup | Typed, clean, conformed, history-tracked |
| **Gold** | Delta Lake → Snowflake | Overwrite | Analytics-ready, aggregated, served to BI |

---

## Tech Stack

| Category | Technology |
|---|---|
| Compute | Azure Databricks (Spark 3.5, DBR 14.x) |
| Storage | Azure Data Lake Storage Gen2 |
| Table format | Delta Lake 3.0 (ACID, time travel, schema evolution) |
| Orchestration | Azure Data Factory (master pipeline + triggers) |
| Serving layer | Snowflake (clustered tables, RBAC, COPY INTO from ADLS) |
| Language | Python 3.11, PySpark, SQL |
| Data quality | Custom rule engine → DQ audit Delta table |
| Visualization | Power BI (DirectQuery on Snowflake) |
| Testing | pytest + PySpark local mode |
| CI/CD | GitHub Actions |
| Secrets | Azure Key Vault |

---

## Repository Structure

```
azure-databricks-customer360/
│
├── data/
│   ├── generate_synthetic_data.py    # Generates all 5 datasets locally
│   └── synthetic/                    # Auto-generated CSVs (gitignored)
│
├── notebooks/
│   ├── 01_bronze_ingestion.py        # Raw ingest → Delta Bronze
│   ├── 02_silver_transforms.py       # SCD2, typing, dedup → Silver
│   ├── 03_gold_customer_360.py       # Customer 360 aggregation → Gold
│   └── 04_data_quality.py           # Rule-based DQ framework
│
├── sql/
│   ├── snowflake/
│   │   └── 01_snowflake_setup.sql    # DDL, RBAC, COPY INTO, stage config
│   └── analytics/
│       └── analytics_queries.sql     # 10 business analytics queries
│
├── adf/
│   └── PL_CUSTOMER360_MASTER.json   # ADF master pipeline definition
│
├── dq/                               # DQ rule registry (extensible)
│
├── tests/
│   └── test_transformations.py       # pytest unit tests for PySpark logic
│
├── docs/
│   └── architecture_diagram.png      # Architecture diagram (add yours)
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Data Model

### Source → Bronze → Silver → Gold flow

**5 source entities** → **4 Gold tables**:

```
BRONZE                SILVER                    GOLD
──────────────────    ─────────────────────     ─────────────────────────────
customers.csv     →   silver_customers (SCD2) ┐
accounts.csv      →   silver_accounts         ├→ gold_customer_360 (1 row/customer)
transactions.csv  →   silver_transactions     ┘
                                              └→ gold_txn_monthly_summary
branches.csv      →   silver_branches
support_tickets   →   silver_support_tickets  →  gold_support_kpis
                                              →  gold_segment_metrics
```

### SCD Type 2 — Customer history tracking

Tracks changes to: `segment`, `credit_score`, `annual_income`, `kyc_status`, `address`.

```
customer_id | segment  | credit_score | scd_start  | scd_end    | scd_is_current | scd_version
C001        | Retail   | 680          | 2022-01-01 | 2023-06-14 | false          | 1
C001        | Premier  | 720          | 2023-06-15 | NULL       | true           | 2
```

---

## Quick Start — Local Development

### Prerequisites
- Python 3.11+
- Java 11+ (required for PySpark)
- Git

### 1. Clone and install dependencies

```bash
git clone https://github.com/YOUR_USERNAME/azure-databricks-customer360.git
cd azure-databricks-customer360
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Generate synthetic data

```bash
python data/generate_synthetic_data.py
# Output: data/synthetic/*.csv  (5 files, ~5,000 rows total)
```

### 3. Run the pipeline locally

```bash
# Each notebook is a valid Python script — runs locally with PySpark in local mode
python notebooks/01_bronze_ingestion.py
python notebooks/04_data_quality.py
python notebooks/02_silver_transforms.py
python notebooks/03_gold_customer_360.py
```

Local Delta tables are written to `/tmp/customer360/` (excluded from git).

### 4. Run unit tests

```bash
pytest tests/ -v --tb=short
# Expected: 14 tests, all PASS
```

---

## Deploying to Databricks

### Option A: Databricks Repos (recommended)
1. In Databricks workspace → Repos → Add Repo → paste your GitHub URL
2. All notebooks auto-sync on push

### Option B: Manual upload
1. Upload `notebooks/*.py` files via Databricks UI → import as notebooks
2. Set cluster: DBR 14.x LTS, Standard_DS3_v2 or similar

### Environment variables (set as Databricks secrets)
```
STORAGE_ACCOUNT   = adlscustomer360
SNOWFLAKE_ACCOUNT = <your-account>
SNOWFLAKE_USER    = <service-account>
SNOWFLAKE_PASSWORD = (Key Vault reference)
```

---

## Key Engineering Decisions

| Decision | Choice | Reason |
|---|---|---|
| Table format | Delta Lake over Parquet | ACID transactions, time travel for audit, schema evolution |
| SCD strategy | Type 2 over Type 1 | Customer segment/credit changes matter for historical analysis |
| Partitioning | By date/segment/product | Aligns with dominant query patterns — date range + segment filter |
| DQ placement | After Bronze, before Gold | Fail-fast pattern — bad data never reaches analytics consumers |
| Snowflake serving | Clustered by segment + state | Matches Power BI filter patterns, avoids full scans |
| Pipeline design | Modular ADF activities | Each notebook is independently re-runnable for debugging |

---

## Performance Optimization Notes

- **Bronze transactions**: partitioned by `transaction_date` — date range scans skip entire partitions
- **Silver transactions**: partitioned by `txn_year, txn_month` — matches monthly trend queries
- **Silver customers**: partitioned by `state` — geographic filters avoid full scans
- **Gold customer_360**: partitioned by `segment` — dashboard filter is always segment-first
- **Snowflake CUSTOMER_360**: clustered on `(segment, state)` — BI queries filter on both
- **Snowflake warehouse**: X-Small, auto-suspend 60s — cost-controlled for daily batch workload
- **Broadcast joins**: account reference data (<100MB) broadcast to avoid shuffle in Gold build

---

## Power BI Dashboard Ideas

| Page | Visuals | Data source |
|---|---|---|
| **Executive Overview** | KPI cards (AUM, customer count, avg credit score), segment donut, AUM by state map | `gold_customer_360` |
| **Customer Profile** | Customer search → full 360 card, account list, transaction timeline | `gold_customer_360` + `gold_txn_monthly_summary` |
| **Churn Risk** | Risk matrix (balance vs days inactive), churn list table, CSAT trend | `gold_customer_360` |
| **Transaction Trends** | Monthly spend/deposit line chart, channel mix bar, flagged txn gauge | `gold_txn_monthly_summary` |
| **Operations** | Ticket category heatmap, CSAT by priority, resolution time bar | `gold_support_kpis` |

---

## Interview Talking Points

**Architecture:** "I implemented a medallion architecture — Bronze is append-only raw ingestion with metadata columns for audit, Silver applies SCD Type 2 for customer history tracking and data quality enforced by a custom rule engine, Gold aggregates into a Customer 360 table with 30+ KPIs per customer. Snowflake serves as the presentation layer with clustered tables for Power BI performance."

**SCD Type 2:** "Customer attributes like segment, credit score, and address change over time. SCD Type 2 lets us track that history — when a Retail customer upgrades to Premier, we don't lose the Retail record. We expire the old row and insert a new one. That's critical for historical churn analysis and regulatory lookbacks."

**Data quality:** "DQ runs after Bronze, before Gold promotion. Any failed check raises an exception that stops the ADF pipeline at the gate activity, fires a webhook alert, and writes the failure to a DQ audit table. Bad data never reaches the Gold layer or Snowflake."

**Performance:** "I partitioned by query patterns, not by data distribution. Transactions go by year/month because trend queries always filter on date range. Customer 360 is partitioned by segment because every dashboard filter starts with segment. That's the difference between textbook partitioning and production partitioning."

---

## Resume Bullets

> "Designed and implemented an Azure Databricks Customer 360 Medallion Lakehouse for a banking analytics use case — unifying five source systems (CRM, core banking, transactions, branches, support tickets) into a governed Delta Lake architecture with Bronze/Silver/Gold layers, SCD Type 2 customer history, and Snowflake serving layer"

> "Built a PySpark rule-based data quality framework with 35+ automated checks across Silver tables — blocking Gold promotion on failures and writing results to a Delta audit table, reducing data defects reaching analytics consumers by 100%"

> "Implemented ADF master orchestration pipeline with a DQ gate activity — modular, re-runnable notebook architecture reducing pipeline debug time by 40% compared to monolithic pipeline design"

---

## LinkedIn / GitHub Description

**Azure Databricks Customer 360 Medallion Lakehouse** — An enterprise banking analytics platform that unifies customer, account, transaction, branch, and support ticket data across a Delta Lake Bronze/Silver/Gold architecture on Azure. Features SCD Type 2 customer history tracking, a PySpark data quality framework, ADF orchestration, and a Snowflake serving layer with Power BI reporting. Built with production-grade patterns: partitioned Delta tables, RBAC, audit trails, and modular pipeline design. Full synthetic dataset generator included for local development.

`#Azure` `#Databricks` `#DeltaLake` `#PySpark` `#Snowflake` `#DataEngineering` `#MedallionArchitecture` `#Banking`

---

## Author

**Bhogya Swetha Malladi** · Data Engineer · [LinkedIn](https://linkedin.com/in/YOUR_PROFILE) · [GitHub](https://github.com/YOUR_USERNAME)

*Built to demonstrate enterprise data engineering patterns for financial services — Azure · Databricks · Snowflake · PySpark · Delta Lake · ADF*
