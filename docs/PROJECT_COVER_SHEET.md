# PROJECT COVER SHEET
## Azure Databricks Customer 360 Medallion Lakehouse

---

**Engineer:** Bhogya Swetha Malladi
**GitHub repo:** `azure-databricks-customer360`
**Domain:** Banking / Retail Financial Services
**Status:** Portfolio Project — synthetic data, production-grade patterns

---

## Business Problem

A retail bank's customer data is siloed across five systems with no unified view.
Risk teams can't identify churn risk. Marketing can't find cross-sell targets.
Audits require 3-day manual data assembly. Analysts query production systems directly.

**Outcome:** A single Customer 360 Gold table per customer — updated daily — powering
self-service analytics for Risk, Marketing, Finance, and Operations.

---

## Architecture Summary

```
5 Source Systems  →  ADLS Gen2  →  Azure Databricks (Medallion)  →  Snowflake  →  Power BI
CRM, Core Banking,    Landing       Bronze  →  Silver  →  Gold       Serving       Dashboard
Transactions,         Zone          (Delta Lake, ACID, time travel)   Layer
Branches, Tickets
```

**Orchestration:** Azure Data Factory — daily at 02:00 UTC
**DQ Gate:** Custom PySpark rule engine (35+ checks) blocks Gold promotion on failure

---

## Key Technical Features

| Feature | Implementation |
|---|---|
| Medallion architecture | Bronze / Silver / Gold in Delta Lake |
| SCD Type 2 | Customer segment, credit score, address history |
| Data quality | Rule engine → DQ audit table → ADF gate |
| Partitioning | By date, segment, product — aligned to query patterns |
| Serving layer | Snowflake with clustering keys + RBAC roles |
| Orchestration | ADF master pipeline with modular activities |
| Testing | pytest unit tests for all PySpark business logic |
| Security | Azure Key Vault for secrets, RBAC on Snowflake |

---

## Tools Used

`Azure Databricks` · `Delta Lake 3.0` · `PySpark 3.5` · `Azure Data Lake Gen2`
`Azure Data Factory` · `Snowflake` · `Python 3.11` · `SQL` · `pytest` · `GitHub Actions`

---

## Datasets

| Dataset | Rows | Source |
|---|---|---|
| Customers | 500 | Synthetic — Python generator |
| Accounts | 800 | Synthetic |
| Transactions | 3,000 | Synthetic |
| Branches | 15 | Synthetic |
| Support Tickets | 700 | Synthetic |
| **Total** | **5,015** | |

---

## Key Outcomes / Business Value

- Unified customer 360 view: 30+ KPIs per customer in one Gold table
- SCD2 history enables regulatory lookbacks and churn pattern analysis
- DQ framework catches data issues before they reach analytics consumers
- Snowflake serving layer reduces Power BI query time vs direct ADLS reads
- Modular pipeline design: each notebook re-runnable independently for debugging
- RBAC on Snowflake: analysts get read-only access; engineers get write access

---

## Resume Bullets Derived from This Project

1. "Designed Azure Databricks Customer 360 Medallion Lakehouse unifying 5 banking source systems into Bronze/Silver/Gold Delta Lake layers with SCD Type 2 customer history and Snowflake serving layer"
2. "Built PySpark data quality framework with 35+ automated checks blocking Gold promotion on failure — ensuring zero defective data reaches analytics consumers"
3. "Implemented ADF master orchestration pipeline with modular Databricks notebook activities, DQ gate, and webhook alerting — daily batch processing for 5,000+ customer records"

---

## 90-Second Interview Explanation

*"The business problem is classic in banking — five systems, no unified customer view.
A customer's profile is in CRM, their accounts are in core banking, their transactions
in a separate ledger, and their service history in the ticketing system.
Relationship managers and risk analysts have no single place to look.*

*I built a medallion lakehouse on Azure Databricks. Bronze is raw — append-only Delta tables,
no transforms, just metadata stamping for audit. Silver is where the engineering lives:
SCD Type 2 for customer history so when a Retail customer moves to Premier we don't lose
that record, data type casting, deduplication, and a rule-based DQ framework — 35 checks
that block the pipeline if anything fails.*

*Gold is the Customer 360 table — one row per current customer with 30-plus KPIs:
total balance, 12-month spend, churn risk flag, CSAT score, product diversity count.
ADF orchestrates the whole thing daily at 2am, with a gate activity that stops at DQ.*

*Snowflake is the serving layer — COPY INTO from ADLS Gold every morning, clustering keys
on segment and state to match Power BI's filter patterns.*

*The thing I'm most proud of is the DQ design — bad data never reaches the analytics layer.
It fails loud and early, writes to an audit table, and fires an alert. That's the pattern
I'd bring to Morgan Stanley."*

---

*Portfolio project — Bhogya Swetha Malladi · Data Engineer · bhogya-swetha.malladi@capgemini.com*
