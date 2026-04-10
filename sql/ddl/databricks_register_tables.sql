-- =============================================================================
-- Databricks SQL — Register Delta tables in Unity Catalog / Hive Metastore
-- =============================================================================
-- Run this once after first pipeline execution to make Delta files queryable
-- via Databricks SQL warehouse (for notebooks, BI, and ad-hoc queries)
-- =============================================================================

-- ── Catalog / Schema setup ────────────────────────────────────────────────────
-- Unity Catalog (recommended for enterprise):
CREATE CATALOG  IF NOT EXISTS customer360;
CREATE SCHEMA   IF NOT EXISTS customer360.silver;
CREATE SCHEMA   IF NOT EXISTS customer360.gold;
CREATE SCHEMA   IF NOT EXISTS customer360.audit;

-- ── Register Silver tables ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customer360.silver.customers
USING DELTA
LOCATION '/mnt/customer360/silver/customers'
COMMENT 'Silver customers — SCD Type 2, one row per version per customer';

CREATE TABLE IF NOT EXISTS customer360.silver.accounts
USING DELTA
LOCATION '/mnt/customer360/silver/accounts'
COMMENT 'Silver accounts — typed, deduped, derived utilization fields';

CREATE TABLE IF NOT EXISTS customer360.silver.transactions
USING DELTA
LOCATION '/mnt/customer360/silver/transactions'
COMMENT 'Silver transactions — signed_amount, txn_year/month partitions, fraud_score';

CREATE TABLE IF NOT EXISTS customer360.silver.branches
USING DELTA
LOCATION '/mnt/customer360/silver/branches'
COMMENT 'Silver branches — reference table, full refresh each run';

CREATE TABLE IF NOT EXISTS customer360.silver.support_tickets
USING DELTA
LOCATION '/mnt/customer360/silver/support_tickets'
COMMENT 'Silver support tickets — resolution_days, is_resolved, csat_score';

-- ── Register Gold tables ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customer360.gold.customer_360
USING DELTA
LOCATION '/mnt/customer360/gold/customer_360'
COMMENT 'Gold Customer 360 — one row per current customer, 30+ KPIs, churn/high-value flags';

CREATE TABLE IF NOT EXISTS customer360.gold.txn_monthly_summary
USING DELTA
LOCATION '/mnt/customer360/gold/txn_monthly_summary'
COMMENT 'Gold monthly transaction roll-up per customer — feeds Power BI trend charts';

CREATE TABLE IF NOT EXISTS customer360.gold.segment_metrics
USING DELTA
LOCATION '/mnt/customer360/gold/segment_metrics'
COMMENT 'Gold segment-level KPIs — one row per segment, refreshed daily';

CREATE TABLE IF NOT EXISTS customer360.gold.support_kpis
USING DELTA
LOCATION '/mnt/customer360/gold/support_kpis'
COMMENT 'Gold daily support ticket KPIs by category and priority';

-- ── Register DQ audit table ───────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customer360.audit.dq_results
USING DELTA
LOCATION '/mnt/customer360/dq/dq_results'
COMMENT 'Data quality check results — one row per check per batch run';

-- ── Validate registration ─────────────────────────────────────────────────────
SHOW TABLES IN customer360.silver;
SHOW TABLES IN customer360.gold;

-- ── Quick sanity check ────────────────────────────────────────────────────────
SELECT
    'customer_360'          AS table_name,
    COUNT(*)                AS row_count,
    COUNT(DISTINCT segment) AS distinct_segments,
    ROUND(SUM(total_balance)/1e6, 2) AS aum_millions
FROM customer360.gold.customer_360

UNION ALL

SELECT
    'txn_monthly_summary',
    COUNT(*),
    COUNT(DISTINCT period),
    NULL
FROM customer360.gold.txn_monthly_summary

UNION ALL

SELECT
    'segment_metrics',
    COUNT(*),
    COUNT(DISTINCT segment),
    ROUND(SUM(aum_total)/1e6, 2)
FROM customer360.gold.segment_metrics;

-- ── Time travel example (interview demonstration) ─────────────────────────────
-- View the state of the customer table as it was 24 hours ago:
-- SELECT * FROM customer360.silver.customers TIMESTAMP AS OF (CURRENT_TIMESTAMP() - INTERVAL 1 DAY)

-- View a specific version:
-- SELECT * FROM customer360.silver.customers VERSION AS OF 1

-- See full history:
-- DESCRIBE HISTORY customer360.silver.customers
