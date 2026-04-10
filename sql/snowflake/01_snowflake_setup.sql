-- =============================================================================
-- Snowflake Serving Layer — Customer 360 Reporting Mart
-- Project: Azure Databricks Customer 360 Medallion Lakehouse
-- =============================================================================
-- Pattern: Gold Delta tables in ADLS → Snowflake External Stage → COPY INTO
-- Or: Databricks Delta Sharing → Snowflake native connector
-- =============================================================================

-- ── 1. Warehouse & Database Setup ─────────────────────────────────────────────

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS CUSTOMER360_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60           -- suspend after 60s idle (cost control)
    AUTO_RESUME    = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS CUSTOMER360_DB;
CREATE SCHEMA  IF NOT EXISTS CUSTOMER360_DB.GOLD;
CREATE SCHEMA  IF NOT EXISTS CUSTOMER360_DB.STAGING;
CREATE SCHEMA  IF NOT EXISTS CUSTOMER360_DB.AUDIT;

USE WAREHOUSE CUSTOMER360_WH;
USE DATABASE  CUSTOMER360_DB;

-- ── 2. RBAC — Roles & Privileges ──────────────────────────────────────────────

CREATE ROLE IF NOT EXISTS ANALYST_ROLE;       -- read-only for BI consumers
CREATE ROLE IF NOT EXISTS ENGINEER_ROLE;      -- read/write for pipeline
CREATE ROLE IF NOT EXISTS ADMIN_ROLE;         -- full access

GRANT USAGE ON WAREHOUSE CUSTOMER360_WH    TO ROLE ANALYST_ROLE;
GRANT USAGE ON DATABASE  CUSTOMER360_DB    TO ROLE ANALYST_ROLE;
GRANT USAGE ON SCHEMA    CUSTOMER360_DB.GOLD TO ROLE ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA CUSTOMER360_DB.GOLD TO ROLE ANALYST_ROLE;

-- ── 3. External Stage pointing to ADLS Gen2 Gold layer ────────────────────────

CREATE STORAGE INTEGRATION IF NOT EXISTS AZURE_ADLS_INTEGRATION
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'AZURE'
    ENABLED                   = TRUE
    AZURE_TENANT_ID           = '<your-tenant-id>'
    STORAGE_ALLOWED_LOCATIONS = ('azure://adlscustomer360.dfs.core.windows.net/customer360/gold/');

CREATE STAGE IF NOT EXISTS CUSTOMER360_DB.GOLD.ADLS_GOLD_STAGE
    URL                  = 'azure://adlscustomer360.dfs.core.windows.net/customer360/gold/'
    STORAGE_INTEGRATION  = AZURE_ADLS_INTEGRATION
    FILE_FORMAT          = (TYPE = 'PARQUET');

-- ── 4. Gold Tables in Snowflake ───────────────────────────────────────────────

USE SCHEMA CUSTOMER360_DB.GOLD;

-- 4a. Customer 360 — master fact table
CREATE OR REPLACE TABLE CUSTOMER_360 (
    customer_id                 VARCHAR(36)     NOT NULL,
    first_name                  VARCHAR(100),
    last_name                   VARCHAR(100),
    email                       VARCHAR(255),
    segment                     VARCHAR(50),
    age                         INTEGER,
    age_band                    VARCHAR(30),
    income_tier                 VARCHAR(20),
    credit_band                 VARCHAR(20),
    credit_score                INTEGER,
    annual_income               FLOAT,
    state                       VARCHAR(2),
    city                        VARCHAR(100),
    kyc_status                  VARCHAR(20),
    is_pep                      BOOLEAN,
    customer_since              DATE,
    tenure_years                FLOAT,
    preferred_channel           VARCHAR(30),
    -- Account KPIs
    active_account_count        INTEGER,
    total_balance               FLOAT,
    avg_balance                 FLOAT,
    total_credit_limit          FLOAT,
    product_diversity_count     INTEGER,
    -- Transaction KPIs
    total_transactions          INTEGER,
    total_spend_12m             FLOAT,
    total_deposits_12m          FLOAT,
    avg_txn_amount              FLOAT,
    max_txn_amount              FLOAT,
    net_cash_flow_12m           FLOAT,
    days_since_last_txn         INTEGER,
    last_transaction_date       DATE,
    txn_per_account             FLOAT,
    flagged_txn_count           INTEGER,
    avg_fraud_score             FLOAT,
    -- Support KPIs
    total_tickets               INTEGER,
    escalated_tickets           INTEGER,
    avg_csat_score              FLOAT,
    avg_resolution_days         FLOAT,
    last_ticket_date            DATE,
    -- Derived flags
    churn_risk_flag             BOOLEAN,
    high_value_flag             BOOLEAN,
    gold_created_at             TIMESTAMP_NTZ,
    -- Snowflake metadata
    _sf_load_ts                 TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_customer_360  PRIMARY KEY (customer_id)
)
CLUSTER BY (segment, state)           -- clustering key matches common filter patterns
COMMENT = 'Customer 360 gold table. One row per current active customer. Updated daily via COPY INTO from ADLS Gold.';

-- 4b. Monthly transaction summary
CREATE OR REPLACE TABLE TXN_MONTHLY_SUMMARY (
    customer_id         VARCHAR(36)     NOT NULL,
    txn_year            INTEGER,
    txn_month           INTEGER,
    period              VARCHAR(7),     -- e.g. '2024-03'
    txn_count           INTEGER,
    total_debit         FLOAT,
    total_credit        FLOAT,
    avg_txn_amount      FLOAT,
    max_fraud_score     FLOAT,
    flagged_count       INTEGER,
    channels_used       INTEGER,
    net_flow            FLOAT,
    _sf_load_ts         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (txn_year, txn_month)
COMMENT = 'Monthly transaction roll-up per customer. Feeds trend analysis in Power BI.';

-- 4c. Segment metrics
CREATE OR REPLACE TABLE SEGMENT_METRICS (
    segment                 VARCHAR(50)     NOT NULL,
    customer_count          INTEGER,
    avg_total_balance       FLOAT,
    avg_credit_score        FLOAT,
    avg_annual_income       FLOAT,
    avg_products_held       FLOAT,
    avg_txn_count           FLOAT,
    avg_spend_12m           FLOAT,
    aum_total               FLOAT,
    churn_risk_count        INTEGER,
    high_value_count        INTEGER,
    churn_risk_pct          FLOAT,
    high_value_pct          FLOAT,
    avg_csat                FLOAT,
    avg_tenure_years        FLOAT,
    calculated_at           TIMESTAMP_NTZ,
    _sf_load_ts             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_segment   PRIMARY KEY (segment)
)
COMMENT = 'Segment-level KPI table. One row per segment, refreshed daily.';

-- 4d. DQ audit table (mirror from Delta)
CREATE OR REPLACE TABLE AUDIT.DQ_RESULTS (
    batch_id        VARCHAR(20),
    run_ts          VARCHAR(30),
    table_name      VARCHAR(100),
    check_name      VARCHAR(200),
    rule            VARCHAR(500),
    status          VARCHAR(10),
    failing_count   INTEGER,
    total_count     INTEGER,
    pass_pct        FLOAT,
    _sf_load_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ── 5. COPY INTO — Load from ADLS Gold (run after Databricks Gold job) ────────

COPY INTO CUSTOMER360_DB.GOLD.CUSTOMER_360
FROM (
    SELECT
        $1:customer_id::VARCHAR(36),
        $1:first_name::VARCHAR(100),
        $1:last_name::VARCHAR(100),
        $1:email::VARCHAR(255),
        $1:segment::VARCHAR(50),
        $1:age::INTEGER,
        $1:age_band::VARCHAR(30),
        $1:income_tier::VARCHAR(20),
        $1:credit_band::VARCHAR(20),
        $1:credit_score::INTEGER,
        $1:annual_income::FLOAT,
        $1:state::VARCHAR(2),
        $1:city::VARCHAR(100),
        $1:kyc_status::VARCHAR(20),
        $1:is_pep::BOOLEAN,
        $1:customer_since::DATE,
        $1:tenure_years::FLOAT,
        $1:preferred_channel::VARCHAR(30),
        $1:active_account_count::INTEGER,
        $1:total_balance::FLOAT,
        $1:avg_balance::FLOAT,
        $1:total_credit_limit::FLOAT,
        $1:product_diversity_count::INTEGER,
        $1:total_transactions::INTEGER,
        $1:total_spend_12m::FLOAT,
        $1:total_deposits_12m::FLOAT,
        $1:avg_txn_amount::FLOAT,
        $1:max_txn_amount::FLOAT,
        $1:net_cash_flow_12m::FLOAT,
        $1:days_since_last_txn::INTEGER,
        $1:last_transaction_date::DATE,
        $1:txn_per_account::FLOAT,
        $1:flagged_txn_count::INTEGER,
        $1:avg_fraud_score::FLOAT,
        $1:total_tickets::INTEGER,
        $1:escalated_tickets::INTEGER,
        $1:avg_csat_score::FLOAT,
        $1:avg_resolution_days::FLOAT,
        $1:last_ticket_date::DATE,
        $1:churn_risk_flag::BOOLEAN,
        $1:high_value_flag::BOOLEAN,
        $1:gold_created_at::TIMESTAMP_NTZ
    FROM @CUSTOMER360_DB.GOLD.ADLS_GOLD_STAGE/customer_360/
)
FILE_FORMAT = (TYPE = 'PARQUET')
ON_ERROR    = 'CONTINUE'
PURGE       = FALSE;
