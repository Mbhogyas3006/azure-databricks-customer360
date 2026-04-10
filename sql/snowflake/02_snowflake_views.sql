-- =============================================================================
-- Snowflake Views — Customer 360 Power BI Reporting Layer
-- =============================================================================
-- These views are what Power BI connects to via DirectQuery.
-- They pre-join and pre-calculate so PBI doesn't do heavy SQL at query time.
-- =============================================================================

USE DATABASE CUSTOMER360_DB;
USE SCHEMA   CUSTOMER360_DB.GOLD;
USE WAREHOUSE CUSTOMER360_WH;

-- ── View 1. Executive KPI view (KPI card page) ────────────────────────────────
CREATE OR REPLACE VIEW VW_EXECUTIVE_KPIS AS
SELECT
    COUNT(*)                                                AS total_customers,
    ROUND(SUM(total_balance) / 1e9, 3)                      AS aum_billions,
    ROUND(AVG(credit_score), 0)                             AS avg_credit_score,
    ROUND(AVG(tenure_years), 1)                             AS avg_tenure_years,
    SUM(CASE WHEN high_value_flag  THEN 1 ELSE 0 END)       AS high_value_customers,
    SUM(CASE WHEN churn_risk_flag  THEN 1 ELSE 0 END)       AS churn_risk_customers,
    ROUND(AVG(product_diversity_count), 2)                  AS avg_products_per_customer,
    ROUND(SUM(total_spend_12m) / 1e6, 2)                   AS total_spend_12m_millions,
    ROUND(AVG(avg_csat_score), 2)                           AS portfolio_avg_csat,
    ROUND(SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END)
          * 100.0 / COUNT(*), 1)                            AS churn_risk_pct,
    CURRENT_TIMESTAMP()                                     AS refreshed_at
FROM CUSTOMER_360
WHERE kyc_status = 'Verified';

-- ── View 2. Segment breakdown (segment donut + bar charts) ────────────────────
CREATE OR REPLACE VIEW VW_SEGMENT_BREAKDOWN AS
SELECT
    s.segment,
    s.customer_count,
    ROUND(s.customer_count * 100.0 / SUM(s.customer_count) OVER(), 1)  AS pct_of_customers,
    ROUND(s.aum_total / 1e6, 2)                                         AS aum_millions,
    ROUND(s.aum_total * 100.0 / SUM(s.aum_total) OVER(), 1)            AS pct_of_aum,
    ROUND(s.avg_total_balance, 0)                                       AS avg_balance,
    ROUND(s.avg_credit_score, 0)                                        AS avg_credit_score,
    ROUND(s.avg_products_held, 2)                                       AS avg_products,
    ROUND(s.avg_spend_12m, 0)                                           AS avg_spend_12m,
    s.churn_risk_pct,
    s.high_value_pct,
    ROUND(s.avg_csat, 2)                                                AS avg_csat,
    ROUND(s.avg_tenure_years, 1)                                        AS avg_tenure_yrs
FROM SEGMENT_METRICS s
ORDER BY s.aum_total DESC;

-- ── View 3. Geographic map view (state-level choropleth) ──────────────────────
CREATE OR REPLACE VIEW VW_GEOGRAPHIC_DISTRIBUTION AS
SELECT
    state,
    COUNT(*)                                                AS customer_count,
    ROUND(SUM(total_balance) / 1e6, 2)                      AS aum_millions,
    ROUND(AVG(credit_score), 0)                             AS avg_credit_score,
    ROUND(AVG(annual_income), 0)                            AS avg_income,
    SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END)        AS churn_risk_count,
    SUM(CASE WHEN high_value_flag THEN 1 ELSE 0 END)        AS high_value_count,
    ROUND(AVG(product_diversity_count), 2)                  AS avg_products,
    ROUND(AVG(avg_csat_score), 2)                           AS avg_csat
FROM CUSTOMER_360
GROUP BY state
ORDER BY aum_millions DESC;

-- ── View 4. Churn risk cohort view ────────────────────────────────────────────
CREATE OR REPLACE VIEW VW_CHURN_RISK_DETAIL AS
SELECT
    customer_id,
    first_name || ' ' || last_name                          AS customer_name,
    email,
    segment,
    age_band,
    income_tier,
    state,
    ROUND(total_balance, 0)                                 AS total_balance,
    ROUND(total_spend_12m, 0)                               AS spend_12m,
    days_since_last_txn,
    ROUND(avg_csat_score, 1)                                AS csat_score,
    escalated_tickets,
    total_tickets,
    preferred_channel,
    CASE
        WHEN days_since_last_txn > 180 AND total_balance > 50000  THEN 'Critical — High Value'
        WHEN days_since_last_txn > 180                             THEN 'Critical'
        WHEN avg_csat_score < 2.5 AND escalated_tickets > 0       THEN 'High — Service Issue'
        WHEN avg_csat_score < 2.5                                  THEN 'High — CSAT'
        WHEN escalated_tickets > 1                                 THEN 'Medium — Escalations'
        ELSE 'Medium'
    END                                                     AS churn_priority,
    CASE
        WHEN preferred_channel = 'Mobile' THEN 'Push notification + app offer'
        WHEN preferred_channel = 'Branch' THEN 'Relationship manager outreach'
        WHEN preferred_channel = 'Phone'  THEN 'Proactive call — retention offer'
        ELSE 'Email campaign'
    END                                                     AS recommended_action
FROM CUSTOMER_360
WHERE churn_risk_flag = TRUE
  AND kyc_status = 'Verified'
ORDER BY total_balance DESC;

-- ── View 5. Monthly transaction trend ────────────────────────────────────────
CREATE OR REPLACE VIEW VW_MONTHLY_TXN_TREND AS
SELECT
    period,
    txn_year,
    txn_month,
    SUM(txn_count)                                          AS total_transactions,
    ROUND(SUM(total_debit) / 1e6, 3)                        AS total_spend_millions,
    ROUND(SUM(total_credit) / 1e6, 3)                       AS total_deposits_millions,
    ROUND(SUM(net_flow) / 1e6, 3)                           AS net_flow_millions,
    COUNT(DISTINCT customer_id)                             AS active_customers,
    ROUND(AVG(avg_txn_amount), 2)                           AS avg_txn_size,
    SUM(flagged_count)                                      AS flagged_txns,
    -- Month-over-month growth
    LAG(SUM(total_debit), 1) OVER (ORDER BY txn_year, txn_month)   AS prev_month_spend,
    ROUND(
        (SUM(total_debit) - LAG(SUM(total_debit), 1) OVER (ORDER BY txn_year, txn_month))
        * 100.0
        / NULLIF(LAG(SUM(total_debit), 1) OVER (ORDER BY txn_year, txn_month), 0),
    1)                                                      AS spend_mom_pct_change
FROM TXN_MONTHLY_SUMMARY
GROUP BY period, txn_year, txn_month
ORDER BY txn_year, txn_month;

-- ── View 6. Cross-sell opportunity list ───────────────────────────────────────
CREATE OR REPLACE VIEW VW_CROSSSELL_OPPORTUNITIES AS
SELECT
    customer_id,
    first_name || ' ' || last_name                          AS customer_name,
    segment,
    income_tier,
    credit_band,
    ROUND(total_balance, 0)                                 AS total_balance,
    ROUND(annual_income, 0)                                 AS annual_income,
    product_diversity_count,
    product_types_held,
    preferred_channel,
    tenure_years,
    -- Identify what they DON'T have
    CASE WHEN NOT ARRAY_CONTAINS('Investment'::VARIANT, product_types_held)
         THEN TRUE ELSE FALSE END                            AS investment_opportunity,
    CASE WHEN NOT ARRAY_CONTAINS('Mortgage'::VARIANT, product_types_held)
         AND annual_income > 60000
         THEN TRUE ELSE FALSE END                            AS mortgage_opportunity,
    CASE WHEN NOT ARRAY_CONTAINS('Credit Card'::VARIANT, product_types_held)
         AND credit_score > 680
         THEN TRUE ELSE FALSE END                            AS credit_card_opportunity
FROM CUSTOMER_360
WHERE kyc_status = 'Verified'
  AND churn_risk_flag = FALSE
  AND segment IN ('Retail', 'Premier', 'Private')
HAVING investment_opportunity OR mortgage_opportunity OR credit_card_opportunity
ORDER BY total_balance DESC;

-- ── View 7. Support operations dashboard ──────────────────────────────────────
CREATE OR REPLACE VIEW VW_SUPPORT_OPERATIONS AS
SELECT
    category,
    priority,
    SUM(ticket_count)                                       AS total_tickets,
    ROUND(AVG(avg_resolution_days), 1)                      AS avg_days_to_resolve,
    ROUND(AVG(avg_csat), 2)                                 AS avg_csat_score,
    ROUND(AVG(resolution_rate), 1)                          AS resolution_rate_pct,
    ROUND(AVG(escalation_rate), 1)                          AS escalation_rate_pct,
    SUM(escalation_count)                                   AS total_escalations,
    SUM(distinct_customers)                                 AS customers_affected
FROM SUPPORT_KPIS
GROUP BY category, priority
ORDER BY avg_days_to_resolve DESC, category;

-- ── Grant Power BI service account access ─────────────────────────────────────
-- Run as SYSADMIN after creating the views
GRANT SELECT ON VIEW VW_EXECUTIVE_KPIS              TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW VW_SEGMENT_BREAKDOWN           TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW VW_GEOGRAPHIC_DISTRIBUTION     TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW VW_CHURN_RISK_DETAIL           TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW VW_MONTHLY_TXN_TREND           TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW VW_CROSSSELL_OPPORTUNITIES     TO ROLE ANALYST_ROLE;
GRANT SELECT ON VIEW VW_SUPPORT_OPERATIONS          TO ROLE ANALYST_ROLE;
