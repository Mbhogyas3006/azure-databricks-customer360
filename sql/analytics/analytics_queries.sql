-- =============================================================================
-- Analytics Queries — Customer 360 Lakehouse
-- Works against: Snowflake GOLD schema  |  Databricks SQL  |  Azure Synapse
-- =============================================================================

-- ── Q1. Customer segmentation summary ────────────────────────────────────────
-- Business question: How is our customer base distributed across segments,
-- and what is the average balance and credit quality per segment?
-- Consumer: Executive dashboard / strategy team
SELECT
    segment,
    COUNT(*)                                        AS customer_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct_of_total,
    ROUND(AVG(total_balance), 0)                    AS avg_balance,
    ROUND(AVG(credit_score), 0)                     AS avg_credit_score,
    ROUND(SUM(total_balance) / 1e6, 2)              AS aum_millions,
    ROUND(AVG(tenure_years), 1)                     AS avg_tenure_yrs,
    ROUND(AVG(product_diversity_count), 1)          AS avg_products_held
FROM CUSTOMER360_DB.GOLD.CUSTOMER_360
GROUP BY segment
ORDER BY aum_millions DESC;


-- ── Q2. Churn risk dashboard ──────────────────────────────────────────────────
-- Business question: Which customers are at highest churn risk, and what
-- is their value to the bank?
-- Consumer: Retention marketing team
SELECT
    customer_id,
    first_name || ' ' || last_name                  AS customer_name,
    segment,
    credit_band,
    ROUND(total_balance, 0)                         AS total_balance,
    days_since_last_txn,
    ROUND(avg_csat_score, 1)                        AS csat,
    escalated_tickets,
    total_tickets,
    CASE
        WHEN days_since_last_txn > 180 THEN 'Critical'
        WHEN days_since_last_txn > 90  THEN 'High'
        WHEN avg_csat_score < 2.5      THEN 'High'
        ELSE 'Medium'
    END                                             AS churn_priority
FROM CUSTOMER360_DB.GOLD.CUSTOMER_360
WHERE churn_risk_flag = TRUE
ORDER BY total_balance DESC, days_since_last_txn DESC
LIMIT 100;


-- ── Q3. High-value customer portfolio view ────────────────────────────────────
-- Business question: Who are our top 50 customers by AUM, and what is
-- their full product and engagement profile?
-- Consumer: Private banking / relationship managers
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name             AS customer_name,
    c.segment,
    c.age_band,
    c.income_tier,
    c.state,
    ROUND(c.total_balance, 0)                       AS total_balance,
    c.active_account_count,
    c.product_diversity_count,
    ROUND(c.total_spend_12m, 0)                     AS spend_12m,
    ROUND(c.net_cash_flow_12m, 0)                   AS net_cash_flow,
    c.preferred_channel,
    ROUND(c.avg_csat_score, 2)                      AS csat_score,
    c.tenure_years
FROM CUSTOMER360_DB.GOLD.CUSTOMER_360 c
WHERE c.high_value_flag = TRUE
  AND c.kyc_status = 'Verified'
ORDER BY c.total_balance DESC
LIMIT 50;


-- ── Q4. Monthly spend trend (for Power BI line chart) ─────────────────────────
-- Business question: How is total customer spending trending month over month?
-- Consumer: Finance / CFO dashboard
SELECT
    t.period,
    t.txn_year,
    t.txn_month,
    SUM(t.txn_count)                                AS total_transactions,
    ROUND(SUM(t.total_debit) / 1e6, 2)              AS total_spend_millions,
    ROUND(SUM(t.total_credit) / 1e6, 2)             AS total_deposits_millions,
    ROUND(SUM(t.net_flow) / 1e6, 2)                 AS net_flow_millions,
    COUNT(DISTINCT t.customer_id)                   AS active_customers,
    ROUND(AVG(t.avg_txn_amount), 2)                 AS avg_txn_size,
    SUM(t.flagged_count)                            AS flagged_transactions
FROM CUSTOMER360_DB.GOLD.TXN_MONTHLY_SUMMARY t
GROUP BY t.period, t.txn_year, t.txn_month
ORDER BY t.txn_year, t.txn_month;


-- ── Q5. Geographic distribution for map visual ────────────────────────────────
SELECT
    state,
    COUNT(*)                                        AS customer_count,
    ROUND(SUM(total_balance) / 1e6, 2)              AS aum_millions,
    ROUND(AVG(credit_score), 0)                     AS avg_credit_score,
    SUM(CASE WHEN churn_risk_flag THEN 1 ELSE 0 END) AS churn_risk_count,
    SUM(CASE WHEN high_value_flag THEN 1 ELSE 0 END) AS high_value_count
FROM CUSTOMER360_DB.GOLD.CUSTOMER_360
GROUP BY state
ORDER BY aum_millions DESC;


-- ── Q6. Product cross-sell opportunities ─────────────────────────────────────
-- Find Retail/Premier customers who have Checking but no investment product
-- Business question: Who should the wealth team target next?
SELECT
    customer_id,
    first_name || ' ' || last_name                  AS customer_name,
    segment,
    ROUND(total_balance, 0)                         AS total_balance,
    ROUND(annual_income, 0)                         AS annual_income,
    income_tier,
    product_diversity_count,
    credit_score,
    tenure_years
FROM CUSTOMER360_DB.GOLD.CUSTOMER_360
WHERE segment IN ('Retail', 'Premier')
  AND ARRAY_CONTAINS('Checking'::VARIANT, product_types_held)
  AND NOT ARRAY_CONTAINS('Investment'::VARIANT, product_types_held)
  AND kyc_status = 'Verified'
  AND annual_income > 75000
  AND churn_risk_flag = FALSE
ORDER BY total_balance DESC
LIMIT 200;


-- ── Q7. Support ticket resolution KPIs by category ───────────────────────────
-- Business question: Which ticket categories have the worst resolution times
-- and lowest CSAT? — Operations SLA review
SELECT
    category,
    COUNT(*)                                        AS total_tickets,
    ROUND(AVG(resolution_days), 1)                  AS avg_resolution_days,
    ROUND(AVG(avg_csat), 2)                         AS avg_csat,
    ROUND(AVG(resolution_rate), 1)                  AS resolution_rate_pct,
    ROUND(AVG(escalation_rate), 1)                  AS escalation_rate_pct,
    SUM(ticket_count)                               AS total_volume
FROM CUSTOMER360_DB.GOLD.SUPPORT_KPIS
GROUP BY category
ORDER BY avg_resolution_days DESC;


-- ── Q8. SCD2 history query — customer segment changes ─────────────────────────
-- Business question: How many customers changed segment in the last 12 months?
-- Runs against Silver table (Databricks SQL / Synapse — not Snowflake Gold)
SELECT
    customer_id,
    segment                                         AS new_segment,
    scd_start_date,
    scd_end_date,
    scd_version,
    scd_is_current
FROM silver_customers
WHERE scd_version > 1
  AND scd_start_date >= DATEADD(month, -12, CURRENT_DATE())
ORDER BY customer_id, scd_version;


-- ── Q9. Fraud exposure summary ────────────────────────────────────────────────
-- Business question: What is the total flagged transaction exposure by segment?
SELECT
    c.segment,
    COUNT(DISTINCT c.customer_id)                   AS customers_with_flags,
    SUM(c.flagged_txn_count)                        AS total_flagged_txns,
    ROUND(AVG(c.avg_fraud_score), 4)                AS avg_fraud_score,
    ROUND(SUM(c.total_spend_12m), 0)                AS total_spend,
    ROUND(SUM(c.flagged_txn_count) * 100.0
          / NULLIF(SUM(c.total_transactions), 0), 2) AS flagged_txn_pct
FROM CUSTOMER360_DB.GOLD.CUSTOMER_360 c
WHERE c.flagged_txn_count > 0
GROUP BY c.segment
ORDER BY total_flagged_txns DESC;


-- ── Q10. Data quality audit view ──────────────────────────────────────────────
-- Shows latest DQ run results for ops monitoring
SELECT
    table_name,
    check_name,
    status,
    failing_count,
    total_count,
    pass_pct,
    run_ts
FROM CUSTOMER360_DB.AUDIT.DQ_RESULTS
WHERE batch_id = (SELECT MAX(batch_id) FROM CUSTOMER360_DB.AUDIT.DQ_RESULTS)
ORDER BY status DESC, table_name, check_name;
