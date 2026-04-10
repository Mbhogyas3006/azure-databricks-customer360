# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Customer 360 Aggregated Marts
# MAGIC **Project:** Azure Databricks Customer 360 Medallion Lakehouse
# MAGIC **Layer:** Gold (analytics-ready, denormalized, reporting-optimized)
# MAGIC
# MAGIC Gold produces four reporting-ready tables:
# MAGIC | Table | Consumer | Key metrics |
# MAGIC |---|---|---|
# MAGIC | `gold_customer_360` | Power BI, Snowflake | Full customer profile + aggregated KPIs |
# MAGIC | `gold_txn_summary` | Risk, Finance | Monthly transaction roll-ups per customer |
# MAGIC | `gold_segment_metrics` | Marketing | Segment-level KPIs |
# MAGIC | `gold_support_kpis` | Operations | Ticket resolution & CSAT metrics |

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F, Window
import os

spark = SparkSession.builder.appName("Customer360-Gold").getOrCreate()

LOCAL_DEV   = not os.path.exists("/mnt/adlscustomer360")
SILVER_PATH = "/tmp/customer360/silver" if LOCAL_DEV else "/mnt/customer360/silver"
GOLD_PATH   = "/tmp/customer360/gold"   if LOCAL_DEV else "/mnt/customer360/gold"

# ── Read Silver tables ────────────────────────────────────────────────────────
customers = (
    spark.read.format("delta").load(f"{SILVER_PATH}/customers")
    .filter(F.col("scd_is_current") == True)
)
accounts    = spark.read.format("delta").load(f"{SILVER_PATH}/accounts")
txns        = spark.read.format("delta").load(f"{SILVER_PATH}/transactions")
tickets     = spark.read.format("delta").load(f"{SILVER_PATH}/support_tickets")
branches    = spark.read.format("delta").load(f"{SILVER_PATH}/branches")

print("Silver tables loaded")
for name, df in [("customers", customers), ("accounts", accounts),
                 ("txns", txns), ("tickets", tickets), ("branches", branches)]:
    print(f"  {name:<15} {df.count():>6,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. gold_customer_360
# MAGIC One row per current customer — full profile + account + txn + ticket KPIs.

# COMMAND ----------

# ── Account aggregations per customer ─────────────────────────────────────────
acct_agg = (
    accounts
    .filter(F.col("account_status") == "Active")
    .groupBy("customer_id")
    .agg(
        F.count("account_id")                         .alias("active_account_count"),
        F.sum("balance")                              .alias("total_balance"),
        F.avg("balance")                              .alias("avg_balance"),
        F.max("balance")                              .alias("max_account_balance"),
        F.sum("credit_limit")                         .alias("total_credit_limit"),
        F.collect_set("product_type")                 .alias("product_types_held"),
        F.countDistinct("product_type")               .alias("product_diversity_count"),
        F.min("open_date")                            .alias("oldest_account_date"),
        F.avg("days_since_activity")                  .alias("avg_days_since_activity"),
    )
)

# ── Transaction aggregations ───────────────────────────────────────────────────
txn_agg = (
    txns
    .groupBy("customer_id")
    .agg(
        F.count("transaction_id")                      .alias("total_transactions"),
        F.sum(F.when(F.col("direction") == "Debit",  F.col("amount")).otherwise(0))
                                                       .alias("total_spend_12m"),
        F.sum(F.when(F.col("direction") == "Credit", F.col("amount")).otherwise(0))
                                                       .alias("total_deposits_12m"),
        F.avg("amount")                                .alias("avg_txn_amount"),
        F.max("amount")                                .alias("max_txn_amount"),
        F.max("transaction_date")                      .alias("last_transaction_date"),
        F.sum(F.col("is_flagged").cast("integer"))     .alias("flagged_txn_count"),
        F.avg("fraud_score")                           .alias("avg_fraud_score"),
        F.countDistinct("channel")                     .alias("distinct_channels_used"),
    )
)

# ── Support ticket aggregations ────────────────────────────────────────────────
ticket_agg = (
    tickets
    .groupBy("customer_id")
    .agg(
        F.count("ticket_id")                           .alias("total_tickets"),
        F.sum(F.col("is_escalated").cast("integer"))   .alias("escalated_tickets"),
        F.avg("resolution_days")                       .alias("avg_resolution_days"),
        F.avg("csat_score")                            .alias("avg_csat_score"),
        F.max("created_date")                          .alias("last_ticket_date"),
        F.countDistinct("category")                    .alias("distinct_ticket_categories"),
    )
)

# ── Assemble Customer 360 ──────────────────────────────────────────────────────
gold_c360 = (
    customers
    .join(acct_agg,   on="customer_id", how="left")
    .join(txn_agg,    on="customer_id", how="left")
    .join(ticket_agg, on="customer_id", how="left")
    # ── Fill nulls for customers with no activity yet ─────────────────────────
    .fillna({
        "active_account_count":   0,
        "total_balance":          0.0,
        "total_transactions":     0,
        "total_spend_12m":        0.0,
        "total_deposits_12m":     0.0,
        "total_tickets":          0,
        "escalated_tickets":      0,
        "flagged_txn_count":      0,
    })
    # ── Derived KPIs ──────────────────────────────────────────────────────────
    .withColumn("days_since_last_txn",
        F.datediff(F.current_date(), F.col("last_transaction_date")))
    .withColumn("net_cash_flow_12m",
        F.col("total_deposits_12m") - F.col("total_spend_12m"))
    .withColumn("txn_per_account",
        F.when(F.col("active_account_count") > 0,
               F.round(F.col("total_transactions") / F.col("active_account_count"), 2))
         .otherwise(0))
    .withColumn("churn_risk_flag",
        (F.col("days_since_last_txn") > 90) |
        (F.col("avg_csat_score") < 3.0) |
        (F.col("escalated_tickets") > 1)
    )
    .withColumn("high_value_flag",
        (F.col("total_balance") > 100_000) |
        (F.col("segment").isin("Premier", "Private"))
    )
    .withColumn("gold_created_at", F.current_timestamp())
    # ── Select final columns ──────────────────────────────────────────────────
    .select(
        "customer_id", "first_name", "last_name", "email",
        "segment", "age", "age_band", "income_tier", "credit_band",
        "credit_score", "annual_income", "state", "city",
        "kyc_status", "is_pep", "customer_since", "tenure_years",
        "preferred_channel",
        "active_account_count", "total_balance", "avg_balance",
        "total_credit_limit", "product_diversity_count", "product_types_held",
        "total_transactions", "total_spend_12m", "total_deposits_12m",
        "avg_txn_amount", "max_txn_amount", "net_cash_flow_12m",
        "days_since_last_txn", "last_transaction_date", "txn_per_account",
        "flagged_txn_count", "avg_fraud_score",
        "total_tickets", "escalated_tickets", "avg_csat_score",
        "avg_resolution_days", "last_ticket_date",
        "churn_risk_flag", "high_value_flag",
        "gold_created_at",
    )
)

(
    gold_c360.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("segment")
    .save(f"{GOLD_PATH}/customer_360")
)
print(f"gold_customer_360: {gold_c360.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. gold_txn_monthly_summary
# MAGIC Monthly roll-up per customer — feeds trend charts in Power BI.

# COMMAND ----------

gold_txn_monthly = (
    txns
    .groupBy("customer_id", "txn_year", "txn_month")
    .agg(
        F.count("transaction_id")                                .alias("txn_count"),
        F.sum(F.when(F.col("direction") == "Debit",  F.col("amount")).otherwise(0))
                                                                 .alias("total_debit"),
        F.sum(F.when(F.col("direction") == "Credit", F.col("amount")).otherwise(0))
                                                                 .alias("total_credit"),
        F.avg("amount")                                          .alias("avg_txn_amount"),
        F.max("fraud_score")                                     .alias("max_fraud_score"),
        F.sum(F.col("is_flagged").cast("integer"))               .alias("flagged_count"),
        F.countDistinct("channel")                               .alias("channels_used"),
    )
    .withColumn("net_flow", F.col("total_credit") - F.col("total_debit"))
    .withColumn("period",
        F.concat_ws("-", F.col("txn_year"), F.lpad(F.col("txn_month"), 2, "0")))
)

(
    gold_txn_monthly.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("txn_year", "txn_month")
    .save(f"{GOLD_PATH}/txn_monthly_summary")
)
print(f"gold_txn_monthly_summary: {gold_txn_monthly.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. gold_segment_metrics
# MAGIC Segment-level KPI table — Marketing & Strategy dashboard.

# COMMAND ----------

gold_segment = (
    gold_c360
    .groupBy("segment")
    .agg(
        F.count("customer_id")              .alias("customer_count"),
        F.avg("total_balance")              .alias("avg_total_balance"),
        F.avg("credit_score")               .alias("avg_credit_score"),
        F.avg("annual_income")              .alias("avg_annual_income"),
        F.avg("product_diversity_count")    .alias("avg_products_held"),
        F.avg("total_transactions")         .alias("avg_txn_count"),
        F.avg("total_spend_12m")            .alias("avg_spend_12m"),
        F.sum("total_balance")              .alias("aum_total"),
        F.sum(F.col("churn_risk_flag").cast("integer")).alias("churn_risk_count"),
        F.sum(F.col("high_value_flag").cast("integer")).alias("high_value_count"),
        F.avg("avg_csat_score")             .alias("avg_csat"),
        F.avg("tenure_years")               .alias("avg_tenure_years"),
    )
    .withColumn("churn_risk_pct",
        F.round(F.col("churn_risk_count") / F.col("customer_count") * 100, 2))
    .withColumn("high_value_pct",
        F.round(F.col("high_value_count") / F.col("customer_count") * 100, 2))
    .withColumn("calculated_at", F.current_timestamp())
)

(
    gold_segment.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{GOLD_PATH}/segment_metrics")
)
print(f"gold_segment_metrics: {gold_segment.count():,} rows")
gold_segment.select("segment", "customer_count", "avg_total_balance", "avg_credit_score",
                    "churn_risk_pct", "avg_csat").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. gold_support_kpis
# MAGIC Daily support ticket KPIs — Operations & Customer Experience team.

# COMMAND ----------

gold_support = (
    tickets
    .groupBy("created_date", "category", "priority")
    .agg(
        F.count("ticket_id")                              .alias("ticket_count"),
        F.sum(F.col("is_escalated").cast("integer"))      .alias("escalation_count"),
        F.avg("resolution_days")                          .alias("avg_resolution_days"),
        F.avg("csat_score")                               .alias("avg_csat"),
        F.countDistinct("customer_id")                    .alias("distinct_customers"),
        F.sum(F.col("is_resolved").cast("integer"))       .alias("resolved_count"),
    )
    .withColumn("resolution_rate",
        F.round(F.col("resolved_count") / F.col("ticket_count") * 100, 2))
    .withColumn("escalation_rate",
        F.round(F.col("escalation_count") / F.col("ticket_count") * 100, 2))
    .withColumn("calculated_at", F.current_timestamp())
)

(
    gold_support.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("created_date")
    .save(f"{GOLD_PATH}/support_kpis")
)
print(f"gold_support_kpis: {gold_support.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold layer audit

# COMMAND ----------

gold_tables = {
    "customer_360":       f"{GOLD_PATH}/customer_360",
    "txn_monthly_summary":f"{GOLD_PATH}/txn_monthly_summary",
    "segment_metrics":    f"{GOLD_PATH}/segment_metrics",
    "support_kpis":       f"{GOLD_PATH}/support_kpis",
}

print("\n=== Gold Layer Audit ===")
for name, path in gold_tables.items():
    cnt = spark.read.format("delta").load(path).count()
    print(f"  {name:<24} {cnt:>6,} rows")
print("========================")

# Sample Customer 360 output
print("\n=== Sample Customer 360 (top 3) ===")
(
    spark.read.format("delta").load(f"{GOLD_PATH}/customer_360")
    .select("first_name","last_name","segment","total_balance","credit_score",
            "total_transactions","churn_risk_flag","high_value_flag")
    .limit(3)
    .show(truncate=False)
)
