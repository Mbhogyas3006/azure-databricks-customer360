# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Framework
# MAGIC **Project:** Azure Databricks Customer 360 Medallion Lakehouse
# MAGIC
# MAGIC Rule-based DQ checks run AFTER Silver transforms, BEFORE Gold promotion.
# MAGIC Any FAIL blocks the Gold write and raises an alert.
# MAGIC Results are written to a `dq_results` Delta table for audit trail.

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F
from datetime import datetime
import os

spark = SparkSession.builder.appName("Customer360-DQ").getOrCreate()

LOCAL_DEV   = not os.path.exists("/mnt/adlscustomer360")
SILVER_PATH = "/tmp/customer360/silver" if LOCAL_DEV else "/mnt/customer360/silver"
DQ_PATH     = "/tmp/customer360/dq"     if LOCAL_DEV else "/mnt/customer360/dq"

BATCH_ID  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
RUN_TS    = datetime.utcnow().isoformat()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQ Engine

# COMMAND ----------

dq_results = []

def check(name: str, table: str, rule: str, passed: bool, failing_count: int = 0, total: int = 0):
    """Record a DQ check result."""
    status = "PASS" if passed else "FAIL"
    pct    = round((1 - failing_count / total) * 100, 2) if total > 0 else 100.0
    dq_results.append({
        "batch_id":      BATCH_ID,
        "run_ts":        RUN_TS,
        "table_name":    table,
        "check_name":    name,
        "rule":          rule,
        "status":        status,
        "failing_count": failing_count,
        "total_count":   total,
        "pass_pct":      pct,
    })
    icon = "PASS" if passed else "FAIL"
    print(f"  [{icon}] {name:<40} failing={failing_count:>4} / {total:>5}  ({pct:.1f}%)")
    return passed


def not_null(df, col_name: str, table: str, threshold_pct: float = 0.0):
    total    = df.count()
    failing  = df.filter(F.col(col_name).isNull()).count()
    pass_pct = (1 - failing / total) * 100 if total > 0 else 100.0
    passed   = (failing / total * 100) <= threshold_pct if total > 0 else True
    return check(
        name=f"not_null_{col_name}",
        table=table,
        rule=f"{col_name} IS NOT NULL (tolerance {threshold_pct}%)",
        passed=passed,
        failing_count=failing,
        total=total,
    )


def unique(df, col_name: str, table: str):
    total    = df.count()
    distinct = df.select(col_name).distinct().count()
    failing  = total - distinct
    return check(
        name=f"unique_{col_name}",
        table=table,
        rule=f"{col_name} must be unique",
        passed=(failing == 0),
        failing_count=failing,
        total=total,
    )


def value_in_set(df, col_name: str, valid_values: list, table: str):
    total   = df.count()
    failing = df.filter(~F.col(col_name).isin(valid_values)).count()
    return check(
        name=f"value_set_{col_name}",
        table=table,
        rule=f"{col_name} IN {valid_values}",
        passed=(failing == 0),
        failing_count=failing,
        total=total,
    )


def range_check(df, col_name: str, min_val, max_val, table: str, threshold_pct: float = 0.0):
    total   = df.count()
    failing = df.filter(
        F.col(col_name).isNotNull() &
        ((F.col(col_name) < min_val) | (F.col(col_name) > max_val))
    ).count()
    pass_pct = (1 - failing / total) * 100 if total > 0 else 100.0
    passed   = (failing / total * 100) <= threshold_pct if total > 0 else True
    return check(
        name=f"range_{col_name}",
        table=table,
        rule=f"{min_val} <= {col_name} <= {max_val}",
        passed=passed,
        failing_count=failing,
        total=total,
    )


def referential_integrity(child_df, parent_df, child_key: str, parent_key: str,
                           child_table: str, parent_table: str):
    total   = child_df.count()
    parent_keys = parent_df.select(parent_key).distinct()
    failing = child_df.join(parent_keys, child_df[child_key] == parent_keys[parent_key], "left_anti").count()
    return check(
        name=f"ri_{child_table}_{child_key}",
        table=child_table,
        rule=f"{child_table}.{child_key} EXISTS IN {parent_table}.{parent_key}",
        passed=(failing == 0),
        failing_count=failing,
        total=total,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run DQ Checks

# COMMAND ----------

customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers").filter("scd_is_current = true")
accounts  = spark.read.format("delta").load(f"{SILVER_PATH}/accounts")
txns      = spark.read.format("delta").load(f"{SILVER_PATH}/transactions")
tickets   = spark.read.format("delta").load(f"{SILVER_PATH}/support_tickets")

print("\n=== CUSTOMERS ===")
not_null(customers, "customer_id",   "silver.customers")
not_null(customers, "email",         "silver.customers", threshold_pct=1.0)
not_null(customers, "segment",       "silver.customers")
not_null(customers, "kyc_status",    "silver.customers")
unique(customers,   "customer_id",   "silver.customers")
value_in_set(customers, "segment",   ["Retail","Premier","Private","SMB","Corporate"], "silver.customers")
value_in_set(customers, "kyc_status",["Verified","Pending","Failed"], "silver.customers")
range_check(customers, "credit_score", 300, 850, "silver.customers", threshold_pct=0.5)
range_check(customers, "annual_income", 0, 10_000_000, "silver.customers")
range_check(customers, "age", 18, 120, "silver.customers", threshold_pct=1.0)

print("\n=== ACCOUNTS ===")
not_null(accounts, "account_id",  "silver.accounts")
not_null(accounts, "customer_id", "silver.accounts")
not_null(accounts, "product_type","silver.accounts")
unique(accounts,   "account_id",  "silver.accounts")
value_in_set(accounts, "account_status", ["Active","Dormant","Closed","Suspended"], "silver.accounts")
range_check(accounts, "balance", 0, 50_000_000, "silver.accounts")
referential_integrity(accounts, customers, "customer_id", "customer_id",
                      "silver.accounts", "silver.customers")

print("\n=== TRANSACTIONS ===")
not_null(txns, "transaction_id",  "silver.transactions")
not_null(txns, "account_id",      "silver.transactions")
not_null(txns, "amount",          "silver.transactions")
not_null(txns, "transaction_date","silver.transactions")
unique(txns,   "transaction_id",  "silver.transactions")
range_check(txns, "amount", 0.01, 10_000_000, "silver.transactions", threshold_pct=0.1)
range_check(txns, "fraud_score", 0.0, 1.0, "silver.transactions")
value_in_set(txns, "direction", ["Debit","Credit"], "silver.transactions")
referential_integrity(txns, accounts, "account_id", "account_id",
                      "silver.transactions", "silver.accounts")

print("\n=== SUPPORT TICKETS ===")
not_null(tickets, "ticket_id",   "silver.support_tickets")
not_null(tickets, "customer_id", "silver.support_tickets")
not_null(tickets, "category",    "silver.support_tickets")
unique(tickets,   "ticket_id",   "silver.support_tickets")
range_check(tickets, "csat_score", 1, 5, "silver.support_tickets", threshold_pct=5.0)
value_in_set(tickets, "priority", ["Low","Medium","High","Critical"], "silver.support_tickets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write DQ results to audit table

# COMMAND ----------

dq_df = spark.createDataFrame(dq_results)
(
    dq_df.write
    .format("delta")
    .mode("append")
    .save(f"{DQ_PATH}/dq_results")
)

fail_count = dq_df.filter(F.col("status") == "FAIL").count()
pass_count = dq_df.filter(F.col("status") == "PASS").count()

print(f"\n=== DQ Summary ===")
print(f"  PASS: {pass_count}")
print(f"  FAIL: {fail_count}")

if fail_count > 0:
    print("\nFailed checks:")
    dq_df.filter(F.col("status") == "FAIL") \
         .select("table_name", "check_name", "failing_count", "total_count") \
         .show(truncate=False)
    raise Exception(f"Data quality check failed — {fail_count} rules violated. Review dq_results table.")

print("All DQ checks passed. Proceeding to Gold layer.")
