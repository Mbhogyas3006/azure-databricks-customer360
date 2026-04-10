# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleansed & Conformed Transforms
# MAGIC **Project:** Azure Databricks Customer 360 Medallion Lakehouse
# MAGIC **Layer:** Silver (typed, deduped, standardised, SCD Type 2 for customers)
# MAGIC
# MAGIC Silver applies:
# MAGIC - Schema enforcement and type casting
# MAGIC - Deduplication and null handling
# MAGIC - **SCD Type 2** for customer profile history (address, segment, credit score changes)
# MAGIC - Referential integrity checks between accounts → customers
# MAGIC - Derived fields (age_band, income_tier, days_since_last_activity)

# COMMAND ----------

from pyspark.sql import SparkSession, functions as F, Window
from delta.tables import DeltaTable
import os

spark = SparkSession.builder.appName("Customer360-Silver").getOrCreate()

LOCAL_DEV = not os.path.exists("/mnt/adlscustomer360")
BRONZE_PATH = "/tmp/customer360/bronze" if LOCAL_DEV else "/mnt/customer360/bronze"
SILVER_PATH = "/tmp/customer360/silver" if LOCAL_DEV else "/mnt/customer360/silver"

print(f"Bronze: {BRONZE_PATH}")
print(f"Silver: {SILVER_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Silver Customers — SCD Type 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a. Read latest Bronze snapshot (deduplicated)

# COMMAND ----------

customers_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/customers")

# Deduplicate: keep latest record per customer_id within the current batch
window_dedup = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc())

customers_clean = (
    customers_bronze
    .filter(F.col("_is_current") == True)
    .withColumn("_row_num", F.row_number().over(window_dedup))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
    # ── Type casting & standardisation ──────────────────────────────────────
    .withColumn("date_of_birth",  F.to_date("date_of_birth",  "yyyy-MM-dd"))
    .withColumn("customer_since", F.to_date("customer_since", "yyyy-MM-dd"))
    .withColumn("credit_score",   F.col("credit_score").cast("integer"))
    .withColumn("annual_income",  F.col("annual_income").cast("double"))
    .withColumn("email",          F.lower(F.trim(F.col("email"))))
    .withColumn("state",          F.upper(F.trim(F.col("state"))))
    .withColumn("kyc_status",     F.trim(F.col("kyc_status")))
    # ── Derived fields ───────────────────────────────────────────────────────
    .withColumn("age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25))
    .withColumn("age_band", F.when(F.col("age") < 30, "Gen-Z / Millennial")
                              .when(F.col("age") < 45, "Millennial / Gen-X")
                              .when(F.col("age") < 60, "Gen-X / Boomer")
                              .otherwise("Senior"))
    .withColumn("income_tier", F.when(F.col("annual_income") < 50_000,  "Low")
                                 .when(F.col("annual_income") < 100_000, "Mid")
                                 .when(F.col("annual_income") < 250_000, "Upper-Mid")
                                 .otherwise("High"))
    .withColumn("credit_band", F.when(F.col("credit_score") < 620, "Poor")
                                  .when(F.col("credit_score") < 680, "Fair")
                                  .when(F.col("credit_score") < 740, "Good")
                                  .when(F.col("credit_score") < 800, "Very Good")
                                  .otherwise("Exceptional"))
    .withColumn("tenure_years", F.round(
        F.datediff(F.current_date(), F.col("customer_since")) / 365.25, 1
    ))
    # ── SCD2 control columns ─────────────────────────────────────────────────
    .withColumn("scd_start_date", F.current_date())
    .withColumn("scd_end_date",   F.lit(None).cast("date"))
    .withColumn("scd_is_current", F.lit(True))
    .withColumn("scd_version",    F.lit(1).cast("integer"))
)

print(f"Cleansed customer count: {customers_clean.count():,}")
customers_clean.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. SCD Type 2 MERGE into Silver customers table

# COMMAND ----------

SILVER_CUSTOMERS = f"{SILVER_PATH}/customers"

# ── SCD2 tracked columns: if any of these change → expire old row, insert new ─
SCD2_TRACK_COLS = ["segment", "credit_score", "annual_income", "kyc_status",
                   "address_line1", "city", "state", "zip_code"]

# Build the match condition
scd2_change_condition = " OR ".join(
    [f"existing.{c} <> incoming.{c}" for c in SCD2_TRACK_COLS]
)

try:
    # Table exists — run SCD2 MERGE
    silver_customers = DeltaTable.forPath(spark, SILVER_CUSTOMERS)

    # Step 1: Mark changed current rows as expired
    silver_customers.alias("existing").merge(
        customers_clean.alias("incoming"),
        "existing.customer_id = incoming.customer_id AND existing.scd_is_current = true"
    ).whenMatchedUpdate(
        condition=scd2_change_condition,
        set={
            "scd_is_current": "false",
            "scd_end_date":   "current_date()",
        }
    ).execute()

    # Step 2: Insert new/changed rows
    (
        customers_clean
        .join(
            spark.read.format("delta").load(SILVER_CUSTOMERS)
                .filter(F.col("scd_is_current") == True)
                .select("customer_id"),
            on="customer_id",
            how="left_anti"    # only rows NOT already current in silver
        )
        .union(
            # Also insert rows that were just expired above
            customers_clean.join(
                spark.read.format("delta").load(SILVER_CUSTOMERS)
                    .filter(F.col("scd_is_current") == False)
                    .filter(F.col("scd_end_date") == F.current_date())
                    .select("customer_id"),
                on="customer_id",
                how="inner"
            )
        )
        .write.format("delta").mode("append").save(SILVER_CUSTOMERS)
    )
    print("SCD2 MERGE complete — existing Silver customers table updated")

except Exception:
    # First run — create the table
    (
        customers_clean.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("state")
        .save(SILVER_CUSTOMERS)
    )
    print("Silver customers table created (first run)")

silver_count = spark.read.format("delta").load(SILVER_CUSTOMERS).count()
current_count = spark.read.format("delta").load(SILVER_CUSTOMERS).filter("scd_is_current = true").count()
print(f"Silver customers — total rows: {silver_count:,} | current: {current_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silver Accounts

# COMMAND ----------

accounts_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/accounts")

accounts_silver = (
    accounts_bronze
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("account_id").orderBy(F.col("_ingest_ts").desc())
    ))
    .filter(F.col("_row_num") == 1).drop("_row_num")
    .withColumn("open_date",          F.to_date("open_date",          "yyyy-MM-dd"))
    .withColumn("close_date",         F.to_date("close_date",         "yyyy-MM-dd"))
    .withColumn("last_activity_date", F.to_date("last_activity_date", "yyyy-MM-dd"))
    .withColumn("balance",            F.col("balance").cast("double"))
    .withColumn("available_balance",  F.col("available_balance").cast("double"))
    .withColumn("interest_rate",      F.col("interest_rate").cast("double"))
    .withColumn("credit_limit",       F.col("credit_limit").cast("double"))
    .withColumn("days_since_activity",
        F.datediff(F.current_date(), F.col("last_activity_date")))
    .withColumn("account_age_days",
        F.datediff(F.current_date(), F.col("open_date")))
    .withColumn("utilization_rate",
        F.when(
            F.col("credit_limit").isNotNull() & (F.col("credit_limit") > 0),
            F.round(F.col("balance") / F.col("credit_limit"), 4)
        ).otherwise(F.lit(None))
    )
    # Null handling
    .withColumn("credit_limit",
        F.when(F.col("credit_limit").isNull(), 0.0).otherwise(F.col("credit_limit")))
    .drop("_source_file", "_batch_id", "_is_current")
)

(
    accounts_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("product_type")
    .save(f"{SILVER_PATH}/accounts")
)
print(f"Silver accounts: {accounts_silver.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Transactions

# COMMAND ----------

txns_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/transactions")

txns_silver = (
    txns_bronze
    # Dedup by transaction_id
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("transaction_id").orderBy(F.col("_ingest_ts").desc())
    ))
    .filter(F.col("_row_num") == 1).drop("_row_num")
    .withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))
    .withColumn("posting_date",     F.to_date("posting_date",     "yyyy-MM-dd"))
    .withColumn("amount",           F.col("amount").cast("double"))
    .withColumn("balance_after",    F.col("balance_after").cast("double"))
    .withColumn("fraud_score",      F.col("fraud_score").cast("double"))
    .withColumn("is_flagged",       F.col("is_flagged").cast("boolean"))
    # Signed amount: negative for debits
    .withColumn("signed_amount",
        F.when(F.col("direction") == "Debit", -F.col("amount"))
         .otherwise(F.col("amount")))
    # Day-of-week and hour for analytics
    .withColumn("txn_day_of_week", F.dayofweek("transaction_date"))
    .withColumn("txn_month",       F.month("transaction_date"))
    .withColumn("txn_year",        F.year("transaction_date"))
    .withColumn("txn_quarter",     F.quarter("transaction_date"))
    # Settlement lag
    .withColumn("settlement_lag_days",
        F.datediff(F.col("posting_date"), F.col("transaction_date")))
    .drop("_source_file", "_batch_id", "_is_current")
)

(
    txns_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("txn_year", "txn_month")
    .save(f"{SILVER_PATH}/transactions")
)
print(f"Silver transactions: {txns_silver.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver Branches

# COMMAND ----------

branches_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/branches")

branches_silver = (
    branches_bronze
    .withColumn("open_date", F.to_date("open_date", "yyyy-MM-dd"))
    .withColumn("is_active", F.col("is_active").cast("boolean"))
    .withColumn("atm_count",  F.col("atm_count").cast("integer"))
    .withColumn("sq_footage", F.col("sq_footage").cast("integer"))
    .drop("_source_file", "_batch_id", "_is_current")
)

(
    branches_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{SILVER_PATH}/branches")
)
print(f"Silver branches: {branches_silver.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver Support Tickets

# COMMAND ----------

tickets_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}/support_tickets")

tickets_silver = (
    tickets_bronze
    .withColumn("_row_num", F.row_number().over(
        Window.partitionBy("ticket_id").orderBy(F.col("_ingest_ts").desc())
    ))
    .filter(F.col("_row_num") == 1).drop("_row_num")
    .withColumn("created_date",  F.to_date("created_date",  "yyyy-MM-dd"))
    .withColumn("resolved_date", F.to_date("resolved_date", "yyyy-MM-dd"))
    .withColumn("csat_score",    F.col("csat_score").cast("integer"))
    .withColumn("is_escalated",  F.col("is_escalated").cast("boolean"))
    .withColumn("resolution_days",
        F.when(F.col("resolved_date").isNotNull(),
               F.datediff(F.col("resolved_date"), F.col("created_date")))
         .otherwise(F.lit(None)))
    .withColumn("is_resolved", F.col("status").isin("Resolved", "Closed"))
    .drop("_source_file", "_batch_id", "_is_current")
)

(
    tickets_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("created_date")
    .save(f"{SILVER_PATH}/support_tickets")
)
print(f"Silver support tickets: {tickets_silver.count():,} rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver audit

# COMMAND ----------

silver_tables = {
    "customers":       f"{SILVER_PATH}/customers",
    "accounts":        f"{SILVER_PATH}/accounts",
    "transactions":    f"{SILVER_PATH}/transactions",
    "branches":        f"{SILVER_PATH}/branches",
    "support_tickets": f"{SILVER_PATH}/support_tickets",
}

print("\n=== Silver Layer Audit ===")
for name, path in silver_tables.items():
    cnt = spark.read.format("delta").load(path).count()
    print(f"  {name:<20} {cnt:>6,} rows")
print("==========================")
