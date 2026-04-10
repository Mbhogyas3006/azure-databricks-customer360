# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC **Project:** Azure Databricks Customer 360 Medallion Lakehouse
# MAGIC **Layer:** Bronze (raw, schema-on-read, immutable)
# MAGIC **Pattern:** Append-only with ingest metadata columns
# MAGIC
# MAGIC All source files land from ADLS Gen2 into Delta Lake Bronze tables.
# MAGIC No business logic here — just type inference, metadata stamping, and partitioning.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, IntegerType, TimestampType
)
from datetime import datetime

spark = SparkSession.builder.appName("Customer360-Bronze").getOrCreate()

# ── Config ────────────────────────────────────────────────────────────────────
# In Databricks, these come from widgets or ADF pipeline parameters.
# For local dev, set them directly.

STORAGE_ACCOUNT = "adlscustomer360"
CONTAINER       = "raw"
DELTA_BASE      = "/mnt/customer360"  # ADLS Gen2 mount point

# Local dev override — point to the synthetic/ folder relative to repo root
import os
LOCAL_DEV = not os.path.exists(f"/mnt/{STORAGE_ACCOUNT}")
if LOCAL_DEV:
    # Running locally — use relative path to synthetic data
    RAW_PATH   = "data/synthetic"
    BRONZE_PATH = "/tmp/customer360/bronze"
    print("[LOCAL DEV] Using local paths")
else:
    RAW_PATH   = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/landing"
    BRONZE_PATH = f"{DELTA_BASE}/bronze"

BATCH_DATE = datetime.utcnow().strftime("%Y-%m-%d")
BATCH_ID   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

print(f"Batch date : {BATCH_DATE}")
print(f"Batch ID   : {BATCH_ID}")
print(f"Raw path   : {RAW_PATH}")
print(f"Bronze path: {BRONZE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper — add ingestion metadata columns

# COMMAND ----------

def add_bronze_metadata(df, source_system: str, source_file: str):
    """
    Stamp every bronze row with pipeline metadata.
    These columns are mandatory across all Bronze tables.
    """
    return (
        df
        .withColumn("_source_system",  F.lit(source_system))
        .withColumn("_source_file",    F.lit(source_file))
        .withColumn("_batch_id",       F.lit(BATCH_ID))
        .withColumn("_ingest_ts",      F.current_timestamp())
        .withColumn("_batch_date",     F.lit(BATCH_DATE))
        .withColumn("_is_current",     F.lit(True))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Ingest Customers

# COMMAND ----------

customers_raw = spark.read.option("header", True).option("inferSchema", True).csv(
    f"{RAW_PATH}/customers.csv"
)

print(f"Customers raw count: {customers_raw.count():,}")
customers_raw.printSchema()

customers_bronze = add_bronze_metadata(
    customers_raw,
    source_system="CRM",
    source_file="customers.csv"
)

(
    customers_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("_batch_date")
    .option("mergeSchema", "true")
    .save(f"{BRONZE_PATH}/customers")
)
print("customers → Bronze DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest Accounts

# COMMAND ----------

accounts_raw = spark.read.option("header", True).option("inferSchema", True).csv(
    f"{RAW_PATH}/accounts.csv"
)
print(f"Accounts raw count: {accounts_raw.count():,}")

accounts_bronze = add_bronze_metadata(
    accounts_raw,
    source_system="Core Banking",
    source_file="accounts.csv"
)

(
    accounts_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("_batch_date")
    .option("mergeSchema", "true")
    .save(f"{BRONZE_PATH}/accounts")
)
print("accounts → Bronze DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest Transactions

# COMMAND ----------

transactions_raw = spark.read.option("header", True).option("inferSchema", True).csv(
    f"{RAW_PATH}/transactions.csv"
)
print(f"Transactions raw count: {transactions_raw.count():,}")

transactions_bronze = add_bronze_metadata(
    transactions_raw,
    source_system="Transaction Processing",
    source_file="transactions.csv"
)

# Partition by transaction_date for efficient downstream reads
(
    transactions_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("transaction_date")          # Query pattern: date range filters
    .option("mergeSchema", "true")
    .save(f"{BRONZE_PATH}/transactions")
)
print("transactions → Bronze DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Ingest Branches

# COMMAND ----------

branches_raw = spark.read.option("header", True).option("inferSchema", True).csv(
    f"{RAW_PATH}/branches.csv"
)
print(f"Branches raw count: {branches_raw.count():,}")

branches_bronze = add_bronze_metadata(
    branches_raw,
    source_system="Branch Management",
    source_file="branches.csv"
)

(
    branches_bronze.write
    .format("delta")
    .mode("overwrite")    # Small reference table — full refresh each run
    .option("mergeSchema", "true")
    .save(f"{BRONZE_PATH}/branches")
)
print("branches → Bronze DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingest Support Tickets

# COMMAND ----------

tickets_raw = spark.read.option("header", True).option("inferSchema", True).csv(
    f"{RAW_PATH}/support_tickets.csv"
)
print(f"Support tickets raw count: {tickets_raw.count():,}")

tickets_bronze = add_bronze_metadata(
    tickets_raw,
    source_system="CRM",
    source_file="support_tickets.csv"
)

(
    tickets_bronze.write
    .format("delta")
    .mode("append")
    .partitionBy("created_date")
    .option("mergeSchema", "true")
    .save(f"{BRONZE_PATH}/support_tickets")
)
print("support_tickets → Bronze DONE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze audit — row counts

# COMMAND ----------

bronze_tables = {
    "customers":       f"{BRONZE_PATH}/customers",
    "accounts":        f"{BRONZE_PATH}/accounts",
    "transactions":    f"{BRONZE_PATH}/transactions",
    "branches":        f"{BRONZE_PATH}/branches",
    "support_tickets": f"{BRONZE_PATH}/support_tickets",
}

print("\n=== Bronze Layer Audit ===")
total = 0
for name, path in bronze_tables.items():
    cnt = spark.read.format("delta").load(path).count()
    total += cnt
    print(f"  {name:<20} {cnt:>6,} rows")
print(f"  {'TOTAL':<20} {total:>6,} rows")
print("==========================")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta table history — time travel verification

# COMMAND ----------

from delta.tables import DeltaTable   # noqa: E402  (import after Delta is registered)

dt = DeltaTable.forPath(spark, f"{BRONZE_PATH}/customers")
dt.history(5).select("version", "timestamp", "operation", "operationParameters").show(truncate=False)
