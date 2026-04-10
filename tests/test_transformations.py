"""
Unit Tests — Customer 360 PySpark Transformations
Run: pytest tests/test_transformations.py -v

Tests validate key business logic without requiring a live Databricks cluster.
Uses pytest + pyspark local mode.
"""

import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType, DateType
)


# ── Fixture: local Spark session ──────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("customer360-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_customer_df(spark, rows):
    schema = StructType([
        StructField("customer_id",   StringType(),  True),
        StructField("segment",       StringType(),  True),
        StructField("credit_score",  IntegerType(), True),
        StructField("annual_income", DoubleType(),  True),
        StructField("age",           IntegerType(), True),
        StructField("total_balance", DoubleType(),  True),
        StructField("days_since_last_txn", IntegerType(), True),
        StructField("avg_csat_score",  DoubleType(), True),
        StructField("escalated_tickets", IntegerType(), True),
    ])
    return spark.createDataFrame(rows, schema)


# ── age_band derivation ───────────────────────────────────────────────────────

class TestAgeBand:
    def test_gen_z_millennial(self, spark):
        df = make_customer_df(spark, [("c1", "Retail", 700, 60000.0, 25, 5000.0, 10, 4.0, 0)])
        result = df.withColumn("age_band",
            F.when(F.col("age") < 30, "Gen-Z / Millennial")
             .when(F.col("age") < 45, "Millennial / Gen-X")
             .when(F.col("age") < 60, "Gen-X / Boomer")
             .otherwise("Senior")
        ).collect()[0]["age_band"]
        assert result == "Gen-Z / Millennial"

    def test_senior(self, spark):
        df = make_customer_df(spark, [("c2", "Premier", 780, 200000.0, 67, 150000.0, 5, 4.5, 0)])
        result = df.withColumn("age_band",
            F.when(F.col("age") < 30, "Gen-Z / Millennial")
             .when(F.col("age") < 45, "Millennial / Gen-X")
             .when(F.col("age") < 60, "Gen-X / Boomer")
             .otherwise("Senior")
        ).collect()[0]["age_band"]
        assert result == "Senior"


# ── income_tier derivation ────────────────────────────────────────────────────

class TestIncomeTier:
    @pytest.mark.parametrize("income,expected", [
        (40_000,  "Low"),
        (75_000,  "Mid"),
        (150_000, "Upper-Mid"),
        (500_000, "High"),
    ])
    def test_income_tier(self, spark, income, expected):
        df = make_customer_df(spark, [("c1", "Retail", 700, float(income), 35, 5000.0, 10, 4.0, 0)])
        result = df.withColumn("income_tier",
            F.when(F.col("annual_income") < 50_000,  "Low")
             .when(F.col("annual_income") < 100_000, "Mid")
             .when(F.col("annual_income") < 250_000, "Upper-Mid")
             .otherwise("High")
        ).collect()[0]["income_tier"]
        assert result == expected


# ── churn_risk_flag logic ─────────────────────────────────────────────────────

class TestChurnRiskFlag:
    def _apply_churn(self, df):
        return df.withColumn("churn_risk_flag",
            (F.col("days_since_last_txn") > 90) |
            (F.col("avg_csat_score") < 3.0) |
            (F.col("escalated_tickets") > 1)
        )

    def test_churn_inactive_customer(self, spark):
        """Customer inactive for 120 days → churn flag = True"""
        df = make_customer_df(spark, [("c1", "Retail", 650, 50000.0, 35, 10000.0, 120, 4.0, 0)])
        assert self._apply_churn(df).collect()[0]["churn_risk_flag"] is True

    def test_churn_low_csat(self, spark):
        """Customer with CSAT < 3.0 → churn flag = True"""
        df = make_customer_df(spark, [("c2", "Premier", 720, 100000.0, 45, 50000.0, 10, 2.5, 0)])
        assert self._apply_churn(df).collect()[0]["churn_risk_flag"] is True

    def test_churn_escalated_tickets(self, spark):
        """Customer with 2+ escalations → churn flag = True"""
        df = make_customer_df(spark, [("c3", "Retail", 680, 60000.0, 40, 8000.0, 5, 4.2, 2)])
        assert self._apply_churn(df).collect()[0]["churn_risk_flag"] is True

    def test_no_churn_healthy_customer(self, spark):
        """Active customer, good CSAT, no escalations → churn flag = False"""
        df = make_customer_df(spark, [("c4", "Premier", 800, 150000.0, 42, 75000.0, 3, 4.8, 0)])
        assert self._apply_churn(df).collect()[0]["churn_risk_flag"] is False


# ── high_value_flag logic ─────────────────────────────────────────────────────

class TestHighValueFlag:
    def _apply_hv(self, df):
        return df.withColumn("high_value_flag",
            (F.col("total_balance") > 100_000) |
            (F.col("segment").isin("Premier", "Private"))
        )

    def test_high_balance_triggers_flag(self, spark):
        df = make_customer_df(spark, [("c1", "Retail", 750, 90000.0, 40, 200000.0, 5, 4.5, 0)])
        assert self._apply_hv(df).collect()[0]["high_value_flag"] is True

    def test_premier_segment_triggers_flag(self, spark):
        df = make_customer_df(spark, [("c2", "Premier", 720, 80000.0, 38, 40000.0, 3, 4.2, 0)])
        assert self._apply_hv(df).collect()[0]["high_value_flag"] is True

    def test_retail_low_balance_no_flag(self, spark):
        df = make_customer_df(spark, [("c3", "Retail", 650, 45000.0, 30, 5000.0, 12, 3.8, 0)])
        assert self._apply_hv(df).collect()[0]["high_value_flag"] is False


# ── deduplication logic ───────────────────────────────────────────────────────

class TestDeduplication:
    def test_dedup_keeps_latest_record(self, spark):
        """When same customer_id has 2 rows, keep the one with later ingest_ts"""
        from pyspark.sql import Window

        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("segment",     StringType(), True),
            StructField("_ingest_ts",  StringType(), True),
        ])
        rows = [
            ("cust-1", "Retail",  "2024-01-01 10:00:00"),
            ("cust-1", "Premier", "2024-01-01 12:00:00"),  # ← this should win
        ]
        df = spark.createDataFrame(rows, schema)
        window = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc())
        result = (
            df.withColumn("_rn", F.row_number().over(window))
              .filter(F.col("_rn") == 1)
              .drop("_rn")
        )
        assert result.count() == 1
        assert result.collect()[0]["segment"] == "Premier"


# ── signed_amount derivation ──────────────────────────────────────────────────

class TestSignedAmount:
    def test_debit_is_negative(self, spark):
        schema = StructType([
            StructField("amount",    DoubleType(), True),
            StructField("direction", StringType(), True),
        ])
        df = spark.createDataFrame([(100.0, "Debit")], schema)
        result = df.withColumn("signed_amount",
            F.when(F.col("direction") == "Debit", -F.col("amount"))
             .otherwise(F.col("amount"))
        ).collect()[0]["signed_amount"]
        assert result == -100.0

    def test_credit_is_positive(self, spark):
        schema = StructType([
            StructField("amount",    DoubleType(), True),
            StructField("direction", StringType(), True),
        ])
        df = spark.createDataFrame([(500.0, "Credit")], schema)
        result = df.withColumn("signed_amount",
            F.when(F.col("direction") == "Debit", -F.col("amount"))
             .otherwise(F.col("amount"))
        ).collect()[0]["signed_amount"]
        assert result == 500.0
