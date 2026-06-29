# bronze -> silver
from pathlib import Path

from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date
)

from spark.common.spark_session import create_spark_session

# Create Spark Session
spark = create_spark_session()

# Paths

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = "s3a://bronze/stock_prices"
SILVER_PATH = "s3a://silver/stock_prices"

# Read Bronze Data

df = spark.read.parquet(BRONZE_PATH)

print("\nBronze Schema:\n")
df.printSchema()

print("\nBronze Sample Data:\n")
df.show(10, truncate=False)

# --------------------------------------------------
# Data Cleaning
# --------------------------------------------------

# Remove null records
df = df.dropna()

# Remove duplicate stock records
df = df.dropDuplicates(
    ["ticker", "timestamp"]
)

# --------------------------------------------------
# Convert Timestamp
# --------------------------------------------------

df = df.withColumn(
    "timestamp",
    to_timestamp(
        col("timestamp")
    )
)

# --------------------------------------------------
# Add Partition Column
# --------------------------------------------------

df = df.withColumn(
    "trade_date",
    to_date(
        col("timestamp")
    )
)

# --------------------------------------------------
# Validation
# --------------------------------------------------

print("\nSilver Schema:\n")
df.printSchema()

print("\nSilver Sample Data:\n")
df.show(10, truncate=False)

# --------------------------------------------------
# Write Silver Layer
# --------------------------------------------------

(
    df.write
    .mode("overwrite")
    .partitionBy("trade_date")
    .parquet(SILVER_PATH)
)

print("\nSilver Layer Created Successfully")
print(f"\nLocation: {SILVER_PATH}")

spark.stop()