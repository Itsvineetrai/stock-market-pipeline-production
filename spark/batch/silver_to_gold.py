from pathlib import Path

from pyspark.sql.functions import (
    avg,
    max,
    min,
    count
)

from spark.common.spark_session import create_spark_session

# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = create_spark_session()

# --------------------------------------------------
# Paths
# --------------------------------------------------

SILVER_PATH = "s3a://silver/stock_prices"

GOLD_PATH = "s3a://gold/daily_stock_summary"

# --------------------------------------------------
# Read Silver Data
# --------------------------------------------------

df = spark.read.parquet(SILVER_PATH)

print("\nSilver Schema:\n")
df.printSchema()

# --------------------------------------------------
# Daily Aggregation
# --------------------------------------------------

gold_df = (
    df.groupBy(
        "ticker",
        "trade_date"
    )
    .agg(
        avg("price").alias("avg_price"),
        max("price").alias("max_price"),
        min("price").alias("min_price"),
        count("*").alias("record_count")
    )
)

print("\nGold Preview:\n")

gold_df.show(
    20,
    truncate=False
)

# --------------------------------------------------
# Write Gold Layer
# --------------------------------------------------

(
    gold_df.write
    .mode("overwrite")
    .partitionBy("trade_date")
    .parquet(GOLD_PATH)
)

print("\nGold Layer Created Successfully")

spark.stop()