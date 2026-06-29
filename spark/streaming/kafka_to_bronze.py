from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

from spark.common.spark_session import create_spark_session

# Create Spark Session
spark = create_spark_session()

# Schema
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read Kafka Stream
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock-prices")
    .option("startingOffsets", "latest")
    .load()
)

# Convert Kafka Binary -> String
json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_data"
)

# Parse JSON
parsed_df = (
    json_df
    .withColumn(
        "parsed_json",
        from_json(col("json_data"), schema)
    )
)

# Flatten JSON
final_df = parsed_df.select(
    col("parsed_json.ticker").alias("ticker"),
    col("parsed_json.price").alias("price"),
    col("parsed_json.timestamp").alias("timestamp")
)

# Write Bronze Layer
query = (
    final_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "s3a://bronze/stock_prices"
    )
    .option(
        "checkpointLocation",
        "s3a://checkpoints/stock_prices"
    )
    .start()
)

print("Writing data to Bronze Layer...")

query.awaitTermination()