from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

from spark.common.spark_session import create_spark_session

spark = create_spark_session()

schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock-prices")
    .option("startingOffsets", "latest")
    .load()
)

json_df = kafka_df.selectExpr(
    "CAST(value AS STRING) AS json_data"
)

parsed_df = (
    json_df
    .withColumn(
        "parsed_json",
        from_json(col("json_data"), schema)
    )
)

final_df = parsed_df.select(
    col("parsed_json.ticker").alias("ticker"),
    col("parsed_json.price").alias("price"),
    col("parsed_json.timestamp").alias("timestamp")
)
final_df.printSchema()
query = (
    final_df.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()