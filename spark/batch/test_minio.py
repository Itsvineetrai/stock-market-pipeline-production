from spark.common.spark_session import create_spark_session

spark = create_spark_session()

data = [
    ("AAPL", 291.13),
    ("MSFT", 390.74)
]

df = spark.createDataFrame(
    data,
    ["ticker", "price"]
)

df.write.mode("overwrite").parquet(
    "s3a://bronze/test_data"
)

print("Write Successful")

spark.stop()