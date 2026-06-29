from pyspark.sql import SparkSession

spark=(
    SparkSession.builder
    .appName("Stock Market Pipeline Test") # pyright: ignore[reportAttributeAccessIssue]
    .master("local[*]")
    .getOrCreate()
)

print("Spark Version:", spark.version)
spark.stop()