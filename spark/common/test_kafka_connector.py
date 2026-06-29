from spark.common.spark_session import create_spark_session

spark = create_spark_session()

print("Spark Session Created")

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "stock-prices")
)

print("Kafka Connector Available")

spark.stop()
