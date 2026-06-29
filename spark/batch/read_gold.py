import sys

from pyspark.errors import AnalysisException

from spark.common.spark_session import create_spark_session

GOLD_PATH = "s3a://gold/daily_stock_summary"


def main() -> None:
    spark = create_spark_session()

    try:
        df = spark.read.parquet(GOLD_PATH)
    except AnalysisException as exc:
        if "PATH_NOT_FOUND" in str(exc):
            print(
                f"Gold data not found at {GOLD_PATH}.\n"
                "Run the pipeline in order:\n"
                "  1. Start infra: docker compose -f infra/docker/docker-compose.yml up -d\n"
                "  2. Stream to bronze: python -m spark.streaming.kafka_to_bronze\n"
                "  3. Produce Kafka events: python kafka/producers/yahoo_finance_producer.py\n"
                "  4. Bronze to silver: python -m spark.batch.bronze_to_silver\n"
                "  5. Silver to gold:   python -m spark.batch.silver_to_gold\n"
                "Then re-run: python -m spark.batch.read_gold",
                file=sys.stderr,
            )
            spark.stop()
            sys.exit(1)
        raise

    df.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()