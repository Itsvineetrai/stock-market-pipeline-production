import os
import sys
from pathlib import Path
from dotenv import load_dotenv 

import pyspark
from pyspark.sql import SparkSession

SPARK_VERSION = pyspark.__version__
_REPO_ROOT = Path(__file__).resolve().parents[2]
_WINUTILS_HOME = _REPO_ROOT / "infra" / "winutils"


def _configure_spark_environment() -> None:
    """Use PySpark's bundled Spark and local winutils for a consistent classpath."""
    pyspark_home = Path(pyspark.__file__).resolve().parent
    if not pyspark_home.exists():
        raise RuntimeError(
            f"PySpark home not found: {pyspark_home}"
        )

    # External SPARK_HOME (e.g. C:\spark) splits the Kafka connector across
    # classloaders and causes KafkaConfigUpdater ClassNotFoundException.
    os.environ["SPARK_HOME"] = str(pyspark_home)

    # Keep Spark workers on the same interpreter as the driver. A system-wide
    # PYSPARK_PYTHON pointing at 3.12+ on Windows causes worker socket crashes.
    python_exec = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_exec
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exec

    winutils_bin = _WINUTILS_HOME / "bin"
    if winutils_bin.is_dir():
        os.environ["HADOOP_HOME"] = str(_WINUTILS_HOME)
        hadoop_bin = str(winutils_bin)
        if hadoop_bin not in os.environ.get("PATH", ""):
            os.environ["PATH"] = f"{hadoop_bin}{os.pathsep}{os.environ.get('PATH', '')}"



def create_spark_session() -> SparkSession:
    env_path = _REPO_ROOT /"infra"/"docker"/".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        load_dotenv()
    MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000"
)      
    _configure_spark_environment()
  
    packages = ",".join(
        [
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
            f"org.apache.spark:spark-token-provider-kafka-0-10_2.12:{SPARK_VERSION}",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "org.postgresql:postgresql:42.7.3"
        ]
    )

    # FIXED: Added default 'minioadmin' strings if os.getenv returns None or Null
    spark = (
        SparkSession.builder.appName("KafkaToBronze") # pyright: ignore[reportAttributeAccessIssue]
        .master("local[2]")
        .config("spark.jars.packages", packages)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER") or "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD") or "minioadmin123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Kolkata")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Kolkata")
        .config("spark.python.worker.reuse", "false")
        .config("spark.thread.useThreadPool", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
