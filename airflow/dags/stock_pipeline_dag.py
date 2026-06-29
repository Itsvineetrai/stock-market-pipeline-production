# DAG: ingestion, validation, warehouse load
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "vineet",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="stock_market_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["stocks", "spark", "minio"]
) as dag:

    # 1. Indented by 4 spaces to belong inside the DAG context
    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="""
        cd /opt/project &&
        python -m spark.batch.bronze_to_silver
        """
    )

    # 2. Indented by 4 spaces to belong inside the DAG context
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="""
        cd /opt/project &&
        python -m spark.batch.silver_to_gold
        """
    )

    # 3. Establish execution sequence link
    bronze_to_silver >> silver_to_gold
