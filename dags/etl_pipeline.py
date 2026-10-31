from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


import os


# Shared configuration passed to pipeline containers
common_env = {
    "BUCKET_NAME": os.getenv("BUCKET_NAME"),
    "S3_ENDPOINT": os.getenv("S3_ENDPOINT"),
    "RUSTFS_USERNAME": os.getenv("RUSTFS_USERNAME"),
    "RUSTFS_PASSWORD": os.getenv("RUSTFS_PASSWORD"),
    "BRONZE_OBJECT_KEY": os.getenv("BRONZE_OBJECT_KEY"),
    "SILVER_OBJECT_KEY": os.getenv("SILVER_OBJECT_KEY"),
    "GOLD_OBJECT_KEY": os.getenv("GOLD_OBJECT_KEY"),
}


default_args = {
    "owner": "oumaima",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="chicago_taxi_pipeline",
    default_args=default_args,
    description="Chicago Taxi data pipeline",
    start_date=datetime(2026, 10, 31),
    schedule=None,  # Manual execution for now
    catchup=False,
    tags=["chicago-taxi", "data-engineering"],
) as dag:
    
    spark_container = "taxipipeline-spark-1"

    ingestion_task = BashOperator(
        task_id="001_bronze_ingestion",
        bash_command=(
            f"docker exec {spark_container} "
            "python3 src/bronze/extraction.py"
        ),
    )

    silver_task = BashOperator(
        task_id="002_silver_transformation",
        bash_command=(
            f"docker exec {spark_container} "
            "python3 src/silver/bronze_to_silver.py"
        ),
    )

    gold_task = BashOperator(
        task_id="003_gold_aggregation",
        bash_command=(
            f"docker exec {spark_container} "
            "python3 src/gold/silver_to_gold.py"
        ),
    )

    # Define execution order
    ingestion_task >> silver_task >> gold_task