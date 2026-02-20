from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes = 5),
}

with DAG(
    "web_log_processing_pipeline",
    default_args = default_args,
    description = "Simple pipeline to ingest and process static Nginx server access logs",
    schedule_interval = "@once",
    start_date = datetime(2026, 2, 20),
    catchup = False,
    tags = ['portfolio', 'spark', 'logs'],
) as dag:

    ingested_data = BashOperator(
        task_id = "download_kaggle_dataset",
        bash_command = "python /workspaces/spark-log-pipeline/src/ingestion/download_logs.py"
    )

    processed_data = BashOperator(
        tash_id = "process_logs_with_spark",
        bash_command = "python /workspaces/spark-log-pipelines/src/processing/spark_parser.py"
    )

    ingested_data >> processed_data
