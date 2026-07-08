"""
hello_databricks_dag.py

Single-task DAG that triggers the Bronze ingestion notebook on Databricks
serverless compute. Purpose: validate the Airflow -> Databricks pipe end-to-end.
"""

from datetime import datetime

from airflow.sdk import dag
from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)

BRONZE_NOTEBOOK_PATH = (
    "/Workspace/Users/minhaz74692@gmail.com/"
    "ecommerce-lakehouse-pipeline/injestion/bronze_ingestion"
)


@dag(
    dag_id="hello_databricks",
    description="Smoke test: Airflow triggers Bronze ingestion on Databricks serverless.",
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["ecommerce", "smoke-test"],
    default_args={
        "owner": "minhaz",
        "retries": 1,
    },
)
def hello_databricks_dag():
    run_bronze = DatabricksSubmitRunOperator(
        task_id="run_bronze",
        databricks_conn_id="databricks_default",
        # Multi-task shape: required for serverless. Even with one task,
        # we wrap it in a list. No cluster spec anywhere -> serverless.
        tasks=[
            {
                "task_key": "bronze_ingestion",
                "notebook_task": {
                    "notebook_path": BRONZE_NOTEBOOK_PATH,
                },
            },
        ],
        polling_period_seconds=30,
    )


hello_databricks_dag()