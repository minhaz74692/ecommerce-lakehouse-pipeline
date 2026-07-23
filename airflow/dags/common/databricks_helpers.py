from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
)

from airflow.operators.bash import BashOperator

DATABRICKS_CONN_ID = "databricks_default"
POLLING_PERIOD_SECONDS = 30


def databricks_notebook_task(task_id: str, notebook_path: str) -> DatabricksSubmitRunOperator:
  
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        tasks=[
            {
                "task_key": f"{task_id}_task",   # Databricks-side ID; distinct from Airflow's
                "notebook_task": {
                    "notebook_path": notebook_path,
                },
            },
        ],
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )


def databricks_sql_notebook_task(
    task_id: str,
    notebook_path: str,
    warehouse_id: str,
) -> DatabricksSubmitRunOperator:
    return DatabricksSubmitRunOperator(
        task_id=task_id,
        databricks_conn_id=DATABRICKS_CONN_ID,
        tasks=[
            {
                "task_key": f"{task_id}_task",
                "notebook_task": {
                    "notebook_path": notebook_path,
                    "warehouse_id": warehouse_id,
                },
            },
        ],
        polling_period_seconds=POLLING_PERIOD_SECONDS,
    )

DBT_PROJECT_DIR = "/opt/airflow/dbt/olist_ecommerce"
def dbt_bash_runs(task_id: str, dbt_model: str) -> BashOperator:
    return BashOperator(
            task_id=task_id,
            bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir . --select {dbt_model}"
        )