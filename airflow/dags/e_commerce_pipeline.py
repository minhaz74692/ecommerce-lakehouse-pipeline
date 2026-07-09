
from datetime import datetime
from airflow.sdk import dag, chain, cross_downstream
import config.ecommerce as config
from common.databricks_helpers import databricks_notebook_task, databricks_sql_notebook_task

@dag(
    dag_id="ecommerce_lakehouse_elt_pipeline",
    description="Olist E-Commerce ELT Lakehouse Pipeline With Airflow",
    schedule=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["ecommerce", "elt_pipeline"],
    default_args={
        "owner": "minhaz",
        "retries": 1,
    },
)
def ecommerce_lakehouse_elt_pipeline():
    # Bronze Ingestion
    bronze_ingestion = databricks_notebook_task(config.bronze_ingestion, config.NOTEBOOKS[config.bronze_ingestion])
    bronze_reviews = databricks_notebook_task(config.bronze_ingestion_reviews, config.NOTEBOOKS[config.bronze_ingestion_reviews])

    # Silver Transformation
    silver_products = databricks_notebook_task(config.silver_products, config.NOTEBOOKS[config.silver_products])
    silver_orders = databricks_notebook_task(config.silver_orders, config.NOTEBOOKS[config.silver_orders])
    silver_order_items = databricks_notebook_task(config.silver_order_items, config.NOTEBOOKS[config.silver_order_items])
    silver_customers = databricks_notebook_task(config.silver_customers, config.NOTEBOOKS[config.silver_customers])
    silver_payments = databricks_notebook_task(config.silver_payments, config.NOTEBOOKS[config.silver_payments])
    silver_reviews = databricks_notebook_task(config.silver_reviews, config.NOTEBOOKS[config.silver_reviews])

    #Data Quality(DQ)
    dq_silver = databricks_notebook_task(config.dq_orders, config.NOTEBOOKS[config.dq_orders])

    # Gold Transformation
    dim_date = databricks_notebook_task(config.dim_date, config.NOTEBOOKS[config.dim_date])
    dim_customer = databricks_notebook_task(config.dim_customer, config.NOTEBOOKS[config.dim_customer])
    dim_product = databricks_notebook_task(config.dim_product, config.NOTEBOOKS[config.dim_product])
    fact_sales = databricks_notebook_task(config.fact_sales, config.NOTEBOOKS[config.fact_sales])

    # Analytics- SQL 
    analytics_monthly = databricks_notebook_task(config.monthly_revenue, config.NOTEBOOKS[config.monthly_revenue])
    analytics_top_products = databricks_notebook_task(config.top_products, config.NOTEBOOKS[config.top_products])
    analytics_customer_retention = databricks_notebook_task(config.customer_retention_cohort, config.NOTEBOOKS[config.customer_retention_cohort])


    # Dependencies expressed as one readable block
    silver_tasks = [silver_customers, silver_orders, silver_order_items,
                    silver_products, silver_payments]
    dim_tasks = [dim_customer, dim_product, dim_date]
    analytics_tasks = [analytics_monthly, analytics_top_products, analytics_customer_retention]

    # List-to-list edges: fan-out-fan-in
    # cross_downstream(bronze_tasks, silver_tasks)

    # From silver onward, chain() works — no adjacent lists.
    # chain(silver_tasks, dq_silver, dim_tasks, fact_sales, analytics_tasks)

    bronze_ingestion >> silver_tasks >> dq_silver >> dim_tasks >> fact_sales >> analytics_tasks
    bronze_reviews >> silver_reviews




# initilize the dag
ecommerce_lakehouse_elt_pipeline()