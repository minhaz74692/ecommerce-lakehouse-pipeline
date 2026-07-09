WORKSPACE_ROOT = "/Workspace/Users/minhaz74692@gmail.com/ecommerce-lakehouse-pipeline"
WAREHOUSE_ID = "7474660081667303"
DATABRICKS_CONNECTION_ID = "databricks_conn_id"


# Bronze ingestion
bronze_ingestion = "bronze_ingestion"
bronze_ingestion_reviews = "bronze_ingestion_reviews"

# Silver Transformation
silver_orders = "silver_orders"
silver_order_items = "silver_order_items"
silver_customers = "silver_customers"
silver_products = "silver_products"
silver_reviews = "silver_reviews"
silver_payments = "silver_payments"

# Data Quality
dq_orders = "dq_orders"

# Gold Transformation
dim_customer = "dim_customer"
dim_date = "dim_date"
dim_product = "dim_product"
fact_sales = "fact_sales"

# Analytics SQL 
monthly_revenue  = "monthly_revenue"
top_products = "top_products"
customer_retention_cohort = "customer_retention_cohort"


BRONZE_NOTEBOOKS = {
    bronze_ingestion: f"{WORKSPACE_ROOT}/injestion/bronze_ingestion",
    bronze_ingestion_reviews: f"{WORKSPACE_ROOT}/injestion/bronze_ingestion_reviews",
}

SILVER_NOTEBOOKS = {
    silver_orders: f"{WORKSPACE_ROOT}/transformations/silver/silver_orders",
    silver_order_items: f"{WORKSPACE_ROOT}/transformations/silver/silver_order_items",
    silver_reviews: f"{WORKSPACE_ROOT}/transformations/silver/silver_reviews",
    silver_products: f"{WORKSPACE_ROOT}/transformations/silver/silver_products",
    silver_customers: f"{WORKSPACE_ROOT}/transformations/silver/silver_customers",
    silver_payments: f"{WORKSPACE_ROOT}/transformations/silver/silver_payments",
}

GOLD_NOTEBOOKS = {
    dim_customer : f"{WORKSPACE_ROOT}/transformations/gold/dim_customer",
    dim_date : f"{WORKSPACE_ROOT}/transformations/gold/dim_date",
    dim_product : f"{WORKSPACE_ROOT}/transformations/gold/dim_product",
    fact_sales : f"{WORKSPACE_ROOT}/transformations/gold/fact_sales",
}

DQ_NOTEBOOKS = {
    dq_orders : f"{WORKSPACE_ROOT}/tests/test_silver_quality",
}

SQL_NOTEBOOKS = {
    monthly_revenue : f"{WORKSPACE_ROOT}/sql/monthly_revenue",
    top_products : f"{WORKSPACE_ROOT}/sql/top_products",
    customer_retention_cohort : f"{WORKSPACE_ROOT}/sql/customer_retention_cohort",
}

NOTEBOOKS = {
    **BRONZE_NOTEBOOKS,
    **SILVER_NOTEBOOKS,
    **GOLD_NOTEBOOKS,
    **DQ_NOTEBOOKS,
    **SQL_NOTEBOOKS,
}
