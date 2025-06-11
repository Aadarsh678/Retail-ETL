from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, task_group
from pyspark.sql import SparkSession
import sys
from dotenv import load_dotenv
import os
import shutil
import shutil
import snowflake.connector
import pyspark
from datetime import datetime
import shutil
import glob
import yaml


load_dotenv()
SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.getenv("SNOWFLAKE_ODS_SCHEMA")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
# Add paths for imports
scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# Add airflow root path for imports
airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if airflow_path not in sys.path:
    sys.path.insert(0, airflow_path)

# Import all transformation functions
from scripts.ods_load.categories import get_merge_query_categories_union
from scripts.ods_load.product import get_merge_query_product_union
from scripts.ods_load.product_variants import get_merge_query_product_variants_union
from scripts.ods_load.inventory import get_merge_query_inventory_union
from scripts.ods_load.customers import get_merge_query_customers_union
from scripts.ods_load.cutomers_address import get_merge_query_customers_address_union
from scripts.ods_load.discounts import get_merge_query_discounts_union
from scripts.ods_load.shopping_carts import get_merge_query_shopping_carts_union
from scripts.ods_load.cart_items import get_merge_query_cart_items_union
from scripts.ods_load.orders import get_merge_query_orders_union
from scripts.ods_load.order_items import get_merge_query_order_items_union
from scripts.ods_load.payments import get_merge_query_payments_union
from scripts.ods_load.shipments import get_merge_query_shipments_union
from scripts.ods_load.returns import get_merge_query_returns_union
from scripts.ods_load.product_reviews import get_merge_query_product_reviews_union
from scripts.ods_load.wishlists import get_merge_query_wishlists_union
from scripts.ods_load.marketing_campaigns import get_merge_query_marketing_campaigns_union


# Configuration
REGIONS = ["asia", "eu", "us"]
TABLES = [
    "categories",
    "products",
    "product_variants",
    "inventory",
    "customers",
    "customer_addresses",
    "discounts",
    "shopping_carts",
    "cart_items",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "returns",
    "product_reviews",
    "wishlists",
    "marketing_campaigns",
]

# Table dependencies
TABLE_DEPENDENCIES = {
    "customers": [],
    "categories": [],
    "products": ["categories"],
    "product_variants": ["products"],
    "inventory": ["product_variants"],
    "customer_addresses": ["customers"],
    "shopping_carts": ["customers"],
    "cart_items": ["shopping_carts", "product_variants"],
    "orders": ["customers", "customer_addresses"],
    "order_items": ["orders", "product_variants"],
    "payments": ["orders"],
    "shipments": ["orders"],
    "returns": ["orders", "order_items"],
    "product_reviews": ["products", "customers"],
    "wishlists": ["customers", "products"],
    "discounts": [],
    "marketing_campaigns":[],
}

# Map table names to transformation functions
MERGE_QUERIES = {
    "categories": get_merge_query_categories_union,
    "products": get_merge_query_product_union,
    "product_variants": get_merge_query_product_variants_union,
    "inventory": get_merge_query_inventory_union,
    "customers": get_merge_query_customers_union,
    "customer_addresses": get_merge_query_customers_address_union,
    "discounts": get_merge_query_discounts_union,
    "shopping_carts": get_merge_query_shopping_carts_union,
    "cart_items": get_merge_query_cart_items_union,
    "orders": get_merge_query_orders_union,
    "order_items": get_merge_query_order_items_union,
    "payments": get_merge_query_payments_union,
    "shipments": get_merge_query_shipments_union,
    "returns": get_merge_query_returns_union,
    "product_reviews": get_merge_query_product_reviews_union,
    "wishlists": get_merge_query_wishlists_union,
    "marketing_campaigns":get_merge_query_marketing_campaigns_union,
}

def get_snowflake_connection():
    """
    Establishes and returns a Snowflake database connection using snowflake.connector.
    Used primarily for DDL operations (table creation and dropping) and MERGE statements.
    """
    conn = None
    try:
        conn = snowflake.connector.connect(
            user=SF_USER,
            password=SF_PASSWORD,
            account=SF_ACCOUNT,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            schema=SF_SCHEMA
        )
        print("Successfully connected to Snowflake via snowflake.connector.")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {e}")
        raise


def run_merge_query(table_name: str):
    """
    Connect to Snowflake and execute the merge query for the specified table.
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        print(f"Running merge for {table_name}...")
        merge_sql = MERGE_QUERIES[table_name]()
        cursor.execute(merge_sql)
        print(f"Merge completed for {table_name}")
    except Exception as e:
        print(f"Error running merge for {table_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

@task_group(group_id="ods_merge_group")
def ods_merge_tasks():
    """
    Creates a task group for all tables with dependency ordering.
    """

    tasks = {}

    # Create all tasks first
    for table in TABLES:
        @task(task_id=f"merge_{table}")
        def merge_table(t=table):
            run_merge_query(t)
        tasks[table] = merge_table()

    # Set dependencies based on TABLE_DEPENDENCIES
    for table, deps in TABLE_DEPENDENCIES.items():
        for dep in deps:
            tasks[dep] >> tasks[table]

    # Return list of tasks for setting upstream/downstream if needed
    return tasks



default_args = {
    "owner": "data-team",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "multi_region_ods_merge_v2",
    schedule=None,  # Daily at midnight
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ods", "merge", "multi-region"],
) as dag:
    merge_group = ods_merge_tasks()
