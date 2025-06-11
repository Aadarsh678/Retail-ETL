from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, task_group
from pyspark.sql import SparkSession
import sys
from dotenv import load_dotenv
import os
import shutil
import snowflake.connector
import glob
import yaml

# Load environment variables
load_dotenv()
SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.getenv("SNOWFLAKE_STAR_SCHEMA")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

# Add paths for imports
scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if airflow_path not in sys.path:
    sys.path.insert(0, airflow_path)

# Import transformation SQL
from scripts.star_load.dim_customer import dim_customer_sql
from scripts.star_load.dim_product import dim_product_sql
from scripts.star_load.dim_marketing_campaign import dim_campaign_sql


TABLES = [
    "products",
    "customers",
    "marketing_campaigns"
]
MERGE_QUERIES = {
    "products": dim_product_sql,
    "customers": dim_customer_sql,
    "marketing_campaigns": dim_campaign_sql,
}

# Define table dependency graph if needed (optional)
TABLE_DEPENDENCIES = {
    "customers": [],
    "products": [],
    "marketing_campaigns": [],
}

def get_snowflake_connection():
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

@task_group(group_id="star_merge_group")
def star_dim_merge_tasks():
    tasks = {}
    for table in TABLES:
        @task(task_id=f"star_dim_{table}")
        def merge_table(t=table):
            run_merge_query(t)
        tasks[table] = merge_table()

    for table, deps in TABLE_DEPENDENCIES.items():
        for dep in deps:
            tasks[dep] >> tasks[table]

    return tasks

default_args = {
    "owner": "data-team",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "load_to_star_schema",
    schedule="0 0 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ods", "merge", "multi-region"],
) as dag:
    merge_group = star_dim_merge_tasks()
