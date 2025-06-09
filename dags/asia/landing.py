import os
import datetime
import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv()
sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

from scripts.pyspark_jobs.extract_to_parquet import extract_postgres_table

TABLES = ["categories", "products", "product_variants",
          "inventory", "customers", "customer_addresses",
          "marketing_campaigns", "discounts", "shopping_carts",
          "cart_items", "orders", "order_items", "shipments",
          "returns", "product_reviews", "payments", "wishlists"]


@dag(
    dag_id="extract_postgres_table_to_parquet",
    schedule="0 0 * * *",  # Daily at midnight
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["postgres", "spark", "parquet"],
    max_active_tasks=20,
    max_active_runs=1
)
def extract_postgres_table_to_parquet():

    @task
    def run_extraction_asia():
        for table in TABLES:
            extract_postgres_table(
                region="asia",
                table=table,
                pg_user=os.getenv("PG_USER", "retail_etl"),
                pg_password=os.getenv("PG_PASSWORD", "retail_etl"),
                load_date=datetime.datetime.now().strftime("%Y-%m-%d")
            )

    @task
    def run_extraction_eu():
        for table in TABLES:
            extract_postgres_table(
                region="eu",
                table=table,
                pg_user=os.getenv("PG_USER", "retail_etl"),
                pg_password=os.getenv("PG_PASSWORD", "retail_etl"),
                load_date=datetime.datetime.now().strftime("%Y-%m-%d")
            )

    @task
    def run_extraction_us():
        for table in TABLES:
            extract_postgres_table(
                region="us",
                table=table,
                pg_user=os.getenv("PG_USER", "retail_etl"),
                pg_password=os.getenv("PG_PASSWORD", "retail_etl"),
                load_date=datetime.datetime.now().strftime("%Y-%m-%d")
            )

    # Run in parallel
    run_extraction_asia()
    run_extraction_eu()
    run_extraction_us()


dag = extract_postgres_table_to_parquet()
