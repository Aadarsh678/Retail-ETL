import os
import datetime
import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv
import sys


import os, sys


sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

from scripts.pyspark_jobs.product_review import transform_product_review_table

TABLE_NAME = "product_reviews"


@dag(
    dag_id="transform_product_review",
    schedule="0 1 * * *",  # Daily at 1 AM (after extraction)
    start_date=pendulum.datetime(2025, 6, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["spark", "transformation", "csv"],
    max_active_tasks=10,
    max_active_runs=1,
)
def transform_product_review_dag():

    @task
    def run_transformation_asia():
        transform_product_review_table(
            region="asia", table=TABLE_NAME, load_date="2025-06-08"
        )

    @task
    def run_transformation_eu():
        transform_product_review_table(
            region="eu", table=TABLE_NAME, load_date="2025-06-08"
        )

    @task
    def run_transformation_us():
        transform_product_review_table(
            region="us", table=TABLE_NAME, load_date="2025-06-08"
        )

    # Run transformations in parallel
    run_transformation_asia()
    run_transformation_eu()
    run_transformation_us()


dag = transform_product_review_dag()
