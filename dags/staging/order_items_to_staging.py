# from airflow.decorators import dag, task
# from datetime import datetime
# from pyspark.sql import SparkSession
# import os
# import sys

# sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
# from scripts.etl.transform.order_items import transform_order_items
# TABLE_NAME = "order_items"
# DEFAULT_ARGS = {
#     "start_date": datetime(2025, 6, 1),
#     "catchup": False,
# }

# @dag(
#     dag_id="transform_order_items_csv",
#     default_args=DEFAULT_ARGS,
#     schedule_interval="@daily",
#     tags=["spark", "transformation", "csv"],
# )
# def transform_order_items_dag():

#     @task
#     def run_order_items_transform(region: str, load_date: str):
#         spark = SparkSession.builder.appName(f"TransformCustomers-{region}").getOrCreate()
#         spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

#         input_path = f"/opt/airflow/data/raw/region={region}/table={TABLE_NAME}/load_date={load_date}/"

#         output_path = f"/opt/airflow/data/staging/{TABLE_NAME}/region={region}/load_date={load_date}/"
        
#         df_raw = spark.read.parquet(input_path)
#         df_transformed = transform_order_items(df_raw, region)
        
#         df_transformed.coalesce(1).write \
#             .option("header", True) \
#             .option("delimiter", ",") \
#             .mode("overwrite") \
#             .csv(output_path)
        
#         spark.stop()

#     load_date = "2025-06-06"
#     regions = ["asia", "eu", "us"]

#     for region in regions:
#         run_order_items_transform(region=region, load_date=load_date)

# transform_order_items_dag = transform_order_items_dag()


import os
import datetime
import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv
import sys


import os, sys


sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

from scripts.pyspark_jobs.order_items import transform_order_items_table

TABLE_NAME = "order_items"

@dag(
    dag_id="transform_order_items",
    schedule="0 1 * * *",  # Daily at 1 AM (after extraction)
    start_date=pendulum.datetime(2025, 6, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["spark", "transformation", "csv"],
    max_active_tasks=10,
    max_active_runs=1
)
def transform_order_items_dag():

    @task
    def run_transformation_asia():
        transform_order_items_table(
            region="asia",
            table=TABLE_NAME,
            load_date="2025-06-08"
        )

    @task
    def run_transformation_eu():
        transform_order_items_table(
            region="eu",
            table=TABLE_NAME,
            load_date="2025-06-08"
        )

    @task
    def run_transformation_us():
        transform_order_items_table(
            region="us",
            table=TABLE_NAME,
            load_date="2025-06-08"
        )

    # Run transformations in parallel
    run_transformation_asia()
    run_transformation_eu()
    run_transformation_us()


dag = transform_order_items_dag()
