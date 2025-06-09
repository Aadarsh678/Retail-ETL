# from airflow.decorators import dag, task
# from datetime import datetime
# from pyspark.sql import SparkSession
# import os
# import sys

# sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
# from scripts.etl.transform.customer_addresses import transform_customer_addressess
# TABLE_NAME = "customer_addresses"
# DEFAULT_ARGS = {
#     "start_date": datetime(2025, 6, 1),
#     "catchup": False,
# }

# @dag(
#     dag_id="transform_customer_addressess_csv",
#     default_args=DEFAULT_ARGS,
#     schedule_interval="@daily",
#     tags=["spark", "transformation", "csv"],
# )
# def transform_customer_addressess_dag():

#     @task
#     def run_customer_addressess_transform(region: str, load_date: str):
#         spark = SparkSession.builder.appName(f"TransformCustomers-{region}").getOrCreate()
#         spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

#         input_path = f"/opt/airflow/data/raw/region={region}/table={TABLE_NAME}/load_date={load_date}/"

#         output_path = f"/opt/airflow/data/staging/{TABLE_NAME}/region={region}/load_date={load_date}/"
        
#         df_raw = spark.read.parquet(input_path)
#         df_transformed = transform_customer_addressess(df_raw, region)
        
#         df_transformed.coalesce(1).write \
#             .option("header", True) \
#             .option("delimiter", ",") \
#             .mode("overwrite") \
#             .csv(output_path)
        
#         spark.stop()

#     load_date = "2025-06-06"
#     regions = ["asia", "eu", "us"]

#     for region in regions:
#         run_customer_addressess_transform(region=region, load_date=load_date)

# transform_customer_addressess_dag = transform_customer_addressess_dag()


import os
import datetime
import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv
import sys


import os, sys


sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

from scripts.pyspark_jobs.customer_addressess import transform_customer_addressess_table

TABLE_NAME = "customer_addressess"

@dag(
    dag_id="transform_customer_addressess",
    schedule="0 1 * * *",  # Daily at 1 AM (after extraction)
    start_date=pendulum.datetime(2025, 6, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["spark", "transformation", "csv"],
    max_active_tasks=10,
    max_active_runs=1
)
def transform_customer_addressess_dag():

    @task
    def run_transformation_asia():
        transform_customer_addressess_table(
            region="asia",
            table=TABLE_NAME,
            load_date="2025-06-08"
        )

    @task
    def run_transformation_eu():
        transform_customer_addressess_table(
            region="eu",
            table=TABLE_NAME,
            load_date="2025-06-08"
        )

    @task
    def run_transformation_us():
        transform_customer_addressess_table(
            region="us",
            table=TABLE_NAME,
            load_date="2025-06-08"
        )

    # Run transformations in parallel
    run_transformation_asia()
    run_transformation_eu()
    run_transformation_us()


dag = transform_customer_addressess_dag()