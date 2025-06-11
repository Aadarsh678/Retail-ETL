# from airflow import DAG
# from datetime import datetime, timedelta
# from airflow.decorators import task
# import sys
# import os

# scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
# if scripts_path not in sys.path:
#     sys.path.insert(0, scripts_path)

# # Add airflow root path for imports
# airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
# if airflow_path not in sys.path:
#     sys.path.insert(0, airflow_path)

# from groups.region_task_groups import process_region_asia, process_region_eu, process_region_us

# default_args = {
#     "owner": "data-team",
#     "depends_on_past": False,
#     "start_date": datetime(2024, 1, 1),
#     "email_on_failure": True,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=1),
# }

# with DAG(
#     "multi_region_to_snowflake",
#     default_args=default_args,
#     description="Multi-region data loading to snowflake",
#     schedule_interval="@daily",
#     catchup=False,
#     max_active_tasks=6,
#     tags=["multi-region", "transformations"],
# ) as dag:

#     load_date = "2025-06-08"

#     asia = process_region_asia(load_date=load_date)
#     eu = process_region_eu(load_date=load_date)
#     us = process_region_us(load_date=load_date)

#     @task
#     def final_summary():
#         print("All regions processed successfully!")

#     summary = final_summary()

#     asia >> summary
#     eu >> summary
#     us >> summary
