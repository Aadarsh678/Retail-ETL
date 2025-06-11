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


# Add paths for imports
scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# Add airflow root path for imports
airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if airflow_path not in sys.path:
    sys.path.insert(0, airflow_path)



from scripts.star_load.geography import upsert_geography_from_csv_spark



default_args = {
    "owner": "data-team",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "geography_to_snowflake",
    schedule='0 1 1 1 *',  #yearly
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ods", "merge", "multi-region"],
) as dag:
    @task
    def Load_geography_method():
 
        
        try:
            # Call the imported function
            # load_csv_to_snowflake(load_date)
            upsert_geography_from_csv_spark('/opt/airflow/data/lookup/geography.csv')
            print(f"Successfully completed processing.")
            
        except Exception as e:
            print(f"Error processing geography data: {str(e)}")
            raise e
    
    # Define the task
    geography_task = Load_geography_method()
   
