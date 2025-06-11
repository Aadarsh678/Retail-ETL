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



from scripts.star_load.payment_method import upsert_payment_methods_from_csv_spark



default_args = {
    "owner": "data-team",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "payment_method_to_snowflake",
    schedule='0 1 15 1 *',  
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ods", "merge", "multi-region"],
) as dag:
    @task
    def Load_payment_method():
 
        
        try:
            # Call the imported function
            # load_csv_to_snowflake(load_date)
            upsert_payment_methods_from_csv_spark('/opt/airflow/data/lookup/payment_method.csv')
            print(f"Successfully completed processing.")
            
        except Exception as e:
            print(f"Error processing payment data: {str(e)}")
            raise e
    
    # Define the task
    payment_task = Load_payment_method()
   
