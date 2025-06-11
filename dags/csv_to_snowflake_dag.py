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



from scripts.pyspark_jobs.csv_ods_draft import load_csv_to_snowflake







default_args = {
    "owner": "data-team",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    "marketing_performance_to_snowflake",
        schedule="0 0 * * *",  
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["ods", "merge", "multi-region"],
) as dag:
    @task
    def process_marketing_data():
        """
        Task to process marketing performance CSV data and load to Snowflake
        """
        # Get current date for processing
        load_date = "2025-06-08"
        
        print(f"Starting marketing performance data processing for date: {load_date}")
        
        try:
            # Call the imported function
            # load_csv_to_snowflake(load_date)
            load_csv_to_snowflake(
            load_date=load_date,
            table_name="MARKETING_PERFORMANCE", 
            merge_key=["campaign_id", "source_region"]  # Using both ID and region as composite key
            )
            print(f"Successfully completed processing for date: {load_date}")
            
        except Exception as e:
            print(f"Error processing marketing data: {str(e)}")
            raise e
    
    # Define the task
    marketing_task = process_marketing_data()
   
