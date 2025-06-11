import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_date, col
from pyspark.sql.types import *
from dotenv import load_dotenv
from airflow.models import Variable
# from table_config import TABLE_CONFIG
load_dotenv()
SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.getenv("SNOWFLAKE_ODS_SCHEMA")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

def load_csv_to_snowflake(load_date):
    # print(f"Starting CSV load for table: {table_name} for date: {load_date}")
    
    # # Snowflake connection options
    snowflake_options = {
        "sfUrl": f"{SF_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SF_USER,
        "sfPassword": SF_PASSWORD,
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE
    }
    
    # --- Incremental Load Logic: Get last processed date
    # var_key = f"last_processed_csv_date_{table_name}"

    # last_processed_date = "2000-01-01"

    # last_processed_date = Variable.get(var_key)
    # print(f"[INFO] Last processed date: {last_processed_date}")

        
    
    spark = (
        SparkSession.builder
        .appName(f"Load_CSV_to_Snowflake")
        .config(
            "spark.jars",
            "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,"
            "/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar"
        )
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )
    
    # Construct file path based on folder structure
    csv_file_path = "/opt/airflow/data/raw_marketing_performance/"



    print(f"Processing csv for region {load_date}")
    folders = os.listdir(csv_file_path)
    date_folders = [f.split("=")[-1] for f in folders if f.startswith("Date=")]
    if not date_folders:
        raise FileNotFoundError(f"No load_date folders found in {csv_file_path}")

        # Convert to datetime for comparison, then back to string
    latest_date = max(
        [datetime.strptime(d, "%Y-%m-%d") for d in date_folders]
    ).strftime("%Y-%m-%d")

    input_path = os.path.join(csv_file_path, f"Date={latest_date}")
    
    # Read CSV file with header inference
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("dateFormat", "yyyy-MM-dd")
        .csv(input_path)
    )
    
    record_count = df.count()
    if record_count == 0:
        spark.stop()
        print(f"[INFO] No data found in {csv_file_path}")
        return
    
    print(f"[INFO] Found {record_count} records in CSV file")
    
    # Show sample data
    print("Sample data:")
    df.show(10, truncate=False)
    
    # Write to Snowflake
    # snowflake_table = f"{snowflake_database}.{snowflake_schema}.{table_name.upper()}"
    # print(f"Writing data to Snowflake table: {snowflake_table}")
    
    # try:
    #     (df.write
    #      .format("snowflake")
    #      .options(**snowflake_options)
    #      .option("dbtable", snowflake_table)
    #      .mode("append")  # Change to "overwrite" if you want to replace data
    #      .save())
        
    #     print(f"[SUCCESS] Successfully loaded {record_count} records to {snowflake_table}")
        
        # --- Update last processed date: Commented Out ---
        # Variable.set(var_key, load_date)
        # print(f"[INFO] Updated Airflow Variable '{var_key}' to {load_date}")
        
    # except Exception as e:
    #     print(f"[ERROR] Failed to load data to Snowflake: {str(e)}")
    #     raise e
    
    # finally:
    spark.stop()
    
    print(f"Finished loading for date {load_date}")

