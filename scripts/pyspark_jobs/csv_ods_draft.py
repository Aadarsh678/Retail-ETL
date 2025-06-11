import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_date, col
from pyspark.sql.types import *
from dotenv import load_dotenv
from airflow.models import Variable
import snowflake.connector
from airflow.exceptions import AirflowSkipException

# from table_config import TABLE_CONFIG
load_dotenv()
SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.getenv("SNOWFLAKE_ODS_SCHEMA")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )

def perform_merge_from_temp_table(conn, full_table_name, temp_table_name, df_columns, merge_key):
    """
    Extracted merge logic that performs UPSERT from temporary staging table to target table
    """
    cur = None
    try:
        cur = conn.cursor()
        
        # Ensure merge_key is a list
        if not isinstance(merge_key, list):
            merge_key = [merge_key]

        # Ensure columns are uppercase for Snowflake compatibility
        df_columns_upper = [c.upper() for c in df_columns]
        
        # Identify columns to update (all except merge key columns)
        update_cols = [c for c in df_columns_upper if c not in [mk.upper() for mk in merge_key]]
        
        # Create UPDATE SET clauses: target.column = source.column
        update_set_clauses = [f"target.{c} = source.{c}" for c in update_cols]

        # Create column list for INSERT
        insert_columns = ', '.join(df_columns_upper)
        insert_values = ', '.join([f"source.{c}" for c in df_columns_upper])

        # Create ON clause for matching records
        on_clauses = [f"target.{mk.upper()} = source.{mk.upper()}" for mk in merge_key]
        on_clause_str = " AND ".join(on_clauses)

        # Construct the MERGE SQL statement
        merge_sql = f"""
            MERGE INTO {full_table_name} AS target
            USING {temp_table_name} AS source
            ON {on_clause_str}
            WHEN MATCHED THEN
                UPDATE SET
                    {', '.join(update_set_clauses)}
            WHEN NOT MATCHED THEN
                INSERT ({insert_columns})
                VALUES ({insert_values});
        """
        
        print(f"Executing MERGE statement for {full_table_name} from staging table...")
        print(f"MERGE SQL: {merge_sql}")
        
        # Execute the merge operation
        cur.execute(merge_sql)
        conn.commit()
        
        print(f"Data successfully MERGED into {full_table_name} from staging table.")
        
    except Exception as e:
        print(f"Error during merge operation: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()

def cleanup_temp_table(temp_table_name):
    """
    Clean up temporary staging table after successful merge
    """
    conn_cleanup = None
    cur_cleanup = None
    try:
        conn_cleanup = get_snowflake_connection()
        cur_cleanup = conn_cleanup.cursor()
        cur_cleanup.execute(f"DROP TABLE {temp_table_name};")
        print(f"Temporary staging table {temp_table_name} dropped successfully.")
    except Exception as e:
        print(f"Warning: Could not drop temporary staging table {temp_table_name}: {e}")
    finally:
        if cur_cleanup:
            cur_cleanup.close()
        if conn_cleanup:
            conn_cleanup.close()

def load_dataframe_to_snowflake_with_merge(spark, df_to_load, table_name, merge_key):
    """
    Load DataFrame to Snowflake using temporary staging table and MERGE operation
    """
    # Snowflake connection options for Spark
    snowflake_options = {
        "sfUrl": f"{SF_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SF_USER,
        "sfPassword": SF_PASSWORD,
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE
    }
    
    full_table_name = f"{SF_SCHEMA}.{table_name.upper()}"
    temp_table_name = f"{SF_SCHEMA}.{table_name.upper()}_TEMP_STAGE_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    
    conn = None
    try:
        # Get Snowflake connection for merge operations
        conn = get_snowflake_connection()
        
        print(f"Writing data to temporary staging table: {temp_table_name}")
        
        # Write DataFrame to temporary staging table
        df_to_load.write \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", temp_table_name) \
            .mode("overwrite") \
            .save()
        
        print(f"Data successfully written to temporary staging table: {temp_table_name}")
        
        # Perform merge operation
        perform_merge_from_temp_table(
            conn=conn,
            full_table_name=full_table_name,
            temp_table_name=temp_table_name,
            df_columns=df_to_load.columns,
            merge_key=merge_key
        )
        
        print(f"Successfully completed UPSERT operation for table: {full_table_name}")
        
    except Exception as e:
        print(f"Error in load_dataframe_to_snowflake_with_merge: {e}")
        raise
    finally:
        if conn:
            conn.close()
        # Clean up temporary table
        cleanup_temp_table(temp_table_name)

def load_csv_to_snowflake(load_date, table_name="MARKETING_PERFORMANCE", merge_key=None):
    """
    Load CSV data to Snowflake with UPSERT capability using temporary staging table
    """
    print(f"Starting CSV load for table: {table_name} for date: {load_date}")
    
    spark = None
    try:
        spark = (
            SparkSession.builder
            .appName(f"Load_CSV_to_Snowflake_{table_name}")
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

        print(f"Processing csv for date {load_date}")
        folders = os.listdir(csv_file_path)
        date_folders = [f.split("=")[-1] for f in folders if f.startswith("Date=")]
        if not date_folders:
            raise FileNotFoundError(f"No load_date folders found in {csv_file_path}")

        # Convert to datetime for comparison, then back to string
        latest_date = max(
            [datetime.strptime(d, "%Y-%m-%d") for d in date_folders]
        ).strftime("%Y-%m-%d")

        input_path = os.path.join(csv_file_path, f"Date={latest_date}")
        var_key = "last_loaded_timestamp"
        last_loaded_timestamp = Variable.get(var_key, default_var="2000-01-01")
        print(f"[INFO] Last loaded timestamp: {last_loaded_timestamp}")

        
        
        # Read CSV file with header inference
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .option("dateFormat", "yyyy-MM-dd")
            .csv(input_path)
        )
        
        # Add load timestamp
        # df = df.withColumn("LOAD_TIMESTAMP", current_timestamp())
        max_csv_ts = df.agg({"updated_at": "max"}).collect()[0][0]
        max_csv_ts_str = max_csv_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        if max_csv_ts_str <= last_loaded_timestamp:
            spark.stop()
            raise AirflowSkipException(
                f"No new data to load. Last loaded: {last_loaded_timestamp}, max found: {max_csv_ts}"
            )

        # Filter new records
        df = df.filter(col("updated_at") > last_loaded_timestamp)
        record_count = df.count()
        if record_count == 0:
            print(f"[INFO] No data found in {csv_file_path}")
            return
        
        
        print(f"[INFO] Found {record_count} records in CSV file")
        
        # Show sample data
        print("Sample data:")
        df.show(10, truncate=False)
        
        # Load data using MERGE operation
        print(f"Loading data to Snowflake table: {table_name} using UPSERT...")
        load_dataframe_to_snowflake_with_merge(
            spark=spark,
            df_to_load=df,
            table_name=table_name,
            merge_key=merge_key
        )
        
        print(f"[SUCCESS] Successfully loaded {record_count} records to {table_name} using UPSERT")
        # Update last_loaded timestamp
        Variable.set(var_key, max_csv_ts)
        print(f"[INFO] Updated Airflow variable '{var_key}' = {max_csv_ts_str}")
        
    except Exception as e:
        print(f"[ERROR] Failed to load data to Snowflake: {str(e)}")
        raise e
    
    finally:
        if spark:
            spark.stop()
    
    print(f"Finished loading for date {load_date}")

# # Example usage
# if __name__ == "__main__":
#     # Example call - you can customize table_name and merge_key as needed
#     load_csv_to_snowflake(
#         load_date="2024-01-15",
#         table_name="MARKETING_PERFORMANCE", 
#         merge_key=["CAMPAIGN_ID", "DATE"]  # Adjust merge key based on your data
#     )