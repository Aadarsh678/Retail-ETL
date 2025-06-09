# File: scripts/pyspark_jobs/transform_customers_job.py

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../scripts"))
# if scripts_path not in sys.path:
#     sys.path.insert(0, scripts_path)

# print(f"script__pathhh::::::{scripts_path}")
# # Now import with
# from etl.transform.customer import transform_customers
scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# Add airflow root path for imports
airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if airflow_path not in sys.path:
    sys.path.insert(0, airflow_path)

from scripts.etl.transform.categories import transform_categories



def transform_categories_table(region, table, load_date):

    print(f"Starting transformation for table: {table} in region: {region}")

    spark = SparkSession.builder \
        .appName(f"TransformCustomers-{region}") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.sql.shuffle.partitions", "10") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()

    try:
        # Input and output paths
        input_path = f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"
        output_path = f"/opt/airflow/data/staging/{table}/region={region}/load_date={load_date}/"
        
        print(f"Reading data from: {input_path}")
        
        # Read raw parquet data
        df_raw = spark.read.parquet(input_path)
        
        record_count = df_raw.count()
        if record_count == 0:
            print(f"[INFO] No data found for {region}.{table} on {load_date}")
            return
        
        print(f"[INFO] Processing {record_count} records for {region}")
        
        # Transform data using region-specific logic
        df_transformed = transform_categories(df_raw, region)
        
        # Show sample of transformed data
        print(f"Sample transformed data for {region}:")
        df_transformed.show(5, truncate=False)
        
        # Write to CSV
        print(f"Writing transformed data to: {output_path}")
        df_transformed.coalesce(1).write \
            .option("header", True) \
            .option("delimiter", ",") \
            .mode("overwrite") \
            .csv(output_path)
        
        print(f"Successfully transformed {record_count} records for {region}")
        
    except Exception as e:
        print(f"Error transforming data for {region}: {str(e)}")
        raise e
    finally:
        spark.stop()
        
    print(f"Finished transformation for {table} in region {region}")



# import os
# import sys
# import pandas as pd

# # Setup import paths
# scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
# if scripts_path not in sys.path:
#     sys.path.insert(0, scripts_path)

# airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
# if airflow_path not in sys.path:
#     sys.path.insert(0, airflow_path)

# from scripts.etl.transform.categories import transform_categories


# def transform_categories_table(region, table, load_date):
#     print(f"Starting transformation for table: {table} in region: {region}")

#     try:
#         input_path = f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"
#         output_path = f"/opt/airflow/data/staging/{table}/region={region}/load_date={load_date}/"

#         # Find the first parquet file (assuming single file per partition)
#         parquet_files = [f for f in os.listdir(input_path) if f.endswith(".parquet")]
#         if not parquet_files:
#             print(f"[INFO] No parquet file found in {input_path}")
#             return

#         parquet_file_path = os.path.join(input_path, parquet_files[0])
#         print(f"Reading data from: {parquet_file_path}")

#         # Read data using pandas
#         df_raw = pd.read_parquet(parquet_file_path)

#         if df_raw.empty:
#             print(f"[INFO] No data found for {region}.{table} on {load_date}")
#             return

#         print(f"[INFO] Processing {len(df_raw)} records for {region}")

#         # Transform using region-specific logic
#         df_transformed = transform_categories(df_raw, region)

#         print(f"Sample transformed data for {region}:")
#         print(df_transformed.head(5))

#         # Ensure output directory exists
#         os.makedirs(output_path, exist_ok=True)

#         # Write to CSV
#         output_csv_path = os.path.join(output_path, "part-00000.csv")
#         df_transformed.to_csv(output_csv_path, index=False)
        
#         print(f"Successfully transformed {len(df_transformed)} records for {region}")
#         print(f"Data written to: {output_csv_path}")

#     except Exception as e:
#         print(f"Error transforming data for {region}: {str(e)}")
#         raise e

#     print(f"Finished transformation for {table} in region {region}")
