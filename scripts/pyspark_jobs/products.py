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

from scripts.etl.transform.products import transform_products


def transform_products_table(region, table, load_date):

    print(f"Starting transformation for table: {table} in region: {region}")

    spark = (
        SparkSession.builder.appName(f"TransformCustomers-{region}")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.shuffle.partitions", "10")
        .getOrCreate()
    )

    try:
        # Input and output paths
        input_path = f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"
        output_path = (
            f"/opt/airflow/data/staging/{table}/region={region}/load_date={load_date}/"
        )

        print(f"Reading data from: {input_path}")

        # Read raw parquet data
        df_raw = spark.read.parquet(input_path)

        record_count = df_raw.count()
        if record_count == 0:
            print(f"[INFO] No data found for {region}.{table} on {load_date}")
            return

        print(f"[INFO] Processing {record_count} records for {region}")

        # Transform data using region-specific logic
        df_transformed = transform_products(df_raw, region)

        # Show sample of transformed data
        print(f"Sample transformed data for {region}:")
        df_transformed.show(5, truncate=False)

        # Write to CSV
        print(f"Writing transformed data to: {output_path}")
        df_transformed.coalesce(1).write.option("header", True).option(
            "delimiter", ","
        ).mode("overwrite").csv(output_path)

        print(f"Successfully transformed {record_count} records for {region}")

    except Exception as e:
        print(f"Error transforming data for {region}: {str(e)}")
        raise e
    finally:
        spark.stop()

    print(f"Finished transformation for {table} in region {region}")
