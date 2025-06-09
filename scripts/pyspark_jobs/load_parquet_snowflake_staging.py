import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def load_parquet_to_snowflake(region, table, load_date, min_load_date=None):
    parquet_path = f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"

    snowflake_options = {
        "sfURL": os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER"),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
        "sfSchema": os.getenv("SNOWFLAKE_STAGING_ASIA_SCHEMA"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "sfRole": os.getenv("SNOWFLAKE_ROLE", ""),
    }

    spark = SparkSession.builder \
        .appName(f"Load_{region}_{table}_to_Snowflake") \
        .config("spark.jars", "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar") \
        .config("spark.driver.extraClassPath", "/opt/airflow/jars/*") \
        .config("spark.executor.extraClassPath", "/opt/airflow/jars/*") \
        .getOrCreate()


    print(f"Reading Parquet from {parquet_path}")
    df = spark.read.parquet(parquet_path)

    if min_load_date:
        print(f"Filtering data for _load_date > {min_load_date}")
        df = df.filter(col("created") > min_load_date)

    print(f"Writing filtered data to Snowflake table: staging.{table}")
    df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", table) \
        .mode("overwrite") \
        .save()

    spark.stop()
