# File: extract_postgres_table.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from airflow.models import Variable
from table_config import TABLE_CONFIG 


def extract_postgres_table(region, table, pg_user, pg_password, load_date):
#     TABLE_CONFIG = {
#     "{table}": {
#         "regions": {
#             "us": {"timestamp_column": "created_at"},
#             "eu": {"timestamp_column": "created_at"},
#             "asia": {"timestamp_column": "created"},
#         }
#     }
# }

    print(f"Starting extraction for table: {table} in region: {region}")
    if table not in TABLE_CONFIG or region not in TABLE_CONFIG[table]["regions"]:
        raise ValueError(f"Missing TABLE_CONFIG for table '{table}' and region '{region}'")

    timestamp_col = TABLE_CONFIG[table]["regions"][region]["timestamp_column"]
    pg_url = f"jdbc:postgresql://retail-postgres:5432/retail_etl"
    # Variable key for tracking last extracted timestamp
    var_key = f"last_extracted_parquet_timestamp_{region}_{table}"
    try:
        last_extracted_parquet_timestamp = Variable.get(var_key)
        print(f"[INFO] Last extracted timestamp: {last_extracted_parquet_timestamp}")
    except KeyError:
        last_extracted_parquet_timestamp = "2000-01-01 00:00:00.000"
        print(f"[INFO] No variable found. Using default: {last_extracted_parquet_timestamp}")

    spark = SparkSession.builder \
        .appName(f"Extract_{region}_{table}") \
        .config(
            "spark.jars",
            "/opt/airflow/jars/postgresql-42.7.3.jar,"
            "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,"
            "/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar"
        ) \
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    # timestamp_col = TABLE_CONFIG[table]["regions"][region]["timestamp_column"]
    # query = f"(SELECT * FROM {region}.{table} WHERE {timestamp_col} > '{last_updated}') AS filtered_data"


    query = f"(SELECT * FROM {region}.{table} WHERE {timestamp_col} > '{last_extracted_parquet_timestamp}') AS filtered_data"
    print(query)
    df = spark.read.format("jdbc") \
        .option("url", pg_url) \
        .option("dbtable", query) \
        .option("user", pg_user) \
        .option("password", pg_password) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    record_count = df.count()
    if record_count == 0:
        spark.stop()
        raise AirflowSkipException(f"No new data to extract from {region}.{table}")

    df = df.withColumn("_region", lit(region)) \
           .withColumn("_source", lit("postgres")) 
    df.show(10)

    output_path = f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"
    print(f"Writing data to {output_path}")
    df.write.mode("append").parquet(output_path)

    # max_ts = df.agg({"created": "max"}).collect()[0][0]
    max_ts = df.agg({timestamp_col: "max"}).collect()[0][0]

    if max_ts:
        max_ts_str = max_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        Variable.set(var_key, max_ts_str)
        print(f"[INFO] Updated Airflow Variable '{var_key}' to {max_ts_str}")

    spark.stop()
    print(f"Finished extraction for {table}")
