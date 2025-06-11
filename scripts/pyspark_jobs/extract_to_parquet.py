# # File: extract_postgres_table.py

# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import lit, current_timestamp

# # from airflow.models import Variable
# # from table_config import TABLE_CONFIG


# def extract_postgres_table(region, table, pg_user, pg_password, load_date):
#     print(f"Starting extraction for table: {table} in region: {region}")

#     # if table not in TABLE_CONFIG or region not in TABLE_CONFIG[table]["regions"]:
#     #     raise ValueError(f"Missing TABLE_CONFIG for table '{table}' and region '{region}'")

#     # timestamp_col = TABLE_CONFIG[table]["regions"][region]["timestamp_column"]

#     pg_url = f"jdbc:postgresql://retail-postgres:5432/retail_etl"

#     # --- Incremental Load Logic: 
#     # var_key = f"last_extracted_parquet_timestamp_{region}_{table}"
#     # try:
#     #     last_extracted_parquet_timestamp = Variable.get(var_key)
#     #     print(f"[INFO] Last extracted timestamp: {last_extracted_parquet_timestamp}")
#     # except KeyError:
#     #     last_extracted_parquet_timestamp = "2000-01-01 00:00:00.000"
#     #     print(f"[INFO] No variable found. Using default: {last_extracted_parquet_timestamp}")

#     spark = (
#         SparkSession.builder.appName(f"Extract_{region}_{table}")
#         .config(
#             "spark.jars",
#             "/opt/airflow/jars/postgresql-42.7.3.jar,"
#             "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,"
#             "/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar",
#         )
#         .config("spark.sql.shuffle.partitions", "10")
#         .getOrCreate()
#     )

#     # --- Full Load Query
#     query = f"(SELECT * FROM {region}.{table}) AS full_data"
#     print(query)

#     df = (
#         spark.read.format("jdbc")
#         .option("url", pg_url)
#         .option("dbtable", query)
#         .option("user", pg_user)
#         .option("password", pg_password)
#         .option("driver", "org.postgresql.Driver")
#         .load()
#     )

#     record_count = df.count()
#     if record_count == 0:
#         spark.stop()
#         print(f"[INFO] No data found in {region}.{table}")
#         return

#     df = df.withColumn("_region", lit(region)).withColumn("_source", lit("postgres"))
#     df.show(10)

#     output_path = (
#         f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"
#     )
#     print(f"Writing data to {output_path}")
#     df.write.mode("append").parquet(output_path)

#     # --- Update max timestamp: Commented Out ---
#     # max_ts = df.agg({timestamp_col: "max"}).collect()[0][0]
#     # if max_ts:
#     #     max_ts_str = max_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
#     #     Variable.set(var_key, max_ts_str)
#     #     print(f"[INFO] Updated Airflow Variable '{var_key}' to {max_ts_str}")

#     spark.stop()
#     print(f"Finished extraction for {table}")

# File: extract_postgres_table.py

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    trim,
    regexp_replace,
    to_timestamp,
    coalesce,
)
from airflow.models import Variable

# Timestamp mappings per region
datetime_columns = {
    "ASIA": {
        "categories": ["modify_time"],
        "products": ["create_date"],
        "product_variants": ["created"],
        "inventory": ["updated"],
        "customers": ["created"],
        "customer_addresses": ["created"],
        "marketing_campaigns": ["created"],
        "discounts": ["created"],
        "shopping_carts": ["updated"],
        "cart_items": ["updated"],
        "orders": ["processed_date"],
        "order_items": ["created"],
        "payments": ["created"],
        "shipments": ["updated"],
        "returns": ["created"],
        "product_reviews": ["created"],
        "wishlists": ["added"],
    },
    "EU": {
        "categories": ["updated_at"],
        "products": ["updated_at"],
        "product_variants": ["created_at"],
        "inventory": ["updated_at"],
        "customers": ["updated_at"],
        "customer_addresses": ["created_at"],
        "marketing_campaigns": ["created_at"],
        "discounts": ["created_at"],
        "shopping_carts": ["updated_at"],
        "cart_items": ["updated_at"],
        "orders": ["updated_at"],
        "order_items": ["created_at"],
        "payments": ["created_at"],
        "shipments": ["updated_at"],
        "returns": ["created_at"],
        "product_reviews": ["created_at"],
        "wishlists": ["added_at"],
    },
    "US": {
        "categories": ["updated_at"],
        "products": ["updated_at"],
        "product_variants": ["created_at"],
        "inventory": ["updated_at"],
        "customers": ["updated_at"],
        "customer_addresses": ["created_at"],
        "marketing_campaigns": ["created_at"],
        "discounts": ["created_at"],
        "shopping_carts": ["updated_at"],
        "cart_items": ["updated_at"],
        "orders": ["updated_at"],
        "order_items": ["created_at"],
        "payments": ["created_at"],
        "shipments": ["updated_at"],
        "returns": ["created_at"],
        "product_reviews": ["created_at"],
        "wishlists": ["added_at"],
    },
}

def parse_dirty_timestamp(col_):
    clean_col = trim(
        regexp_replace(
            regexp_replace(col_, r"(?i)(st|nd|rd|th)", ""),
            " +",
            " ",
        )
    )
    ts1 = to_timestamp(clean_col, "yyyy-MM-dd HH:mm:ss")
    ts2 = to_timestamp(clean_col, "yyyy-MM-dd")
    ts3 = to_timestamp(clean_col, "dd-MM-yyyy")
    ts4 = to_timestamp(clean_col, "MM/dd/yyyy")
    ts5 = to_timestamp(clean_col, "dd/MM/yyyy")
    ts6 = to_timestamp(clean_col, "yyyy/MM/dd")
    ts7 = to_timestamp(clean_col, "MMMM d yyyy")
    ts8 = to_timestamp(clean_col, "MMM d yyyy")
    return coalesce(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8)

def extract_postgres_table(region, table, pg_user, pg_password, load_date):
    print(f"Starting extraction for table: {table} in region: {region}")
    print(f"load_date:::::{load_date}")

    pg_url = f"jdbc:postgresql://retail-postgres:5432/retail_etl"

    spark = (
        SparkSession.builder.appName(f"Extract_{region}_{table}")
        .config(
            "spark.jars",
            "/opt/airflow/jars/postgresql-42.7.3.jar,"
            "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,"
            "/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar",
        )
        .config("spark.sql.shuffle.partitions", "10") \
        .getOrCreate()
    )
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    print(spark.sparkContext._jsc.sc().listJars())



    query = f"(SELECT * FROM {region}.{table}) AS full_data"
    df = (
        spark.read.format("jdbc")
        .option("url", pg_url)
        .option("dbtable", query)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    if df.rdd.isEmpty():
        print(f"[INFO] No data found in {region}.{table}")
        spark.stop()
        return

    timestamp_col = datetime_columns[region.upper()][table][0]
    var_key = f"last_extracted_parquet_timestamp_{region}_{table}"
    last_loaded_timestamp = Variable.get(var_key, default_var="2000-01-01 00:00:00.000")
    print(f"[INFO] Last loaded timestamp: {last_loaded_timestamp}")


    df = df.withColumn("_normalized_ts", parse_dirty_timestamp(col(timestamp_col)))
    df = df.filter(col("_normalized_ts") > last_loaded_timestamp)

    if df.rdd.isEmpty():
        print(f"[INFO] No new records found in {region}.{table}")
        spark.stop()
        return

    max_ts = df.agg({"_normalized_ts": "max"}).collect()[0][0]
    if max_ts:
        Variable.set(var_key, str(max_ts))
        print(f"[INFO] Updated Airflow Variable {var_key} to {max_ts}")

    df = df.drop("_normalized_ts")
    df = df.withColumn("_region", lit(region)).withColumn("_source", lit("postgres"))

    output_path = f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"
    print(f"Writing data to {output_path}")
    df.write.mode("append").parquet(output_path)
    csv_load = "csv_load_time"
    current_val = Variable.get(csv_load, default_var=None)
    if current_val != load_date:
        Variable.set(csv_load, load_date)

    df.show()

    spark.stop()
    print(f"Finished extraction for {table}")
