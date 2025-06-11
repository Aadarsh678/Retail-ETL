from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, task_group
from pyspark.sql import SparkSession
import sys
from airflow.models import Variable
from dotenv import load_dotenv
import os
import shutil
import snowflake.connector
import pyspark
from datetime import datetime
import shutil
import glob
import yaml
from pyspark.sql.functions import col

load_dotenv()


# Add paths for imports
scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# Add airflow root path for imports
airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if airflow_path not in sys.path:
    sys.path.insert(0, airflow_path)

# Import all transformation functions
from scripts.etl.transform.cart_items import transform_cart_items
from scripts.etl.transform.customer import transform_customers
from scripts.etl.transform.orders import transform_order
from scripts.etl.transform.categories import transform_categories
from scripts.etl.transform.customer_addresses import transform_customer_addressess
from scripts.etl.transform.discounts import transform_discounts
from scripts.etl.transform.inventory import transform_inventory
from scripts.etl.transform.order_items import transform_order_items
from scripts.etl.transform.payments import transform_payments
from scripts.etl.transform.product_reviews import transform_product_reviews
from scripts.etl.transform.product_variants import transform_product_variants
from scripts.etl.transform.products import transform_products
from scripts.etl.transform.returns import transform_returns
from scripts.etl.transform.shipments import transform_shipments
from scripts.etl.transform.shopping_carts import transform_shopping_cart
from scripts.etl.transform.wishlists import transform_whishlist
from scripts.etl.transform.marketing_campaigns import transform_marketing_campaigns

from scripts.spark_connector import get_spark_session

# Configuration
REGIONS = ["asia", "eu", "us"]
TABLES = [
    "categories",
    "products",
    "product_variants",
    "inventory",
    "customers",
    "customer_addresses",
    "discounts",
    "shopping_carts",
    "cart_items",
    "orders",
    "order_items",
    "payments",
    "shipments",
    "returns",
    "product_reviews",
    "wishlists",
    "marketing_campaigns"
]

# Table dependencies
TABLE_DEPENDENCIES = {
    "customers": [],
    "categories": [],
    "products": ["categories"],
    "product_variants": ["products"],
    "inventory": ["product_variants"],
    "customer_addresses": ["customers"],
    "shopping_carts": ["customers"],
    "cart_items": ["shopping_carts", "product_variants"],
    "orders": ["customers", "customer_addresses"],
    "order_items": ["orders", "product_variants"],
    "payments": ["orders"],
    "shipments": ["orders"],
    "returns": ["orders", "order_items"],
    "product_reviews": ["products", "customers"],
    "wishlists": ["customers", "products"],
    "discounts": [],
    "marketing_campaigns":[]
}

# Map table names to transformation functions
TRANSFORMERS = {
    "cart_items": transform_cart_items,
    "customers": transform_customers,
    "orders": transform_order,
    "categories": transform_categories,
    "customer_addresses": transform_customer_addressess,
    "discounts": transform_discounts,
    "inventory": transform_inventory,
    "order_items": transform_order_items,
    "payments": transform_payments,
    "product_reviews": transform_product_reviews,
    "product_variants": transform_product_variants,
    "products": transform_products,
    "returns": transform_returns,
    "shipments": transform_shipments,
    "shopping_carts": transform_shopping_cart,
    "wishlists": transform_whishlist,
    "marketing_campaigns": transform_marketing_campaigns,
}

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "multi_region_transform_staging_v2",
    default_args=default_args,
    description="Multi-region data loading to snowflake",
    schedule="0 0 * * *",  # Daily at midnight
    catchup=False,
    max_active_tasks=6,
    tags=["multi-region", "transformations"],
)


# def get_spark_session(app_name: str):
#     """Get Spark session"""
#     return (
#         SparkSession.builder.appName(app_name)
#         .config(
#             "spark.jars",
#             "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar",
#         )
#         .config("spark.driver.extraClassPath", "/opt/airflow/jars/*")
#         .config("spark.executor.extraClassPath", "/opt/airflow/jars/*")
#         .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
#         .config("spark.speculation", "false")
#         .config("parquet.enable.summary-metadata", "false")
#         .getOrCreate()
#     )

def get_latest_exchange_rates(spark: SparkSession, sf_options_curr: dict) -> dict:
    df_rates = (
        spark.read.format("snowflake")
        .options(**sf_options_curr)
        .option("dbtable", "exchange_curr.exchange_rates")
        .load()
    )

    latest_ts = df_rates.agg({"FETCHED_AT": "max"}).collect()[0][0]
    latest_rates_df = df_rates.filter(col("FETCHED_AT") == latest_ts)

    rates = {
        row["currency_pair"]: row["rate"]
        for row in latest_rates_df.collect()
    }

    return rates  # e.g., { "EUR/USD": 1.14146, "JPY/USD": 0.0068969 }



@task
def process_table(region: str, table: str, load_date: str):
    """Process a single table for a specific region"""
    # spark = (
    #     SparkSession.builder.appName(f"Extract_{region}_{table}")
    #     .config(
    #         "spark.jars",
    #         "/opt/airflow/jars/postgresql-42.7.3.jar,"
    #         "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,"
    #         "/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar",
    #     )
    #     .config("spark.driver.extraClassPath", "/opt/airflow/jars/*") # Add this line
    #     .config("spark.executor.extraClassPath", "/opt/airflow/jars/*") # Add this line
    #     .config("spark.sql.shuffle.partitions", "10")
    #     .getOrCreate()
    # )
    spark = get_spark_session()

    # spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    print(f"Debugggg::::{spark.sparkContext._jsc.sc().listJars()}")

    try:
        # Input and output paths
        input_path = f"/opt/airflow/data/raw/region={region}/table={table}/"
        # input_path = f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"
        output_path = f"/opt/airflow/data/processed/region={region}/table={table}/load_date={load_date}/"

        print(f"Processing {table} for region {region}")
        folders = os.listdir(input_path)
        date_folders = [f.split("=")[-1] for f in folders if f.startswith("load_date=")]
        if not date_folders:
            raise FileNotFoundError(f"No load_date folders found in {input_path}")

        # Convert to datetime for comparison, then back to string
        latest_date = max(
            [datetime.strptime(d, "%Y-%m-%d") for d in date_folders]
        ).strftime("%Y-%m-%d")
        var_key = f"processed_{region}_{table}"
        last_processed = Variable.get(var_key, default_var="2000-01-01")
        if last_processed == latest_date:
            print(f"Skipping {table} for {region}, already processed for {latest_date}")
            return {
                "region": region,
                "table": table,
                "record_count": 0,
                "status": "skipped",
            }

        input_path = os.path.join(input_path, f"load_date={latest_date}")
        print(input_path)

        input_path_parquet = input_path

        # Read data
        df_raw = spark.read.parquet(input_path_parquet)
        SF_OPTIONS_CURR = {
            "sfURL": os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
            "sfSchema": os.getenv("SNOWFLAKE_EXCHANGE_curr_SCHEMA"),
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "sfRole": os.getenv("SNOWFLAKE_ROLE"),
            "sfUser": os.getenv("SNOWFLAKE_USER"),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        }

        exchange_rates = get_latest_exchange_rates(spark, SF_OPTIONS_CURR)

        # Get transformation function and apply it
        transform_func = TRANSFORMERS.get(table)
        if transform_func:
            df_transformed = transform_func(df_raw, region,exchange_rates)
        else:
            # For tables without specific transformers, just pass through
            df_transformed = df_raw
            print(f"No transformer found for {table}, using raw data")

        region_schemas = {
            "asia": os.getenv("SNOWFLAKE_STAGING_ASIA_SCHEMA"),
            "eu": os.getenv("SNOWFLAKE_STAGING_EU_SCHEMA"),
            "us": os.getenv("SNOWFLAKE_STAGING_US_SCHEMA"),
        }
        sf_schema = region_schemas.get(region.lower())
        if not sf_schema:
            raise ValueError(f"Unsupported region: {region}")

        print(sf_schema)

        SF_OPTIONS = {
            "sfURL": os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
            "sfSchema": sf_schema,
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "sfRole": os.getenv("SNOWFLAKE_ROLE"),
            "sfUser": os.getenv("SNOWFLAKE_USER"),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        }

        def write_to_snowflake(df, table_name: str, region: str, sf_options: dict):
            """Write DataFrame to Snowflake"""
            df.write.format("snowflake").options(**sf_options).option(
                "dbtable", table_name
            ).mode("overwrite").save()

        # Write to Parquet (for testing)
        df_transformed.show(10)
        # Assuming the natural key is (cart_id, source_region)

        # df_transformed.write.mode("overwrite").parquet(output_path)

        print(f"Inserting into snowflake")
        # write to Snowflake
        write_to_snowflake(
            df_transformed, table_name=table, region=region, sf_options=SF_OPTIONS
        )

        print("successfully inserted in snowflake")
        # Update Airflow variable 
        Variable.set(var_key, latest_date)

        record_count = df_transformed.count()
        print(f"Successfully processed {record_count} records for {table} in {region}")

        return {
            "region": region,
            "table": table,
            "record_count": record_count,
            "status": "success",
        }

    except Exception as e:
        print(f"Error processing {table} for {region}: {str(e)}")
        raise
    finally:
        spark.stop()


@task_group
def process_region_asia(load_date: str):
    """Process all tables for Asia region"""
    tasks = {}

    for table in TABLES:
        task = process_table.override(task_id=f"process_{table}_asia")(
            region="asia", table=table, load_date=load_date
        )
        tasks[table] = task

    # Set dependencies within the task group
    for table in TABLES:
        deps = TABLE_DEPENDENCIES.get(table, [])
        for dep_table in deps:
            if dep_table in tasks:
                tasks[dep_table] >> tasks[table]


@task_group
def process_region_eu(load_date: str):
    """Process all tables for EU region"""
    tasks = {}

    # Create all tasks
    for table in TABLES:
        task = process_table.override(task_id=f"process_{table}_eu")(
            region="eu", table=table, load_date=load_date
        )
        tasks[table] = task

    # Set dependencies within the task group
    for table in TABLES:
        deps = TABLE_DEPENDENCIES.get(table, [])
        for dep_table in deps:
            if dep_table in tasks:
                tasks[dep_table] >> tasks[table]


@task_group
def process_region_us(load_date: str):
    """Process all tables for US region"""
    tasks = {}

    # Create all tasks
    for table in TABLES:
        task = process_table.override(task_id=f"process_{table}_us")(
            region="us", table=table, load_date=load_date
        )
        tasks[table] = task

    # Set dependencies within the task group
    for table in TABLES:
        deps = TABLE_DEPENDENCIES.get(table, [])
        for dep_table in deps:
            if dep_table in tasks:
                tasks[dep_table] >> tasks[table]


@task
def final_summary():
    """Final summary of processing"""
    print(" All regions processed successfully!")
    return {"status": "completed"}

@task
def get_load_date(**kwargs):
    return kwargs["ds"]



# Build the DAG
with dag:
    var_key = f"csv_load_time"
    last_processed = Variable.get(var_key)
    load_date = last_processed
    print(f"load date:::::::::::{load_date}")

    # Process all regions in parallel
    asia_tasks = process_region_asia(load_date=load_date)
    eu_tasks = process_region_eu(load_date=load_date)
    us_tasks = process_region_us(load_date=load_date)

    # Final summary
    summary = final_summary()

    # Set dependencies
    asia_tasks >> summary
    eu_tasks >> summary
    us_tasks >> summary

# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.decorators import task, task_group
# from airflow.models import Variable
# import os
# import sys
# from dotenv import load_dotenv
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col

# # Load environment variables
# load_dotenv()

# # Add import paths
# scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
# if scripts_path not in sys.path:
#     sys.path.insert(0, scripts_path)

# airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
# if airflow_path not in sys.path:
#     sys.path.insert(0, airflow_path)

# # Import transformation functions
# from scripts.etl.transform.cart_items import transform_cart_items
# from scripts.etl.transform.customer import transform_customers
# from scripts.etl.transform.orders import transform_order
# from scripts.etl.transform.categories import transform_categories
# from scripts.etl.transform.customer_addresses import transform_customer_addressess
# from scripts.etl.transform.discounts import transform_discounts
# from scripts.etl.transform.inventory import transform_inventory
# from scripts.etl.transform.order_items import transform_order_items
# from scripts.etl.transform.payments import transform_payments
# from scripts.etl.transform.product_reviews import transform_product_reviews
# from scripts.etl.transform.product_variants import transform_product_variants
# from scripts.etl.transform.products import transform_products
# from scripts.etl.transform.returns import transform_returns
# from scripts.etl.transform.shipments import transform_shipments
# from scripts.etl.transform.shopping_carts import transform_shopping_cart
# from scripts.etl.transform.wishlists import transform_whishlist
# from scripts.etl.transform.marketing_campaigns import transform_marketing_campaigns
# from scripts.spark_connector import get_spark_session

# TABLES = [
#     "categories", "products", "product_variants", "inventory", "customers", "customer_addresses",
#     "discounts", "shopping_carts", "cart_items", "orders", "order_items", "payments",
#     "shipments", "returns", "product_reviews", "wishlists", "marketing_campaigns"
# ]

# TABLE_DEPENDENCIES = {
#     "customers": [], "categories": [], "products": ["categories"],
#     "product_variants": ["products"], "inventory": ["product_variants"],
#     "customer_addresses": ["customers"], "shopping_carts": ["customers"],
#     "cart_items": ["shopping_carts", "product_variants"], "orders": ["customers", "customer_addresses"],
#     "order_items": ["orders", "product_variants"], "payments": ["orders"], "shipments": ["orders"],
#     "returns": ["orders", "order_items"], "product_reviews": ["products", "customers"],
#     "wishlists": ["customers", "products"], "discounts": [], "marketing_campaigns": []
# }

# TRANSFORMERS = {
#     "cart_items": transform_cart_items, "customers": transform_customers,
#     "orders": transform_order, "categories": transform_categories,
#     "customer_addresses": transform_customer_addressess, "discounts": transform_discounts,
#     "inventory": transform_inventory, "order_items": transform_order_items,
#     "payments": transform_payments, "product_reviews": transform_product_reviews,
#     "product_variants": transform_product_variants, "products": transform_products,
#     "returns": transform_returns, "shipments": transform_shipments,
#     "shopping_carts": transform_shopping_cart, "wishlists": transform_whishlist,
#     "marketing_campaigns": transform_marketing_campaigns,
# }

# default_args = {
#     "owner": "data-team",
#     "depends_on_past": False,
#     "start_date": datetime(2024, 1, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=1),
# }

# dag = DAG(
#     "multi_region_transform_staging_v2",
#     default_args=default_args,
#     description="Multi-region data loading to Snowflake using shared Spark session",
#     schedule=None,
#     catchup=False,
#     max_active_tasks=6,
#     tags=["multi-region", "transformations"]
# )

# def get_latest_exchange_rates(spark: SparkSession, sf_options_curr: dict) -> dict:
#     df_rates = (
#         spark.read.format("snowflake")
#         .options(**sf_options_curr)
#         .option("dbtable", "exchange_curr.exchange_rates")
#         .load()
#     )
#     latest_ts = df_rates.agg({"FETCHED_AT": "max"}).collect()[0][0]
#     latest_rates_df = df_rates.filter(col("FETCHED_AT") == latest_ts)
#     return {row["currency_pair"]: row["rate"] for row in latest_rates_df.collect()}

# def write_to_snowflake(df, table_name: str, sf_options: dict):
#     df.write.format("snowflake").options(**sf_options).option("dbtable", table_name).mode("overwrite").save()

# @task
# def process_region(region: str, load_date: str):
#     spark = get_spark_session()
#     print(f"Spark session started for {region}: {spark.sparkContext.appName}")
#     print(f"Debugggg::::{spark.sparkContext._jsc.sc().listJars()}")

#     try:
#         region_schemas = {
#             "asia": os.getenv("SNOWFLAKE_STAGING_ASIA_SCHEMA"),
#             "eu": os.getenv("SNOWFLAKE_STAGING_EU_SCHEMA"),
#             "us": os.getenv("SNOWFLAKE_STAGING_US_SCHEMA"),
#         }
#         sf_schema = region_schemas[region.lower()]
#         SF_OPTIONS = {
#             "sfURL": os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
#             "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
#             "sfSchema": sf_schema,
#             "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
#             "sfRole": os.getenv("SNOWFLAKE_ROLE"),
#             "sfUser": os.getenv("SNOWFLAKE_USER"),
#             "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
#         }
#         SF_OPTIONS_CURR = {
#             **SF_OPTIONS,
#             "sfSchema": os.getenv("SNOWFLAKE_EXCHANGE_curr_SCHEMA"),
#         }
#         exchange_rates = get_latest_exchange_rates(spark, SF_OPTIONS_CURR)

#         processed = set()
#         for table in TABLES:
#             deps = TABLE_DEPENDENCIES.get(table, [])
#             if any(dep not in processed for dep in deps):
#                 continue  # Skip until dependencies are met

#             var_key = f"processed_{region}_{table}"
#             print(f"var_key::::{var_key}")
#             input_base = f"/opt/airflow/data/raw/region={region}/table={table}/"
#             folders = [f for f in os.listdir(input_base) if f.startswith("load_date=")]
#             if not folders:
#                 print(f"No load_date folders for {table} in {region}")
#                 continue
#             latest_date = max([f.split("=")[-1] for f in folders])
#             if Variable.get(var_key, default_var="2000-01-01") == latest_date:
#                 print(f"{region}-{table} already processed for {latest_date}")
#                 continue

#             input_path = os.path.join(input_base, f"load_date={latest_date}")
#             print(input_path)
#             df_raw = spark.read.parquet(input_path)
#             transformer = TRANSFORMERS.get(table)
#             df_transformed = transformer(df_raw, region, exchange_rates) if transformer else df_raw
#             df_transformed.show(5)
#             write_to_snowflake(df_transformed, table_name=table, sf_options=SF_OPTIONS)
#             Variable.set(var_key, latest_date)
#             processed.add(table)
#             print(f"✔ Processed {table} for {region}")

#     finally:
#         spark.stop()
#         print(f"Spark session stopped for {region}")

# @task
# def final_summary():
#     print("✅ All regions completed")
#     return {"status": "completed"}

# with dag:
#     load_date = Variable.get("csv_load_time")

#     asia = process_region.override(task_id="process_asia")("asia", load_date)
#     eu = process_region.override(task_id="process_eu")("eu", load_date)
#     us = process_region.override(task_id="process_us")("us", load_date)

#     summary = final_summary()
#     asia >> eu >> us >> summary
