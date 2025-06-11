import os
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Load environment variables
load_dotenv()


REGION = "asia"
TABLE = "customers"


@dag(
    dag_id="load_parquet_to_snowflake_staging",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
)
def load_to_snowflake():
    @task
    def load_latest_data():
        base_path = f"/opt/airflow/data/raw/region={REGION}/table={TABLE}/"

        # Get latest load_date folder
        folders = os.listdir(base_path)
        date_folders = [d.split("=")[-1] for d in folders if d.startswith("load_date=")]
        if not date_folders:
            raise FileNotFoundError("No load_date folders found.")
        latest_date = max(date_folders)
        parquet_path = os.path.join(base_path, f"load_date={latest_date}/")

        # Get last loaded timestamp from Airflow Variable
        var_key = f"last_loaded_timestamp_{REGION}_{TABLE}"
        try:
            last_loaded_timestamp = Variable.get(var_key)
            print(f"[INFO] Last loaded timestamp: {last_loaded_timestamp}")
        except KeyError:
            last_loaded_timestamp = "2000-01-01 00:00:00.000"
            print(f"[INFO] Variable not found, defaulting to {last_loaded_timestamp}")

        # Initialize Spark session
        spark = (
            SparkSession.builder.appName(f"Load_{REGION}_{TABLE}_to_Snowflake")
            .config(
                "spark.jars",
                "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar",
            )
            .config("spark.driver.extraClassPath", "/opt/airflow/jars/*")
            .config("spark.executor.extraClassPath", "/opt/airflow/jars/*")
            .getOrCreate()
        )

        print(f"[INFO] Reading Parquet from: {parquet_path}")
        df = spark.read.parquet(parquet_path)

        # Check if new data exists
        max_parquet_ts = df.agg({"created": "max"}).collect()[0][0]
        max_parquet_ts_str = max_parquet_ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        if max_parquet_ts_str <= last_loaded_timestamp:
            spark.stop()
            raise AirflowSkipException(
                f"No new data to load. Last loaded: {last_loaded_timestamp}, max found: {max_parquet_ts}"
            )

        # Filter new records
        df = df.filter(col("created") > last_loaded_timestamp)

        # Count new records to load
        new_records_count = df.count()
        df.show()
        print(f"[INFO] Number of new records to load: {new_records_count}")

        # Snowflake write options
        snowflake_options = {
            "sfURL": os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
            "sfUser": os.getenv("SNOWFLAKE_USER"),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
            "sfSchema": os.getenv("SNOWFLAKE_STAGING_ASIA_SCHEMA"),
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "sfRole": os.getenv("SNOWFLAKE_ROLE", ""),
        }

        print(f"[INFO] Writing to Snowflake table: staging.{TABLE}")
        df.write.format("snowflake").options(**snowflake_options).option(
            "dbtable", TABLE
        ).mode("overwrite").save()

        # Update last_loaded timestamp
        Variable.set(var_key, max_parquet_ts)
        print(f"[INFO] Updated Airflow variable '{var_key}' = {max_parquet_ts_str}")

        spark.stop()

    load_latest_data()


dag = load_to_snowflake()

# import os
# import sys
# import shutil
# import pendulum
# from datetime import timedelta, datetime
# from airflow.decorators import dag, task
# from pyspark.sql import SparkSession

# # Add paths for module imports
# scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
# airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
# for path in [scripts_path, airflow_path]:
#     if path not in sys.path:
#         sys.path.insert(0, path)

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

# # Config
# REGIONS = ["asia", "eu", "us"]
# TABLES = [
#     "categories", "products", "product_variants", "inventory",
#     "customers", "customer_addresses", "discounts", "shopping_carts",
#     "cart_items", "orders", "order_items", "payments",
#     "shipments", "returns", "product_reviews", "wishlists"
# ]

# TABLE_DEPENDENCIES = {
#     "customers": [],
#     "categories": [],
#     "products": ["categories"],
#     "product_variants": ["products"],
#     "inventory": ["product_variants"],
#     "customer_addresses": ["customers"],
#     "shopping_carts": ["customers"],
#     "cart_items": ["shopping_carts", "product_variants"],
#     "orders": ["customers", "customer_addresses"],
#     "order_items": ["orders", "product_variants"],
#     "payments": ["orders"],
#     "shipments": ["orders"],
#     "returns": ["orders", "order_items"],
#     "product_reviews": ["products", "customers"],
#     "wishlists": ["customers", "products"],
#     "discounts": []
# }

# TRANSFORMERS = {
#     "cart_items": transform_cart_items,
#     "customers": transform_customers,
#     "orders": transform_order,
#     "categories": transform_categories,
#     "customer_addresses": transform_customer_addressess,
#     "discounts": transform_discounts,
#     "inventory": transform_inventory,
#     "order_items": transform_order_items,
#     "payments": transform_payments,
#     "product_reviews": transform_product_reviews,
#     "product_variants": transform_product_variants,
#     "products": transform_products,
#     "returns": transform_returns,
#     "shipments": transform_shipments,
#     "shopping_carts": transform_shopping_cart,
#     "wishlists": transform_whishlist,
# }

# def get_spark_session(app_name: str) -> SparkSession:
#     return SparkSession.builder \
#         .appName(app_name) \
#         .config("spark.jars.packages", "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar") \
#         .config("spark.driver.extraClassPath", "/opt/airflow/jars/*") \
#         .config("spark.executor.extraClassPath", "/opt/airflow/jars/*") \
#         .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
#         .config("spark.speculation", "false") \
#         .config("parquet.enable.summary-metadata", "false") \
#         .getOrCreate()

# def clear_output_path(path: str):
#     if os.path.exists(path):
#         shutil.rmtree(path)
#         print(f"[INFO] Cleared output path: {path}")

# @dag(
#     dag_id="multi_region_transform_dag-102",
#     schedule="0 0 * * *",
#     start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
#     catchup=False,
#     default_args={
#         'owner': 'data-team',
#         'retries': 1,
#         'retry_delay': timedelta(minutes=5),
#     },
#     tags=["multi-region", "transformations"],
# )
# def multi_region_transform():
#     load_date = "2025-06-08"

#     @task
#     def process_table(region: str, table: str):
#         spark = None
#         try:
#             spark = get_spark_session(f"Process_{table}_{region}_{load_date}")
#             input_path = f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"
#             output_path = f"/opt/airflow/data/processed/region={region}/table={table}/load_date={load_date}/"

#             print(f"[INFO] Processing {table} for {region}")
#             clear_output_path(output_path)

#             df_raw = spark.read.parquet(input_path)
#             transform_func = TRANSFORMERS.get(table)
#             df_transformed = transform_func(df_raw, region) if transform_func else df_raw

#             df_transformed.write.mode("overwrite").parquet(output_path)
#             count = df_transformed.count()
#             print(f"[INFO] {region}/{table} - {count} records written.")

#             return {"region": region, "table": table, "count": count}

#         except Exception as e:
#             raise RuntimeError(f"[ERROR] Failed processing {region}/{table}: {e}")
#         finally:
#             if spark:
#                 spark.stop()

#     @task
#     def final_summary():
#         print("[INFO] All regions processed.")
#         return {
#             "timestamp": datetime.now().isoformat(),
#             "status": "completed"
#         }

#     # Generate task dependencies
#     task_refs = {}
#     for region in REGIONS:
#         task_refs[region] = {}
#         for table in TABLES:
#             task_refs[region][table] = process_table(region=region, table=table)

#     for region in REGIONS:
#         for table, table_task in task_refs[region].items():
#             for dependency in TABLE_DEPENDENCIES.get(table, []):
#                 task_refs[region][dependency] >> table_task

#     # Attach final summary after all independent leaf tables complete
#     leaf_tasks = []
#     for region in REGIONS:
#         for table in TABLES:
#             if all(table not in deps for deps in TABLE_DEPENDENCIES.values()):
#                 leaf_tasks.append(task_refs[region][table])
#     summary_task = final_summary()
#     leaf_tasks >> summary_task

# dag = multi_region_transform()
