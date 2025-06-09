from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, task_group
from pyspark.sql import SparkSession
import sys
from dotenv import load_dotenv
import os
import shutil

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

# Configuration
REGIONS = ["asia", "eu", "us"]
TABLES = [
    "categories", "products", "product_variants", "inventory", 
    "customers", "customer_addresses", "discounts", "shopping_carts", 
    "cart_items", "orders", "order_items", "payments", 
    "shipments", "returns", "product_reviews", "wishlists"
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
    "discounts": []
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
}

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'multi_region_to_snowflake',
    default_args=default_args,
    description='Multi-region data loading to snowflake',
    schedule_interval='@daily',
    catchup=False,
    max_active_tasks=6,
    tags=['multi-region', 'transformations']
)

def get_spark_session(app_name: str):
    """Get Spark session"""
    return SparkSession.builder \
    .appName(app_name) \
    .config("spark.jars", "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar") \
    .config("spark.driver.extraClassPath", "/opt/airflow/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/airflow/jars/*") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.speculation", "false") \
    .config("parquet.enable.summary-metadata", "false") \
    .getOrCreate()


@task
def process_table(region: str, table: str, load_date: str):
    """Process a single table for a specific region"""
    
    spark = get_spark_session(f"Process_{table}_{region}_{load_date}")
    
    try:
        # Input and output paths
        input_path = f"/opt/airflow/data/raw/region={region}/table={table}/"
        # input_path = f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"
        output_path = f"/opt/airflow/data/processed/region={region}/table={table}/load_date={load_date}/"
        
        print(f"Processing {table} for region {region}")
        folders = os.listdir(input_path)
        date_folders = [
            f.split("=")[-1] for f in folders if f.startswith("load_date=")
        ]
        if not date_folders:
            raise FileNotFoundError(f"No load_date folders found in {input_path}")

        # Convert to datetime for comparison, then back to string
        latest_date = max([datetime.strptime(d, "%Y-%m-%d") for d in date_folders]).strftime("%Y-%m-%d")

        input_path = os.path.join(input_path, f"load_date={latest_date}")
        output_path = os.path.join(output_path, f"load_date={latest_date}")

        input_path_parquet = input_path 
        
        # Read data
        df_raw = spark.read.parquet(input_path_parquet)
        
        # Get transformation function and apply it
        transform_func = TRANSFORMERS.get(table)
        if transform_func:
            df_transformed = transform_func(df_raw, region)
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
            df.write \
                .format("snowflake") \
                .options(**sf_options) \
                .option("dbtable", table_name) \
                .mode("overwrite") \
                .save()



        # Write to Parquet (optional intermediate step)
        df_transformed.show(10)
        # df_transformed.write.mode("overwrite").parquet(output_path)
        
        print(f"Inserting into snowflake")
        # Then write to Snowflake
        write_to_snowflake(df_transformed, table_name=table, region=region, sf_options=SF_OPTIONS)

        print("successfully inserted in snowflake")
        
        record_count = df_transformed.count()
        print(f"Successfully processed {record_count} records for {table} in {region}")
        
        return {
            "region": region,
            "table": table,
            "record_count": record_count,
            "status": "success"
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

# Build the DAG
with dag:
    load_date = "2025-06-08"
    
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
