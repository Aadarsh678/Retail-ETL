# File: scripts/pyspark_jobs/generic_table_transform.py
from datetime import time
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

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

# Add more imports as needed for other tables


def get_transformation_function(table_name):
    """
    Returns the appropriate transformation function based on table name
    """
    TRANSFORM_FUNCTIONS = {
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

    if table_name not in TRANSFORM_FUNCTIONS:
        raise ValueError(f"No transformation function found for table: {table_name}")

    return TRANSFORM_FUNCTIONS[table_name]


def transform_table(region, table, load_date):
    """
    Generic function to transform any table for any region

    Args:
        region (str): Region to process (asia, eu, us, etc.)
        table (str): Table name to process (cart_items, customers, orders, etc.)
        load_date (str): Date to process in YYYY-MM-DD format
    """
    print(f"Starting transformation for table: {table} in region: {region}")

    # Create Spark session with dynamic app name based on table and region
    spark = (
        SparkSession.builder.appName(f"Transform-{table.title()}-{region.upper()}")
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

        # Get the appropriate transformation function for this table
        transform_function = get_transformation_function(table)

        # Transform data using the table-specific transformation function
        df_transformed = transform_function(df_raw, region)

        # Show sample of transformed data
        print(f"Sample transformed data for {table} in {region}:")
        df_transformed.show(5, truncate=False)

        # Write to CSV
        print(f"Writing transformed data to: {output_path}")
        df_transformed.coalesce(1).write.option("header", True).option(
            "delimiter", ","
        ).mode("overwrite").csv(output_path)

        print(
            f"Successfully transformed {record_count} records for {table} in {region}"
        )

    except Exception as e:
        print(f"Error transforming {table} data for {region}: {str(e)}")
        raise e
    finally:
        spark.stop()
        time.sleep(2)
        print(f"Finished transformation for {table} in region {region}")


# Backward compatibility - keep existing function names if needed
def transform_cart_items_table(region, table, load_date):
    """Wrapper for backward compatibility"""
    return transform_table(region, table, load_date)


def transform_customers_table(region, table, load_date):
    """Transform customers table"""
    return transform_table(region, table, load_date)


def transform_orders_table(region, table, load_date):
    """Transform orders table"""
    return transform_table(region, table, load_date)
