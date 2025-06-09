from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, initcap, lower, when, to_timestamp, coalesce, lit, udf,regexp_replace
)
from pyspark.sql.types import StringType
import hashlib
import re
from pyspark.sql import DataFrame


def safe_to_timestamp_multi_formats(date_col):
    clean_col = trim(
        regexp_replace(
            regexp_replace(date_col, "(?i)(st|nd|rd|th)", ""),  # Remove ordinal suffixes
            " +", " "  # Normalize extra spaces
        )
    )

    ts1 = to_timestamp(clean_col, "yyyy-MM-dd HH:mm:ss")
    ts2 = to_timestamp(clean_col, "yyyy-MM-dd")
    ts3 = to_timestamp(clean_col, "dd-MM-yyyy")
    ts4 = to_timestamp(clean_col, "MM/dd/yyyy")
    ts5 = to_timestamp(clean_col, "dd/MM/yyyy")
    ts6 = to_timestamp(clean_col, "yyyy/MM/dd")
    ts7 = to_timestamp(clean_col, "MMMM d yyyy")  # Handles "March 5 2024"
    ts8 = to_timestamp(clean_col, "MMM d yyyy")    # Handles "Mar 5 2024"

    return coalesce(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8)
# --- ASIA ---
def transform_order_items_asia(df):
    return df.select(
        col("order_item_id"),
        col("order_id"),
        col("variant_id"),
        col("qty").alias("quantity"),
        col("unit_price_jpy").alias("unit_price_jpy"),
        col("total_price_jpy").alias("total_price_jpy"),
        safe_to_timestamp_multi_formats(col("created")).alias("created_at"),
        col("_region"),
        col("_source")
    )

# --- EU ---
def transform_order_items_eu(df):
    return df.select(
        col("order_item_id"),
        col("order_id"),
        col("variant_id"),
        col("quantity"),
        col("unit_price_eur").alias("unit_price_eur"),
        col("vat_rate"),
        col("total_price_eur").alias("total_price_eur"),
        col("created_at"),
        col("_region"),
        col("_source")
    )

# --- US ---
def transform_order_items_us(df):
    return df.select(
        col("order_item_id"),
        col("order_id"),
        col("variant_id"),
        col("quantity"),
        col("unit_price_usd"),
        col("total_price_usd"),
        col("created_at"),
        col("_region"),
        col("_source")
    )

# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=order_items/load_date={load_date}/"
#     print(f"\n=== Reading Order Items for Region: {region.upper()} ===")
#     df_raw = spark.read.parquet(input_path)

#     df_transformed = (
#         transform_order_items_asia(df_raw) if region == "asia" else
#         transform_order_items_eu(df_raw) if region == "eu" else
#         transform_order_items_us(df_raw)
#     )

#     print(f"\n--- Transformed Order Items for {region.upper()} ---")
#     df_transformed.show(truncate=False, vertical=True)

# print("Order Items normalization completed.")

def transform_order_items(df: DataFrame, region: str) -> DataFrame:
    if region == "asia":
        return transform_order_items_asia(df)
    elif region == "eu":
        return transform_order_items_eu(df)
    elif region == "us":
        return transform_order_items_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")