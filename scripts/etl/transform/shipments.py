from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    initcap,
    lower,
    when,
    to_timestamp,
    coalesce,
    lit,
    udf,
    regexp_replace,
)
from pyspark.sql.types import StringType
import hashlib
import re
from pyspark.sql import DataFrame


from pyspark.sql.functions import col, lit


def safe_to_timestamp_multi_formats(date_col):
    clean_col = trim(
        regexp_replace(
            regexp_replace(
                date_col, "(?i)(st|nd|rd|th)", ""
            ),  # Remove ordinal suffixes
            " +",
            " ",  # Normalize extra spaces
        )
    )

    ts1 = to_timestamp(clean_col, "yyyy-MM-dd HH:mm:ss")
    ts2 = to_timestamp(clean_col, "yyyy-MM-dd")
    ts3 = to_timestamp(clean_col, "dd-MM-yyyy")
    ts4 = to_timestamp(clean_col, "MM/dd/yyyy")
    ts5 = to_timestamp(clean_col, "dd/MM/yyyy")
    ts6 = to_timestamp(clean_col, "yyyy/MM/dd")
    ts7 = to_timestamp(clean_col, "MMMM d yyyy")  # Handles "March 5 2024"
    ts8 = to_timestamp(clean_col, "MMM d yyyy")  # Handles "Mar 5 2024"

    return coalesce(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8)


# --- ASIA ---
def transform_shipments_asia(df):
    return df.select(
        col("shipment_id"),
        col("order_id"),
        col("tracking_no").alias("tracking_number"),
        col("carrier"),
        col("method").alias("shipping_method"),
        safe_to_timestamp_multi_formats(col("shipped_time")).alias("shipped_timestamp"),
        safe_to_timestamp_multi_formats(col("est_delivery")).alias(
            "estimated_delivery_date"
        ),
        safe_to_timestamp_multi_formats(col("actual_delivery")).alias(
            "actual_delivery_timestamp"
        ),
        col("status").alias("shipment_status"),
        safe_to_timestamp_multi_formats(col("created")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


# --- EU ---
def transform_shipments_eu(df):
    return df.select(
        col("shipment_id"),
        col("order_id"),
        col("tracking_number"),
        col("carrier"),
        col("shipping_method"),
        col("shipped_timestamp"),
        col("estimated_delivery_date"),
        col("actual_delivery_timestamp"),
        col("shipment_status"),
        col("created_at"),
        col("updated_at"),
        col("_region"),
        col("_source"),
    )


# --- US ---
def transform_shipments_us(df):
    return df.select(
        col("shipment_id"),
        col("order_id"),
        col("tracking_number"),
        col("carrier"),
        col("shipping_method"),
        col("shipped_date").alias("shipped_timestamp"),
        col("estimated_delivery_date"),
        col("actual_delivery_date").alias("actual_delivery_timestamp"),
        col("shipment_status"),
        col("created_at"),
        col("updated_at"),
        col("_region"),
        col("_source"),
    )


# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=shipments/load_date={load_date}/"
#     print(f"\n=== Reading Shipments for Region: {region.upper()} ===")
#     df_raw = spark.read.parquet(input_path)

#     df_transformed = (
#         transform_shipments_asia(df_raw) if region == "asia" else
#         transform_shipments_eu(df_raw) if region == "eu" else
#         transform_shipments_us(df_raw)
#     )

#     print(f"\n--- Transformed Shipments for {region.upper()} ---")
#     df_transformed.show(truncate=False, vertical=True)

# print("Shipment normalization completed.")


def transform_shipments(df: DataFrame, region: str) -> DataFrame:
    if region == "asia":
        return transform_shipments_asia(df)
    elif region == "eu":
        return transform_shipments_eu(df)
    elif region == "us":
        return transform_shipments_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")
