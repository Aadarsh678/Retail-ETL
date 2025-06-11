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
from pyspark.sql import DataFrame

from pyspark.sql.functions import split, regexp_extract, expr


# Unified timestamp conversion for various formats
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


# ASIA Inventory Transformation
def transform_inventory_asia(df):
    return df.select(
        col("inventory_id"),
        col("variant_id"),
        col("warehouse").alias("warehouse_location"),  # normalized name
        col("qty_available").alias("quantity_available"),
        col("qty_reserved").alias("quantity_reserved"),
        col("reorder_point").alias("reorder_level"),
        safe_to_timestamp_multi_formats(col("last_restock")).alias("last_restocked_at"),
        safe_to_timestamp_multi_formats(col("updated")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


# EU Inventory Transformation
def transform_inventory_eu(df):
    return df.select(
        col("inventory_id"),
        col("variant_id"),
        col("warehouse_location"),
        col("quantity_available"),
        col("quantity_reserved"),
        col("reorder_level"),
        safe_to_timestamp_multi_formats(col("last_restocked_at")).alias(
            "last_restocked_at"
        ),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


# US Inventory Transformation
def transform_inventory_us(df):
    return df.select(
        col("inventory_id"),
        col("variant_id"),
        col("warehouse_location"),
        col("quantity_available"),
        col("quantity_reserved"),
        col("reorder_level"),
        safe_to_timestamp_multi_formats(col("last_restocked_at")).alias(
            "last_restocked_at"
        ),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=inventory/load_date={load_date}/"
#     print(f"\n=== Reading data for region: {region} ===")
#     df_raw = spark.read.parquet(input_path)

#     print(f"\n--- Raw Data for {region.upper()} ---")
#     df_raw.show(truncate=False)

#     if region == "asia":
#         df_transformed = transform_inventory_asia(df_raw)
#     elif region == "eu":
#         df_transformed = transform_inventory_eu(df_raw)
#     elif region == "us":
#         df_transformed = transform_inventory_us(df_raw)
#     else:
#         raise ValueError(f"Unsupported region: {region}")

#     print(f"\n--- Transformed Data for {region.upper()} ---")
#     df_transformed.show(truncate=False,vertical=True)


# print("Regional transformations with GDPR compliance completed.")


def transform_inventory(df: DataFrame, region: str) -> DataFrame:
    if region == "asia":
        return transform_inventory_asia(df)
    elif region == "eu":
        return transform_inventory_eu(df)
    elif region == "us":
        return transform_inventory_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")
