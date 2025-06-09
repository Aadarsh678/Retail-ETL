from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, to_timestamp,lower,regexp_replace,trim
)
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

def normalize_active(flag_col):
    return when(lower(col(flag_col)).isin('y', 'yes'), True) \
           .when(lower(col(flag_col)).isin('n', 'no'), False) \
           .otherwise(None)

# ASIA
def transform_discounts_asia(df):
    return df.select(
        col("discount_id"),
        col("code").alias("discount_code"),
        col("name").alias("discount_name"),
        col("type").alias("discount_type"),
        col("value").cast("double").alias("discount_value"),
        col("min_order_jpy").alias("minimum_order_amount"),
        col("max_discount_jpy").alias("maximum_order_amount"),
        col("usage_limit"),
        col("usage_count"),
        safe_to_timestamp_multi_formats(col("start_dt")).alias("start_date"),
        safe_to_timestamp_multi_formats(col("end_dt")).alias("end_date"),
        normalize_active("active").alias("is_active"),
        safe_to_timestamp_multi_formats(col("created")).alias("created_at"),
        col("_region"),
        col("_source")
    )

# EU
def transform_discounts_eu(df):
    return df.select(
        col("discount_id"),
        col("discount_code"),
        col("discount_name"),
        col("discount_type"),
        col("discount_value").cast("double"),
        col("minimum_order_amount_eur").alias("minimum_order_amount"),
        col("maximum_discount_amount_eur").alias("maximum_order_amount"),
        col("usage_limit"),
        col("usage_count"),
        safe_to_timestamp_multi_formats(col("start_date")).alias("start_date"),
        safe_to_timestamp_multi_formats(col("end_date")).alias("end_date"),
        col("is_active").alias("is_active"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        col("_region"),
        col("_source")
    )

# US
def transform_discounts_us(df):
    return df.select(
        col("discount_id"),
        col("discount_code"),
        col("discount_name"),
        col("discount_type"),
        col("discount_value").cast("double"),
        col("minimum_order_amount_usd").cast("double").alias("minimum_order_amount"),
        col("maximum_discount_amount_usd").cast("double").alias("maximum_discount_amount"),
        col("usage_limit"),
        col("usage_count"),
        safe_to_timestamp_multi_formats(col("start_date")).alias("start_date"),
        safe_to_timestamp_multi_formats(col("end_date")).alias("end_date"),
        col("is_active").cast("boolean"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        col("_region"),
        col("_source")
    )

# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=discounts/load_date={load_date}/"
#     print(f"\n=== Reading data for region: {region} ===")
#     df_raw = spark.read.parquet(input_path)

#     print(f"\n--- Raw Data for {region.upper()} ---")
#     df_raw.show(truncate=False)

#     if region == "asia":
#         df_transformed = transform_discounts_asia(df_raw)
#     elif region == "eu":
#         df_transformed = transform_discounts_eu(df_raw)
#     elif region == "us":
#         df_transformed = transform_discounts_us(df_raw)
#     else:
#         raise ValueError(f"Unsupported region: {region}")

#     print(f"\n--- Transformed Data for {region.upper()} ---")
#     df_transformed.show(truncate=False,vertical=True)

# print("Discount transformations completed.")

def transform_discounts(df: DataFrame, region: str) -> DataFrame:
    if region == "asia":
        return transform_discounts_asia(df)
    elif region == "eu":
        return transform_discounts_eu(df)
    elif region == "us":
        return transform_discounts_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")