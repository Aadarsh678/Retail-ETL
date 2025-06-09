from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, coalesce, to_timestamp, lit,regexp_replace,trim
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

def normalize_is_abandoned(flag_col):
    return when(col(flag_col).cast("string").rlike("(?i)^1|true|yes|y$"), lit(True)) \
           .when(col(flag_col).cast("string").rlike("(?i)^0|false|no|n$"), lit(False)) \
           .otherwise(None)

# --- ASIA ---
def transform_cart_asia(df):
    return df.select(
        col("cart_id"),
        col("cust_id").alias("customer_id"),
        col("session_id"),
        safe_to_timestamp_multi_formats(col("created")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated")).alias("updated_at"),
        safe_to_timestamp_multi_formats(col("abandoned")).alias("abandoned_at"),
        col("_region"),
        col("_source")
    )

# --- EU ---
def transform_cart_eu(df):
    return df.select(
        col("cart_id"),
        col("customer_id"),
        col("session_id"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        safe_to_timestamp_multi_formats(col("abandoned_at")).alias("abandoned_at"),
        col("is_abandoned"),
        lit("eu").alias("_region"),
        col("_source")
    )

# --- US ---
def transform_cart_us(df):
    return df.select(
        col("cart_id"),
        col("customer_id"),
        col("session_id"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        safe_to_timestamp_multi_formats(col("abandoned_at")).alias("abandoned_at"),
        col("is_abandoned").cast("boolean"),
        col("_region"),
        col("_source")
    )
# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=shopping_carts/load_date={load_date}/"
#     print(f"\n=== Reading Shopping Cart Data for Region: {region.upper()} ===")
#     df_raw = spark.read.parquet(input_path)

#     df_transformed = (
#         transform_cart_asia(df_raw) if region == "asia" else
#         transform_cart_eu(df_raw) if region == "eu" else
#         transform_cart_us(df_raw)
#     )

#     print(f"\n--- Transformed Shopping Cart Data for {region.upper()} ---")
#     df_transformed.show(truncate=False, vertical=True)

# print("Shopping cart normalization with regional differences complete.")

def transform_shopping_cart(df: DataFrame, region: str) -> DataFrame:
    if region == "asia":
        return transform_cart_asia(df)
    elif region == "eu":
        return transform_cart_eu(df)
    elif region == "us":
        return transform_cart_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")
