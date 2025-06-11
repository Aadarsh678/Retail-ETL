from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    initcap,
    lower,
    when,
    regexp_replace,
    to_timestamp,
    coalesce,
    lit,
    udf,
)
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame


from pyspark.sql.functions import split, regexp_extract, expr

from pyspark.sql.functions import col, lit, to_timestamp


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
def transform_wishlists_asia(df):
    return df.select(
        col("wishlist_id"),
        col("cust_id").alias("customer_id"),
        col("product_id"),
        safe_to_timestamp_multi_formats("added").alias("added_at"),
        col("_region"),
        col("_source"),
    )


# --- EU ---
def transform_wishlists_eu(df):
    return df.select(
        col("wishlist_id"),
        col("customer_id"),
        col("product_id"),
        col("added_at"),
        col("_region"),
        col("_source"),
    )


# --- US ---
def transform_wishlists_us(df):
    return df.select(
        col("wishlist_id"),
        col("customer_id"),
        col("product_id"),
        col("added_at"),
        col("_region"),
        col("_source"),
    )


# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=wishlists/load_date={load_date}/"
#     print(f"\n=== Reading Wishlists for Region: {region.upper()} ===")
#     df_raw = spark.read.parquet(input_path)

#     df_transformed = (
#         transform_wishlists_asia(df_raw) if region == "asia" else
#         transform_wishlists_eu(df_raw) if region == "eu" else
#         transform_wishlists_us(df_raw)
#     )

#     print(f"\n--- Transformed Wishlists for {region.upper()} ---")
#     df_transformed.show(truncate=False, vertical=True)

# print("Wishlist normalization completed.")


def transform_whishlist(df: DataFrame, region: str) -> DataFrame:
    if region == "asia":
        return transform_wishlists_asia(df)
    elif region == "eu":
        return transform_wishlists_eu(df)
    elif region == "us":
        return transform_wishlists_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")
