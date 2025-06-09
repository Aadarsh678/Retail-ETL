from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, initcap, lower, when, to_timestamp, coalesce, lit, udf,regexp_replace
)
from pyspark.sql.types import StringType
import hashlib
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
# --- ASIA ---
def transform_product_reviews_asia(df):
    return df.select(
        col("review_id"),
        col("product_id"),
        col("cust_id").alias("customer_id"),
        col("order_id"),
        col("rating"),
        col("title").alias("review_title"),
        col("review_text"),
        normalize_active("verified").alias("is_verified_purchase"),
        col("helpful_count").alias("helpful_votes"),
        safe_to_timestamp_multi_formats("created").alias("created_at"),
        col("_region"),
        col("_source")
    )

# --- EU ---
def transform_product_reviews_eu(df):
    return df.select(
        col("review_id"),
        col("product_id"),
        col("customer_id"),
        col("order_id"),
        col("rating"),
        col("review_title"),
        col("review_text"),
        col("is_verified_purchase"),
        col("helpful_votes"),
        col("moderation_status"),
        col("created_at"),
        col("_region"),
        col("_source")
    )

# --- US ---
def transform_product_reviews_us(df):
    return df.select(
        col("review_id"),
        col("product_id"),
        col("customer_id"),
        col("order_id"),
        col("rating"),
        col("review_title"),
        col("review_text"),
        col("is_verified_purchase"),
        col("helpful_votes"),
        col("created_at"),
        col("_region"),
        col("_source")
    )
# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=product_reviews/load_date={load_date}/"
#     print(f"\n=== Reading Product Reviews for Region: {region.upper()} ===")
#     df_raw = spark.read.parquet(input_path)

#     df_transformed = (
#         transform_product_reviews_asia(df_raw) if region == "asia" else
#         transform_product_reviews_eu(df_raw) if region == "eu" else
#         transform_product_reviews_us(df_raw)
#     )

#     print(f"\n--- Transformed Reviews for {region.upper()} ---")
#     df_transformed.show(truncate=False, vertical=True)

# print("Product reviews normalization completed.")

def transform_product_reviews(df: DataFrame, region: str) -> DataFrame:
    if region == "asia":
        return transform_product_reviews_asia(df)
    elif region == "eu":
        return transform_product_reviews_eu(df)
    elif region == "us":
        return transform_product_reviews_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")
