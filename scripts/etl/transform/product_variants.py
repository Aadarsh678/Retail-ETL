# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col,
#     trim,
#     initcap,
#     lower,
#     when,
#     to_timestamp,
#     coalesce,
#     lit,
#     udf,
#     regexp_replace,
# )
# from pyspark.sql.types import StringType
# from pyspark.sql import DataFrame


# from pyspark.sql.functions import split, regexp_extract, expr


# def normalize_is_active(region, col_val):
#     if region == "asia":
#         return (
#             when(lower(col_val) == "active", True)
#             .when(lower(col_val).isin("inactive", "discontinued"), False)
#             .otherwise(None)
#             .alias("is_active")
#         )
#     elif region == "eu":
#         return (col(col_val) == 1).alias("is_active")
#     elif region == "us":
#         return col(col_val).alias("is_active")
#     else:
#         return lit(None).alias("is_active")


# def normalize_created_at(region, col_val):
#     if region == "asia":
#         return to_timestamp(col_val).alias("created_at")
#     else:
#         return col(col_val).alias("created_at")


# def transform_product_variants_asia(df):
#     return df.select(
#         col("variant_id"),
#         col("product_id"),
#         col("sku").alias("product_sku"),
#         col("variant_name"),
#         col("variant_type"),
#         col("variant_value"),
#         col("price_diff_jpy").alias("price_diff_jpy"),
#         normalize_is_active("asia", "status"),
#         normalize_created_at("asia", "created"),
#         col("_region"),
#         col("_source"),
#     )


# def transform_product_variants_eu(df):
#     return df.select(
#         col("variant_id"),
#         col("product_id"),
#         col("sku").alias("product_sku"),
#         col("variant_name"),
#         col("variant_type"),
#         col("variant_value"),
#         col("price_adjustment_eur").alias("price_diff_eur"),
#         col("is_active").alias("is_active_eu"),
#         normalize_is_active("eu", "is_active"),
#         normalize_created_at("eu", "created_at"),
#         col("_region"),
#         col("_source"),
#     )


# def transform_product_variants_us(df):
#     return df.select(
#         col("variant_id"),
#         col("product_id"),
#         col("sku").alias("product_sku"),
#         col("variant_name"),
#         col("variant_type"),
#         col("variant_value"),
#         col("price_adjustment_usd").alias("price_diff_usd"),
#         col("is_active").alias("is_active_us"),
#         normalize_is_active("us", "is_active"),
#         normalize_created_at("us", "created_at"),
#         col("_region"),
#         col("_source"),
#     )


# # regions = ["asia", "eu", "us"]
# # load_date = "2025-06-06"
# # base_raw_path = "/opt/airflow/data/raw/"

# # for region in regions:
# #     input_path = f"{base_raw_path}region={region}/table=product_variants/load_date={load_date}/"
# #     print(f"\n=== Reading data for region: {region} ===")
# #     df_raw = spark.read.parquet(input_path)

# #     print(f"\n--- Raw Data for {region.upper()} ---")
# #     df_raw.show(truncate=False)

# #     if region == "asia":
# #         df_transformed = transform_product_variants_asia(df_raw)
# #     elif region == "eu":
# #         df_transformed = transform_product_variants_eu(df_raw)
# #     elif region == "us":
# #         df_transformed = transform_product_variants_us(df_raw)
# #     else:
# #         raise ValueError(f"Unsupported region: {region}")

# #     print(f"\n--- Transformed Data for {region.upper()} ---")
# #     df_transformed.show(truncate=False,vertical=True)


# # print("Regional transformations with GDPR compliance completed.")
# def transform_product_variants(df: DataFrame, region: str) -> DataFrame:
#     if region == "asia":
#         return transform_product_variants_asia(df)
#     elif region == "eu":
#         return transform_product_variants_eu(df)
#     elif region == "us":
#         return transform_product_variants_us(df)
#     else:
#         raise ValueError(f"Unsupported region: {region}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    lower,
    when,
    to_timestamp,
    lit,
    regexp_replace,
    coalesce,
)
from pyspark.sql import DataFrame


def normalize_is_active(region, col_val):
    if region == "asia":
        return (
            when(lower(col(col_val)) == "active", True)
            .when(lower(col(col_val)).isin("inactive", "discontinued"), False)
            .otherwise(None)
            .alias("is_active")
        )
    elif region == "eu":
        return (col(col_val) == 1).alias("is_active")
    elif region == "us":
        return col(col_val).alias("is_active")
    else:
        return lit(None).alias("is_active")


def normalize_created_at(region, col_val):
    return to_timestamp(col(col_val)).alias("created_at") if region == "asia" else col(col_val).alias("created_at")


# --- ASIA ---
def transform_product_variants_asia(df: DataFrame, exchange_rates: dict) -> DataFrame:
    rate = exchange_rates.get("JPY/USD", 0.0068)
    return df.select(
        col("variant_id"),
        col("product_id"),
        col("sku").alias("product_sku"),
        col("variant_name"),
        col("variant_type"),
        col("variant_value"),
        (col("price_diff_jpy") * lit(rate)).alias("price_diff_usd"),
        normalize_is_active("asia", "status"),
        normalize_created_at("asia", "created"),
        col("_region"),
        col("_source"),
    )


# --- EU ---
def transform_product_variants_eu(df: DataFrame, exchange_rates: dict) -> DataFrame:
    rate = exchange_rates.get("EUR/USD", 1.08)
    return df.select(
        col("variant_id"),
        col("product_id"),
        col("sku").alias("product_sku"),
        col("variant_name"),
        col("variant_type"),
        col("variant_value"),
        (col("price_adjustment_eur") * lit(rate)).alias("price_diff_usd"),
        col("is_active").alias("is_active_eu"),
        normalize_is_active("eu", "is_active"),
        normalize_created_at("eu", "created_at"),
        col("_region"),
        col("_source"),
    )


# --- US ---
def transform_product_variants_us(df: DataFrame, exchange_rates: dict) -> DataFrame:
    return df.select(
        col("variant_id"),
        col("product_id"),
        col("sku").alias("product_sku"),
        col("variant_name"),
        col("variant_type"),
        col("variant_value"),
        col("price_adjustment_usd").alias("price_diff_usd"),
        col("is_active").alias("is_active_us"),
        normalize_is_active("us", "is_active"),
        normalize_created_at("us", "created_at"),
        col("_region"),
        col("_source"),
    )


def transform_product_variants(df: DataFrame, region: str, exchange_rates: dict) -> DataFrame:
    if region == "asia":
        return transform_product_variants_asia(df, exchange_rates)
    elif region == "eu":
        return transform_product_variants_eu(df, exchange_rates)
    elif region == "us":
        return transform_product_variants_us(df, exchange_rates)
    else:
        raise ValueError(f"Unsupported region: {region}")
