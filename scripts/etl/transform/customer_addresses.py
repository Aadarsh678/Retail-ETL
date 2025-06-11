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

from pyspark.sql.functions import col, lit, when, coalesce, to_timestamp


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


def normalize_active(flag_col):
    return (
        when(lower(col(flag_col)).isin("y", "yes"), True)
        .when(lower(col(flag_col)).isin("n", "no"), False)
        .otherwise(None)
    )


def normalize_name(name_col):
    return initcap(trim(name_col))


# ASIA Region
def transform_customer_addresses_asia(df):
    return df.select(
        col("addr_id").alias("address_id"),
        col("cust_id").alias("customer_id"),
        normalize_name(col("addr_type")).alias("address_type"),
        col("street").alias("street_address"),
        normalize_name(col("city")).alias("city"),
        normalize_name(col("prefecture")).alias("region"),  # Normalize region
        col("postal").alias("postal_code"),
        normalize_name(col("country")).alias("country"),
        normalize_active("is_default").alias("is_default"),
        safe_to_timestamp_multi_formats(col("created")).alias("created_at"),
        col("_region"),
        col("_source"),
    )


# EU Region
def transform_customer_addresses_eu(df):
    return df.select(
        col("address_id"),
        col("customer_id"),
        col("address_type"),
        col("street_address"),
        col("city"),
        col("region"),
        col("postal_code"),
        col("country_code").alias("country"),
        col("is_default").cast("boolean"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        col("_region"),
        col("_source"),
    )


# US Region
def transform_customer_addresses_us(df):
    return df.select(
        col("address_id"),
        col("customer_id"),
        col("address_type"),
        col("street_address"),
        col("city"),
        col("state").alias("region"),
        col("zip_code").alias("postal_code"),
        col("country"),
        col("is_default").cast("boolean"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        col("_region"),
        col("_source"),
    )


# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=customer_addresses/load_date={load_date}/"
#     print(f"\n=== Reading data for region: {region} ===")
#     df_raw = spark.read.parquet(input_path)

#     print(f"\n--- Raw Data for {region.upper()} ---")
#     df_raw.show(truncate=False)

#     if region == "asia":
#         df_transformed = transform_customer_addresses_asia(df_raw)
#     elif region == "eu":
#         df_transformed = transform_customer_addresses_eu(df_raw)
#     elif region == "us":
#         df_transformed = transform_customer_addresses_us(df_raw)
#     else:
#         raise ValueError(f"Unsupported region: {region}")

#     print(f"\n--- Transformed Data for {region.upper()} ---")
#     df_transformed.show(truncate=False,vertical = True)


# print("Regional transformations with GDPR compliance completed.")
def transform_customer_addressess(df: DataFrame, region: str,exchange_rates: dict) -> DataFrame:
    if region == "asia":
        return transform_customer_addresses_asia(df)
    elif region == "eu":
        return transform_customer_addresses_eu(df)
    elif region == "us":
        return transform_customer_addresses_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")
