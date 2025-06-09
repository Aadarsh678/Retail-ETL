from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, initcap, lower, when, to_timestamp, coalesce, lit, udf,regexp_replace
)
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame



# Helpers for normalization and masking
def normalize_active(flag_col):
    return when(lower(col(flag_col)).isin('y', 'yes'), True) \
           .when(lower(col(flag_col)).isin('n', 'no'), False) \
           .otherwise(None)

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
    ts9 = to_timestamp(clean_col,"2024.01.14 ")

    return coalesce(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8,ts9)



def normalize_name(name_col):
    return initcap(trim(name_col))


def transform_asia(df):
    return df.select(
        col("cat_id").alias("categories_id"),
        normalize_name(col("name")).alias("category_name"),
        col("parent_id").alias("parent_category_id"),
        col("path").alias("category_path"),
        normalize_active("active_flag").alias("is_active"),
        safe_to_timestamp_multi_formats(col("create_time")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("modify_time")).alias("updated_at"),
        col("_region"),
        col("_source")
    )


def transform_eu(df):
    return df.select(
        col("category_id"),
        normalize_name(col("category_name")).alias("category_name"),
        normalize_name(col("category_name_local")).alias("category_name_local"),
        col("parent_category_id"),
        col("category_path"),
        col("is_active"),
        col("gdpr_sensitive"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source")
    )

def transform_us(df):
    return df.select(
        col("category_id"),
        normalize_name(col("category_name")).alias("category_name"),
        col("parent_category_id"),
        col("category_path"),
        col("is_active"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source")
    )

# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=customers/load_date={load_date}/"
#     df_raw = spark.read.parquet(input_path)

#     if region == "asia":
#         df_transformed = transform_asia(df_raw)
#     elif region == "eu":
#         df_transformed = transform_eu(df_raw)
#     elif region == "us":
#         df_transformed = transform_us(df_raw)
#     else:
#         raise ValueError(f"Unsupported region: {region}")

#     df_transformed.show(truncate=False)
# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=categories/load_date={load_date}/"
#     print(f"\n=== Reading data for region: {region} ===")
#     df_raw = spark.read.parquet(input_path)

#     print(f"\n--- Raw Data for {region.upper()} ---")
#     df_raw.show(truncate=False)

#     if region == "asia":
#         df_transformed = transform_asia(df_raw)
#     elif region == "eu":
#         df_transformed = transform_eu(df_raw)
#     elif region == "us":
#         df_transformed = transform_us(df_raw)
#     else:
#         raise ValueError(f"Unsupported region: {region}")

#     print(f"\n--- Transformed Data for {region.upper()} ---")
#     df_transformed.show(truncate=False)


# print("Regional transformations with GDPR compliance completed.")

def transform_categories(df: DataFrame, region: str) -> DataFrame:
    if region == "asia":
        return transform_asia(df)
    elif region == "eu":
        return transform_eu(df)
    elif region == "us":
        return transform_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")