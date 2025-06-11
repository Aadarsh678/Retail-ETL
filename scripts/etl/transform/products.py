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


from pyspark.sql.functions import col, lit, round

def normalize_weight(region, weight_col, precision=2):
    return {
        "asia": round(col(weight_col) / lit(1000), precision),
        "eu": round(col(weight_col), precision),
        "us": round(col(weight_col) * lit(0.453592), precision),
    }[region].alias("weight_kg")



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


def normalize_name(name_col):
    return initcap(trim(name_col))


def parse_dimensions_inches(dim_col):
    length = regexp_extract(dim_col, r"([0-9.]+)", 1).cast("double")
    width = regexp_extract(dim_col, r"[xX]\s*([0-9.]+)", 1).cast("double")
    height = regexp_extract(dim_col, r"[xX]\s*[0-9.]+\s*[xX]\s*([0-9.]+)", 1).cast(
        "double"
    )
    return length.alias("length_cm"), width.alias("width_cm"), height.alias("height_cm")


# def transform_asia_products(df):
#     length = regexp_extract(col("size_info"), r"([0-9.]+)mm", 1).cast("double") / 10
#     width = regexp_extract(col("size_info"), r"x\s*([0-9.]+)mm", 1).cast("double") / 10
#     height = (
#         regexp_extract(col("size_info"), r"x\s*[0-9.]+mm\s*x\s*([0-9.]+)mm", 1).cast(
#             "double"
#         )
#         / 10
#     )

#     return df.select(
#         col("product_id"),
#         trim(col("item_code")).alias("product_sku"),
#         normalize_name(trim(col("name"))).alias("product_name"),
#         col("description").alias("product_description"),
#         col("cat_id").alias("category_id"),
#         col("price_jpy"),
#         col("cost_jpy"),
#         normalize_weight("asia", "weight_g"),
#         length.alias("length_cm"),
#         width.alias("width_cm"),
#         height.alias("height_cm"),
#         when(lower(col("active_status")) == "active", lit(True))
#         .otherwise(lit(False))
#         .alias("is_active"),
#         safe_to_timestamp_multi_formats(col("create_date")).alias("created_at"),
#         col("_region"),
#         col("_source"),
#     )


# def transform_eu_products(df):
#     return df.select(
#         col("product_id"),
#         col("product_sku"),
#         normalize_name(col("product_name")).alias("product_name"),
#         col("product_description"),
#         col("category_id"),
#         col("base_price_eur").alias("base_eur"),
#         col("cost_price_eur").alias("cost_eur"),
#         col("vat_rate"),
#         normalize_weight("eu", "weight_kg"),
#         col("length_cm"),
#         col("width_cm"),
#         col("height_cm"),
#         col("energy_rating"),
#         (col("is_active") == 1).alias("is_active"),
#         safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
#         safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
#         col("_region"),
#         col("_source"),
#     )


# def transform_us_products(df):
#     length_cm, width_cm, height_cm = parse_dimensions_inches(col("dimensions_inches"))

#     return df.select(
#         col("product_id"),
#         col("product_code").alias("product_sku"),
#         normalize_name(col("product_name")).alias("product_name"),
#         col("product_description"),
#         col("category_id"),
#         col("base_price_usd").alias("base_usd"),
#         col("cost_price_usd").alias("cost_usd"),
#         normalize_weight("us", "weight_lbs"),
#         length_cm,
#         width_cm,
#         height_cm,
#         col("is_active"),
#         safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
#         safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
#         col("_region"),
#         col("_source"),
#     )


# # regions = ["asia", "eu", "us"]
# # load_date = "2025-06-06"
# # base_raw_path = "/opt/airflow/data/raw/"

# # for region in regions:
# #     input_path = f"{base_raw_path}region={region}/table=products/load_date={load_date}/"
# #     print(f"\n=== Reading data for region: {region} ===")
# #     df_raw = spark.read.parquet(input_path)

# #     print(f"\n--- Raw Data for {region.upper()} ---")
# #     df_raw.show(truncate=False)

# #     if region == "asia":
# #         df_transformed = transform_asia_products(df_raw)
# #     elif region == "eu":
# #         df_transformed = transform_eu_products(df_raw)
# #     elif region == "us":
# #         df_transformed = transform_us_products(df_raw)
# #     else:
# #         raise ValueError(f"Unsupported region: {region}")

# #     print(f"\n--- Transformed Data for {region.upper()} ---")
# #     df_transformed.show(truncate=False,vertical=True)


# # print("Regional transformations with GDPR compliance completed.")


# def transform_products(df: DataFrame, region: str) -> DataFrame:
#     if region == "asia":
#         return transform_asia_products(df)
#     elif region == "eu":
#         return transform_eu_products(df)
#     elif region == "us":
#         return transform_us_products(df)
#     else:
#         raise ValueError(f"Unsupported region: {region}")

def transform_asia_products(df: DataFrame, exchange_rates: dict) -> DataFrame:
    rate = exchange_rates.get("JPY/USD", 0.0068)

    length = regexp_extract(col("size_info"), r"([0-9.]+)mm", 1).cast("double") / 10
    width = regexp_extract(col("size_info"), r"x\s*([0-9.]+)mm", 1).cast("double") / 10
    height = (
        regexp_extract(col("size_info"), r"x\s*[0-9.]+mm\s*x\s*([0-9.]+)mm", 1)
        .cast("double") / 10
    )

    return df.select(
        col("product_id"),
        trim(col("item_code")).alias("product_sku"),
        normalize_name(trim(col("name"))).alias("product_name"),
        col("description").alias("product_description"),
        col("cat_id").alias("category_id"),
        (col("price_jpy") * lit(rate)).alias("base_usd"),
        (col("cost_jpy") * lit(rate)).alias("cost_usd"),
        normalize_weight("asia", "weight_g"),
        length.alias("length_cm"),
        width.alias("width_cm"),
        height.alias("height_cm"),
        when(lower(col("active_status")) == "active", lit(True)).otherwise(lit(False)).alias("is_active"),
        safe_to_timestamp_multi_formats(col("create_date")).alias("created_at"),
        col("_region"),
        col("_source"),
    )


def transform_eu_products(df: DataFrame, exchange_rates: dict) -> DataFrame:
    rate = exchange_rates.get("EUR/USD", 1.08)

    return df.select(
        col("product_id"),
        col("product_sku"),
        normalize_name(col("product_name")).alias("product_name"),
        col("product_description"),
        col("category_id"),
        (col("base_price_eur") * lit(rate)).alias("base_usd"),
        (col("cost_price_eur") * lit(rate)).alias("cost_usd"),
        col("vat_rate"),
        normalize_weight("eu", "weight_kg"),
        col("length_cm"),
        col("width_cm"),
        col("height_cm"),
        col("energy_rating"),
        (col("is_active") == 1).alias("is_active"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


def transform_us_products(df: DataFrame, exchange_rates: dict) -> DataFrame:
    length_cm, width_cm, height_cm = parse_dimensions_inches(col("dimensions_inches"))

    return df.select(
        col("product_id"),
        col("product_code").alias("product_sku"),
        normalize_name(col("product_name")).alias("product_name"),
        col("product_description"),
        col("category_id"),
        col("base_price_usd").alias("base_usd"),
        col("cost_price_usd").alias("cost_usd"),
        normalize_weight("us", "weight_lbs"),
        length_cm,
        width_cm,
        height_cm,
        col("is_active"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


def transform_products(df: DataFrame, region: str, exchange_rates: dict) -> DataFrame:
    if region == "asia":
        return transform_asia_products(df, exchange_rates)
    elif region == "eu":
        return transform_eu_products(df, exchange_rates)
    elif region == "us":
        return transform_us_products(df, exchange_rates)
    else:
        raise ValueError(f"Unsupported region: {region}")
