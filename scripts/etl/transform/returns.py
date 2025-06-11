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
def transform_returns_asia(df,exchange_rates: dict):
    rate = exchange_rates.get("JPY/USD", 0.0068)
    return df.select(
        col("return_id"),
        col("order_id"),
        col("reason").alias("return_reason"),
        col("status").alias("return_status"),
        safe_to_timestamp_multi_formats(col("return_time")).alias("return_timestamp"),
        (col("refund_jpy") * lit(rate)).alias("refund_amount"),
        col("refund_time").alias("refund_timestamp"),
        safe_to_timestamp_multi_formats(col("created")).alias("created_at"),
        col("_region"),
        col("_source"),
    )


# --- EU ---
def transform_returns_eu(df,exchange_rates: dict):
    rate = exchange_rates.get("EUR/USD", 1.08)
    return df.select(
        col("return_id"),
        col("order_id"),
        col("return_reason"),
        col("return_status"),
        col("return_timestamp"),
        (col("refund_amount_eur")* lit(rate)).alias("refund_amount"),
        col("refund_timestamp"),
        col("created_at"),
        col("cooling_off_period_days"),
        col("_region"),
        col("_source"),
    )


# --- US ---
def transform_returns_us(df,exchange_rates: dict):
    return df.select(
        col("return_id"),
        col("order_id"),
        col("return_reason"),
        col("return_status"),
        col("return_date").alias("return_timestamp"),
        col("refund_amount_usd").alias("refund_amount"),
        col("refund_date").alias("refund_timestamp"),
        col("created_at"),
        col("_region"),
        col("_source"),
    )


# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=returns/load_date={load_date}/"
#     print(f"\n=== Reading Returns for Region: {region.upper()} ===")
#     df_raw = spark.read.parquet(input_path)

#     df_transformed = (
#         transform_returns_asia(df_raw) if region == "asia" else
#         transform_returns_eu(df_raw) if region == "eu" else
#         transform_returns_us(df_raw)
#     )

#     print(f"\n--- Transformed Returns for {region.upper()} ---")
#     df_transformed.show(truncate=False, vertical=True)

# print("Returns normalization completed.")


def transform_returns(df: DataFrame, region: str, exchange_rates: dict) -> DataFrame:
    if region == "asia":
        return transform_returns_asia(df,exchange_rates)
    elif region == "eu":
        return transform_returns_eu(df,exchange_rates)
    elif region == "us":
        return transform_returns_us(df,exchange_rates)
    else:
        raise ValueError(f"Unsupported region: {region}")
