from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, initcap, lower, when,regexp_replace, to_timestamp, coalesce, lit, udf
)
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame


from pyspark.sql.functions import split, regexp_extract, expr

from pyspark.sql.functions import (
    col, lit, when, coalesce, to_timestamp
)

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
# --- ASIA ---
def transform_payments_asia(df):
    return df.select(
        col("payment_id"),
        col("order_id"),
        col("method").alias("payment_method"),
        col("status").alias("payment_status"),
        col("amount_jpy").alias("payment_amount_jpy"),
        safe_to_timestamp_multi_formats(col("payment_time")).alias("payment_timestamp"),
        col("transaction_ref").alias("transaction_id"),
        col("gateway_resp").alias("gateway_response"),
        safe_to_timestamp_multi_formats(col("created")).alias("created_at"),
        col("_region"),
        col("_source")
    )

# --- EU ---
def transform_payments_eu(df):
    return df.select(
        col("payment_id"),
        col("order_id"),
        col("payment_method"),
        col("payment_status"),
        col("payment_amount_eur").alias("payment_amount_eur"),
        col("payment_timestamp"),
        col("transaction_id"),
        col("gateway_response"),
        col("psd2_compliant"),
        col("created_at"),
        col("_region"),
        col("_source")
    )

# --- US ---
def transform_payments_us(df):
    return df.select(
        col("payment_id"),
        col("order_id"),
        col("payment_method"),
        col("payment_status"),
        col("payment_amount_usd"),
        col("payment_date").alias("payment_timestamp"),
        col("transaction_id"),
        col("gateway_response"),
        col("created_at"),
        col("_region"),
        col("_source")
    )

# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=payments/load_date={load_date}/"
#     print(f"\n=== Reading Payments for Region: {region.upper()} ===")
#     df_raw = spark.read.parquet(input_path)

#     df_transformed = (
#         transform_payments_asia(df_raw) if region == "asia" else
#         transform_payments_eu(df_raw) if region == "eu" else
#         transform_payments_us(df_raw)
#     )

#     print(f"\n--- Transformed Payments for {region.upper()} ---")
#     df_transformed.show(truncate=False, vertical=True)

# print("Payments normalization completed.")
def transform_payments(df: DataFrame, region: str) -> DataFrame:
    if region == "asia":
        return transform_payments_asia(df)
    elif region == "eu":
        return transform_payments_eu(df)
    elif region == "us":
        return transform_payments_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")
