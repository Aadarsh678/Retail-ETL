# from pyspark.sql import SparkSession
# from pyspark.sql.functions import (
#     col,
#     lit,
#     to_timestamp,
#     coalesce,
#     current_timestamp,
#     regexp_replace,
#     trim,
# )
# from pyspark.sql import DataFrame


# def safe_to_timestamp_multi_formats(date_col):
#     clean_col = trim(
#         regexp_replace(
#             regexp_replace(
#                 date_col, "(?i)(st|nd|rd|th)", ""
#             ),  # Remove ordinal suffixes
#             " +",
#             " ",  # Normalize extra spaces
#         )
#     )

#     ts1 = to_timestamp(clean_col, "yyyy-MM-dd HH:mm:ss")
#     ts2 = to_timestamp(clean_col, "yyyy-MM-dd")
#     ts3 = to_timestamp(clean_col, "dd-MM-yyyy")
#     ts4 = to_timestamp(clean_col, "MM/dd/yyyy")
#     ts5 = to_timestamp(clean_col, "dd/MM/yyyy")
#     ts6 = to_timestamp(clean_col, "yyyy/MM/dd")
#     ts7 = to_timestamp(clean_col, "MMMM d yyyy")  # Handles "March 5 2024"
#     ts8 = to_timestamp(clean_col, "MMM d yyyy")  # Handles "Mar 5 2024"

#     return coalesce(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8)


# # --- ASIA ---
# def transform_orders_asia(df):
#     return df.select(
#         col("order_id"),
#         col("order_no").alias("order_reference"),
#         col("cust_id").alias("customer_id"),
#         col("status").alias("order_status"),
#         safe_to_timestamp_multi_formats(col("order_time")).alias("order_timestamp"),
#         col("subtotal_jpy").alias("subtotal_usd"),
#         col("tax_jpy").alias("tax_amount_jpy"),
#         col("shipping_jpy").alias("shipping_amount_jpy"),
#         col("discount_jpy").alias("discount_amount_jpy"),
#         col("total_jpy").alias("total_amount_jpy"),
#         col("billing_addr_id").alias("billing_address_id"),
#         col("shipping_addr_id").alias("shipping_address_id"),
#         col("campaign_id"),
#         col("discount_id"),
#         safe_to_timestamp_multi_formats(col("processed_date")).alias("created_at"),
#         col("_region"),
#         col("_source"),
#     )


# # --- EU ---
# def transform_orders_eu(df):
#     return df.select(
#         col("order_id"),
#         col("order_reference"),
#         col("customer_id"),
#         col("order_status"),
#         safe_to_timestamp_multi_formats(col("order_timestamp")).alias("order_timestamp"),
#         col("subtotal_eur").alias("subtotal_eur"),
#         col("vat_amount_eur").alias("tax_amount_eur"),
#         col("shipping_eur").alias("shipping_amount_eur"),
#         col("discount_eur").alias("discount_amount_eur"),
#         col("total_eur").alias("total_amount_eur"),
#         col("billing_address_id"),
#         col("shipping_address_id"),
#         col("campaign_id"),
#         col("discount_id"),
#         col("invoice_required"),
#         safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
#         safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
#         col("_region"),
#         col("_source"),
#     )


# # --- US ---
# def transform_orders_us(df):
#     return df.select(
#         col("order_id"),
#         col("order_number").alias("order_reference"),
#         col("customer_id"),
#         col("order_status"),
#         safe_to_timestamp_multi_formats(col("order_date")).alias("order_timestamp"),
#         col("subtotal_usd"),
#         col("tax_amount_usd"),
#         col("shipping_amount_usd"),
#         col("discount_amount_usd"),
#         col("total_amount_usd"),
#         col("billing_address_id"),
#         col("shipping_address_id"),
#         col("campaign_id"),
#         col("discount_id"),
#         safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
#         safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
#         col("_region"),
#         col("_source"),
#     )


# # regions = ["asia", "eu", "us"]
# # load_date = "2025-06-06"
# # base_raw_path = "/opt/airflow/data/raw/"

# # for region in regions:
# #     input_path = f"{base_raw_path}region={region}/table=orders/load_date={load_date}/"
# #     print(f"\n=== Reading Orders Data for Region: {region.upper()} ===")
# #     df_raw = spark.read.parquet(input_path)

# #     df_transformed = (
# #         transform_orders_asia(df_raw) if region == "asia" else
# #         transform_orders_eu(df_raw) if region == "eu" else
# #         transform_orders_us(df_raw)
# #     )

# #     print(f"\n--- Transformed Orders for {region.upper()} ---")
# #     df_transformed.show(truncate=False, vertical=True)

# # print("Orders normalization completed.")


# def transform_order(df: DataFrame, region: str) -> DataFrame:
#     if region == "asia":
#         return transform_orders_asia(df)
#     elif region == "eu":
#         return transform_orders_eu(df)
#     elif region == "us":
#         return transform_orders_us(df)
#     else:
#         raise ValueError(f"Unsupported region: {region}")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    to_timestamp,
    coalesce,
    current_timestamp,
    regexp_replace,
    trim,
)
from pyspark.sql import DataFrame


def safe_to_timestamp_multi_formats(date_col):
    clean_col = trim(
        regexp_replace(
            regexp_replace(
                date_col, "(?i)(st|nd|rd|th)", ""
            ),
            " +",
            " ",
        )
    )

    ts1 = to_timestamp(clean_col, "yyyy-MM-dd HH:mm:ss")
    ts2 = to_timestamp(clean_col, "yyyy-MM-dd")
    ts3 = to_timestamp(clean_col, "dd-MM-yyyy")
    ts4 = to_timestamp(clean_col, "MM/dd/yyyy")
    ts5 = to_timestamp(clean_col, "dd/MM/yyyy")
    ts6 = to_timestamp(clean_col, "yyyy/MM/dd")
    ts7 = to_timestamp(clean_col, "MMMM d yyyy")
    ts8 = to_timestamp(clean_col, "MMM d yyyy")

    return coalesce(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8)


# --- ASIA ---
def transform_orders_asia(df: DataFrame, exchange_rates: dict) -> DataFrame:
    rate = exchange_rates.get("JPY/USD", 0.0068)

    return df.select(
        col("order_id"),
        col("order_no").alias("order_reference"),
        col("cust_id").alias("customer_id"),
        col("status").alias("order_status"),
        safe_to_timestamp_multi_formats(col("order_time")).alias("order_timestamp"),
        (col("subtotal_jpy") * lit(rate)).alias("subtotal_usd"),
        (col("tax_jpy") * lit(rate)).alias("tax_amount_usd"),
        (col("shipping_jpy") * lit(rate)).alias("shipping_amount_usd"),
        (col("discount_jpy") * lit(rate)).alias("discount_amount_usd"),
        (col("total_jpy") * lit(rate)).alias("total_amount_usd"),
        col("billing_addr_id").alias("billing_address_id"),
        col("shipping_addr_id").alias("shipping_address_id"),
        col("campaign_id"),
        col("discount_id"),
        safe_to_timestamp_multi_formats(col("processed_date")).alias("created_at"),
        col("_region"),
        col("_source"),
    )


# --- EU ---
def transform_orders_eu(df: DataFrame, exchange_rates: dict) -> DataFrame:
    rate = exchange_rates.get("EUR/USD", 1.08)

    return df.select(
        col("order_id"),
        col("order_reference"),
        col("customer_id"),
        col("order_status"),
        safe_to_timestamp_multi_formats(col("order_timestamp")).alias("order_timestamp"),
        (col("subtotal_eur") * lit(rate)).alias("subtotal_usd"),
        (col("vat_amount_eur") * lit(rate)).alias("tax_amount_usd"),
        (col("shipping_eur") * lit(rate)).alias("shipping_amount_usd"),
        (col("discount_eur") * lit(rate)).alias("discount_amount_usd"),
        (col("total_eur") * lit(rate)).alias("total_amount_usd"),
        col("billing_address_id"),
        col("shipping_address_id"),
        col("campaign_id"),
        col("discount_id"),
        col("invoice_required"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


# --- US ---
def transform_orders_us(df: DataFrame) -> DataFrame:
    return df.select(
        col("order_id"),
        col("order_number").alias("order_reference"),
        col("customer_id"),
        col("order_status"),
        safe_to_timestamp_multi_formats(col("order_date")).alias("order_timestamp"),
        col("subtotal_usd"),
        col("tax_amount_usd"),
        col("shipping_amount_usd"),
        col("discount_amount_usd"),
        col("total_amount_usd"),
        col("billing_address_id"),
        col("shipping_address_id"),
        col("campaign_id"),
        col("discount_id"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


def transform_order(df: DataFrame, region: str, exchange_rates: dict) -> DataFrame:
    if region == "asia":
        return transform_orders_asia(df, exchange_rates)
    elif region == "eu":
        return transform_orders_eu(df, exchange_rates)
    elif region == "us":
        return transform_orders_us(df, exchange_rates)
    else:
        raise ValueError(f"Unsupported region: {region}")
