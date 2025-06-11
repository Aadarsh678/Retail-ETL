from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, coalesce, lit, regexp_replace, trim
from pyspark.sql import DataFrame


def safe_to_timestamp_multi_formats(date_col):
    clean_col = trim(
        regexp_replace(regexp_replace(date_col, "(?i)(st|nd|rd|th)", ""), " +", " ")
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
def transform_cart_items_asia(df,exchange_rates: dict):
    rate = exchange_rates.get("JPY/USD", 0.0068)
    return df.select(
        col("cart_item_id"),
        col("cart_id"),
        col("variant_id"),
        col("qty").alias("quantity"),
        (col("unit_price_jpy") * lit(rate)).alias("unit_price_usd"),
        safe_to_timestamp_multi_formats(col("added")).alias("added_at"),
        safe_to_timestamp_multi_formats(col("updated")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


# --- EU ---
def transform_cart_items_eu(df,exchange_rates: dict):
    rate = exchange_rates.get("EUR/USD", 1.08)
    return df.select(
        col("cart_item_id"),
        col("cart_id"),
        col("variant_id"),
        col("quantity"),
        (col("unit_price_eur") * lit(rate)).alias("unit_price_usd"),
        safe_to_timestamp_multi_formats(col("added_at")).alias("added_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


# --- US ---
def transform_cart_items_us(df,exchange_rates: dict):
    return df.select(
        col("cart_item_id"),
        col("cart_id"),
        col("variant_id"),
        col("quantity"),
        col("unit_price_usd"),
        safe_to_timestamp_multi_formats(col("added_at")).alias("added_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"

# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=cart_items/load_date={load_date}/"
#     print(f"\n=== Reading Cart Items Data for Region: {region.upper()} ===")
#     df_raw = spark.read.parquet(input_path)

#     df_transformed = (
#         transform_cart_items_asia(df_raw) if region == "asia" else
#         transform_cart_items_eu(df_raw) if region == "eu" else
#         transform_cart_items_us(df_raw)
#     )

#     print(f"\n--- Transformed Cart Items for {region.upper()} ---")
#     df_transformed.show(truncate=False, vertical=True)

# print("Cart items normalization with regional pricing completed.")


def transform_cart_items(df: DataFrame, region: str, exchange_rates: dict) -> DataFrame:
    if region == "asia":
        return transform_cart_items_asia(df,exchange_rates)
    elif region == "eu":
        return transform_cart_items_eu(df,exchange_rates)
    elif region == "us":
        return transform_cart_items_us(df,exchange_rates)
    else:
        raise ValueError(f"Unsupported region: {region}")
