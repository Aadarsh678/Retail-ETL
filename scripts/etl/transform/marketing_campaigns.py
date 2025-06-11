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
from pyspark.sql import DataFrame


def safe_to_timestamp_multi_formats(date_col):
    return coalesce(
        to_timestamp(date_col, 'yyyy-MM-dd HH:mm:ss'),
        to_timestamp(date_col, 'yyyy-MM-dd'),
        to_timestamp(date_col, 'MM/dd/yyyy'),
        to_timestamp(date_col, 'dd-MM-yyyy'),
        to_timestamp(date_col, 'dd/MM/yyyy'),
        to_timestamp(date_col, 'yyyy/MM/dd')
    )
def normalize_active(flag_col):
    return (
        when(lower(col(flag_col)).isin("y", "yes", "active"), True)
        .when(lower(col(flag_col)).isin("n", "no", "completed", "scheduled"), False)
        .otherwise(None)
    )


# ASIA
def transform_marketing_campaigns_asia(df,exchange_rates: dict):
    rate = exchange_rates.get("JPY/USD", 0.0068)
    return df.select(
        col("campaign_id"),
        col("name").alias("campaign_name"),
        col("type").alias("campaign_type"),
        col("channel"),
        safe_to_timestamp_multi_formats(col("start_dt")).alias("start_date"),
        safe_to_timestamp_multi_formats(col("end_dt")).alias("end_date"),
        (col("budget_jpy") * lit(rate)).alias("budget_usd"),  # normalize to float
        col("target").alias("target_audience"),
        normalize_active("status").alias("campaign_status"),
        lit(None).cast("boolean").alias("gdpr_compliant"),  # missing in ASIA
        safe_to_timestamp_multi_formats(col("created")).alias("created_at"),
        lit("asia").alias("_region"),
        col("_source")
    )

# EU
def transform_marketing_campaigns_eu(df,exchange_rates: dict):
    rate = exchange_rates.get("EUR/USD", 1.08)
    return df.select(
        col("campaign_id"),
        col("campaign_name"),
        col("campaign_type"),
        col("channel"),
        safe_to_timestamp_multi_formats(col("start_date")).alias("start_date"),
        safe_to_timestamp_multi_formats(col("end_date")).alias("end_date"),
        (col("budget_eur") *lit(rate)).alias("budget_usd"),
        col("target_audience"),
        normalize_active("campaign_status").alias("campaign_status"),
        col("gdpr_compliant"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        lit("eu").alias("_region"),
        col("_source")
    )

# US
def transform_marketing_campaigns_us(df,exchange_rates: dict):
    return df.select(
        col("campaign_id"),
        col("campaign_name"),
        col("campaign_type"),
        col("channel"),
        col("start_date"),
        col("end_date"),
        col("budget_usd").cast("double").alias("budget_usd"),
        col("target_audience"),
        normalize_active("campaign_status").alias("campaign_status"),
        lit(None).cast("boolean").alias("gdpr_compliant"),  # missing in US
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        lit("us").alias("_region"),
        col("_source")
    )



def transform_marketing_campaigns(df: DataFrame, region: str, exchange_rates: dict) -> DataFrame:
    if region == "asia":
        return transform_marketing_campaigns_asia(df, exchange_rates)
    elif region == "eu":
        return transform_marketing_campaigns_eu(df, exchange_rates)
    elif region == "us":
        return transform_marketing_campaigns_us(df, exchange_rates)
    else:
        raise ValueError(f"Unsupported region: {region}")
