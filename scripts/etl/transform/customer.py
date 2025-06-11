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
import hashlib
import re
import os, sys


scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# Add airflow root path for imports
airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if airflow_path not in sys.path:
    sys.path.insert(0, airflow_path)

# Now import with
from scripts.etl.transform.phone_validate import validate_any_phone_udf


# Helpers for normalization and masking
def normalize_sex(sex_col):
    return (
        when(lower(sex_col).isin("m", "male"), "Male")
        .when(lower(sex_col).isin("f", "female"), "Female")
        .when(lower(sex_col).isin("other", "o"), "Other")
        .otherwise(None)
    )


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


email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"


def validate_email(email_col):
    return when(email_col.rlike(email_regex), True).otherwise(False)


def normalize_name(name_col):
    return initcap(trim(name_col))


# GDPR-compliant masking


def hash_email(email):
    if email:
        email_clean = email.strip()
        if re.match(email_regex, email_clean):
            return hashlib.sha256(email_clean.encode("utf-8")).hexdigest()
    return None


def mask_name(name):
    if name and len(name.strip()) > 0:
        return name.strip()[0].upper() + "***"
    return None


def mask_phone(phone):
    if not phone:
        return None
    match = re.match(
        r"^(\+\d{1,3})(.*?)(\d{4})$", phone.strip().replace(" ", "").replace(".", "")
    )
    if match:
        return f"{match.group(1)}-***-***-{match.group(3)}"
    return "+XX-***-***-XXXX"


hash_email_udf = udf(hash_email, StringType())
mask_name_udf = udf(mask_name, StringType())
mask_phone_udf = udf(mask_phone, StringType())


def transform_asia(df):
    return df.select(
        col("cust_id").alias("customer_id"),
        when(validate_email(trim(lower(col("email")))), trim(lower(col("email"))))
        .otherwise(None)
        .alias("email"),
        normalize_name(col("fname")).alias("first_name"),
        normalize_name(col("lname")).alias("last_name"),
        validate_any_phone_udf(col("phone")).alias("phone"),
        safe_to_timestamp_multi_formats(col("birth_date")).alias("birth_date"),
        normalize_sex(col("sex")).alias("gender"),
        safe_to_timestamp_multi_formats(col("reg_date")).alias("registration_date"),
        safe_to_timestamp_multi_formats(col("login_time")).alias("last_login"),
        col("segment").alias("customer_segment"),
        col("source").alias("acquisition_channel"),
        safe_to_timestamp_multi_formats(col("created")).alias("created_at"),
        col("_region"),
        col("_source"),
    )


def transform_eu(df):
    is_active = (
        when(col("status_code") == 1, True)
        .when(col("status_code").isin(0, 2, 3), False)
        .otherwise(None).alias("is_active")
    )

    return df.select(
        col("customer_id"),
        hash_email_udf(col("email_hash")).alias("email"),
        mask_name_udf(col("first_name_masked")).alias("first_name"),
        mask_name_udf(col("last_name_masked")).alias("last_name"),
        mask_phone_udf(col("phone_masked")).alias("phone"),
        col("country_code"),
        safe_to_timestamp_multi_formats(col("registration_date")).alias(
            "registration_date"
        ),
        safe_to_timestamp_multi_formats(col("last_login_at")).alias("last_login"),
        is_active,
        col("customer_tier").alias("customer_segment"),
        safe_to_timestamp_multi_formats(col("gdpr_consent_date")).alias(
            "gdpr_consent_date"
        ),
        safe_to_timestamp_multi_formats(col("data_retention_until")).alias(
            "data_retention_until"
        ),
        col("acquisition_channel"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("_region"),
        col("_source"),
    )


def transform_us(df):
    return df.select(
        col("customer_id"),
        when(validate_email(trim(lower(col("email")))), trim(lower(col("email"))))
        .otherwise(None)
        .alias("email"),
        normalize_name(col("first_name")).alias("first_name"),
        normalize_name(col("last_name")).alias("last_name"),
        validate_any_phone_udf(col("phone")).alias("phone"),
        col("date_of_birth").alias("birth_date"),
        normalize_sex(col("gender")).alias("gender"),
        safe_to_timestamp_multi_formats(col("registration_date")).alias(
            "registration_date"
        ),
        safe_to_timestamp_multi_formats(col("last_login_at")).alias("last_login"),
        col("customer_segment"),
        col("acquisition_channel"),
        safe_to_timestamp_multi_formats(col("created_at")).alias("created_at"),
        safe_to_timestamp_multi_formats(col("updated_at")).alias("updated_at"),
        col("is_active"),
        col("_region"),
        col("_source"),
    )


def transform_customers(df: DataFrame, region: str,exchange_rates: dict) -> DataFrame:
    if region == "asia":
        return transform_asia(df)
    elif region == "eu":
        return transform_eu(df)
    elif region == "us":
        return transform_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")


# regions = ["asia", "eu", "us"]
# load_date = "2025-06-06"
# base_raw_path = "/opt/airflow/data/raw/"
# base_staging_path = "/opt/airflow/data/staging/customers/"


# for region in regions:
#     input_path = f"{base_raw_path}region={region}/table=customers/load_date={load_date}/"
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
#     df_transformed.show(truncate=False,vertical=True)


# print("Regional transformations with GDPR compliance completed.")
