from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, initcap, lower, upper, when,regexp_replace, to_date, to_timestamp,
    regexp_extract, lit, coalesce
)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re
import sys
import os
import phonenumbers
from phonenumbers import NumberParseException

from phone_validate import validate_any_phone_udf

spark = SparkSession.builder.appName("NormalizeCustomers").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

region = "asia"
table = "customers1"
load_date = "2025-06-05"
input_path = f"/opt/airflow/data/raw/region={region}/table={table}/load_date={load_date}/"

df = spark.read.parquet(input_path)
df.show()

# Normalize sex values to standard categories or NULL
def normalize_sex(sex_col):
    return when(lower(sex_col).isin('m', 'male'), 'Male') \
           .when(lower(sex_col).isin('f', 'female'), 'Female') \
           .when(lower(sex_col).isin('other', 'o'), 'Other') \
           .otherwise(None)

# Try parsing date in multiple formats
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

# Email validation regex
email_regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

def validate_email(email_col):
    return when(email_col.rlike(email_regex), True).otherwise(False)


# Full name formatter: trim + capitalize first letter of each word
def normalize_name(name_col):
    return initcap(trim(name_col))  # e.g., "  jOHN   " → "John"

df_normalized = df.select(
    col("cust_id").alias("customer_id"),
    when(validate_email(trim(lower(col("email")))), trim(lower(col("email"))))
        .otherwise(None).alias("email"),
    normalize_name(col("fname")).alias("first_name"),
    normalize_name(col("lname")).alias("last_name"),
    validate_any_phone_udf(col("phone")).alias("phone"),
    safe_to_timestamp_multi_formats(col("birth_date")).alias("birth_date"),
    normalize_sex(col("sex")).alias("sex"),
    safe_to_timestamp_multi_formats(col("reg_date")).alias("registration_date"),
    safe_to_timestamp_multi_formats(col("login_time")).alias("last_login_time"),
    col("segment"),
    col("source"),
    safe_to_timestamp_multi_formats(col("created")).alias("created_at"),
    col("_region"),
    col("_source")
)
# .withColumn(
#     "email_valid", validate_email(col("email"))
# )
# .filter(
#     col("customer_id").isNotNull() &
#     col("email_valid")
# )

df_normalized.show(truncate=False,vertical=True)
