from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import snowflake.connector
import os
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()
SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
SF_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.getenv("SNOWFLAKE_STAR_SCHEMA")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")


def get_snowflake_connection():
    """Create and return a Snowflake connection"""
    return snowflake.connector.connect(
        user=SF_USER,
        password=SF_PASSWORD,
        account=SF_ACCOUNT,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        schema=SF_SCHEMA
    )


def upsert_geography_from_csv_spark(csv_path):
    # Initialize Spark session with Snowflake connector
    spark = SparkSession.builder \
        .appName("UpsertGeographyMethods") \
        .config(
            "spark.jars",
            "/opt/airflow/jars/postgresql-42.7.3.jar,"
            "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,"
            "/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar",
        ) \
        .getOrCreate()
    
    snowflake_options = {
        "sfUrl": f"{SF_ACCOUNT}.snowflakecomputing.com",
        "sfUser": SF_USER,
        "sfPassword": SF_PASSWORD,
        "sfDatabase": SF_DATABASE,
        "sfSchema": SF_SCHEMA,
        "sfWarehouse": SF_WAREHOUSE
    }

    # Read the CSV file
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    table_name = "dim_geography"
    full_table_name = f'{SF_SCHEMA.upper()}.{table_name.upper()}'
    temp_table_name = f'{SF_SCHEMA.upper()}.{table_name.upper()}_TEMP'

    # Write the data into a temporary Snowflake table
    df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", temp_table_name) \
        .mode("overwrite") \
        .save()
    
    print("Data written to temporary Snowflake table.")

    conn = None
    cur = None

    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()

        merge_sql = f"""
            MERGE INTO {full_table_name} AS target
            USING {temp_table_name} AS source
            ON target.country_code = source.country_code
            WHEN MATCHED THEN 
                UPDATE SET 
                    country_name = source.country_name,
                    region = source.region,
                    continent = source.continent,
                    currency_code = source.currency_code,
                    timezone = source.timezone,
                    gdpr_applicable = source.gdpr_applicable
            WHEN NOT MATCHED THEN
                INSERT (
                    country_code, country_name, region, continent, currency_code, timezone, gdpr_applicable
                )
                VALUES (
                    source.country_code, source.country_name, source.region, source.continent,
                    source.currency_code, source.timezone, source.gdpr_applicable
                );
        """

        cur.execute(merge_sql)
        conn.commit()
        print("Merge operation completed successfully.")

    except Exception as e:
        print(f"Error during merge operation: {e}")
        raise

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

        # Drop the temporary table in a separate connection to ensure cleanup
        try:
            conn = get_snowflake_connection()
            cur = conn.cursor()
            drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
            cur.execute(drop_sql)
            conn.commit()
            print(f"Temporary table {temp_table_name} dropped successfully.")
        except Exception as drop_err:
            print(f"Error dropping temp table {temp_table_name}: {drop_err}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    print("Upsert completed from Spark CSV to Snowflake.")
