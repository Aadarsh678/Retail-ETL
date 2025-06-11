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


def upsert_payment_methods_from_csv_spark(csv_path):
    # Initialize Spark session with Snowflake connector
    spark = SparkSession.builder \
        .appName("UpsertPaymentMethods") \
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

    table_name = "dim_payment_method"
    full_table_name = f'{SF_SCHEMA.upper()}.{table_name.upper()}'
    temp_table_name = f'{SF_SCHEMA.upper()}.{table_name.upper()}_TEMP_STAGE_{datetime.now().strftime("%Y%m%d%H%M%S%f")}'

    # Write to temporary Snowflake stage table
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
            ON target.payment_method = source.payment_method
            WHEN MATCHED THEN
                UPDATE SET
                    payment_category = source.payment_category,
                    is_digital = source.is_digital,
                    processing_fee_percentage = source.processing_fee_percentage,
                    psd2_required = source.psd2_required
            WHEN NOT MATCHED THEN
                INSERT (payment_method, payment_category, is_digital, processing_fee_percentage, psd2_required)
                VALUES (source.payment_method, source.payment_category, source.is_digital, source.processing_fee_percentage, source.psd2_required)
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

        # Drop temporary table in separate connection to ensure cleanup
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
