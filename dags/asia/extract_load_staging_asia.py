import os
from pyspark.sql import SparkSession
import datetime
import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv

# PostgreSQL connection details from environment variables
load_dotenv()


@dag(
    dag_id="copy_postgres_table_to_table",
    schedule="0 0 * * *",  # Daily at midnight
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def copy_postgres_table_to_table():
    @task
    def transfer_table_data():
        # Print to debug connection info (don't print passwords in production logs!)
        postgres_options = {
            "url": f"jdbc:postgresql://retail-postgres:5432/retail_etl",
            "user": "retail_etl",
            "password": "retail_etl",
            "driver": "org.postgresql.Driver",
        }
        snowflake_options = {
            "sfURL": os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
            "sfUser": os.getenv("SNOWFLAKE_USER"),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
            "sfSchema": os.getenv("SNOWFLAKE_LANDING_SCHEMA"),
            # "sfSTAGINGSchema": os.getenv("SNOWFLAKE_STAGING_SCHEMA"),
            # "sfODSSchema": os.getenv("SNOWFLAKE_ODS_SCHEMA"),
            # "sfSTARSchema": os.getenv("SNOWFLAKE_STAR_SCHEMA"),
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "sfRole": os.getenv("SNOWFLAKE_ROLE", ""),
        }
        print("url:", snowflake_options["sfURL"])
        print("Postgres JDBC URL:", postgres_options["url"])
        print("Postgres User:", postgres_options["user"])
        print("Postgres Password is set:", postgres_options["password"] is not None)
        print("Postgres Driver:", postgres_options["driver"])

        # Start Spark session
        spark = (
            SparkSession.builder.appName("Postgres Table Copy")
            .config(
                "spark.jars",
                "/opt/airflow/jars/postgresql-42.7.3.jar,/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar",
            )
            .getOrCreate()
        )

        # Read from source table in PostgreSQL
        source_df = (
            spark.read.format("jdbc")
            .option("url", postgres_options["url"])
            .option("dbtable", "asia.customers")
            .option("user", postgres_options["user"])
            .option("password", postgres_options["password"])
            .option("driver", postgres_options["driver"])
            .load()
        )

        # Write to destination table in PostgreSQL
        # source_df.write \
        #     .format("jdbc") \
        #     .option("url", postgres_options["url"]) \
        #     .option("dbtable", "asia.customers_test") \
        #     .option("user", postgres_options["user"]) \
        #     .option("password", postgres_options["password"]) \
        #     .option("driver", postgres_options["driver"]) \
        #     .mode("overwrite") \
        #     .save()
        source_df.write.format("snowflake").options(**snowflake_options).option(
            "dbtable", "landing.customers_snowflake"
        ).mode("overwrite").save()

        spark.stop()

    transfer_table_data()


dag = copy_postgres_table_to_table()
