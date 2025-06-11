from pyspark.sql import SparkSession

def get_spark_session(app_name: str):
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars",
            "/opt/airflow/jars/snowflake-jdbc-3.13.17.jar,/opt/airflow/jars/spark-snowflake_2.12-2.16.0-spark_3.2.jar",
        )
        .config("spark.driver.extraClassPath", "/opt/airflow/jars/*")
        .config("spark.executor.extraClassPath", "/opt/airflow/jars/*")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.speculation", "false")
        .config("parquet.enable.summary-metadata", "false")
        .getOrCreate()
    )
