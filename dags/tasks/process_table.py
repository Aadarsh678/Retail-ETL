from airflow.decorators import task
from datetime import datetime
import os
import sys

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# Add airflow root path for imports
airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if airflow_path not in sys.path:
    sys.path.insert(0, airflow_path)

from .spark_utils import get_spark_session
from config.transformers_map import TRANSFORMERS

@task
def process_table(region: str, table: str, load_date: str):
    spark = get_spark_session(f"Process_{table}_{region}_{load_date}")
    try:
        input_path = f"/opt/airflow/data/raw/region={region}/table={table}/"
        output_path = f"/opt/airflow/data/processed/region={region}/table={table}/load_date={load_date}/"

        folders = os.listdir(input_path)
        date_folders = [f.split("=")[-1] for f in folders if f.startswith("load_date=")]
        if not date_folders:
            raise FileNotFoundError(f"No load_date folders in {input_path}")

        latest_date = max([datetime.strptime(d, "%Y-%m-%d") for d in date_folders]).strftime("%Y-%m-%d")
        input_path = os.path.join(input_path, f"load_date={latest_date}")
        df_raw = spark.read.parquet(input_path)

        transform_func = TRANSFORMERS.get(table)
        df_transformed = transform_func(df_raw, region) if transform_func else df_raw

        SF_OPTIONS = {
            "sfURL": os.getenv("SNOWFLAKE_ACCOUNT") + ".snowflakecomputing.com",
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
            "sfSchema": os.getenv(f"SNOWFLAKE_STAGING_{region.upper()}_SCHEMA"),
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "sfRole": os.getenv("SNOWFLAKE_ROLE"),
            "sfUser": os.getenv("SNOWFLAKE_USER"),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
        }

        df_transformed.write.format("snowflake").options(**SF_OPTIONS).option("dbtable", table).mode("overwrite").save()
        return {"region": region, "table": table, "record_count": df_transformed.count(), "status": "success"}

    finally:
        spark.stop()
