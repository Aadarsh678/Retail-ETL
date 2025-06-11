import os
import datetime
import pendulum
from airflow.decorators import dag, task
from dotenv import load_dotenv
import subprocess
import sys


load_dotenv()


sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))

SCRIPT_PATH = "/opt/airflow/scripts/pyspark_jobs/exchange_rate.py"


@dag(
    dag_id="store_exchange_rates_to_snowflake",
    schedule="0 0 * * *",  # Every day at 06:00 UTC
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=30),
    tags=["forex", "snowflake"],
)
def store_exchange_rates_to_snowflake():

    @task
    def run_exchange_script():
        result = subprocess.run(
            ["python3", SCRIPT_PATH], capture_output=True, text=True
        )
        if result.returncode != 0:
            raise RuntimeError(f"Script failed: {result.stderr}")
        print(result.stdout)

    run_exchange_script()


dag = store_exchange_rates_to_snowflake()
