from airflow.decorators import task_group
import os
import sys

scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if scripts_path not in sys.path:
    sys.path.insert(0, scripts_path)

# Add airflow root path for imports
airflow_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
if airflow_path not in sys.path:
    sys.path.insert(0, airflow_path)

from config.constant import TABLES, TABLE_DEPENDENCIES
from tasks.process_table import process_table

def make_region_group(region_name):
    @task_group(group_id=f"process_region_{region_name}")
    def region_group(load_date: str):
        tasks = {}
        for table in TABLES:
            tasks[table] = process_table.override(task_id=f"process_{table}_{region_name}")(
                region=region_name, table=table, load_date=load_date
            )
        for table in TABLES:
            for dep in TABLE_DEPENDENCIES.get(table, []):
                if dep in tasks:
                    tasks[dep] >> tasks[table]
        return tasks
    return region_group

process_region_asia = make_region_group("asia")
process_region_eu = make_region_group("eu")
process_region_us = make_region_group("us")
