import os
from datetime import datetime
from airflow.models import Variable

def check_and_process_latest_file(input_path: str, region: str, table: str):
    var_key = f"last_processed_date__{region}__{table}"
    
    # Default to a old date to ensure first run always processes
    last_processed = Variable.get(var_key, default_var="1900-01-01")

    folders = os.listdir(input_path)
    date_folders = [f.split("=")[-1] for f in folders if f.startswith("load_date=")]

    if not date_folders:
        raise FileNotFoundError(f"No load_date folders found in {input_path}")

    # Get the latest available date from folder names
    latest_date = max(
        [datetime.strptime(d, "%Y-%m-%d") for d in date_folders]
    ).strftime("%Y-%m-%d")

    # Compare dates
    if latest_date <= last_processed:
        print(f"No new file to process for {region} - {table}. Last processed: {last_processed}")
        return

    print(f"Processing new file for {region} - {table}. Latest: {latest_date}, Last: {last_processed}")

    # TODO: Add actual data transformation or loading logic here

    # Update Airflow Variable with the latest date
    Variable.set(var_key, latest_date)
    print(f"Updated Airflow variable '{var_key}' to {latest_date}")
