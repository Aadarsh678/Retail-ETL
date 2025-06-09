#!/bin/bash

set -e

echo "Creating project folder structure..."

# Create folders
mkdir -p dags/us dags/eu dags/asia dags/ods dags/dwh
mkdir -p scripts/etl scripts/pyspark_jobs scripts/sql/snowflake scripts/sql/postgres
mkdir -p config tests/etl tests/pyspark_jobs tests/dags data/us data/eu data/asia docs

# Create empty Python DAG files
touch dags/us/extract_load_staging_us.py
touch dags/eu/extract_load_staging_eu.py
touch dags/asia/extract_load_staging_asia.py
touch dags/ods/transform_combine_ods.py
touch dags/dwh/load_snowflake_star_schema.py

# Create empty ETL Python scripts
touch scripts/etl/extract.py
touch scripts/etl/transform.py
touch scripts/etl/load.py
touch scripts/etl/snowflake_load.py
touch scripts/etl/currency_api.py
touch scripts/etl/utils.py

# Create empty PySpark jobs
touch scripts/pyspark_jobs/staging_to_ods.py
touch scripts/pyspark_jobs/ods_to_star_schema.py

# Create empty SQL scripts
touch scripts/sql/snowflake/create_star_schema.sql
touch scripts/sql/snowflake/merge_fact_dim.sql
touch scripts/sql/postgres/create_staging.sql
touch scripts/sql/postgres/create_ods.sql

# Create config files
touch config/airflow.yaml
touch config/database.yaml
touch config/currency_api.yaml
touch config/logging.yaml

# Create README and requirements
touch README.md
touch requirements.txt

# Create Docker files placeholders
touch Dockerfile
touch docker-compose.yml

echo "Project structure created successfully."
