Multi-Region Retail Data Pipeline
This project demonstrates a robust and scalable data pipeline designed to integrate and normalize
multi-region retail data for advanced analytics and reporting. It covers the full data lifecycle-from
ingestion to transformation and loading-leveraging modern data engineering tools and best
practices.
Tech Stack
* Apache Airflow - Orchestrates end-to-end DAGs for ETL processes.
* PySpark - Processes and normalizes large-scale data across regions.
* PostgreSQL - Serves as the raw data source (ASIA, EU, US).
* Snowflake - Cloud data warehouse for analytics-ready data.
* Parquet - Efficient intermediate storage format.
Key Features & Working
* Multi-Region Support: Handles regional data variations across ASIA, EU, and US.
* Incremental & Idempotent Loads: Tracks loaded data using Airflow Variables to avoid duplication.
* Currency & Unit Standardization: Converts currencies (JPY, EUR, USD) and measurements to
common formats.
* Snowflake Merge Strategy: Dynamically builds SQL MERGE statements for upsert operations.
* Analytics-Ready Design: Transforms data into star schema for BI tools like Superset.
Use Case
This pipeline is ideal for retail companies with international operations. It enables unified reporting,
cross-region comparisons, and campaign performance tracking by consolidating disparate datasets
into a clean, analytics-ready Snowflake warehouse.
