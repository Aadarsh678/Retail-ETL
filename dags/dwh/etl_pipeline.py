# from airflow import DAG
# from airflow.operators.dagrun_operator import TriggerDagRunOperator
# from airflow.utils.dates import days_ago
# from airflow.operators.empty import EmptyOperator
# from datetime import datetime, timedelta

# with DAG(
#     "master_etl_dag",
#     start_date = datetime(2024, 1, 1),
#     schedule="0 0 * * *",
#     catchup=False,
# ) as dag:
#     start = EmptyOperator(task_id="start")

#     # Step 1: Fetch data
#     fetch_postgres = TriggerDagRunOperator(
#         task_id="fetch_postgres_data_to_parquet",
#         trigger_dag_id="extract_postgres_table_to_parquet",
#         wait_for_completion=True,
#     )

#     fetch_currency = TriggerDagRunOperator(
#         task_id="fetch_currency_rates",
#         trigger_dag_id="store_exchange_rates_to_snowflake",
#         wait_for_completion=True,
#     )

#     fetch_dim_geography = TriggerDagRunOperator(
#         task_id="load_dim_geography",
#         trigger_dag_id="geography_to_snowflake",
#     )

#     fetch_dim_payment_method = TriggerDagRunOperator(
#         task_id="load_dim_payment_method",
#         trigger_dag_id="payment_method_to_snowflake",
#     )

#     # Step 2: Transform and stage
#     transform_and_stage = TriggerDagRunOperator(
#         task_id="transform_to_staging",
#         trigger_dag_id="multi_region_to_snowflake",
#     )

#     # Step 3: Load to ODS
#     load_ods = TriggerDagRunOperator(
#         task_id="load_to_ods",
#         trigger_dag_id="multi_region_ods_merge_v2",
#     )

#     # Step 4: Load to DW
#     load_dim = TriggerDagRunOperator(
#         task_id="load_dim_star_schema",
#         trigger_dag_id="load_to_star_schema",
#     )

#     load_fact = TriggerDagRunOperator(
#         task_id="load_fact_star_schema",
#         trigger_dag_id="load_to_star_schema_fact",
#     )

#     end = EmptyOperator(task_id="end")

#     # Define dependencies
#     start >> [fetch_postgres, fetch_currency] >> transform_and_stage
#     start >> [fetch_dim_geography, fetch_dim_payment_method]

#     transform_and_stage >> load_ods >> [load_dim, load_fact] >> end

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    "master_etl_dag_v4",
    default_args=default_args,
    description='Master ETL DAG for ODS to Star Schema pipeline',
    schedule_interval="0 2 * * *",  # Run at 2 AM daily
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['etl', 'master', 'star-schema'],
) as dag:
    
    start = EmptyOperator(task_id="start")

    # Step 1: Fetch raw data (can run in parallel)
    fetch_postgres = TriggerDagRunOperator(
        task_id="fetch_postgres_data_to_parquet",
        trigger_dag_id="extract_postgres_table_to_parquet",
        wait_for_completion=True,
        poke_interval=30,  # Check every 30 seconds
    )

    fetch_currency = TriggerDagRunOperator(
        task_id="fetch_currency_rates",
        trigger_dag_id="store_exchange_rates_to_snowflake",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Step 2: Load reference dimensions (can run in parallel with data fetch)
    load_dim_geography = TriggerDagRunOperator(
        task_id="load_dim_geography",
        trigger_dag_id="geography_to_snowflake",
        wait_for_completion=True,
        poke_interval=30,
    )

    load_dim_payment_method = TriggerDagRunOperator(
        task_id="load_dim_payment_method",
        trigger_dag_id="payment_method_to_snowflake",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Step 3: Transform and stage (depends on raw data)
    transform_and_stage = TriggerDagRunOperator(
        task_id="transform_to_staging",
        trigger_dag_id="multi_region_transform_staging_v2",
        wait_for_completion=True,
        poke_interval=60,
    )

    # Step 4: Load to ODS (depends on staging)
    load_ods = TriggerDagRunOperator(
        task_id="load_to_ods",
        trigger_dag_id="multi_region_ods_merge_v2",
        wait_for_completion=True,
        poke_interval=60,
    )

    # Step 5: Load dimensions (depends on ODS and reference dims)
    load_dim_star_schema = TriggerDagRunOperator(
        task_id="load_dim_star_schema",
        trigger_dag_id="load_to_star_schema",
        wait_for_completion=True,
        poke_interval=60,
    )

    # Step 6: Load facts (depends on dimensions)
    load_fact_star_schema = TriggerDagRunOperator(
        task_id="load_fact_star_schema",
        trigger_dag_id="load_to_star_schema_fact",
        wait_for_completion=True,
        poke_interval=60,
    )

    end = EmptyOperator(task_id="end")

    # Define dependencies with proper sequencing
    # Phase 1: Start parallel data extraction and reference dimension loading
    start >> [fetch_postgres, fetch_currency, load_dim_geography, load_dim_payment_method]
    
    # Phase 2: Transform and stage (needs raw data)
    [fetch_postgres, fetch_currency] >> transform_and_stage
    
    # Phase 3: Load to ODS (needs staged data)
    transform_and_stage >> load_ods
    
    # Phase 4: Load star schema dimensions (needs ODS and reference dims)
    [load_ods, load_dim_geography, load_dim_payment_method] >> load_dim_star_schema
    
    # Phase 5: Load facts (needs dimensions)
    load_dim_star_schema >> load_fact_star_schema
    
    # Phase 6: Complete
    load_fact_star_schema >> end
