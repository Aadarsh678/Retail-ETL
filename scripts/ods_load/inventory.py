from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.decorators import dag, task
from datetime import datetime

def get_merge_query_inventory_union():
    return """
    MERGE INTO ods.inventory AS ods
    USING (
        SELECT
            pv.variant_id,
            inv.warehouse_location,
            inv.quantity_available,
            inv.quantity_reserved,
            inv.reorder_level,
            inv.last_restocked_at,
            inv.updated_at,
            inv._region AS source_region,
            inv._source AS source_system
        FROM (
            SELECT * FROM staging_asia.inventory
            UNION ALL
            SELECT * FROM staging_eu.inventory
            UNION ALL
            SELECT * FROM staging_us.inventory
        ) inv
        JOIN ods.product_variants pv
          ON inv.variant_id = pv.variant_id
         AND pv.source_region = UPPER(inv._region)
    ) src
    ON ods.variant_id = src.variant_id
    AND ods.source_region = src.source_region
    AND ods.warehouse_location = src.warehouse_location
    WHEN MATCHED THEN UPDATE SET
        quantity_available = src.quantity_available,
        quantity_reserved = src.quantity_reserved,
        reorder_level = src.reorder_level,
        last_restocked_at = src.last_restocked_at,
        updated_at = src.updated_at,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        variant_id, warehouse_location, quantity_available, quantity_reserved,
        reorder_level, last_restocked_at, updated_at, source_region, source_system
    ) VALUES (
        src.variant_id, src.warehouse_location, src.quantity_available, src.quantity_reserved,
        src.reorder_level, src.last_restocked_at, src.updated_at, src.source_region, src.source_system
    );
    """