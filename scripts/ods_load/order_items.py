from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.decorators import dag, task
from datetime import datetime

def get_merge_query_order_items_union():
    return """
    MERGE INTO ods.order_items AS ods
    USING (
        -- ASIA
        SELECT
            order_item_id,
            order_id,
            variant_id,
            quantity,
            CAST(unit_price_usd AS DECIMAL(10,2)) As unit_price_usd, 
            CAST(total_price_usd AS DECIMAL(10,2)) As total_price_usd, 
            created_at,
            NULL AS vat_rate,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.order_items

        UNION ALL

        -- EU
        SELECT
            order_item_id,
            order_id,
            variant_id,
            quantity,
            CAST(unit_price_usd AS DECIMAL(10,2)) As unit_price_usd, 
            CAST(total_price_usd AS DECIMAL(10,2)) As total_price_usd , 
            created_at,
            vat_rate,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.order_items

        UNION ALL

        -- US
        SELECT
            order_item_id,
            order_id,
            variant_id,
            quantity,
            unit_price_usd,
            total_price_usd,
            created_at,
            NULL AS vat_rate,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.order_items
    ) src
    ON ods.order_item_id = src.order_item_id AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        order_id = src.order_id,
        variant_id = src.variant_id,
        quantity = src.quantity,
        unit_price_usd = src.unit_price_usd,
        total_price_usd = src.total_price_usd,
        created_at = src.created_at,
        vat_rate = src.vat_rate,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        order_item_id,
        order_id,
        variant_id,
        quantity,
        unit_price_usd,
        total_price_usd,
        created_at,
        vat_rate,
        source_region,
        source_system
    ) VALUES (
        src.order_item_id,
        src.order_id,
        src.variant_id,
        src.quantity,
        src.unit_price_usd,
        src.total_price_usd,
        src.created_at,
        src.vat_rate,
        src.source_region,
        src.source_system
    );
    """