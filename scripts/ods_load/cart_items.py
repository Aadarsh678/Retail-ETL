def get_merge_query_cart_items_union() -> str:
    return """
    MERGE INTO ods.cart_items AS ods
    USING (
        -- ASIA region (convert JPY to USD here or assume ETL does it)
        SELECT
            cart_item_id,
            cart_id,
            variant_id,
            quantity,
            CAST(unit_price_usd AS DECIMAL(10,2)) As unit_price_usd,  
            added_at,
            updated_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.cart_items

        UNION ALL

        -- EU region (convert EUR to USD here or assume ETL does it)
        SELECT
            cart_item_id,
            cart_id,
            variant_id,
            quantity,
            CAST(unit_price_usd AS DECIMAL(10,2)) As unit_price_usd,
            added_at,
            updated_at,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.cart_items

        UNION ALL

        -- US region (already in USD)
        SELECT
            cart_item_id,
            cart_id,
            variant_id,
            quantity,
            unit_price_usd,
            added_at,
            updated_at,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.cart_items
    ) AS src
    ON ods.cart_item_id = src.cart_item_id AND ods.source_region = src.source_region

    WHEN MATCHED THEN UPDATE SET
        cart_id = src.cart_id,
        variant_id = src.variant_id,
        quantity = src.quantity,
        unit_price_usd = src.unit_price_usd,
        added_at = src.added_at,
        updated_at = src.updated_at,
        source_system = src.source_system

    WHEN NOT MATCHED THEN INSERT (
        cart_item_id, cart_id, variant_id, quantity, unit_price_usd, added_at, updated_at, source_region, source_system
    ) VALUES (
        src.cart_item_id, src.cart_id, src.variant_id, src.quantity, src.unit_price_usd, src.added_at, src.updated_at, src.source_region, src.source_system
    );

    """
