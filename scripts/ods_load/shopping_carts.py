def get_merge_query_shopping_carts_union() -> str:
    return """
    MERGE INTO ods.shopping_carts AS ods
    USING (
        -- ASIA region
        SELECT
            cart_id,
            customer_id,
            session_id,
            created_at,
            updated_at,
            abandoned_at,
            CAST(NULL AS BOOLEAN) AS is_abandoned,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.shopping_carts

        UNION ALL

        -- EU region
        SELECT
            cart_id,
            customer_id,
            session_id,
            created_at,
            updated_at,
            abandoned_at,
            CAST(is_abandoned AS BOOLEAN) AS is_abandoned,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.shopping_carts

        UNION ALL

        -- US region
        SELECT
            cart_id,
            customer_id,
            session_id,
            created_at,
            updated_at,
            abandoned_at,
            CAST(is_abandoned AS BOOLEAN) AS is_abandoned,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.shopping_carts
    ) AS src
    ON ods.cart_id = src.cart_id AND ods.source_region = src.source_region

    WHEN MATCHED THEN UPDATE SET
        customer_id = src.customer_id,
        session_id = src.session_id,
        created_at = src.created_at,
        updated_at = src.updated_at,
        abandoned_at = src.abandoned_at,
        is_abandoned = src.is_abandoned,
        source_system = src.source_system

    WHEN NOT MATCHED THEN INSERT (
        cart_id, customer_id, session_id, created_at, updated_at,
        abandoned_at, is_abandoned, source_region, source_system
    ) VALUES (
        src.cart_id, src.customer_id, src.session_id, src.created_at, src.updated_at,
        src.abandoned_at, src.is_abandoned, src.source_region, src.source_system
    );




    """
