def get_merge_query_wishlists_union() -> str:
    return """
        
    MERGE INTO ods.wishlists AS ods
    USING (
        -- ASIA region
        SELECT
            wishlist_id,
            customer_id,
            product_id,
            added_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.wishlists

        UNION ALL

        -- EU region
        SELECT
            wishlist_id,
            customer_id,
            product_id,
            added_at,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.wishlists

        UNION ALL

        -- US region
        SELECT
            wishlist_id,
            customer_id,
            product_id,
            added_at,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.wishlists
    ) AS src
    ON ods.wishlist_id = src.wishlist_id AND ods.source_region = src.source_region

    WHEN MATCHED THEN UPDATE SET
        customer_id = src.customer_id,
        product_id = src.product_id,
        added_at = src.added_at,
        source_system = src.source_system

    WHEN NOT MATCHED THEN INSERT (
        wishlist_id, customer_id, product_id, added_at, source_region, source_system
    ) VALUES (
        src.wishlist_id, src.customer_id, src.product_id, src.added_at, src.source_region, src.source_system
    );

    """
