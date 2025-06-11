def get_merge_query_product_reviews_union() -> str:
    return """

    MERGE INTO ods.product_reviews AS ods
    USING (
        -- ASIA
        SELECT
            review_id,
            product_id,
            customer_id,
            order_id,
            rating,
            review_title,
            review_text,
            is_verified_purchase,
            helpful_votes,
            NULL AS moderation_status,
            created_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.product_reviews

        UNION ALL

        -- EU
        SELECT
            review_id,
            product_id,
            customer_id,
            order_id,
            rating,
            review_title,
            review_text,
            is_verified_purchase,
            helpful_votes,
            moderation_status,
            created_at,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.product_reviews

        UNION ALL

        -- US
        SELECT
            review_id,
            product_id,
            customer_id,
            order_id,
            rating,
            review_title,
            review_text,
            is_verified_purchase,
            helpful_votes,
            NULL AS moderation_status,
            created_at,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.product_reviews
    ) src
    ON ods.review_id = src.review_id AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        product_id = src.product_id,
        customer_id = src.customer_id,
        order_id = src.order_id,
        rating = src.rating,
        review_title = src.review_title,
        review_text = src.review_text,
        is_verified_purchase = src.is_verified_purchase,
        helpful_votes = src.helpful_votes,
        moderation_status = src.moderation_status,
        created_at = src.created_at,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        review_id,
        product_id,
        customer_id,
        order_id,
        rating,
        review_title,
        review_text,
        is_verified_purchase,
        helpful_votes,
        moderation_status,
        created_at,
        source_region,
        source_system
    ) VALUES (
        src.review_id,
        src.product_id,
        src.customer_id,
        src.order_id,
        src.rating,
        src.review_title,
        src.review_text,
        src.is_verified_purchase,
        src.helpful_votes,
        src.moderation_status,
        src.created_at,
        src.source_region,
        src.source_system
    );
    
    """
