def get_merge_query_discounts_union() -> str:
    return """
    MERGE INTO ods.discounts AS ods
    USING (
        SELECT
            discount_id,
            discount_code,
            discount_name,
            discount_type,
            discount_value,
            minimum_order_amount,
            maximum_discount_amount,
            usage_limit,
            usage_count,
            start_date,
            end_date,
            is_active,
            created_at,
            CURRENT_TIMESTAMP AS updated_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.discounts

        UNION ALL

        SELECT
            discount_id,
            discount_code,
            discount_name,
            discount_type,
            discount_value,
            minimum_order_amount,
            maximum_discount_amount,
            usage_limit,
            usage_count,
            start_date,
            end_date,
            is_active,
            created_at,
            CURRENT_TIMESTAMP AS updated_at,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.discounts

        UNION ALL

        SELECT
            discount_id,
            discount_code,
            discount_name,
            discount_type,
            discount_value,
            minimum_order_amount,
            maximum_discount_amount,
            usage_limit,
            usage_count,
            start_date,
            end_date,
            is_active,
            created_at,
            CURRENT_TIMESTAMP AS updated_at,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.discounts
    ) src
    ON ods.discount_code = src.discount_code AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        discount_name = src.discount_name,
        discount_type = src.discount_type,
        discount_value = src.discount_value,
        minimum_order_amount = src.minimum_order_amount,
        maximum_discount_amount = src.maximum_discount_amount,
        usage_limit = src.usage_limit,
        usage_count = src.usage_count,
        start_date = src.start_date,
        end_date = src.end_date,
        is_active = src.is_active,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        discount_code, discount_name, discount_type, discount_value,
        minimum_order_amount, maximum_discount_amount, usage_limit, usage_count,
        start_date, end_date, is_active, created_at,
        source_region, source_system
    ) VALUES (
        src.discount_code, src.discount_name, src.discount_type, src.discount_value,
        src.minimum_order_amount, src.maximum_discount_amount, src.usage_limit, src.usage_count,
        src.start_date, src.end_date, src.is_active, src.created_at,
        src.source_region, src.source_system);
    """
