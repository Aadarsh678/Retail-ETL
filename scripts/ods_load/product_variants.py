def get_merge_query_product_variants_union() -> str:
    return """
    MERGE INTO ods.product_variants AS ods
    USING (
        SELECT
            p.product_id,
            v.product_sku,
            v.variant_name,
            v.variant_type,
            v.variant_value,
            CAST(v.price_diff_usd AS DECIMAL(10,2)) As price_diff_usd , 
            v.is_active,
            v.created_at,
            NULL as updated_at,
            'ASIA' AS source_region,
            v._source AS source_system
        FROM staging_asia.product_variants v
        JOIN ods.products p ON v.product_sku = p.product_sku AND p.source_region = 'ASIA'

        UNION ALL

        SELECT
            p.product_id,
            v.product_sku,
            v.variant_name,
            v.variant_type,
            v.variant_value,
            CAST(v.price_diff_usd AS DECIMAL(10,2)) As price_diff_usd, 
            v.is_active,
            v.created_at,
            CURRENT_TIMESTAMP AS updated_at,
            'EU' AS source_region,
            v._source AS source_system
        FROM staging_eu.product_variants v
        JOIN ods.products p ON v.product_sku = p.product_sku AND p.source_region = 'EU'


        UNION ALL

        SELECT
            p.product_id,
            v.product_sku,
            v.variant_name,
            v.variant_type,
            v.variant_value,
            v.price_diff_usd as price_diff_usd,
            v.is_active,
            v.created_at,
            NULL as updated_at,
            'US' AS source_region,
            v._source AS source_system
        FROM staging_us.product_variants v
        JOIN ods.products p ON v.product_sku = p.product_sku AND p.source_region = 'US'
    ) src
    ON ods.product_sku = src.product_sku AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        product_id = src.product_id,
        variant_name = src.variant_name,
        variant_type = src.variant_type,
        variant_value = src.variant_value,
        price_diff_usd = src.price_diff_usd,
        is_active = src.is_active,
        updated_at = src.updated_at,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        product_id, product_sku, variant_name, variant_type, variant_value,
        price_diff_usd, is_active, created_at, updated_at, source_region, source_system
    ) VALUES (
        src.product_id, src.product_sku, src.variant_name, src.variant_type, src.variant_value,
        src.price_diff_usd, src.is_active, src.created_at, src.updated_at, src.source_region, src.source_system
    );
    """
