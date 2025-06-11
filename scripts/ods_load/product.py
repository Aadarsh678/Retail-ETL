def get_merge_query_product_union() -> str:
    return """
    MERGE INTO ods.products AS ods
    USING (
        SELECT
            p.product_sku,
            p.product_name,
            p.product_description,
            p.category_id,
            CAST(p.price_usd AS DECIMAL(10,2)) AS price_usd, 
            CAST(p.cost_usd AS DECIMAL(10,2)) AS cost_usd, 
            p.weight_kg,
            p.length_cm,
            p.width_cm,
            p.height_cm,
            NULL AS vat_rate,
            NULL AS energy_rating,
            p.is_active,
            p.created_at,
            CURRENT_TIMESTAMP AS updated_at,
            'ASIA' AS source_region,
            p._source AS source_system
        FROM staging_asia.products p


        UNION ALL

        SELECT
            p.product_sku,
            p.product_name,
            p.product_description,
            p.category_id,
            CAST(p.price_usd AS DECIMAL(10,2)) AS price_usd, 
            CAST(p.cost_usd AS DECIMAL(10,2)) AS cost_usd, 
            p.weight_kg,
            p.length_cm,
            p.width_cm,
            p.height_cm,
            p.vat_rate,
            p.energy_rating,
            p.is_active,
            p.created_at,
            CURRENT_TIMESTAMP AS updated_at,
            'EU' AS source_region,
            p._source AS source_system
        FROM staging_eu.products p


        UNION ALL

        SELECT
            p.product_sku,
            p.product_name,
            p.product_description,
            p.category_id,
            p.price_usd AS price_usd,
            p.cost_usd AS cost_usd,
            p.weight_kg,
            p.length_cm,
            p.width_cm,
            p.height_cm,
            NULL AS vat_rate,
            NULL AS energy_rating,
            p.is_active,
            p.created_at,
            CURRENT_TIMESTAMP AS updated_at,
            'US' AS source_region,
            p._source AS source_system
        FROM staging_us.products p
    ) src
    ON ods.product_sku = src.product_sku AND ods.source_region = src.source_region
    WHEN MATCHED THEN UPDATE SET
        product_name = src.product_name,
        product_description = src.product_description,
        category_id = src.category_id,
        price_usd = src.price_usd,
        cost_usd = src.cost_usd,
        weight_kg = src.weight_kg,
        length_cm = src.length_cm,
        width_cm = src.width_cm,
        height_cm = src.height_cm,
        vat_rate = src.vat_rate,
        energy_rating = src.energy_rating,
        is_active = src.is_active,
        updated_at = src.updated_at,
        source_system = src.source_system
    WHEN NOT MATCHED THEN INSERT (
        product_sku, product_name, product_description, category_id,
        price_usd, cost_usd, weight_kg, length_cm, width_cm, height_cm,
        vat_rate, energy_rating, is_active, created_at, updated_at,
        source_region, source_system
    ) VALUES (
        src.product_sku, src.product_name, src.product_description, src.category_id,
        src.price_usd, src.cost_usd, src.weight_kg, src.length_cm, src.width_cm, src.height_cm,
        src.vat_rate, src.energy_rating, src.is_active, src.created_at, src.updated_at,
        src.source_region, src.source_system
    );
    """
