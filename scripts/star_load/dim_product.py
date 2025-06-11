def dim_product_sql() -> str:
    return """
MERGE INTO dim_product AS target
USING (
    SELECT 
        p.product_id,
        p.product_sku,
        p.product_name,
        p.product_description,
        p.category_id,
        COALESCE(c.category_name, 'Uncategorized') as category_name,
        COALESCE(c.category_path, 'Root') as category_path,
        CASE 
            WHEN c.category_path IS NOT NULL AND POSITION('/' IN c.category_path) > 0 
            THEN SUBSTR(c.category_path, 1, POSITION('/' IN c.category_path) - 1)
            ELSE COALESCE(c.category_name, 'Root')
        END as parent_category,
        p.price_usd,
        p.cost_usd,
        (COALESCE(p.price_usd, 0) - COALESCE(p.cost_usd, 0)) as margin_usd,
        CASE 
            WHEN p.price_usd > 0 THEN 
                ROUND(((COALESCE(p.price_usd, 0) - COALESCE(p.cost_usd, 0)) / p.price_usd) * 100, 2)
            ELSE 0 
        END as margin_percentage,
        p.weight_kg,
        CASE 
            WHEN p.length_cm IS NOT NULL AND p.width_cm IS NOT NULL AND p.height_cm IS NOT NULL
            THEN CAST(p.length_cm AS TEXT) || 'x' || 
                 CAST(p.width_cm AS TEXT) || 'x' || 
                 CAST(p.height_cm AS TEXT)
            WHEN p.length_cm IS NOT NULL AND p.width_cm IS NOT NULL
            THEN CAST(p.length_cm AS TEXT) || 'x' || CAST(p.width_cm AS TEXT)
            WHEN p.length_cm IS NOT NULL
            THEN CAST(p.length_cm AS TEXT)
            ELSE 'N/A'
        END as dimensions_cm,
        p.vat_rate,
        p.energy_rating,
        p.source_region,
        p.is_active,
        CURRENT_TIMESTAMP as updated_at
    FROM ods.products p
    LEFT JOIN ods.categories c ON p.category_id = c.category_id 
                               AND p.source_region = c.source_region
) AS source
ON target.product_id = source.product_id
AND target.source_region = source.source_region
WHEN MATCHED THEN
    UPDATE SET
        product_sku = source.product_sku,
        product_name = source.product_name,
        product_description = source.product_description,
        category_id = source.category_id,
        category_name = source.category_name,
        category_path = source.category_path,
        parent_category = source.parent_category,
        price_usd = source.price_usd,
        cost_usd = source.cost_usd,
        margin_usd = source.margin_usd,
        margin_percentage = source.margin_percentage,
        weight_kg = source.weight_kg,
        dimensions_cm = source.dimensions_cm,
        vat_rate = source.vat_rate,
        energy_rating = source.energy_rating,
        is_active = source.is_active,
        updated_at = source.updated_at
WHEN NOT MATCHED THEN
    INSERT (
        product_id,
        product_sku,
        product_name,
        product_description,
        category_id,
        category_name,
        category_path,
        parent_category,
        price_usd,
        cost_usd,
        margin_usd,
        margin_percentage,
        weight_kg,
        dimensions_cm,
        vat_rate,
        energy_rating,
        source_region,
        is_active,
        updated_at
    )
    VALUES (
        source.product_id,
        source.product_sku,
        source.product_name,
        source.product_description,
        source.category_id,
        source.category_name,
        source.category_path,
        source.parent_category,
        source.price_usd,
        source.cost_usd,
        source.margin_usd,
        source.margin_percentage,
        source.weight_kg,
        source.dimensions_cm,
        source.vat_rate,
        source.energy_rating,
        source.source_region,
        source.is_active,
        source.updated_at
    );
"""