def get_merge_query_customers_address_union() -> str:
    return """
    MERGE INTO ods.customer_addresses AS ods
    USING (
        -- ASIA region
        SELECT
            address_id,
            customer_id,
            address_type,
            street_address,
            city,
            region,
            postal_code,
            country,
            is_default,
            created_at,
            'ASIA' AS source_region,
            _source AS source_system
        FROM staging_asia.customer_addresses

        UNION ALL

        -- EU region
        SELECT
            address_id,
            customer_id,
            address_type,
            street_address,
            city,
            region,
            postal_code,
            country,
            is_default,
            created_at,
            'EU' AS source_region,
            _source AS source_system
        FROM staging_eu.customer_addresses

        UNION ALL

        -- US region
        SELECT
            address_id,
            customer_id,
            address_type,
            street_address,
            city,
            region,
            postal_code,
            country,
            is_default,
            created_at,
            'US' AS source_region,
            _source AS source_system
        FROM staging_us.customer_addresses
    ) src
    ON ods.address_id = src.address_id AND ods.source_region = src.source_region

    WHEN MATCHED THEN UPDATE SET
        customer_id = src.customer_id,
        address_type = src.address_type,
        street_address = src.street_address,
        city = src.city,
        region = src.region,
        postal_code = src.postal_code,
        country = src.country,
        is_default = src.is_default,
        created_at = src.created_at,
        source_system = src.source_system

    WHEN NOT MATCHED THEN INSERT (
        address_id,
        customer_id,
        address_type,
        street_address,
        city,
        region,
        postal_code,
        country,
        is_default,
        created_at,
        source_region,
        source_system
    ) VALUES (
        src.address_id,
        src.customer_id,
        src.address_type,
        src.street_address,
        src.city,
        src.region,
        src.postal_code,
        src.country,
        src.is_default,
        src.created_at,
        src.source_region,
        src.source_system
    );

    """
